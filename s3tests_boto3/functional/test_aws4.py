from cStringIO import StringIO
import boto.connection
import boto.exception
import boto.s3.connection
import boto.s3.acl
import boto.utils
import bunch
import nose
import operator
import random
import string
import socket
import ssl
import os
import re
from email.utils import formatdate

from urlparse import urlparse

from boto.s3.connection import S3Connection

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .utils import assert_raises
import AnonymousAuth

from email.header import decode_header

from . import (
    _make_raw_request,
    nuke_prefixed_buckets,
    get_new_bucket,
    s3,
    config,
    get_prefix,
    TargetConnection,
    targets,
    )


_orig_conn = {}
_orig_authorize = None
_custom_headers = {}
_remove_headers = []
boto_type = None


# HeaderS3Connection and _our_authorize are necessary to be able to arbitrarily
# overwrite headers. Depending on the version of boto, one or the other is
# necessary. We later determine in setup what needs to be used.

def _update_headers(headers):
    """ update a set of headers with additions/removals
    """
    global _custom_headers, _remove_headers

    headers.update(_custom_headers)

    for header in _remove_headers:
        try:
            del headers[header]
        except KeyError:
            pass


# Note: We need to update the headers twice. The first time so the
# authentication signing is done correctly. The second time to overwrite any
# headers modified or created in the authentication step.

class HeaderS3Connection(S3Connection):
    """ establish an authenticated connection w/customized headers
    """
    def fill_in_auth(self, http_request, **kwargs):
        _update_headers(http_request.headers)
        S3Connection.fill_in_auth(self, http_request, **kwargs)
        _update_headers(http_request.headers)

        return http_request


def _our_authorize(self, connection, **kwargs):
    """ perform an authentication w/customized headers
    """
    _update_headers(self.headers)
    _orig_authorize(self, connection, **kwargs)
    _update_headers(self.headers)


def setup():
    global boto_type

    # we determine what we need to replace by the existence of particular
    # attributes. boto 2.0rc1 as fill_in_auth for S3Connection, while boto 2.0
    # has authorize for HTTPRequest.
    if hasattr(S3Connection, 'fill_in_auth'):
        global _orig_conn

        boto_type = 'S3Connection'
        for conn in s3:
            _orig_conn[conn] = s3[conn]
            header_conn = HeaderS3Connection(
                aws_access_key_id=s3[conn].aws_access_key_id,
                aws_secret_access_key=s3[conn].aws_secret_access_key,
                is_secure=s3[conn].is_secure,
                port=s3[conn].port,
                host=s3[conn].host,
                calling_format=s3[conn].calling_format
                )

            s3[conn] = header_conn
    elif hasattr(boto.connection.HTTPRequest, 'authorize'):
        global _orig_authorize

        boto_type = 'HTTPRequest'

        _orig_authorize = boto.connection.HTTPRequest.authorize
        boto.connection.HTTPRequest.authorize = _our_authorize
    else:
        raise RuntimeError


def teardown():
    global boto_type

    # replace original functionality depending on the boto version
    if boto_type is 'S3Connection':
        global _orig_conn
        for conn in s3:
            s3[conn] = _orig_conn[conn]
        _orig_conn = {}
    elif boto_type is 'HTTPRequest':
        global _orig_authorize

        boto.connection.HTTPRequest.authorize = _orig_authorize
        _orig_authorize = None
    else:
        raise RuntimeError


def _clear_custom_headers():
    """ Eliminate any header customizations
    """
    global _custom_headers, _remove_headers
    _custom_headers = {}
    _remove_headers = []


def _add_custom_headers(headers=None, remove=None):
    """ Define header customizations (additions, replacements, removals)
    """
    global _custom_headers, _remove_headers
    if not _custom_headers:
        _custom_headers = {}

    if headers is not None:
        _custom_headers.update(headers)
    if remove is not None:
        _remove_headers.extend(remove)


def _setup_bad_object(headers=None, remove=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket = get_new_bucket()

    _add_custom_headers(headers=headers, remove=remove)
    return bucket.new_key('foo')

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

#
# AWS4 specific tests
#

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
       raise SkipTest

@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_invalid_garbage_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Content-MD5':'AWS4 HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidDigest')
