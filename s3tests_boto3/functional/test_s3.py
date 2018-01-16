import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
import isodate
import email.utils
import datetime
import threading
import re
import botocore.session
import pytz
from cStringIO import StringIO
from ordereddict import OrderedDict
import requests
import json
import base64
import hmac
import sha
import xml.etree.ElementTree as ET
import time
import operator

from email.header import decode_header

from .utils import assert_raises

from . import (
    get_client,
    get_prefix,
    get_anon_client,
    get_anon_resource,
    get_new_bucket,
    get_new_bucket_name,
    get_config_is_secure,
    get_config_host,
    get_config_port,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_main_display_name,
    get_main_user_id,
    get_main_email,
    get_main_api_name,
    get_alt_aws_access_key,
    get_alt_aws_secret_key,
    get_alt_display_name,
    get_alt_user_id,
    get_alt_email,
    get_alt_client,
    get_buckets_list,
    )


def _bucket_is_empty(bucket):
    is_empty = True
    for obj in bucket.objects.all():
        is_empty = False
        break
    return is_empty

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty buckets return no contents')
def test_bucket_list_empty():
    bucket = get_new_bucket()
    is_empty = _bucket_is_empty(bucket) 
    eq(is_empty, True)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='distinct buckets have different contents')
def test_bucket_list_distinct():
    bucket1 = get_new_bucket()
    bucket2 = get_new_bucket()
    obj = bucket1.put_object(Body='str', Key='asdf')
    is_empty = _bucket_is_empty(bucket2) 
    eq(is_empty, True)
    
def _create_objects(bucket=None, bucket_name=None, keys=[]):
    """
    Populate a (specified or new) bucket with objects with
    specified names (and contents identical to their names).
    """
    if bucket_name is None:
        bucket_name = get_new_bucket_name()
    if bucket is None:
        bucket = get_new_bucket(name=bucket_name)

    for key in keys:
        obj = bucket.put_object(Body=key, Key=key)

    return bucket_name

def _get_keys(response):
    """
    return lists of strings that are the keys from a client.list_objects() response
    """
    keys = []
    if 'Contents' in response:
        objects_list = response['Contents']
        keys = [obj['Key'] for obj in objects_list]
    return keys

def _get_prefixes(response):
    """
    return lists of strings that are prefixes from a client.list_objects() response
    """
    prefixes = []
    if 'CommonPrefixes' in response:
        prefix_list = response['CommonPrefixes']
        prefixes = [prefix['Prefix'] for prefix in prefix_list]
    return prefixes

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=2, no marker')
def test_bucket_list_many():
    bucket_name = _create_objects(keys=['foo', 'bar', 'baz'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    eq(len(keys), 2)
    eq(keys, ['bar', 'baz'])
    eq(response['IsTruncated'], True)

    response = client.list_objects(Bucket=bucket_name, Marker='baz',MaxKeys=2)
    keys = _get_keys(response)
    eq(len(keys), 1)
    eq(response['IsTruncated'], False)
    eq(keys, ['foo'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_basic():
    bucket_name = _create_objects(keys=['foo/bar', 'foo/bar/xyzzy', 'quux/thud', 'asdf'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/')
    eq(response['Delimiter'], '/')
    keys = _get_keys(response)
    eq(keys, ['asdf'])

    prefixes = _get_prefixes(response)
    eq(len(prefixes), 2)
    eq(prefixes, ['foo/', 'quux/'])

def validate_bucket_list(bucket_name, prefix, delimiter, marker, max_keys,
                         is_truncated, check_objs, check_prefixes, next_marker):
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, Marker=marker, MaxKeys=max_keys, Prefix=prefix)
    eq(response['IsTruncated'], is_truncated)
    if 'NextMarker' not in response:
        response['NextMarker'] = None
    eq(response['NextMarker'], next_marker)

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)

    eq(len(keys), len(check_objs))
    eq(len(prefixes), len(check_prefixes))
    eq(keys, check_objs)
    eq(prefixes, check_prefixes)

    return response['NextMarker']

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_prefix():
    bucket_name = _create_objects(keys=['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla'])

    delim = '/'
    marker = ''
    prefix = ''

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['asdf'], [], 'asdf')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, True, [], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['cquux/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, True, ['asdf'], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 2, False, [], ['cquux/'], None)

    prefix = 'boo/'

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['boo/bar'], [], 'boo/bar')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['boo/baz/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, False, ['boo/bar'], ['boo/baz/'], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefix and delimiter handling when object ends with delimiter')
def test_bucket_list_delimiter_prefix_ends_with_delimiter():
    bucket_name = _create_objects(keys=['asdf/'])
    validate_bucket_list(bucket_name, 'asdf/', '/', '', 1000, False, ['asdf/'], [], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-slash delimiter characters')
def test_bucket_list_delimiter_alt():
    bucket_name = _create_objects(keys=['bar', 'baz', 'cab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='a')
    eq(response['Delimiter'], 'a')

    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    eq(keys, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefixes = _get_prefixes(response)
    eq(len(prefixes), 2)
    eq(prefixes, ['ba', 'ca'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes starting with underscore')
def test_bucket_list_delimiter_prefix_underscore():
    bucket_name = _create_objects(keys=['_obj1_','_under1/bar', '_under1/baz/xyzzy', '_under2/thud', '_under2/bla'])

    delim = '/'
    marker = ''
    prefix = ''
    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['_obj1_'], [], '_obj1_')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, True, [], ['_under1/'], '_under1/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['_under2/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, True, ['_obj1_'], ['_under1/'], '_under1/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 2, False, [], ['_under2/'], None)

    prefix = '_under1/'

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['_under1/bar'], [], '_under1/bar')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['_under1/baz/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, False, ['_under1/bar'], ['_under1/baz/'], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='percentage delimiter characters')
def test_bucket_list_delimiter_percentage():
    bucket_name = _create_objects(keys=['b%ar', 'b%az', 'c%ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='%')
    eq(response['Delimiter'], '%')
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    eq(keys, ['foo'])

    prefixes = _get_prefixes(response)
    eq(len(prefixes), 2)
    # bar, baz, and cab should be broken up by the 'a' delimiters
    eq(prefixes, ['b%', 'c%'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='whitespace delimiter characters')
def test_bucket_list_delimiter_whitespace():
    bucket_name = _create_objects(keys=['b ar', 'b az', 'c ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=' ')
    eq(response['Delimiter'], ' ')
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    eq(keys, ['foo'])

    prefixes = _get_prefixes(response)
    eq(len(prefixes), 2)
    # bar, baz, and cab should be broken up by the 'a' delimiters
    eq(prefixes, ['b ', 'c '])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='dot delimiter characters')
def test_bucket_list_delimiter_dot():
    bucket_name = _create_objects(keys=['b.ar', 'b.az', 'c.ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='.')
    eq(response['Delimiter'], '.')
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    eq(keys, ['foo'])

    prefixes = _get_prefixes(response)
    eq(len(prefixes), 2)
    # bar, baz, and cab should be broken up by the 'a' delimiters
    eq(prefixes, ['b.', 'c.'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-printable delimiter can be specified')
def test_bucket_list_delimiter_unreadable():
    key_names=['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='\x0a')
    eq(response['Delimiter'], '\x0a')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty delimiter can be specified')
def test_bucket_list_delimiter_empty():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='')
    # putting an empty value into Delimiter will not return a value in the response
    eq('Delimiter' in response, False)

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unspecified delimiter defaults to none')
def test_bucket_list_delimiter_none():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    # putting an empty value into Delimiter will not return a value in the response
    eq('Delimiter' in response, False)

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unused delimiter is not found')
def test_bucket_list_delimiter_not_exist():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/')
    # putting an empty value into Delimiter will not return a value in the response
    eq(response['Delimiter'], '/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='returns only objects under prefix')
def test_bucket_list_prefix_basic():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='foo/')
    eq(response['Prefix'], 'foo/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['foo/bar', 'foo/baz'])
    eq(prefixes, [])

# just testing that we can do the delimeter and prefix logic on non-slashes
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='prefixes w/o delimiters')
def test_bucket_list_prefix_alt():
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='ba')
    eq(response['Prefix'], 'ba')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['bar', 'baz'])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='empty prefix returns everything')
def test_bucket_list_prefix_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='')
    eq(response['Prefix'], '')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='unspecified prefix returns everything')
def test_bucket_list_prefix_none():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='')
    eq(response['Prefix'], '')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, key_names)
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='nonexistent prefix returns nothing')
def test_bucket_list_prefix_not_exist():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='d')
    eq(response['Prefix'], 'd')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, [])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='non-printable prefix can be specified')
def test_bucket_list_prefix_unreadable():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='\x0a')
    eq(response['Prefix'], '\x0a')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, [])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='returns only objects directly under prefix')
def test_bucket_list_prefix_delimiter_basic():
    key_names = ['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/', Prefix='foo/')
    eq(response['Prefix'], 'foo/')
    eq(response['Delimiter'], '/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['foo/bar'])
    eq(prefixes, ['foo/baz/'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='non-slash delimiters')
def test_bucket_list_prefix_delimiter_alt():
    key_names = ['bar', 'bazar', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='a', Prefix='ba')
    eq(response['Prefix'], 'ba')
    eq(response['Delimiter'], 'a')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['bar'])
    eq(prefixes, ['baza'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix')
def test_bucket_list_prefix_delimiter_prefix_not_exist():
    key_names = ['b/a/r', 'b/a/c', 'b/a/g', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='d', Prefix='/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, [])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='over-ridden slash ceases to be a delimiter')
def test_bucket_list_prefix_delimiter_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='z', Prefix='b')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['b/a/c', 'b/a/g', 'b/a/r'])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix and delimiter')
def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='z', Prefix='y')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, [])
    eq(prefixes, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=1, marker')
def test_bucket_list_maxkeys_one():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=1)
    eq(response['IsTruncated'], True)

    keys = _get_keys(response)
    eq(keys, key_names[0:1])

    response = client.list_objects(Bucket=bucket_name, Marker=key_names[0])
    eq(response['IsTruncated'], False)

    keys = _get_keys(response)
    eq(keys, key_names[1:])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=0')
def test_bucket_list_maxkeys_zero():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=0)

    eq(response['IsTruncated'], False)
    keys = _get_keys(response)
    eq(keys, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/o max_keys')
def test_bucket_list_maxkeys_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    eq(response['IsTruncated'], False)
    keys = _get_keys(response)
    eq(keys, key_names)
    eq(response['MaxKeys'], 1000)

def _get_status_and_error_code(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    error_code = response['Error']['Code']
    return status, error_code

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='invalid max_keys')
def test_bucket_list_maxkeys_invalid():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    # adds invalid max keys to url
    # before list_objects is called
    def add_invalid_maxkeys(**kwargs):
        kwargs['params']['url'] += "&max-keys=blah"
    client.meta.events.register('before-call.s3.ListObjects', add_invalid_maxkeys)

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidArgument')

#@attr('fails_on_rgw')
#@attr(resource='bucket')
#@attr(method='get')
#@attr(operation='list all keys')
#@attr(assertion='non-printing max_keys')
#def test_bucket_list_maxkeys_unreadable():
    #TODO: Remove this test and document it 
    # Boto3 is url encoding the string before it is ever sent out
    # thus this test should be removed, because the unreadable string
    # never makes it to the server
    #key_names = ['bar', 'baz', 'foo', 'quxx']
    #bucket_name = _create_objects(keys=key_names)
    #client = get_client()

    # adds unreadable max keys to url
    # before list_objects is called
    #def add_unreadable_maxkeys(**kwargs):
        #kwargs['params']['url'] += "&max-keys=%0A"
    #client.meta.events.register('before-call.s3.ListObjects', add_unreadable_maxkeys)

    #e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)
    #status, error_code = _get_status_and_error_code(e.response)
    # COMMENT FROM BOTO2 TEST:
    # some proxies vary the case
    # Weird because you can clearly see an InvalidArgument error code. What's
    # also funny is the Amazon tells us that it's not an interger or within an
    # integer range. Is 'blah' in the integer range?
    #eq(status, 403)
    #eq(error_code, 'SignatureDoesNotMatch')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, no marker')
def test_bucket_list_marker_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    eq(response['Marker'], '')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, empty marker')
def test_bucket_list_marker_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='')
    eq(response['Marker'], '')
    eq(response['IsTruncated'], False)
    keys = _get_keys(response)
    eq(keys, key_names)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='non-printing marker')
def test_bucket_list_marker_unreadable():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='\x0a')
    eq(response['Marker'], '\x0a')
    eq(response['IsTruncated'], False)
    keys = _get_keys(response)
    eq(keys, key_names)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker not-in-list')
def test_bucket_list_marker_not_in_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='blah')
    eq(response['Marker'], 'blah')
    keys = _get_keys(response)
    eq(keys, ['foo', 'quxx'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker after list')
def test_bucket_list_marker_after_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='zzz')
    eq(response['Marker'], 'zzz')
    keys = _get_keys(response)
    eq(response['IsTruncated'], False)
    eq(keys, [])

def _compare_dates(datetime1, datetime2):
    """
    changes ms from datetime1 to 0, compares it to datetime2
    """
    # both times are in datetime format but datetime1 has 
    # microseconds and datetime2 does not
    datetime1 = datetime1.replace(microsecond=0)
    eq(datetime1, datetime2)

@attr(resource='object')
@attr(method='head')
@attr(operation='compare w/bucket list')
@attr(assertion='return same metadata')
def test_bucket_list_return_data():
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    data = {}
    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
        data.update({
            key_name: {
                'DisplayName': acl_response['Owner']['DisplayName'],
                'ID': acl_response['Owner']['ID'],
                'ETag': obj_response['ETag'],
                'LastModified': obj_response['LastModified'],
                'ContentLength': obj_response['ContentLength'],
                }
            })

    response  = client.list_objects(Bucket=bucket_name)
    objs_list = response['Contents']
    for obj in objs_list:
        key_name = obj['Key']
        key_data = data[key_name]
        eq(obj['ETag'],key_data['ETag'])
        eq(obj['Size'],key_data['ContentLength'])
        eq(obj['Owner']['DisplayName'],key_data['DisplayName'])
        eq(obj['Owner']['ID'],key_data['ID'])
        _compare_dates(obj['LastModified'],key_data['LastModified'])

# amazon is eventual consistent, retry a bit if failed
def check_configure_versioning_retry(bucket_name, status, expected_string):
    if status == True:
        status = 'Enabled'
    else:
        status = 'Disabled'

    client = get_client()

    response = client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'MFADelete': 'Disabled','Status': status})

    read_status = None

    for i in xrange(5):
        try:
            response = client.get_bucket_versioning(Bucket=bucket_name)
            read_status = response['Status']
        except KeyError:
            read_status = None

        if (expected_string == read_status):
            break

        time.sleep(1)

    eq(expected_string, read_status)


@attr(resource='object')
@attr(method='head')
@attr(operation='compare w/bucket list when bucket versioning is configured')
@attr(assertion='return same metadata')
def test_bucket_list_return_data_versioning():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    check_configure_versioning_retry(bucket_name, True, "Enabled")
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(bucket=bucket,bucket_name=bucket_name,keys=key_names)

    client = get_client()
    data = {}

    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
        data.update({
            key_name: {
                'ID': acl_response['Owner']['ID'],
                'DisplayName': acl_response['Owner']['DisplayName'],
                'ETag': obj_response['ETag'],
                'LastModified': obj_response['LastModified'],
                'ContentLength': obj_response['ContentLength'],
                'VersionId': obj_response['VersionId']
                }
            })

    response  = client.list_object_versions(Bucket=bucket_name)
    objs_list = response['Versions']

    for obj in objs_list:
        key_name = obj['Key']
        key_data = data[key_name]
        eq(obj['Owner']['DisplayName'],key_data['DisplayName'])
        eq(obj['ETag'],key_data['ETag'])
        eq(obj['Size'],key_data['ContentLength'])
        eq(obj['Owner']['ID'],key_data['ID'])
        eq(obj['VersionId'], key_data['VersionId'])
        _compare_dates(obj['LastModified'],key_data['LastModified'])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='succeeds')
def test_bucket_list_objects_anonymous():
    bucket_name = get_new_bucket_name() 
    get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    anon_client = get_anon_client()
    anon_client.list_objects(Bucket=bucket_name)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='fails')
def test_bucket_list_objects_anonymous_fail():
    bucket_name = get_new_bucket_name() 
    get_new_bucket(name=bucket_name)

    anon_client = get_anon_client()
    #anon_client.list_objects(Bucket=bucket_name)
    e = assert_raises(ClientError, anon_client.list_objects, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_notexist():
    bucket_name = get_new_bucket_name() 
    client = get_client()

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_delete_notexist():
    bucket_name = get_new_bucket_name() 
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-empty bucket')
@attr(assertion='fails 409')
def test_bucket_delete_nonempty():
    key_names = ['foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 409)
    eq(error_code, 'BucketNotEmpty')

def _do_set_bucket_canned_acl(client, bucket_name, canned_acl, i, results):
    try:
        client.put_bucket_acl(ACL=canned_acl, Bucket=bucket_name)
        results[i] = True
    except:
        results[i] = False

def _do_set_bucket_canned_acl_concurrent(client, bucket_name, canned_acl, num, results):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_set_bucket_canned_acl, args=(client, bucket_name, canned_acl, i, results))
        thr.start()
        t.append(thr)
    return t

def _do_wait_completion(t):
    for thr in t:
        thr.join()

@attr(resource='bucket')
@attr(method='put')
@attr(operation='concurrent set of acls on a bucket')
@attr(assertion='works')
def test_bucket_concurrent_set_canned_acl():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    num_threads = 50 # boto2 retry defaults to 5 so we need a thread to fail at least 5 times
                     # this seems like a large enough number to get through retry (if bug
                     # exists)
    results = [None] * num_threads

    t = _do_set_bucket_canned_acl_concurrent(client, bucket_name, 'public-read', num_threads, results)
    _do_wait_completion(t)

    for r in results:
        eq(r, True)

@attr(resource='object')
@attr(method='put')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_object_write_to_nonexist_bucket():
    key_names = ['foo']
    bucket_name = 'whatchutalkinboutwillis'
    client = get_client()

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='foo')

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')


@attr(resource='bucket')
@attr(method='del')
@attr(operation='deleted bucket')
@attr(assertion='fails 404')
def test_bucket_create_delete():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written')
@attr(assertion='fails 404')
def test_object_read_notexist():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchKey')

http_response = None

def get_http_response(**kwargs):
    global http_response 
    http_response = kwargs['http_response'].__dict__

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written to raise one error response')
@attr(assertion='RequestId appears in the error response')
def test_object_requestid_matches_header_on_error():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    # get http response after failed request
    client.meta.events.register('after-call.s3.GetObject', get_http_response)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')
    response_body = http_response['_content']
    request_id = re.search(r'<RequestId>(.*)</RequestId>', response_body.encode('utf-8')).group(1)
    assert request_id is not None
    eq(request_id, e.response['ResponseMetadata']['RequestId'])

def _make_objs_dict(key_names):
    objs_list = []
    for key in key_names:
        obj_dict = {'Key': key}
        objs_list.append(obj_dict)
    objs_dict = {'Objects': objs_list}
    return objs_dict

@attr(resource='object')
@attr(method='post')
@attr(operation='delete multiple objects')
@attr(assertion='deletes multiple objects with a single call')
def test_multi_object_delete():
    key_names = ['key0', 'key1', 'key2']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()
    response = client.list_objects(Bucket=bucket_name)
    eq(len(response['Contents']), 3)
    
    objs_dict = _make_objs_dict(key_names=key_names)
    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict) 

    eq(len(response['Deleted']), 3)
    assert 'Errors' not in response
    response = client.list_objects(Bucket=bucket_name)
    assert 'Contents' not in response

    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict) 
    eq(len(response['Deleted']), 3)
    assert 'Errors' not in response
    response = client.list_objects(Bucket=bucket_name)
    assert 'Contents' not in response

@attr(resource='object')
@attr(method='put')
@attr(operation='write zero-byte key')
@attr(assertion='correct content length')
def test_object_head_zero_bytes():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='')

    response = client.head_object(Bucket=bucket_name, Key='foo')
    eq(response['ContentLength'], 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct etag')
def test_object_write_check_etag():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
    eq(response['ETag'], '"37b51d194a7513e45b56f6524f2d51f2"')

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct cache control header')
def test_object_write_cache_control():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    cache_control = 'public, max-age=14400'
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', CacheControl=cache_control)

    response = client.head_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPHeaders']['cache-control'], cache_control)

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct expires header')
def test_object_write_expires():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Expires=expires)

    response = client.head_object(Bucket=bucket_name, Key='foo')
    _compare_dates(expires, response['Expires'])

@attr(resource='object')
@attr(method='all')
@attr(operation='complete object life cycle')
@attr(assertion='read back what we wrote and rewrote')
def test_object_write_read_update_read_delete():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    # Write
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    # Read
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    got = body.read()
    eq(got, 'bar')
    # Update
    client.put_object(Bucket=bucket_name, Key='foo', Body='soup')
    # Read
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    got = body.read()
    eq(got, 'soup')
    # Delete
    client.delete_object(Bucket=bucket_name, Key='foo')

def _set_get_metadata(metadata, bucket_name=None):
    """
    create a new bucket new or use an existing
    name to create an object that bucket,
    set the meta1 property to a specified, value,
    and then re-read and return that property
    """
    if bucket_name is None:
        bucket_name = get_new_bucket_name()
        get_new_bucket(name=bucket_name)

    client = get_client()
    metadata_dict = {'meta1': metadata}
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

    response = client.get_object(Bucket=bucket_name, Key='foo')
    return response['Metadata']['meta1']

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='reread what we wrote')
def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata('mymeta')
    eq(got, 'mymeta')

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='write empty value, returns empty value')
def test_object_set_get_metadata_none_to_empty():
    got = _set_get_metadata('')
    eq(got, '')

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='empty value replaces old')
def test_object_set_get_metadata_overwrite_to_empty():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    got = _set_get_metadata('oldmeta', bucket_name)
    eq(got, 'oldmeta')
    got = _set_get_metadata('', bucket_name)
    eq(got, '')

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='UTF-8 values passed through')
def test_object_set_get_unicode_metadata():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    def set_unicode_metadata(**kwargs):
        kwargs['params']['headers']['x-amz-meta-meta1'] = u"Hello World\xe9"

    client.meta.events.register('before-call.s3.PutObject', set_unicode_metadata)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    got = response['Metadata']['meta1'].decode('utf-8')
    eq(got, u"Hello World\xe9")

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='non-UTF-8 values detected, but preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_non_utf8_metadata():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    metadata_dict = {'meta1': '\x04mymeta'}
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

    response = client.get_object(Bucket=bucket_name, Key='foo')
    got = response['Metadata']['meta1']
    eq(got, '=?UTF-8?Q?=04mymeta?=')

def _set_get_metadata_unreadable(metadata, bucket_name=None):
    """
    set and then read back a meta-data value (which presumably
    includes some interesting characters), and return a list
    containing the stored value AND the encoding with which it
    was returned.
    """
    got = _set_get_metadata(metadata, bucket_name)
    got = decode_header(got)
    return got


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting in-fixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = '\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting in-fixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])

@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write')
@attr(assertion='replaces previous metadata')
def test_object_metadata_replaced_on_put():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    metadata_dict = {'meta1': 'bar'}
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    got = response['Metadata']
    eq(got, {})

@attr(resource='object')
@attr(method='put')
@attr(operation='data write from file (w/100-Continue)')
@attr(assertion='succeeds and returns written data')
def test_object_write_file():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()
    data = StringIO('bar')
    client.put_object(Bucket=bucket_name, Key='foo', Body=data)
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

def _get_post_url(bucket_name):
    is_secure = get_config_is_secure()
    if is_secure == True:
        protocol='https'
    else:
        protocol='http'

    host = get_config_host()
    port = get_config_port()

    url = '{protocol}://{host}:{port}/{bucket_name}'.format(protocol=protocol,\
                host=host, port=port, bucket_name=bucket_name)
    return url

@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_anonymous_request():
    bucket_name = get_new_bucket_name()
    client = get_client()
    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_authenticated_request():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request, bad access key')
@attr(assertion='fails')
def test_post_object_authenticated_request_bad_access_key():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , 'foo'),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 201')
def test_post_object_set_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("success_action_status" , "201"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 201)
    message = ET.fromstring(r.content).find('Key')
    eq(message.text,'foo.txt')

@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_set_invalid_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("success_action_status" , "404"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    eq(r.content,'')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_upload_larger_than_chunk():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 5*1024*1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    foo_string = 'foo' * 1024*1024

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', foo_string)])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = response['Body']
    eq(body.read(), foo_string)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_set_key_from_filename():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "${filename}"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('foo.txt', 'bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_ignored_header():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),("x-ignore-foo" , "bar"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_case_insensitive_condition_fields():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bUcKeT": bucket_name},\
    ["StArTs-WiTh", "$KeY", "foo"],\
    {"AcL": "private"},\
    ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    foo_string = 'foo' * 1024*1024

    payload = OrderedDict([ ("kEy" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("aCl" , "private"),("signature" , signature),("pOLICy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with escaped leading $ and returns written data')
def test_post_object_escaped_field_values():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='\$foo.txt')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns redirect url')
def test_post_object_success_redirect_action():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    redirect_url = _get_post_url(bucket_name)

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["eq", "$success_action_redirect", redirect_url],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),("success_action_redirect" , redirect_url),\
    ('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 200)
    url = r.url
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    eq(url,
    '{rurl}?bucket={bucket}&key={key}&etag=%22{etag}%22'.format(rurl = redirect_url,\
    bucket = bucket_name, key = 'foo.txt', etag = response['ETag'].strip('"')))

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid signature error')
def test_post_object_invalid_signature():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())[::-1]

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with access key does not exist error')
def test_post_object_invalid_access_key():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id[::-1]),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid expiration error')
def test_post_object_invalid_date_format():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": str(expires),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing key error')
def test_post_object_no_key_specified():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing signature error')
def test_post_object_missing_signature():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with extra input fields policy error')
def test_post_object_missing_policy_condition():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds using starts-with restriction on metadata header')
def test_post_object_user_specified_header():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["starts-with", "$x-amz-meta-foo",  "bar"]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    eq(response['Metadata']['foo'], 'barclamp')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy condition failed error due to missing field in POST request')
def test_post_object_request_missing_policy_specified_field():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["starts-with", "$x-amz-meta-foo",  "bar"]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with conditions must be list error')
def test_post_object_condition_is_case_sensitive():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "CONDITIONS": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with expiration must be string error')
def test_post_object_expires_is_case_sensitive():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy expired error')
def test_post_object_expired_policy():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails using equality restriction on metadata header')
def test_post_object_invalid_request_field_value():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["eq", "$x-amz-meta-foo",  ""]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())
    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing expiration error')
def test_post_object_missing_expires_condition():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing conditions error')
def test_post_object_missing_conditions_list():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with allowable upload size exceeded error')
def test_post_object_upload_size_limit_exceeded():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid content length error')
def test_post_object_missing_content_length_argument():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid JSON error')
def test_post_object_invalid_content_length_argument():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", -1, 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with upload size less than minimum allowable error')
def test_post_object_upload_size_below_minimum():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 512, 1000],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='empty conditions return appropriate error response')
def test_post_object_empty_conditions():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    { }\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files = payload)
    eq(r.status_code, 400)

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Match: the latest ETag')
@attr(assertion='succeeds')
def test_get_object_ifmatch_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    etag = response['ETag']

    response = client.get_object(Bucket=bucket_name, Key='foo', IfMatch=etag)
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Match: bogus ETag')
@attr(assertion='fails 412')
def test_get_object_ifmatch_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfMatch='"ABCORZ"')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-None-Match: the latest ETag')
@attr(assertion='fails 304')
def test_get_object_ifnonematch_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    etag = response['ETag']

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfNoneMatch=etag)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 304)
    eq(e.response['Error']['Message'], 'Not Modified')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-None-Match: bogus ETag')
@attr(assertion='succeeds')
def test_get_object_ifnonematch_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfNoneMatch='ABCORZ')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Modified-Since: before')
@attr(assertion='succeeds')
def test_get_object_ifmodifiedsince_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfModifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Modified-Since: after')
@attr(assertion='fails 304')
def test_get_object_ifmodifiedsince_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    last_modified = str(response['LastModified'])
    
    last_modified = last_modified.split('+')[0]
    mtime = datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')

    after = mtime + datetime.timedelta(seconds=1)
    after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

    time.sleep(1)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfModifiedSince=after_str)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 304)
    eq(e.response['Error']['Message'], 'Not Modified')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Unmodified-Since: before')
@attr(assertion='fails 412')
def test_get_object_ifunmodifiedsince_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfUnmodifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Unmodified-Since: after')
@attr(assertion='succeeds')
def test_get_object_ifunmodifiedsince_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfUnmodifiedSince='Sat, 29 Oct 2100 19:43:31 GMT')
    body = response['Body']
    eq(body.read(), 'bar')


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write w/ If-Match: the latest ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    etag = response['ETag'].replace('"', '')

    # pass in custom header 'If-Match' before PutObject call
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': etag}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'zar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Match: bogus ETag')
@attr(assertion='fails 412')
def test_put_object_ifmatch_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    # pass in custom header 'If-Match' before PutObject call
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '"ABCORZ"'}))
    client.meta.events.register('before-call.s3.PutObject', lf)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-Match: *')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_overwrite_existed_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'zar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifmatch_nonexisted_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='bar')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: outdated ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': 'ABCORZ'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'zar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: the latest ETag')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    etag = response['ETag'].replace('"', '')

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': etag}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-None-Match: *')
@attr(assertion='succeeds')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_nonexisted_good():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_overwrite_existed_failed():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 412)
    eq(error_code, 'PreconditionFailed')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = response['Body']
    eq(body.read(), 'bar')

def _setup_bucket_object_acl(bucket_acl, object_acl):
    """
    add a foo key, and specified key and bucket acls to
    a (new or existing) bucket.
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)
    client.put_object(ACL=object_acl, Bucket=bucket_name, Key='foo')

    return bucket_name 

def _setup_bucket_acl(bucket_acl=None):
    """
    set up a new bucket with specified acl
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    return bucket_name

@attr(resource='object')
@attr(method='get')
@attr(operation='publically readable bucket')
@attr(assertion='bucket is readable')
def test_object_raw_get():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')

    anon_client = get_anon_client()
    response = anon_client.get_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 404')
def test_object_raw_get_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    anon_client = get_anon_client()

    e = assert_raises(ClientError, anon_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 404')
def test_object_delete_key_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    anon_client = get_anon_client()

    e = assert_raises(ClientError, anon_client.delete_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object')
@attr(assertion='fails 404')
def test_object_raw_get_object_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')

    anon_client = get_anon_client()

    e = assert_raises(ClientError, anon_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchKey')

@attr(resource='bucket')
@attr(method='head')
@attr(operation='head bucket')
@attr(assertion='succeeds')
def test_bucket_head():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    response = client.head_bucket(Bucket=bucket_name)
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='head')
@attr(operation='read bucket extended information')
@attr(assertion='extended information is getting updated')
def test_bucket_head_extended():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    response = client.head_bucket(Bucket=bucket_name)
    #TODO: check to see if strings for values is ok
    eq(response['ResponseMetadata']['HTTPHeaders']['x-rgw-object-count'], '0')
    eq(response['ResponseMetadata']['HTTPHeaders']['x-rgw-bytes-used'], '0')

    _create_objects(bucket=bucket,bucket_name=bucket_name, keys=['foo','bar','baz'])
    response = client.head_bucket(Bucket=bucket_name)

    eq(response['ResponseMetadata']['HTTPHeaders']['x-rgw-object-count'], '3')
    eq(response['ResponseMetadata']['HTTPHeaders']['x-rgw-bytes-used'], '9')

@attr(resource='bucket.acl')
@attr(method='get')
@attr(operation='unauthenticated on private bucket')
@attr(assertion='succeeds')
def test_object_raw_get_bucket_acl():
    bucket_name = _setup_bucket_object_acl('private', 'public-read')

    anon_client = get_anon_client()
    response = anon_client.get_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object.acl')
@attr(method='get')
@attr(operation='unauthenticated on private object')
@attr(assertion='fails 403')
def test_object_raw_get_object_acl():
    bucket_name = _setup_bucket_object_acl('public-read', 'private')

    anon_client = get_anon_client()
    e = assert_raises(ClientError, anon_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/object')
@attr(assertion='succeeds')
def test_object_raw_authenticated():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on private bucket/private object with modified response headers')
@attr(assertion='succeeds')
@attr('fails_on_rgw')
def test_object_raw_response_headers():
    bucket_name = _setup_bucket_object_acl('private', 'private')

    client = get_client()

    response = client.get_object(Bucket=bucket_name, Key='foo', ResponseCacheControl='no-cache', ResponseContentDisposition='bla', ResponseContentEncoding='aaa', ResponseContentLanguage='esperanto', ResponseContentType='foo/bar', ResponseExpires='123')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
    eq(response['ResponseMetadata']['HTTPHeaders']['content-type'], 'foo/bar')
    eq(response['ResponseMetadata']['HTTPHeaders']['content-disposition'], 'bla')
    eq(response['ResponseMetadata']['HTTPHeaders']['content-language'], 'esperanto')
    eq(response['ResponseMetadata']['HTTPHeaders']['content-encoding'], 'aaa')
    eq(response['ResponseMetadata']['HTTPHeaders']['expires'], '123')
    eq(response['ResponseMetadata']['HTTPHeaders']['cache-control'], 'no-cache')

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on private bucket/public object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_bucket_acl():
    bucket_name = _setup_bucket_object_acl('private', 'public-read')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/private object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_object_acl():
    bucket_name = _setup_bucket_object_acl('public-read', 'private')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object and bucket')
@attr(assertion='fails 404')
def test_object_raw_authenticated_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object')
@attr(assertion='fails 404')
def test_object_raw_authenticated_object_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 404)
    eq(error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='get')
@attr(operation='x-amz-expires check not expired')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_not_expired():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=100000, HttpMethod='GET')

    res = requests.get(url).__dict__
    eq(res['status_code'], 200)

@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of range zero')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_range_zero():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=0, HttpMethod='GET')

    res = requests.get(url).__dict__
    eq(res['status_code'], 403)

@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of max range')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_max_range():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=609901, HttpMethod='GET')

    res = requests.get(url).__dict__
    eq(res['status_code'], 403)

@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of positive range')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_out_positive_range():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=-7, HttpMethod='GET')

    res = requests.get(url).__dict__
    eq(res['status_code'], 403)


@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, no object acls')
@attr(assertion='fails 403')
def test_object_anon_put():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo')

    anon_client = get_anon_client()

    e = assert_raises(ClientError, anon_client.put_object, Bucket=bucket_name, Key='foo', Body='foo')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, publically writable object')
@attr(assertion='succeeds')
def test_object_anon_put_write_access():
    bucket_name = _setup_bucket_acl('public-read-write')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo')

    anon_client = get_anon_client()

    response = anon_client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_put_authenticated():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_raw_put_authenticated_expired():
    bucket_name = get_new_bucket_name()
    bucket = get_new_bucket(name=bucket_name)
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo')

    params = {'Bucket': bucket_name, 'Key': 'foo'}
    url = client.generate_presigned_url(ClientMethod='put_object', Params=params, ExpiresIn=-1000, HttpMethod='PUT')

    # params wouldn't take a 'Body' parameter so we're passing it in here
    res = requests.put(url,data="foo").__dict__
    eq(res['status_code'], 403)

def check_bad_bucket_name(bucket_name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid bucket name.
    """
    client = get_client()
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidBucketName')


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='name begins with underscore')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_starts_nonalpha():
    bucket_name = get_new_bucket_name()
    check_bad_bucket_name('_' + bucket_name)

def check_invalid_bucketname(invalid_name):
    """
    Send a create bucket_request with an invalid bucket name
    that will bypass the ParamValidationError that would be raised
    if the invalid bucket name that was passed in normally.
    This function returns the status and error code from the failure
    """
    client = get_client()
    valid_bucket_name = get_new_bucket_name()
    def replace_bucketname_from_url(**kwargs):
        url = kwargs['params']['url']
        new_url = url.replace(valid_bucket_name, invalid_name)
        kwargs['params']['url'] = new_url
    client.meta.events.register('before-call.s3.CreateBucket', replace_bucketname_from_url)
    e = assert_raises(ClientError, client.create_bucket, Bucket=valid_bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    return (status, error_code)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='empty name')
@attr(assertion='fails 405')
def test_bucket_create_naming_bad_short_empty():
    invalid_bucketname = ''
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    eq(status, 405)
    eq(error_code, 'MethodNotAllowed')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (one character) name')
@attr(assertion='fails 400')
def test_bucket_create_naming_bad_short_one():
    check_bad_bucket_name('a')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (two character) name')
@attr(assertion='fails 400')
def test_bucket_create_naming_bad_short_two():
    check_bad_bucket_name('aa')

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='excessively long names')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_long():
    invalid_bucketname = 256*'a'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    eq(status, 400)

    invalid_bucketname = 280*'a'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    eq(status, 400)

    invalid_bucketname = 3000*'a'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    eq(status, 400)

def check_good_bucket_name(name, _prefix=None):
    """
    Attempt to create a bucket with a specified name
    and (specified or default) prefix, returning the
    results of that effort.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    if _prefix is None:
        _prefix = get_prefix()
    bucket_name = '{prefix}{name}'.format(
            prefix=_prefix,
            name=name,
            )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

def _test_bucket_create_naming_good_long(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    prefix = get_new_bucket_name()
    assert len(prefix) < 255
    num = length - len(prefix)
    name=num*'a'

    bucket_name = '{prefix}{name}'.format(
            prefix=prefix,
            name=name,
            )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/250 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_250():
    _test_bucket_create_naming_good_long(250)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/251 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_251():
    _test_bucket_create_naming_good_long(251)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/252 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_252():
    _test_bucket_create_naming_good_long(252)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/253 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_253():
    _test_bucket_create_naming_good_long(253)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/254 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_254():
    _test_bucket_create_naming_good_long(254)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/255 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_255():
    _test_bucket_create_naming_good_long(255)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list w/251 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_list_long_name():
    prefix = get_new_bucket_name()
    length = 251
    num = length - len(prefix)
    name=num*'a'

    bucket_name = '{prefix}{name}'.format(
            prefix=prefix,
            name=name,
            )
    bucket = get_new_bucket(name=bucket_name)
    is_empty = _bucket_is_empty(bucket) 
    eq(is_empty, True)
    
# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/ip address for name')
@attr(assertion='fails on aws')
def test_bucket_create_naming_bad_ip():
    check_bad_bucket_name('192.168.5.123')

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/! in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_bad_punctuation():
    # characters other than [a-zA-Z0-9._-]
    invalid_bucketname = 'alpha!soup'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    eq(status, 400)
    eq(error_code, 'InvalidBucketName')

# test_bucket_create_naming_dns_* are valid but not recommended
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/underscore in name')
@attr(assertion='succeeds')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_underscore():
    check_good_bucket_name('foo_bar')

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/100 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_long():
    prefix = get_prefix()
    assert len(prefix) < 50
    num = 100 - len(prefix)
    check_good_bucket_name(num * 'a')

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/dash at end of name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dash_at_end():
    check_good_bucket_name('foo-')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.. in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dot_dot():
    check_good_bucket_name('foo..bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.- in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dot_dash():
    check_good_bucket_name('foo.-bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/-. in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dash_dot():
    check_good_bucket_name('foo-.bar')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create')
def test_bucket_create_exists():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name)
    try:
        response = client.create_bucket(Bucket=bucket_name)
    except ClientError, e:
        status, error_code = _get_status_and_error_code(e.response)
        eq(e.status, 409)
        eq(e.error_code, 'BucketAlreadyOwnedByYou')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='get location')
def test_bucket_get_location():
    bucket_name = get_new_bucket_name()
    client = get_client()

    location_constraint = get_main_api_name()
    client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': location_constraint})

    response = client.get_bucket_location(Bucket=bucket_name)
    if location_constraint == "":
        location_constraint = None
    eq(response['LocationConstraint'], location_constraint)
    
@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create by non-owner')
@attr(assertion='fails 409')
def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket_name = get_new_bucket_name()
    client = get_client()

    alt_client = get_alt_client()

    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(ClientError, alt_client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 409)
    eq(error_code, 'BucketAlreadyExists')

def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(ClientError, fn, *args, **kwargs)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)

def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    eq(len(got), len(want))
    for g, w in zip(got, want):
        w = dict(w)
        g = dict(g)
        eq(g.pop('Permission', None), w['Permission'])
        eq(g['Grantee'].pop('DisplayName', None), w['DisplayName'])
        eq(g['Grantee'].pop('ID', None), w['ID'])
        eq(g['Grantee'].pop('Type', None), w['Type'])
        eq(g['Grantee'].pop('URI', None), w['URI'])
        eq(g['Grantee'].pop('EmailAddress', None), w['EmailAddress'])
        eq(g, {'Grantee': {}})

@attr(resource='bucket')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_bucket_acl_default():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    
    eq(response['Owner']['DisplayName'], display_name)
    eq(response['Owner']['ID'], user_id)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='get')
@attr(operation='public-read acl')
@attr(assertion='read back expected defaults')
@attr('fails_on_aws') # <Error><Code>IllegalLocationConstraintException</Code><Message>The unspecified location constraint is incompatible for the region specific endpoint this request was sent to.</Message>
def test_bucket_acl_canned_during_create():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: public-read,private')
@attr(assertion='read back expected values')
def test_bucket_acl_canned():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    client.put_bucket_acl(ACL='private', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='bucket.acls')
@attr(method='put')
@attr(operation='acl: public-read-write')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='WRITE',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: authenticated-read')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_authenticatedread():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='authenticated-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_object_acl_default():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_during_create():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read,private')
@attr(assertion='read back expected values')
def test_object_acl_canned():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    # Since it defaults to private, set it public-read first
    client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    # Then back to private.
    client.put_object_acl(ACL='private',Bucket=bucket_name, Key='foo')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')
    grants = response['Grants']

    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object')
@attr(method='put')
@attr(operation='acl public-read-write')
@attr(assertion='read back expected values')
def test_object_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(ACL='public-read-write', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='WRITE',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl authenticated-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_authenticatedread():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(ACL='authenticated-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerread():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
    
    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
    bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

    alt_client.put_object(ACL='bucket-owner-read', Bucket=bucket_name, Key='foo')
    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ',
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerfullcontrol():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
    
    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
    bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

    alt_client.put_object(ACL='bucket-owner-full-control', Bucket=bucket_name, Key='foo')
    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify owner')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_object_acl_full_control_verify_owner():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
    
    main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = { 'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}], 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)
    
    grant = { 'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'READ_ACP'}], 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    alt_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)

    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')
    eq(response['Owner']['ID'], main_user_id)

def add_obj_user_grant(bucket_name, key, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
    
    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    grants = response['Grants']
    grants.append(grant)

    grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    return grant

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify other attributes')
def test_object_acl_full_control_verify_attributes():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
    
    header = {'x-amz-foo': 'bar'}
    # lambda to add any header
    add_header = (lambda **kwargs: kwargs['params']['headers'].update(header))

    main_client.meta.events.register('before-call.s3.PutObject', add_header)
    main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = main_client.get_object(Bucket=bucket_name, Key='foo')
    content_type = response['ContentType']
    etag = response['ETag']

    alt_user_id = get_alt_user_id()

    grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}

    grants = add_obj_user_grant(bucket_name, 'foo', grant)

    main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grants)

    response = main_client.get_object(Bucket=bucket_name, Key='foo')
    eq(content_type, response['ContentType'])
    eq(etag, response['ETag'])

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl private')
@attr(assertion='a private object can be set to private')
def test_bucket_acl_canned_private_to_private():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    response = client.put_bucket_acl(Bucket=bucket_name, ACL='private')
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

def add_bucket_user_grant(bucket_name, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    grants.append(grant)

    grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    return grant

def _check_object_acl(permission):
    """
    Sets the permission on an object then checks to see 
    if it was set
    """
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    policy = {}
    policy['Owner'] = response['Owner']
    policy['Grants'] = response['Grants']
    policy['Grants'][0]['Permission'] = permission

    client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=policy)

    response = client.get_object_acl(Bucket=bucket_name, Key='foo')
    grants = response['Grants']

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl FULL_CONTRO')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl():
    _check_object_acl('FULL_CONTROL')

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_write():
    _check_object_acl('WRITE')

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE_ACP')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_writeacp():
    _check_object_acl('WRITE_ACP')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_read():
    _check_object_acl('READ')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ_ACP')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_readacp():
    _check_object_acl('READ_ACP')


def _bucket_acl_grant_userid(permission):
    """
    create a new bucket, grant a specific user the specified
    permission, read back the acl and verify correct setting
    """
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': permission}

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    return bucket_name

def _check_bucket_acl_grant_can_read(bucket_name):
    """
    verify ability to read the specified bucket
    """
    alt_client = get_alt_client()
    response = alt_client.head_bucket(Bucket=bucket_name)

def _check_bucket_acl_grant_cant_read(bucket_name):
    """
    verify inability to read the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.head_bucket, Bucket=bucket_name)

def _check_bucket_acl_grant_can_readacp(bucket_name):
    """
    verify ability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    alt_client.get_bucket_acl(Bucket=bucket_name)

def _check_bucket_acl_grant_cant_readacp(bucket_name):
    """
    verify inability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.get_bucket_acl, Bucket=bucket_name)

def _check_bucket_acl_grant_can_write(bucket_name):
    """
    verify ability to write the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_object(Bucket=bucket_name, Key='foo-write', Body='bar')

def _check_bucket_acl_grant_cant_write(bucket_name):

    """
    verify inability to write the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key='foo-write', Body='bar')

def _check_bucket_acl_grant_can_writeacp(bucket_name):
    """
    verify ability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

def _check_bucket_acl_grant_cant_writeacp(bucket_name):
    """
    verify inability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.put_bucket_acl,Bucket=bucket_name, ACL='public-read')

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid FULL_CONTROL')
@attr(assertion='can read/write data/acls')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_grant_userid_fullcontrol():
    bucket_name = _bucket_acl_grant_userid('FULL_CONTROL')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)

    client = get_client()

    bucket_acl_response = client.get_bucket_acl(Bucket=bucket_name)
    owner_id = bucket_acl_response['Owner']['ID']
    owner_display_name = bucket_acl_response['Owner']['DisplayName']

    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()

    eq(owner_id, main_user_id)
    eq(owner_display_name, main_display_name)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ')
@attr(assertion='can read data, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_read():
    bucket_name = _bucket_acl_grant_userid('READ')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ_ACP')
@attr(assertion='can read acl, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_readacp():
    bucket_name = _bucket_acl_grant_userid('READ_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acp
    #_check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE')
@attr(assertion='can write data, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_write():
    bucket_name = _bucket_acl_grant_userid('WRITE')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE_ACP')
@attr(assertion='can write acls, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_writeacp():
    bucket_name = _bucket_acl_grant_userid('WRITE_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/invalid userid')
@attr(assertion='fails 400')
def test_bucket_acl_grant_nonexist_user():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    bad_user_id = '_foo'

    #response = client.get_bucket_acl(Bucket=bucket_name)
    grant = {'Grantee': {'ID': bad_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy=grant)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidArgument')

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='can: read obj, get/set bucket acl, cannot write objs')
def test_bucket_acl_no_grants():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_bucket_acl(Bucket=bucket_name)
    old_grants = response['Grants']
    policy = {}
    policy['Owner'] = response['Owner']
    # clear grants
    policy['Grants'] = []

    # remove read/write permission
    response = client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    # can read
    client.get_object(Bucket=bucket_name, Key='foo')

    # can't write
    check_access_denied(client.put_object, Bucket=bucket_name, Key='baz', Body='a')
    #check_access_denied(client.put_object, Bucket=bucket_name, Key='baz', Body='')


    #TODO figure out why this is failing
    # owner can read acl
    client.get_bucket_acl(Bucket=bucket_name)

    # owner can write acl
    client.put_bucket_acl(Bucket=bucket_name, ACL='private')

    # set policy back to original so that bucket can be cleaned up
    policy['Grants'] = old_grants
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

def _get_acl_header(user_id=None, perms=None):
    all_headers = ["read", "write", "read-acp", "write-acp", "full-control"]
    headers = []

    if user_id == None:
        user_id = get_alt_user_id()

    if perms != None:
        for perm in perms:
            header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
            headers.append(header)

    else:
        for perm in all_headers:
            header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
            headers.append(header)

    return headers

@attr(resource='object')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_object_header_acl_grants():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
        kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

    client.meta.events.register('before-sign.s3.PutObject', add_headers_before_sign)

    client.put_object(Bucket=bucket_name, Key='foo_key', Body='bar')

    response = client.get_object_acl(Bucket=bucket_name, Key='foo_key')
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_header_acl_grants():
    headers = _get_acl_header()
    bucket_name = get_new_bucket_name()
    client = get_client()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
        kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

    client.meta.events.register('before-sign.s3.CreateBucket', add_headers_before_sign)

    client.create_bucket(Bucket=bucket_name)

    response = client.get_bucket_acl(Bucket=bucket_name)
    
    grants = response['Grants']
    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    alt_client = get_alt_client()

    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    # set bucket acl to public-read-write so that teardown can work
    # TODO: rewrite teardown code to make it so I don't need to reset this
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')
    

# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add second FULL_CONTROL user')
@attr(assertion='works for S3, fails for DHO')
@attr('fails_on_aws') #  <Error><Code>AmbiguousGrantByEmailAddress</Code><Message>The e-mail address you provided is associated with more than one account. Please retry your request using a different identification method or after resolving the ambiguity.</Message>
def test_bucket_acl_grant_email():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = {'Grantee': {'EmailAddress': alt_email_address, 'Type': 'AmazonCustomerByEmail' }, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy = grant)

    response = client.get_bucket_acl(Bucket=bucket_name)
    
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
        ]
    )

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add acl for nonexistent user')
@attr(assertion='fail 400')
def test_bucket_acl_grant_email_notexist():
    # behavior not documented by amazon
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    NONEXISTENT_EMAIL = 'doesnotexist@dreamhost.com.invalid'
    grant = {'Grantee': {'EmailAddress': NONEXISTENT_EMAIL, 'Type': 'AmazonCustomerByEmail'}, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy = grant)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'UnresolvableGrantByEmailAddress')

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='acls read back as empty')
def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_bucket_acl(Bucket=bucket_name)
    old_grants = response['Grants']
    policy = {}
    policy['Owner'] = response['Owner']
    # clear grants
    policy['Grants'] = []

    # remove read/write permission for everyone
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    response = client.get_bucket_acl(Bucket=bucket_name)

    eq(len(response['Grants']), 0)

    # set policy back to original so that bucket can be cleaned up
    policy['Grants'] = old_grants
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

# TODO rgw log_bucket.set_as_logging_target() gives 403 Forbidden
# http://tracker.newdream.net/issues/984
@attr(resource='bucket.log')
@attr(method='put')
@attr(operation='set/enable/disable logging target')
@attr(assertion='operations succeed')
@attr('fails_on_rgw')
def test_logging_toggle():
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()

    status = {'LoggingEnabled': {'TargetBucket': bucket_name, 'TargetGrants': [{'Grantee': {'DisplayName': main_display_name, 'ID': main_user_id,'Type': 'CanonicalUser'},'Permission': 'FULL_CONTROL'}], 'TargetPrefix': 'foologgingprefix'}}

    client.put_bucket_logging(Bucket=bucket_name, BucketLoggingStatus=status)
    client.get_bucket_logging(Bucket=bucket_name)
    status = {'LoggingEnabled': {}}
    client.put_bucket_logging(Bucket=bucket_name, BucketLoggingStatus=status)
    # NOTE: this does not actually test whether or not logging works

def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:
    - a: owning user, given ACL
    - a2: same object accessed by some other user
    - b: owning user, default ACL in bucket w/given ACL
    - b2: same object accessed by a some other user
    """
    bucket_name = get_new_bucket_name()
    get_new_bucket(name=bucket_name)
    client = get_client()

    key1 = 'foo'
    key2 = 'bar'
    newkey = 'new'

    client.put_bucket_acl(Bucket=bucket_name, ACL=bucket_acl)
    client.put_object(Bucket=bucket_name, Key=key1, Body='foocontent')
    client.put_object_acl(Bucket=bucket_name, Key=key1, ACL=object_acl)
    client.put_object(Bucket=bucket_name, Key=key2, Body='barcontent')

    return bucket_name, key1, key2, newkey

def get_bucket_key_names(bucket_name):
    objs_list = get_objects_list(bucket_name)
    return frozenset(obj for obj in objs_list)

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/private')
@attr(assertion='public has no access to bucket or objects')
def test_access_bucket_private_object_private():
    client = get_client()
    alt_client = get_alt_client()
    # all the test_access_* tests follow this template
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='private')
    # a should be public-read, b gets default (private)
    # acled object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    # acled object write fail
    #boto3.set_stream_logger(name='botocore')
    #data = StringIO('barcontent')
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='')
    # default object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    # default object write fail
    #check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')
    # bucket read fail
    check_access_denied(alt_client.list_objects, Bucket=bucket_name)
    # bucket write fail
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets')
@attr(assertion='returns all expected buckets')
def test_buckets_create_then_list():
    client = get_client()
    bucket_names = []
    for i in xrange(5):
        bucket_name = get_new_bucket_name()
        bucket_names.append(bucket_name)

    for name in bucket_names:
        client.create_bucket(Bucket=name)

    buckets_list = get_buckets_list(client=client)

    for name in bucket_names:
        if name not in buckets_list:
            raise RuntimeError("S3 implementation's GET on Service did not return bucket we created: %r", bucket.name)

# Goal 4742!
