import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
import ConfigParser
import os
import bunch
import random
import string
import itertools

config = bunch.Bunch

# this will be assigned by setup()
prefix = None

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

def get_buckets_list(client=None, prefix=None):
    if client == None:
        client = get_client()
    response = client.list_buckets()
    bucket_dicts = response['Buckets']
    buckets_list = []
    if bucket_dicts != []:
        for bucket in bucket_dicts:
            if prefix in bucket['Name']:
                buckets_list.append(bucket['Name'])

    return buckets_list

def get_objects_list(bucket, client=None):
    if client == None:
        client = get_client()
    response = client.list_objects(Bucket=bucket)
    objects_list = []

    if 'Contents' in response:
        contents = response['Contents']
        for obj in contents:
            objects_list.append(obj['Key'])

    return objects_list

def get_versioned_objects_list(bucket, client=None):
    if client == None:
        client = get_client()
    response = client.list_object_versions(Bucket=bucket)
    versioned_objects_list = []

    if 'Versions' in response:
        contents = response['Versions']
        for obj in contents:
            key = obj['Key']
            version_id = obj['VersionId']
            versioned_obj = (key,version_id)
            versioned_objects_list.append(versioned_obj)

    return versioned_objects_list

def get_delete_markers_list(bucket, client=None):
    if client == None:
        client = get_client()
    response = client.list_object_versions(Bucket=bucket)
    delete_markers = []

    if 'DeleteMarkers' in response:
        contents = response['DeleteMarkers']
        for obj in contents:
            key = obj['Key']
            version_id = obj['VersionId']
            versioned_obj = (key,version_id)
            delete_markers.append(versioned_obj)

    return delete_markers


def nuke_prefixed_buckets(prefix):
    endpoint_url = "http://%s:%d" % (config.host, config.port)

    client = get_client()

    buckets = get_buckets_list(client, prefix)

    if buckets != []:
        for bucket_name in buckets:
            objects_list = get_objects_list(bucket_name, client)
            for obj in objects_list:
                response = client.delete_object(Bucket=bucket_name,Key=obj)
            versioned_objects_list = get_versioned_objects_list(bucket_name, client)
            for obj in versioned_objects_list:
                response = client.delete_object(Bucket=bucket_name,Key=obj[0],VersionId=obj[1])
            delete_markers = get_delete_markers_list(bucket_name, client)
            for obj in delete_markers:
                response = client.delete_object(Bucket=bucket_name,Key=obj[0],VersionId=obj[1])
            client.delete_bucket(Bucket=bucket_name)

    print('Done with cleanup of test buckets.')

def setup():
    cfg = ConfigParser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    with file(path) as f:
        cfg.readfp(f)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')

    global prefix

    defaults = cfg.defaults()

    config.host = defaults.get("host")
    config.port = int(defaults.get("port"))
    config.access_key = defaults.get("access_key")
    config.secret_key = defaults.get("secret_key")
    config.is_secure = defaults.get("is_secure")

    try:
        template = defaults.get("bucket_prefix")
    except (ConfigParser.NoOptionError):
        template = 'test-{random}-'
    prefix = choose_bucket_prefix(template=template)

    nuke_prefixed_buckets(prefix=prefix)

def teardown():
    nuke_prefixed_buckets(prefix=prefix)

def get_client(session=boto3):

    endpoint_url = "http://%s:%d" % (config.host, config.port)

    client = session.client(service_name='s3',
                        aws_access_key_id=config.access_key,
                        aws_secret_access_key=config.secret_key,
                        endpoint_url=endpoint_url,
                        use_ssl=config.is_secure,
                        verify=False)
    return client

def get_anon_client(session=boto3):

    endpoint_url = "http://%s:%d" % (config.host, config.port)

    client = session.client(service_name='s3',
                        aws_access_key_id='',
                        aws_secret_access_key='',
                        endpoint_url=endpoint_url,
                        use_ssl=False,
                        verify=False,
                        config=Config(signature_version=UNSIGNED))
    return client

def get_anon_resource():

    resource = boto3.resource('s3')

    resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    return resource



bucket_counter = itertools.count(1)

def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name

def get_new_bucket(session=boto3, name=None, headers=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    endpoint_url = "http://%s:%d" % (config.host, config.port)

    s3 = session.resource('s3', 
                        use_ssl=False,
                        verify=False,
                        endpoint_url=endpoint_url, 
                        aws_access_key_id=config.access_key,
                        aws_secret_access_key=config.secret_key)
    if name is None:
        name = get_new_bucket_name()
    bucket = s3.Bucket(name)
    bucket_location = bucket.create()
    return bucket

def get_config_is_secure():
    return config.is_secure

def get_config_host():
    return config.host

def get_config_port():
    return config.port

def get_config_aws_access_key():
    return config.access_key

def get_config_aws_secret_key():
    return config.secret_key
