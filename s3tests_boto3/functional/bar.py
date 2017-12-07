#!/usr/bin/python
import ConfigParser
import os
import bunch
import boto3

config = bunch.Bunch()

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

defaults = cfg.defaults()

host = defaults.get("host")
port = int(defaults.get("port"))
access_key = defaults.get("access_key")
secret_key = defaults.get("secret_key")

print host
print port
print access_key
print secret_key

endpoint_url = "http://%s:%d" % (host, port)

conn = boto3.client(service_name='s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url,
                    use_ssl=False,
                    verify=False)

bucket_name = 'sorrydave'
object_name = 'jocaml'

conn.upload_file('sega', bucket_name, object_name)
#connection_type = 'client'
