import boto3
import ConfigParser
import os
import bunch

from . import (
    get_client,
    get_new_bucket,
    )

def setup():
    print "Setting up in test_play_around.py"

def teardown():
    print "Tearing down in test_play_around.py"

def test_upload_file():
    client = get_client()
    bucket_name = 'sorrydave6'

    #bucket_dict = client.create_bucket(Bucket=bucket_name)
    #client.put_object(Bucket=bucket_name, Key='sega')

    #s3 = boto3.resource('s3',)
    #bucket = s3.Bucket(bucket_name)



    response = client.list_buckets()

    # Get of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    bucket = get_new_bucket(bucket_name)
    obj = bucket.put_object(Key='sega')

    client_obj_list = client.list_objects(Bucket=bucket_name)

    # Print out the bucket list
    print("Bucket List: %s" % buckets)
    print("Objects in Bucket sorrydave2 from client: %s" % client_obj_list)
