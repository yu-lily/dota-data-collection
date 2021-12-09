import json
from logging import exception
import boto3
import base64
import os
import io
import time


s3_client = boto3.client("s3")


def get_apikey():

    secret_name = "stratz/apikey"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    
    return secret

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

