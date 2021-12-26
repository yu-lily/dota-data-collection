import json
from logging import exception
import boto3
import base64
import os
import io
import time

lambdaClient = boto3.client('lambda', region_name="us-west-2")

client = boto3.resource('dynamodb')
query_window_table = client.Table(os.environ['AGHANIM_QUERY_WINDOW_TABLE'])

sqs = boto3.resource('sqs')
staging_queue = sqs.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])
api_caller_queue = sqs.get_queue_by_name(QueueName=os.environ["API_CALLER_QUEUE"])

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    BATCH_SIZE = 10
    # Move items from staging queue to api_caller queue
    for message in staging_queue.receive_messages(MaxNumberOfMessages=BATCH_SIZE):
        api_caller_queue.send_message(MessageBody=json.dumps(message.body))
        message.delete()
