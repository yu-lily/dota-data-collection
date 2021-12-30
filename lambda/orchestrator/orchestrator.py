import json
from logging import exception
import boto3
import base64
import os
import io
import time

sqs = boto3.resource('sqs')
staging_queue = sqs.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])
api_caller_queue = sqs.get_queue_by_name(QueueName=os.environ["API_CALLER_QUEUE"])

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    BATCH_SIZE = 5
    CYCLE_LIMIT = 20
    INVOCATION_INTERVAL = 2 #seconds
    # STRATZ api limit is 250 per minute, this function is called by the minute
    # 20 batches of size 5 calls each = 100 calls/min (Staying safe amount under limit)

    last_cycle_time = 0

    for _ in range(CYCLE_LIMIT):
        # Make sure each cycle doesn't run faster than 1/sec
        gap = round((time.time_ns() - last_cycle_time) / 1000000000, 2)
        if gap < INVOCATION_INTERVAL:
            time.sleep(INVOCATION_INTERVAL - gap)
        last_cycle_time = time.time_ns()

        messages = staging_queue.receive_messages(MaxNumberOfMessages=BATCH_SIZE)

        #Stop execution if queue is empty
        if len(messages) == 0:
            print("Queue is empty")
            return
            
        # Move a batch from staging queue to api_caller queue
        for message in messages:
            print(message.body)
            api_caller_queue.send_message(MessageBody=message.body)
            message.delete()
