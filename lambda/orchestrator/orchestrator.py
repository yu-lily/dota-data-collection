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

    BATCH_SIZE = 10
    CYCLE_LIMIT = 20
    # STRATZ api limit is 250 per minute, this function is called by the minute
    # 20 batches of size 10 calls each = 200 calls/min (Staying safe amount under limit)

    last_cycle_time = 0

    for _ in range(CYCLE_LIMIT):
        # Make sure each cycle doesn't run faster than 1/sec
        if time.time() - last_cycle_time < 1:
            time.sleep(1)
        last_cycle_time = time.time()

        # Move a batch from staging queue to api_caller queue
        for message in staging_queue.receive_messages(MaxNumberOfMessages=BATCH_SIZE):
            print(message.body)
            if len(message.body) == 0:
                print("Queue is empty")
                return

            api_caller_queue.send_message(MessageBody=message.body)
            message.delete()
