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

    REGIONS = ['AMERICAS', 'SE_ASIA', 'EUROPE', 'CHINA']
    update_players = os.environ['UPDATE_PLAYERS_FUNC_NAME']
    
    # for region in REGIONS:
    #     lambdaClient.invoke(FunctionName=update_players, InvocationType='Event', Payload=json.dumps({'region': region}))

    #Populate queue and pop in groups of x?

    #By groups of x players, query for their match history and write the matchids to db


    #Put all matches in sqs queue

    #Call find_matchids lambda in a throttled fashion, working through the queue

    AGHANIM_RELEASE = 1639533060
    CURRENT_TIME = int(time.time())
    WINDOW_SIZE = 1800
    DIFFICULTY = event['difficulty']

    AGHS_MATCHES_LAMBDA = os.environ['AGHANIM_MATCHES_FUNC_NAME']

    start_time = AGHANIM_RELEASE


    BATCH_SIZE = 10
    # Move items from staging queue to api_caller queue
    for message in staging_queue.receive_messages(MaxNumberOfMessages=BATCH_SIZE):
        api_caller_queue.send_message(MessageBody=json.dumps(message.body))
        message.delete()


    # Write to api caller queue
    test_event = {
        "query_type": "aghs_matches",
        "window": 1800,
        "start_time": 1639533060,
        "difficulty": "APEXMAGE",
        "take": 100,
        "skip": 0
        }
    api_caller_queue.send_message(MessageBody=json.dumps(test_event))
    