import json
from logging import exception
import boto3
import base64
import os
import io
import time

client = boto3.resource('dynamodb')
query_window_table = client.Table(os.environ['AGHANIM_QUERY_WINDOW_TABLE'])

sqs = boto3.resource('sqs')
staging_queue = sqs.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])

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

    start_time = AGHANIM_RELEASE
    while start_time < CURRENT_TIME - 2 * WINDOW_SIZE:
        #Check if window has been handled
        item = query_window_table.get_item(Key={'start_time': start_time, 'difficulty': DIFFICULTY})
        if 'Item' in item:
            print(item)
            if item['Item']['reached_end'] == 'true':
                print(f'Window {start_time}-{start_time + WINDOW_SIZE} already handled')
                start_time += WINDOW_SIZE
                continue


        #Get matches
        aghs_payload = {
            "query_type": "aghs_matches",
            "window": WINDOW_SIZE,
            "start_time": start_time,
            "difficulty": DIFFICULTY,
            "take": 100, #TODO:CHANGE THESE
            "skip": 0, #TODO:CHANGE THESE
        }
        #TODO: Push to queue instead of calling lambda
        staging_queue.send_message(MessageBody=json.dumps(aghs_payload))
        start_time += WINDOW_SIZE
