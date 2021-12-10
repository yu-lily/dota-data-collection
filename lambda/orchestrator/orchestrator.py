import json
from logging import exception
import boto3
import base64
import os
import io
import time

lambdaClient = boto3.client('lambda', region_name="us-west-2")

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    REGIONS = ['AMERICAS', 'SE_ASIA', 'EUROPE', 'CHINA']
    update_players = os.environ['UPDATE_PLAYERS_FUNC_NAME']

    for region in REGIONS:
        lambdaClient.invoke(FunctionName=update_players, InvocationType='Event', Payload=json.dumps({'region': region}))

    
    #Put all matches in sqs queue

    #Call find_matchids lambda in a throttled fashion, working through the queue