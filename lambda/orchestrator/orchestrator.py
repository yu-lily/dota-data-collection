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

    #Populate queue and pop in groups of x?

    #By groups of x players, query for their match history and write the matchids to db


    #Put all matches in sqs queue

    #Call find_matchids lambda in a throttled fashion, working through the queue

    AGHANIM_RELEASE = 1639533060
    aghs_matches = os.environ['AGHANIM_MATCHES_FUNC_NAME']
    aghs_payload = {
        "window": 1800,
        "end_time": 1639863998,
        "difficulty": "APEXMAGE",
    }
    #lambdaClient.invoke(FunctionName=aghs_matches, InvocationType='Event', Payload=json.dumps({'region': region}))