import json
from logging import exception
import boto3
import base64
import os
import io
import time
import requests
import random

from api_caller.lib import APICaller, API_Call_Metadata

client = boto3.resource('dynamodb')
players_table = client.Table(os.environ['PLAYERS_TABLE'])
api_calls_table = client.Table(os.environ['API_CALLS_TABLE'])
FUNC_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
LOG_GROUP = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
LOG_STREAM = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']

query = """
query ($region: LeaderboardDivision) {
leaderboard {
    season(request: {leaderBoardDivision: $region, take: 6000}) {
    steamAccountId 
    }
}
}
"""

metadata = API_Call_Metadata(FUNC_NAME, LOG_GROUP, LOG_STREAM)

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    region = event['region']
    variables = {'region': region}
    pkey = os.environ['PARTITION_KEY']

    api_caller = APICaller(api_calls_table)
    data = api_caller.query(query, variables, metadata)
    players = data['data']['leaderboard']['season']

    #Without batch writing: ~3200 entires in 10m
    #With batch writing: ~5200 entries in 10m
    with players_table.batch_writer() as batch:
        for player in players:
            #Check if the player existsin the table already
            #table.get_item(Key={pkey: player['steamAccountId']})
            #TODO: Batch get items, and only write if the item doesn't exist
            item = {pkey: player['steamAccountId'], 'region': region}
            batch.put_item(Item=item)