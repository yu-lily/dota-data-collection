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
players_table = client.Table(os.environ['TABLE_NAME'])
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

    variables = {}#{'playerids': playerids}


    api_caller = APICaller(api_calls_table)
    data = api_caller.query(query, variables, metadata)
    data = data['data']['leaderboard']['season']
