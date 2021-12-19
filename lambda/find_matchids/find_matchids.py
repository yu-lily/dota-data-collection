import json
import boto3
import os

from api_caller.lib import APICaller, API_Call_Metadata

#Create clients for interfacting with AWS resources
client = boto3.resource('dynamodb')
players_table = client.Table(os.environ['PLAYERS_TABLE'])
api_calls_table = client.Table(os.environ['API_CALLS_TABLE'])

#Load metadata for logging
FUNC_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
LOG_GROUP = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
LOG_STREAM = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
metadata = API_Call_Metadata(FUNC_NAME, LOG_GROUP, LOG_STREAM)

query = """
query ($region: LeaderboardDivision) {
leaderboard {
    season(request: {leaderBoardDivision: $region, take: 6000}) {
    steamAccountId 
    }
}
}
"""


def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    variables = {}#{'playerids': playerids}


    api_caller = APICaller(api_calls_table)
    data = api_caller.query(query, variables, metadata)
    data = data['data']['leaderboard']['season']
