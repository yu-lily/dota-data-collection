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

with open('query.txt', 'r') as f:
    query = f.read()
    
def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    region = event['region']
    variables = {'region': region}

    pkey = os.environ['PARTITION_KEY']

    #Make API call
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