import json
import boto3
import os

from api_caller.lib import APICaller, API_Call_Metadata

#Create clients for interfacting with AWS resources
client = boto3.resource('dynamodb')
api_calls_table = client.Table(os.environ['API_CALLS_TABLE'])

#Load metadata for logging
FUNC_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
LOG_GROUP = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
LOG_STREAM = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
metadata = API_Call_Metadata(FUNC_NAME, LOG_GROUP, LOG_STREAM)

with open('query.txt', 'r') as f:
    query = f.read()

aghs_matches_table = client.Table(os.environ['AGHANIM_MATCHES_TABLE'])

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    variables = {
        "createdAfterDateTime": event['start_time'],
        "createdBeforeDateTime": event['end_time'],
        "difficulty": event['difficulty'],
        "take": 100,
        "skip": 0
    }

    #Make API call
    api_caller = APICaller(api_calls_table)
    data = api_caller.query(query, variables, metadata)
    matches = data['data']['stratz']['page']['aghanim']['matches']


    with aghs_matches_table.batch_writer() as batch:
        for match in matches:
            batch.put_item(Item=match)