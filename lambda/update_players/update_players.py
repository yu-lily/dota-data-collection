import json
from logging import exception
import boto3
import base64
import os
import io
import time
import requests
import random

client = boto3.resource('dynamodb')
players_table = client.Table(os.environ['TABLE_NAME'])
api_calls_table = client.Table(os.environ['API_CALLS_TABLE'])
FUNC_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
LOG_GROUP = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
LOG_STREAM = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']

def get_apikey():

    secret_name = "stratz/apikey"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        print("Decoded from binary")
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    
    return json.loads(secret)['apikey']

def query_leaderboard(region):
    endpoint = "https://api.stratz.com/graphql"
    query = """
    query ($region: LeaderboardDivision) {
    leaderboard {
        season(request: {leaderBoardDivision: $region, take: 6000}) {
        steamAccountId 
        }
    }
    }
    """
    
    variables = {'region': region}
    headers = {'Authorization': 'Bearer ' + get_apikey()}
    print(query, variables)
    r = requests.post(endpoint, json={'query': query , 'variables': variables}, headers=headers)
    
    ts = int(time.time())
    rand = random.randint(0, 999)
    api_call_id = (ts * 1000) + rand
    #Log that the request was made
    item = {
        'api_call_id': api_call_id,
        'function': FUNC_NAME,
        'logGroup': LOG_GROUP,
        'logStream': LOG_STREAM,
        'timestamp': ts,
        'statusCode': r.status_code
        }
    api_calls_table.put_item(Item=item)

    if r.status_code == 200:
        return json.loads(r.text)['data']['leaderboard']['season']
    else:
        print(f"Query failed with status code: {r.status_code}")
        print(f"Response: {r.text}")

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    region = event['region']
    pkey = os.environ['PARTITION_KEY']

    #Without batch writing: ~3200 entires in 10m
    #With batch writing: ~5200 entries in 10m
    with players_table.batch_writer() as batch:
        players = query_leaderboard(region)
        for player in players:
            #Check if the player existsin the table already
            #table.get_item(Key={pkey: player['steamAccountId']})
            #TODO: Batch get items, and only write if the item doesn't exist
            item = {pkey: player['steamAccountId'], 'region': region}
            batch.put_item(Item=item)