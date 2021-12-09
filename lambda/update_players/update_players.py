import json
from logging import exception
import boto3
import base64
import os
import io
import time
import requests


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
    r = requests.post(endpoint, json={'query': query , 'variables': variables}, headers=headers)
    if r.status_code == 200:
        return json.loads(r.text)['data']['leaderboard']['season']
    else:
        print(f"Query failed with status code: {r.status_code}")
        print(f"Response: {r.text}")

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    client = boto3.resource('dynamodb')
    table = client.Table(os.environ['TABLE_NAME'])
    region = event['region']

    #Without batch writing: ~3200 entires in 10m
    #With batch writing: ~5200 entries in 10m
    with table.batch_writer() as batch:
        players = query_leaderboard(region)
        for player in players:
            item = {os.environ['PARTITION_KEY']: player['steamAccountId'], 'region': region}
            batch.put_item(Item=item)