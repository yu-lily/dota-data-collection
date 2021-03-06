import requests
import json
import time
import random
import boto3
import base64
from dataclasses import dataclass

@dataclass
class API_Call_Metadata:
    func_name: str
    log_group: str
    log_stream: str
    query_type: str


def get_apikey(secret_name:str="stratz/apikey", region_name:str="us-west-2"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        print("Decoded from binary")
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    
    return json.loads(secret)['apikey']

class APICaller:

    def __init__(self, api_calls_table_client, endpoint: str = "https://api.stratz.com/graphql") -> None:
        self.api_calls_table_client = api_calls_table_client
        self.endpoint = endpoint
        self.apikey = get_apikey()
    
    def graphql_query(self):
        self.graphql_query(self.query, self.variables, self.metadata)

    def graphql_query(self, query, variables, metadata) -> dict:
        endpoint = "https://api.stratz.com/graphql"
        headers = {'Authorization': 'Bearer ' + self.apikey}
        print(variables)
        r = requests.post(endpoint, json={'query': query , 'variables': variables}, headers=headers)

        rl_s, rl_m, rl_h, rl_d = (r.headers['X-RateLimit-Remaining-Second'],
        r.headers['X-RateLimit-Limit-Minute'],
        r.headers['X-RateLimit-Limit-Hour'],
        r.headers['X-RateLimit-Limit-Day'])

        #Log that the request was made
        ts = int(time.time())
        rand = random.randint(0, 999)
        api_call_id = (ts * 1000) + rand
        item = {
            'api_call_id': api_call_id,
            'function': metadata.func_name,
            'logGroup': metadata.log_group,
            'logStream': metadata.log_stream,
            'queryType': metadata.query_type,
            'timestamp': ts,
            'statusCode': r.status_code,
            'rateLimitRemainingSecond': rl_s,
            'rateLimitRemainingMinute': rl_m,
            'rateLimitRemainingHour': rl_h,
            'rateLimitRemainingDay': rl_d,
            }
        self.api_calls_table_client.put_item(Item=item)
        
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            print(f"Query failed with status code: {r.status_code}")
            print(f"Response: {r.text}")
            raise Exception(f"Query failed with status code: {r.status_code}, response: {r.text}")

    # def query(self, query: str, variables: dict, metadata:API_Call_Metadata) -> dict:
    #     """
    #     Wrapper for the graphql_query function.
    #     Checks the status code of the response and raises an exception if it is not 200.
    #     """
    #     r = self.graphql_query(query, variables, metadata)

