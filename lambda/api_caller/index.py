import json
import boto3
import os
#import psycopg2

from api_caller.lib import APICaller, API_Call_Metadata

#Create clients for interfacing with AWS resources
ddb_resource = boto3.resource('dynamodb')

sqs = boto3.resource('sqs')
staging_queue = sqs.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])


session = boto3.session.Session()

def choose_handler(query_type):
    if query_type == 'aghs_matches':
        from aghs_matches import AghsMatchesHandler
        return AghsMatchesHandler

def connect_to_rds():
    sm_client = session.client(service_name='secretsmanager', region_name='us-west-2')
    secret = sm_client.get_secret_value(SecretId=os.environ['RDS_CREDS_NAME'])
    rds_creds = secret['SecretString']
    rds_creds = json.loads(rds_creds)
    db_endpoint = os.environ['AGHANIM_MATCHES_DB_ENDPOINT']

    # conn = psycopg2.connect(
    #     host = os.environ['db_endpoint'],
    #     database=secret['dbname'],
    #     user=secret['username'],
    #     password=secret['password']
    # )
    #cur = conn.cursor()

    return None, None


def handler(event, context):
    print(f'event: {json.dumps(event)}')

    #Read from SQS queue event
    event = json.loads(event['Records'][0]['body'])

    conn, cur = connect_to_rds()

    query_type = event['query_type']
    errors = 0

    HandlerClass = choose_handler(query_type)
    handler = HandlerClass(ddb_resource, query_type, event, staging_queue, conn, cur)

    handler.extract_vars()
    handler.make_query()
    handler.write_results()
    handler.log_window()
    handler.queue_next_query()
    #Make API call

    
    errors += handler.get_errors()
