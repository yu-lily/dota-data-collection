import json
import boto3
import os

from api_caller.lib import APICaller, API_Call_Metadata

#Create clients for interfacing with AWS resources
ddb_resource = boto3.resource('dynamodb')

sqs = boto3.resource('sqs')
staging_queue = sqs.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])

def choose_handler(query_type):
    if query_type == 'aghs_matches':
        from aghs_matches import AghsMatchesHandler
        return AghsMatchesHandler


def handler(event, context):
    print(f'event: {json.dumps(event)}')

    #Read from SQS queue event
    event = json.loads(event['Records'][0]['body'])


    query_type = event['query_type']
    errors = 0

    HandlerClass = choose_handler(query_type)
    handler = HandlerClass(ddb_resource, query_type, event, staging_queue)

    handler.extract_vars()
    handler.make_query()
    handler.write_results()
    handler.log_window()
    handler.queue_next_query()
    #Make API call

    
    errors += handler.get_errors()
