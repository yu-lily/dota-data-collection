import json
import boto3
import os

from api_caller.lib import APICaller, API_Call_Metadata

#Create clients for interfacting with AWS resources
ddb_resource = boto3.resource('dynamodb')


def choose_handler(query_type):
    if query_type == 'aghs_matches':
        from aghs_matches import AghsMatchesHandler
        return AghsMatchesHandler


def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    query_type = event['query_type']

    errors = 0

    HandlerClass = choose_handler(query_type)
    handler = HandlerClass(ddb_resource, query_type, event)

    handler.extract_vars(event)
    handler.make_query()
    handler.write_results()
    handler.log_window()

    #Make API call

    
    errors += handler.get_errors()
