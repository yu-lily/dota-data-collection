import psycopg2
import os
import boto3
import aws

# Create a Secrets Manager client
session = boto3.session.Session()


def handler(event, context):
    print(event)
    print(f'DB Proxy str representation: {os.environ["db_endpoint"]}')

    #client = session.client(service_name='secretsmanager', region_name=event['region'])
    client = session.client(service_name='secretsmanager', region_name=event['us-west-2'])
    get_secret_value_response = client.get_secret_value(SecretId=os.environ['rds_creds'])

    secret = get_secret_value_response['SecretString']

    print(secret)


    conn = psycopg2.connect(
        host = os.environ['db_endpoint'],
    )