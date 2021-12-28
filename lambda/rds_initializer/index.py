import psycopg2
import os
import boto3
import json
# Create a Secrets Manager client
session = boto3.session.Session()


def handler(event, context):
    print(event)
    print(f'DB Proxy str representation: {os.environ["db_endpoint"]}')

    #client = session.client(service_name='secretsmanager', region_name=event['region'])
    client = session.client(service_name='secretsmanager', region_name='us-west-2')
    get_secret_value_response = client.get_secret_value(SecretId=os.environ['rds_creds_name'])

    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    print(secret)


    conn = psycopg2.connect(
        host = os.environ['db_endpoint'],
        database=secret['dbname'],
        user=secret['username'],
        password=secret['password']
    )
    cur = conn.cursor()


    cur.execute("""SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type='BASE TABLE';""")

    for table in cur.fetchall():
        print(table)

    cur.close()
    conn.close()
    print("Reached End.")