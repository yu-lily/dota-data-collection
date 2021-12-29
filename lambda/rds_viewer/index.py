import json
import boto3
import os
import psycopg2
session = boto3.session.Session()

def connect_to_rds():
    sm_client = session.client(service_name='secretsmanager', region_name='us-west-2')
    secret = sm_client.get_secret_value(SecretId=os.environ['RDS_CREDS_NAME'])
    rds_creds = secret['SecretString']
    rds_creds = json.loads(rds_creds)
    db_endpoint = os.environ['AGHANIM_MATCHES_DB_ENDPOINT']

    print(secret)

    conn = psycopg2.connect(
        host = db_endpoint,
        database=secret['dbname'],
        user=secret['username'],
        password=secret['password']
    )
    cur = conn.cursor()

    return conn, cur


def handler(event, context):
    print(f'event: {json.dumps(event)}')
    

    conn, cur = connect_to_rds()

    cur.execute("""SELECT * FROM matches;""")

    matches = cur.fetchall()
    print(f'Matches table: {matches}')

    cur.close()
    conn.close()
    print("Reached End.")