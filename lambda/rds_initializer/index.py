import psycopg2
import os
import boto3
import json
# Create a Secrets Manager client
session = boto3.session.Session()


def handler(event, context):
    print(event)
    print(f'DB Proxy str representation: {os.environ["db_endpoint"]}')

    sm_client = session.client(service_name='secretsmanager', region_name='us-west-2')
    get_secret_value_response = sm_client.get_secret_value(SecretId=os.environ['rds_creds_name'])

    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    conn = psycopg2.connect(
        host = os.environ['db_endpoint'],
        database=secret['dbname'],
        user=secret['username'],
        password=secret['password']
    )
    cur = conn.cursor()

    # #Destroy existing tables
    cur.execute("""DROP TABLE IF EXISTS matches CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS players CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS playerDepthList CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS playerBlessings CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS depthList CASCADE;""")
    cur.execute("""DROP TABLE IF EXISTS ascenionAbilities CASCADE;""")
    conn.commit()

    #Get all tables
    cur.execute("""SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type='BASE TABLE';""")

    tables = cur.fetchall()
    print(f'Existing tables: {tables}')

    #Create tables
    if ('matches',) not in tables:
        
        print("Creating tables")
        cur.execute(open("aghs_schema.sql", "r").read())
        conn.commit()

    cur.close()
    conn.close()
    print("Reached End.")