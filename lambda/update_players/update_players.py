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
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    
    return secret

def query_leaderboard(region):
    endpoint = "https://api.stratz.com/graphql/"
    query = '''
    {
    leaderboard {
        season(request: {leaderBoardDivision:$region, take: 6000}) {
        steamAccountId 
        }
    }
    }
    '''
    # lastUpdateDateTime
    # rank
    # rankShift

    variables = {'region': region}
    headers = {'Authorization': 'Bearer ' + get_apikey()}
    r = requests.post(endpoint, json={'query': query , 'variables': variables}, headers=headers)
    if r.status_code == 200:
        return json.loads(r.text)['data']['leaderboard']['season']
    else:
        print(f"Query failed with status code: {r.status_code}")

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    client = boto3.resource('dynamodb')
    table = client.Table(os.environ['TABLE_NAME'])

    REGIONS = ['AMERICAS', 'SE_ASIA', 'EUROPE', 'CHINA']
    for region in REGIONS:
        players = query_leaderboard(region)
        for player in players:
            item = {os.environ['PARTITION_KEY']: player['steamAccountId'], 'region': region}
            table.put_item(item)



    # #Get metadata about buckets/file
    # input_bucket_name = event['Records'][0]['s3']['bucket']['name']
    # output_bucket_name = os.environ['OUTPUT_BUCKET']
    # table = boto3.resource('dynamodb').Table(os.environ['DATABASE_NAME'])

    # file_key_name = event['Records'][0]['s3']['object']['key']
    # input_file_path = f'/tmp/{file_key_name}'
    # print("INPUT FILE: " + file_key_name)
    
    # #Log start
    # start_ts=time.time()
    # item={'fname+time':f'{file_key_name}-{str(start_ts)}', 'Timestamp':str(start_ts), 'Action': 'START PROCESSING'}
    # print(item)
    # table.put_item(Item=item)

    # #Get the input file to convert
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(input_bucket_name)
    # object = bucket.Object(file_key_name)

    # with open('/tmp/' + file_key_name, 'wb') as f:
    #     object.download_fileobj(f)

    # s3_client.delete_object(Bucket=input_bucket_name, Key=file_key_name)

    # #Convert the file
    # try:
    #     ex = read_exchange(input_file_path, parallelize=None)
    #     ex_xr = ex.to_xarray()
    #     output_file_name = ex_xr.cchdo.gen_fname()
    #     output_file_path = f'/tmp/{output_file_name}'
    #     ex_xr.to_netcdf(output_file_path)
    #     print(output_file_path)

    #     #Move converted file to output bucket
    #     s3_client.upload_file(output_file_path, output_bucket_name, output_file_name)
    #     success = True
    #     e=''
    # except exception as e:
    #     success = False
    
    # #Log finish
    # item={'fname+time':f'{file_key_name}-{str(start_ts)}', 'Timestamp':str(start_ts), 'Action': 'FINISH PROCESSING', "Output File Name": output_file_name,'Success': str(success), "Error Message": e}
    # print(item)
    # table.put_item(Item=item)
    # print("Done.")

