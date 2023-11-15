import json
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import datetime
from botocore.exceptions import ClientError

import boto3

REGION = 'us-east-1'
HOST = 'search-photos-tc4d7zim35gsil6s6zrjbbd5ye.us-east-1.es.amazonaws.com'
INDEX = 'photos'

params = {

        'MaxNumberOfMessages': 1,  # Adjust based on your use case
        'MessageAttributeNames': ['All'],
        'VisibilityTimeout': 6,
        'WaitTimeSeconds': 6  # Set to a non-zero value for long polling
    }
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    lex = boto3.client('lexv2-runtime')

    print(event)
    msg = event['queryStringParameters']['q']
    msg = msg.replace("show me ", "").replace("search for images with ", "").replace("find pictures of ", "").replace("show me photos with ", "").replace(" in them", "")
    response = lex.recognize_text(
        botId='KUJ1JIDAW4', # MODIFY HERE
        botAliasId='XRXUTS2W4S', # MODIFY HERE
        localeId='en_US',
        sessionId='testuser',
        text=msg)
    print(response)
    slots = response['sessionState']['intent']['slots']
    q = ""
    for i in slots.keys():
        if slots[i]:
            q = slots[i]['value']['originalValue']
    # opensearch_result = query(event['inputTranscript'])
    if q=="":
        q = msg
    print(q)
    opensearch_result = query(q)
    #[{'objectKey': '5.jpg', 'bucket': 'photo-bucket-6998', 'createdTimestamp': '2023-11-12T16:31:57', 'labels': ['Bridge', 'Suspension Bridge', 'Logo', 'Arch', 'Architecture']}]

    result = {'results':[]}
    if opensearch_result:
        for i in opensearch_result:
            bucket_name = i['bucket']
            object_key = i['objectKey']
            labels = i['labels']
            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=3600  # URL expiration time in seconds (adjust as needed)
                )
        
            item = {
                "url": url,
                "labels": labels
                }
            
            result['results'].append(item)
    print(result)
    return {
        'statusCode': 200,
        'headers':{
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps(result)
    }
            
    

def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results

        
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
