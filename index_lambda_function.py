import json
import os
import boto3
import requests
import base64

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

region = 'us-east-1'
host = 'https://search-photos-tc4d7zim35gsil6s6zrjbbd5ye.us-east-1.es.amazonaws.com'
index = 'photos'

datatype = '_doc'
url = host + '/' + index + '/' + datatype

credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region)

rekognition = boto3.client('rekognition')
s3_client = boto3.client('s3')

headers = { "Content-Type": "application/json" }


def lambda_handler(event, context):
    print(event)
    
    for record in event['Records']:

        bucket = record['s3']['bucket']['name']
        # 'name': 'photo-bucket-6998'
        key = record['s3']['object']['key']
        # 'key': '6.jpg'
        
        try:
            
            # from s3
            s3_response = s3_client.head_object(Bucket=bucket, Key=key)
            s3_object = s3_client.get_object(Bucket=bucket, Key=key)
            print(s3_response)
            print(s3_object)
            # {'ResponseMetadata': {'RequestId': '7CEEJPE7A083BMQW', 'HostId': 'LtijrC5qft8wcl4ZVzkvN/HPcrip9/Di6Us4iZrfmuhxWsEUMUouYZYgmwakrUl1wthh3IVO4w8=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'LtijrC5qft8wcl4ZVzkvN/HPcrip9/Di6Us4iZrfmuhxWsEUMUouYZYgmwakrUl1wthh3IVO4w8=', 'x-amz-request-id': '7CEEJPE7A083BMQW', 'date': 'Sun, 12 Nov 2023 15:45:25 GMT', 'last-modified': 'Sun, 12 Nov 2023 15:45:23 GMT', 'etag': '"8504cc9ea5f06a415e73dde22fb22ad9"', 'x-amz-server-side-encryption': 'AES256', 'accept-ranges': 'bytes', 'content-type': 'image/jpeg', 'server': 'AmazonS3', 'content-length': '72027'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2023, 11, 12, 15, 45, 23, tzinfo=tzutc()), 'ContentLength': 72027, 'ETag': '"8504cc9ea5f06a415e73dde22fb22ad9"', 'ContentType': 'image/jpeg', 'ServerSideEncryption': 'AES256', 'Metadata': {}}
            timestamp = s3_response['LastModified']
            timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
            # 2023-11-12T15:53:23
            
            item = {
                'objectKey': key,
                'bucket': bucket,
                'createdTimestamp': timestamp,
                'labels': []
            }
            # image_base64= base64.b64encode(s3_object['Body'].read())
            Bytes = base64.b64decode(s3_object['Body'].read().decode('utf-8'))
            print(1)
            print(Bytes)
            # from rekognition
            rekognition_response = rekognition.detect_labels(
                Image={'Bytes': Bytes}
            )

            labels = rekognition_response['Labels']
            # print('Detected labels:', labels)
            # Detected labels: [{'Name': 'Bridge', 'Confidence': 98.66751861572266, 'Instances': [], 'Parents': [], 'Aliases': [], 'Categories': [{'Name': 'Buildings and Architecture'}]}, {'Name': 'Suspension Bridge', 'Confidence': 98.66751861572266, 'Instances': [], 'Parents': [{'Name': 'Bridge'}], 'Aliases': [], 'Categories': [{'Name': 'Buildings and Architecture'}]}, {'Name': 'Logo', 'Confidence': 55.72846984863281, 'Instances': [], 'Parents': [], 'Aliases': [], 'Categories': [{'Name': 'Symbols and Flags'}]}, {'Name': 'Arch', 'Confidence': 55.031349182128906, 'Instances': [], 'Parents': [{'Name': 'Architecture'}], 'Aliases': [{'Name': 'Arched'}], 'Categories': [{'Name': 'Buildings and Architecture'}]}, {'Name': 'Architecture', 'Confidence': 55.031349182128906, 'Instances': [], 'Parents': [], 'Aliases': [], 'Categories': [{'Name': 'Buildings and Architecture'}]}]

            for i in labels:
                item['labels'].append(i['Name'])
            print(item)
                
            opensearch_response = requests.post(
                url,
                auth=auth,
                json=item,
                headers=headers
                )
                
            print(opensearch_response)
                
        except Exception as e:
            print('Error:', str(e))
    
    
    return
