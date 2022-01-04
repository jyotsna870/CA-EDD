"""
**********************************************************************************************

company : Accenture EDD
File sqs_api.py
Module Spark application

@functionality :
1. read data from api
2. send data to SQS


************************************************************************************************
"""

import json
import boto3
import requests

sqs = boto3.client('sqs', region_name="us-east-2",
                   aws_access_key_id="test",

                   aws_secret_access_key="test"

                   )

queue_url = 'https://us-east-2.queue.amazonaws.com/961731304429/test'

jsondata = requests.request('GET', 'http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json')

response = sqs.get_queue_url(
    QueueName='test'

)

sqs.send_message(
    QueueUrl='https://us-east-2.queue.amazonaws.com/961731304429/test',
    MessageBody=json.dumps(jsondata.json()))
response = sqs.receive_message(
    QueueUrl='https://us-east-2.queue.amazonaws.com/961731304429/test',
    MaxNumberOfMessages=5,
    WaitTimeSeconds=10,
)
print(response)
