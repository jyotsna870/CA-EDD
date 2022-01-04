import boto3
import datetime
"""
**********************************************************************************************

company : Accenture EDD
File Logic_test.py
Module Spark application

@functionality :
Extend class Logictest from Spark Job supper class
Override the run function


************************************************************************************************
"""
recieve_message = " "
part = 0
x = "transunion_" + str(datetime.datetime.now())
bucket_name = "baltesttesttest"
directory_name = x

sqs = boto3.client('sqs', region_name="us-east-2",
                   aws_access_key_id="AKIA5725HDPWVU3OADKB",
                   aws_secret_access_key="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"
                   )
s3 = boto3.client('s3', region_name="us-east-2",
                  aws_access_key_id="AKIA5725HDPWVU3OADKB",
                  aws_secret_access_key="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"
                  )

response = sqs.receive_message(
    QueueUrl='https://us-east-2.queue.amazonaws.com/961731304429/test.fifo',
    MaxNumberOfMessages=1,
    WaitTimeSeconds=1,
)

while 'Messages' in response:
    print(response['Messages'])
    print(type(response['Messages']))
    print((response['Messages'][0]['ReceiptHandle']))
    sqs.delete_message(QueueUrl='https://us-east-2.queue.amazonaws.com/961731304429/test.fifo',
                       ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
    response = sqs.receive_message(
        QueueUrl='https://us-east-2.queue.amazonaws.com/961731304429/test.fifo',
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    recieve_message = recieve_message + str(response)
    res = len(recieve_message.encode('utf-8'))
    if (res > 5):
        s3.put_object(Body=recieve_message, Bucket=bucket_name, Key=(directory_name) + "/" + "part" + str(part))
        part = part + 1
