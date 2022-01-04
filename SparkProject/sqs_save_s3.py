import boto3
import datetime
"""
**********************************************************************************************

company : Accenture EDD
File sqs_save_s3.py
Module Spark application

@functionality :
1 receive the message from sqs
2. Store data into buffer based on threshold 
3. when buffer is full and spill into the disk(store s3)




************************************************************************************************
"""
recieve_message = " "
part = 0
x = "transunion_" + str(datetime.datetime.now())
bucket_name = "baltesttesttest"
directory_name = x

sqs = boto3.client('sqs', region_name="us-east-2",
                   aws_access_key_id="test",
                   aws_secret_access_key="test"
                   )
s3 = boto3.client('s3', region_name="us-east-2",
                  aws_access_key_id="test",
                  aws_secret_access_key="test"
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
