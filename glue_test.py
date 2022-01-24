"""
**********************************************************************************************
Company : Accenture EDD
Date: 12/23/3021
Module : Spark application
File : Glue test file
@function : Run the Spark application on AWS Glue using boto3 python SDK
**********************************************************************************************
 """
import json
from argparse import ArgumentParser
import boto3

"""
Python class GlueTest with contructor Job_name and S3 path where do you store the python file
"""


class GlueTest:
    """
    A GlueTest standardizes how spark jobs are submitted to AWSglue via boto3.
    """

    def __init__(
            self,
            spark_job_name: str,
            path: str

    ):
        self.spark_job_name = spark_job_name
        self.path = path

    """
    @function : submit function to run spark job
    Pass aws access key and secret key
    Setup the profile aws configure
    Commands to create profile
    commands : 
  
    aws configure
    AWS Access Key ID [None]: None
    AWS Secret Access Key [None]: None
    Default region name [None]: None
    Default output format [None]: json
    
    """

    def submit(self):
        """
        Submit job to Glue.
        Use boto3 to create client for glue
        """

        client = boto3.client('glue', region_name="us-east-2",
                              aws_access_key_id="key",
                              aws_secret_access_key="secret key"

                              )
        """
        create job to pass spark parameter such as no worker and memory  memory etc
        """
        response = client.create_job(
            Name=self.spark_job_name,
            Role='test_spark',
            Command={
                'Name': 'glueetl',
                'ScriptLocation': self.path,
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--TempDir': 's3://baltesttesttest/temp_dir',
            },
            MaxRetries=1,
            GlueVersion='3.0',
            NumberOfWorkers=2,
            WorkerType='Standard'
        )
        # run the job
        response = client.start_job_run(
            JobName=self.spark_job_name
        )

        print(json.dumps(response, indent=4, sort_keys=True, default=str))


"""
 main method to run the job pass the arguments
  """
if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("--job-name", dest="job_name")
    parser.add_argument("--path", dest="path")

    parser.set_defaults(
        job_name=None,
        path=None,

    )

    args = parser.parse_args()

    gluetest = GlueTest(
        spark_job_name="sqs8",
        path="s3://baltesttesttest/sqs1.py"

    )

    gluetest.submit()
