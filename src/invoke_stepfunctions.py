import json
import boto3
import datetime
import os
from botocore.client import ClientError
status_code = 200

client = boto3.client('stepfunctions')
s3 = boto3.resource('s3')
myStateMachineArn=os.environ['MY_STATE_MACHINE_ARN']

def run(event, context):
    try:
        status_code = client.start_execution(
        stateMachineArn=myStateMachineArn,
        name= str(datetime.datetime.now().timestamp()),
        input= json.dumps(event)
        )
        print(event)
        print("invoke and transfer event to the AWS Step functions sucessfully")

    except Exception as e:
        print (e)