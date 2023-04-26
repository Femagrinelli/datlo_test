import boto3
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', 'lib')))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', 'src')))

import requests

ec2 = boto3.client('ec2')
ssm = boto3.client('ssm')

def lambda_handler(event, context):

    instance_id = 'i-02ea80c30089f80ff'
    ec2.start_instances(InstanceIds=[instance_id])

    time.sleep(180)
    
    ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        OutputS3BucketName='datlo-companies/logsSendCommand',
        Parameters={'commands': [
            "sudo -i -u ubuntu /home/ubuntu/run_script.sh"
        ]}
    )
    
    time.sleep(360)
    
    ec2.stop_instances(InstanceIds=[instance_id])
    
    return {
        'statusCode': 200,
        'body': f'Instance created with ID and command sent'
    }
