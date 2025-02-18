import json
import boto3
import csv
import io
 
s3Client = boto3.client('s3')
glue_client = boto3.client('glue')
 
 
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(bucket)
    print(key)
    glue_client.start_job_run(
        JobName = 'Spotifygluejob',
        Arguments = {}
    )
    return {
            'statusCode': 200,
            'body': json.dumps('Glue Job Triggered Successfully!')
        }
