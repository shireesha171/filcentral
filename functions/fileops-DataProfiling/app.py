"""This module is for DataProfiling"""
import json
import boto3
import os
env = os.environ.get('Environment')
#s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
def lambda_handler(event, context):
    print("Received Event: ", str(event))
    """This method is for handling data profiling"""
    try:
        query_params = event.get('queryStringParameters', None)
        if query_params is not None and "job_id" in query_params:
            query_params = event['queryStringParameters']
            job_id = query_params['job_id']
            # getting file from s3
            get_file = getfilefromS3(job_id)
            if get_file is False:
                statuscode = 400
            else:
                statuscode = 200
            return response_body(statuscode,"Data Profile details fetched successfully",get_file)
        else:
            return response_body(400, "queryParams not found", None)
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
        return response_body(500, 'Data Profiling API has failed due to an Unexpected Error', str(e))


def getfilefromS3(uuid):
    """Creating a S3 client"""
    try:
        bucket_name = s3_bucket
        folder_path = 'profiling/' + uuid + "/"
        s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        for obj in response['Contents']:
            if obj is not None and obj["Key"].endswith("html"):
                url = get_s3_object_url(bucket_name=bucket_name, object_key=obj['Key'])
                return url
            else :
                return False
    except Exception as e:
        print(f"getfilefromS3::Unknown error caught: {e}")
        raise e

def get_s3_object_url(bucket_name, object_key):
    """Generating presigned URL"""
    try:
        s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key}
        )
        return url
    except Exception as e:
        print(f"get_s3_object_url::Unknown error caught: {e}")
        raise e

def response_body(status_code, message, body):
    """This is a response method"""
    response = {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": body})
    }
    return response
