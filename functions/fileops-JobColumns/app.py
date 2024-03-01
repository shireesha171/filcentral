import os
import boto3
import json
import pandas as pd
from dbconnection import pool, getConnection
import psycopg2

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    print("Received Event: ", str(event))
    query_params = event.get('queryStringParameters', None)
    if event['resource'] == "/job/columns" and event[
            'httpMethod'] == 'GET' and query_params is not None and "job_id" in query_params:
        uuid = query_params['job_id']
        print(f"Lambda handler invoked to retrieve the columns for job id : {uuid}")
        s3_object = get_file_from_s3(uuid)
        if s3_object is None:
            return response_body(400,"Sample file not found", None)
        try:
            sample_df = readings3file(s3_object['Body'], s3_object['file'], uuid)
            columns = sample_df.columns.tolist() if sample_df is not None else []
            print(f"Job columns identified are : {columns}")
            return response_body(200, "Job columns are retrieved successfully", columns)
        except Exception as error:
            return response_body(500, "GET /job/columns Request processing error", str(error))
    else:
        return response_body(400, "Bad request received to retrieve job columns", None)


def get_file_from_s3(uuid):
    """This method is getting file from S3"""
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + uuid + "/"
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        for obj in response['Contents']:
            if obj['Key'].endswith(".csv"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'csv'}
            elif obj['Key'].endswith(".xlsx"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'xlsx'}
            elif obj['Key'].endswith(".json"):
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'json'}
        return response_body(400, f"Invalid source file format for job_id: {uuid}.\nSupported file format : csv", None)
    except Exception as error:
        print("Source sample file read error", str(error))
        return response_body(500, f"Source sample file read error for job_id: {uuid}", str(error))


def response_body(statuscode, message, body):
    """This is a response method"""
    response = {
        "statusCode": statuscode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": body})
    }
    return response


def readings3file(body, file, job_id):
    """This method is for reading file from S3"""
    try:
        if file == 'csv':
            # Here calling a method to get the delimiter
            delimiter = get_delimiter(job_id)
            if delimiter:
                return pd.read_csv(body, delimiter=delimiter)
            else:
                return pd.read_csv(body)
        elif file == 'xlsx':
            return pd.read_excel(body)
        return None
    except Exception as error:
        print(f"Error occurred while reading s3 file in job {job_id} : ", str(error))
        return response_body(500, f"Error occurred while reading s3 file in job {job_id} : ", str(error))


def get_delimiter(job_uuid):
    conn, cursor = None, None
    """This method is for getting the delimiter given in file configurations"""
    try:
        conn, cursor = getConnection()
        query = f"SELECT delimiter from jobs WHERE uuid ='{job_uuid}'"
        cursor.execute(query)
        data = cursor.fetchone()
        if data is not None:
            delimiter = data[0]
            print(f"delimiter for job_id : {job_uuid} : ", delimiter)
            return delimiter
        else:
            print(f"Delimiter not found for job_id : {job_uuid}")
            raise ValueError(f"Delimiter not found for job_id : {job_uuid}")
    except psycopg2.DatabaseError as e:
        print(f"Database error happened while retrieving delimiter for job : {job_uuid} \n Error :  {e}")
        return response_body(500, f"Database error happened while retrieving delimiter for job : {job_uuid}", str(e))
    except ValueError as e:
        return response_body(500, f"No delimiter found for job : {job_uuid}", str(e))
    except Exception as error:
        print(f"Unknown error while fetching the delimiter for job : {job_uuid}", error)
        return response_body(500, f"Unknown error while fetching the delimiter for job : {job_uuid}", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)
