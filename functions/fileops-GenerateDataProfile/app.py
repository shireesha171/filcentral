import json
import boto3
import os
import csv
import psycopg2
from dbconnection import pool, getConnection
import chardet
from urllib import parse
from botocore import client
from botocore.exceptions import ClientError

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
task_definition_version = os.environ.get('task_definition_ver')
security_groups = json.loads(os.environ.get('security_groups'))
subnets = json.loads(os.environ.get('subnet_at'))

# Initialize the ECS client
ecs = boto3.client('ecs', region_name='us-east-1')
cluster_name = os.environ.get('cluster_name')
account = os.environ.get('Account')
task_definition_arn = f'arn:aws:ecs:us-east-1:{account}:task-definition/FileOps-data-profilling:{task_definition_version}'
# Define the container name if you want to override environment variables
container_name = os.environ.get('container_name')


def lambda_handler(event, ctx):
    try:
        print("Received event : \n", event)
        value = parse.unquote_plus(event['Records'][0]["s3"]["object"]["key"])
        bucket_name = event['Records'][0]["s3"]["bucket"]["name"]
        s3_filepath = bucket_name + '__init__' + value
        job_id = value.split('/')[1]
        print("profile dataset for job_id: ", job_id)
        # Here calling this method for saving the delimiter file.
        save_delimiter(value, job_id)
        environment_variables = [
            {
                'name': 'file_path',
                'value': s3_filepath
            },

        ]
        task_overrides = {
            'containerOverrides': [
                {
                    'name': container_name,
                    "command": ["python", "main.py"],
                    'environment': environment_variables
                }
            ]
        }

        network_config = {
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                'assignPublicIp': 'ENABLED'
            },
        }
        response = ecs.run_task(
            cluster=cluster_name,
            taskDefinition=task_definition_arn,
            overrides=task_overrides,
            networkConfiguration=network_config,
            launchType='FARGATE',
            count=1  # Specify the number of tasks to run
        )
        print("ECS task started successfully:", response)
        return response_body(200,"ECS task started successfully:",response)

    except csv.Error as csv_error:
        return response_body(500, "The job onboarding failed due to invalid file format or delimiter", str(csv_error))
    except PermissionError as error:
        return response_body(403, "Access to the file is denied", str(error))
    except FileNotFoundError as error:
        return response_body(404, "File not found in S3 bucket", str(error))
    except ValueError as error:
        return response_body(500, f"Value Error: {str(error)}", str(error))
    except UnicodeDecodeError as error:
        return response_body(500, "Error decoding file", str(error))
    except psycopg2.DatabaseError as error:
        return response_body(500, "Couldn't save the delimiter in the table", str(error))
    except Exception as error:
        print("Error occurred while triggering the data profile task in ECS", error)
        return response_body(500, "Profile data ECS task failed due to unknown error", error)


def get_delimiter(file_key):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = getFileFromS3(file_key)
        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        delimiter = sniffer.sniff(sample, delimiters='|\t,:;').delimiter
        return delimiter
    except csv.Error as csv_error:
        print("getDelimiter::The job onboarding failed due to invalid file format or delimiter ", str(csv_error))
        raise csv_error
    except Exception as error:
        print(f"getDelimiter::Unknown error occurred during the detection of delimiter for the file {str(error)}")
        raise error


def getFileFromS3(file_key):
    """This method is for getting the file from s3"""
    try:
        if not file_key:
            raise ValueError("file_key cannot be empty or None.")

        s3 = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        response = s3.get_object(
            Bucket=s3_bucket,
            Key=file_key,
        )
        object_content = response['Body'].read()

        detected_encoding = chardet.detect(object_content)['encoding']
        if detected_encoding is None:
            raise ValueError("Could not detect the encoding of the file content.")
        decoded_file_content = object_content.decode(detected_encoding)
        return decoded_file_content

    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise FileNotFoundError(f"File {file_key} not found in bucket {s3_bucket}.")
        elif e.response['Error']['Code'] == "403":
            raise PermissionError(f"Access denied to file {file_key} in bucket {s3_bucket}.")
    except UnicodeDecodeError as error:
        raise UnicodeDecodeError(f"Error decoding file {file_key}: {error}")
    except Exception as error:
        print(f"Unexpected error accessing file {file_key}: {error}")
        raise error


def save_delimiter(file_key, job_id):
    conn, cursor = None, None
    print(f"The file location : {file_key} for the job : {job_id}")
    """This method is for saving the delimiter in database"""
    try:
        conn,cursor = getConnection()
        delimiter = get_delimiter(file_key)
        print(f"The delimiter detected for the file : {delimiter}")
        update_query = f"""
                            UPDATE jobs
                            SET delimiter = %s
                            WHERE uuid = %s
                          """
        cursor.execute(update_query, (delimiter, job_id))
        conn.commit()
    except psycopg2.DatabaseError as error:
        conn.rollback()
        print(f"Database error: Couldn't save the delimiter in the table :  {error}")
        raise error
    except Exception as error:
        conn.rollback()
        print(f"Unknown error occurred during delimiter save : {error}")
        raise error
    finally:
        cursor.close()
        pool.putconn(conn)


def response_body(status_code, message, data):
    """
    This is a response method
    """
    return {
      "statusCode": status_code,
      "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
      },
      "body": json.dumps({"message": message, "data": data}),
    }
