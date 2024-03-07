"""This module is for source file configuration"""
import json
import uuid
import chardet
import psycopg2
import os
import boto3
from botocore.exceptions import ClientError
from dbconnection import pool, getConnection

from botocore import client
import csv

env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    """This handler is for creating and getting the source file configurations"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/configuration" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                get_obj_remaining = getFilesFromS3Bucket(body['job_id'])
                if len(get_obj_remaining) > 0:
                    delete_files_in_s3(get_obj_remaining)

                return SaveDelimiter(body)
            else:
                return response(400, 'Missing or invalid payload', None)
        elif event['resource'] == "/job/configuration" and event['httpMethod'] == 'GET':
            query_params = event.get("queryStringParameters", None)
            if query_params is not None and query_params["job_id"] != "":
                return getSourceFileConfigurations(query_params)
            else:
                return response(400, "Missing or invalid query parameters", None)
        elif event['resource'] == "/job/configuration" and event['httpMethod'] == 'PUT':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return updateSourceFileConfigurations(body)
            else:
                return response(400, 'Missing or invalid payload', None)
        else:
            return response(404, 'resource or endpoint not found', None)

    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'Job Configuration API has failed due to an Unexpected Error', str(e))


def getJobDetails(job_uuid):
    """This method is for getting job details"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        query = "SELECT id, created_by FROM jobs where uuid = %s"
        cursor.execute(query, (job_uuid,))
        data = cursor.fetchone()
        if data is not None:
            job_int_id, created_by = data
            return job_int_id, created_by
        else:
            job_int_id = None
            created_by = None
            return job_int_id, created_by

    except psycopg2.DatabaseError as e:
        print(f"getJobDetails:: Database error: {e}")
        return response(500, 'Saving Job configuration failed', str(e))
    except Exception as e:
        print(f"getJobDetails:: Unknown error caught: {e}")
        return response(500, 'Saving Job configuration failed', str(e))

    finally:
        cursor.close()
        pool.putconn(conn)


def getIntIds(table_name, uuid):
    """This method is for getting source int id and target int id"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if uuid != '':
            get_query = f"SELECT id from {table_name} WHERE uuid = %s"
            cursor.execute(get_query, (uuid,))
            int_id = cursor.fetchone()
            return int_id
        else:
            return None

    except psycopg2.DatabaseError as e:
        print(f"getIntIds::Database error: {e}")
        return response(500, 'Saving Job configuration failed', str(e))
    except Exception as e:
        print(f"getIntIds::Unknown error caught: {e}")
        return response(500, 'Saving Job configuration failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def getUuid(table_name, id):
    """This method is for getting source_uuid and target_uuid"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if id != '':
            get_query = f"SELECT uuid from {table_name} WHERE id = %s"
            cursor.execute(get_query, (id,))
            uuid_ = cursor.fetchone()
            return uuid_
        else:
            return None

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'Get Job configuration details failed', str(e))
    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'Get Job configuration details failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def getSourceFileConfigurations(query_params):
    """This method is for getting the source file configurations"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_uuid = query_params['job_id']
        job_int_id, created_by = getJobDetails(job_uuid)

        if job_int_id is not None:
            get_source_file_configuration_details = """
                                                        SELECT sfc.source_id, sfc.target_id, sfc.file_config_details, sfc.standard_validations, j.job_type, sfc.email_notification, j.delimiter,
                                                        sfc.target_file_config_details
                                                        FROM jobs j LEFT JOIN source_file_config sfc ON j.id = sfc.job_id 
                                                        WHERE j.id = %s      
                                                    """
            cursor.execute(get_source_file_configuration_details, (job_int_id,))
            data = cursor.fetchone()

            if data is not None:
                source_uuid = getUuid('source_config', data[0])
                target_uuid = getUuid('target_config', data[1])

                target_file_config_details = json.loads(data[7]) if data[7] else {}

                response_data = {
                    "job_id": job_uuid,
                    "source_id": source_uuid[0] if source_uuid is not None else None,
                    "target_id": target_uuid[0] if target_uuid is not None else None,
                    "file_configurations": data[2],
                    "standard_validations": data[3],
                    "job_type": data[4],
                    "email_notification": data[5],
                    "delimiter": data[6],
                    "partitions_type": target_file_config_details.get('parquet_partition_type', ''),
                    "partitions_columns": target_file_config_details.get('parquet_partitions_schema', [{}]),
                    "target_file_type": target_file_config_details.get('target_file_type', ''),
                    "target_filename_pattern": target_file_config_details.get('target_filename_pattern', '')
                }
                return response(200, "Data retrieved successfully", response_data)
            else:
                return response(404, "No configuration data found for the given job_id", None)
        else:
            return response(404, "Could not fetch job details", None)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'Get Job configuration details failed', str(e))
    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'Get Job configuration details failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def updateSourceFileConfigurations(body):
    """This method is for updating the source file configurations"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_uuid = body['job_id']
        job_int_id, created_by = getJobDetails(job_uuid)
        source_int_id = getIntIds('source_config', body.get('source_id', ''))
        target_int_id = getIntIds('target_config', body.get('target_id', ''))

        target_file_type = body.get('target_file_type', '')
        target_filename_pattern = body.get('target_filename_pattern', '')
        parquet_partition_type = body.get('partitions_type', '')
        parquet_partitions_schema = body.get('partitions_columns', [{}])

        target_file_config_details = json.dumps({
            "target_file_type": target_file_type,
            "target_filename_pattern": target_filename_pattern,
            "parquet_partition_type": parquet_partition_type,
            "parquet_partitions_schema": parquet_partitions_schema
        })

        if job_int_id is not None:
            insert_into_sfc = """
                                  UPDATE source_file_config
                                  SET 
                                    source_id = %s,
                                    target_id = %s,
                                    email_notification = %s,
                                    file_config_details = %s,
                                    standard_validations = %s,
                                    target_file_config_details = %s
                                  WHERE job_id = %s
                              """
            values = (
                source_int_id,
                target_int_id,
                body.get('email_notification') == "True",
                json.dumps(body.get("file_configurations", {})),
                json.dumps(body.get("standard_validations", {})),
                target_file_config_details,
                job_int_id
            )
            cursor.execute(insert_into_sfc, values)
            conn.commit()

            # Updating the jobs table
            update_jobs = """
                                        UPDATE jobs 
                                        SET job_type = %s
                                        WHERE uuid = %s 
                                      """
            if body.get("job_type", "").lower() == "scheduled":
                job_type = "scheduled"
            elif body.get("job_type", "").lower() == "s3_trigger":
                job_type = "s3_trigger"
            else:
                job_type = "manual"
            # job_type = "scheduled" if body.get("scheduled", "").lower() == "true" else "adhoc"
            cursor.execute(update_jobs, (job_type, job_uuid))
            conn.commit()
            return response(200, "Job configuration updated successfully", body)
        else:
            return response(400, "Please provide valid job_id", None)

    except psycopg2.DatabaseError as e:
        print(f"updateSourceFileConfigurations::Database error: {e}")
        return response(500, 'Update Job configuration details failed', str(e))
    except Exception as e:
        print(f"updateSourceFileConfigurations:: caught unknown Error: {e}")
        return response(500, 'Update Job configuration details failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_delimiter(file_key, job_uuid):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = getFileFromS3(file_key)
        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        delimiter = sniffer.sniff(sample, delimiters='| , : ; \t').delimiter
        return delimiter
    except csv.Error as csv_error:
        print("getDelimiter::The job onboarding failed due to invalid file format or delimiter ", str(csv_error))
        # Calling a method Update_file_details
        Update_file_details(job_uuid)
        raise csv_error
    except Exception as error:
        print(f"getDelimiter::Unknown error occurred during the detection of delimiter for the file {str(error)}")
        raise error


def Update_file_details(job_uuid):
    """This method is for updating the file details to None"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        query = """
                    UPDATE jobs 
                    SET file_size = %s,
                        file_name = %s
                    WHERE uuid = %s
                """
        values = (None, None, job_uuid)
        cursor.execute(query, values)
        conn.commit()
    except psycopg2.DatabaseError as db_error:
        print("Update_file_details::Unable to update file details to database ", str(db_error))
        raise db_error
    except Exception as error:
        print("Update_file_details::Unknown error occurred while updating the file details to database ", str(error))
        raise error
    finally:
        cursor.close()
        pool.putconn(conn)


def getFileFromS3(file_key):
    """This method is for getting the file from s3"""
    try:
        if not file_key:
            raise ValueError("file_key cannot be empty or None.")

        s3 = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        s3_response = s3.get_object(
            Bucket=s3_bucket,
            Key=file_key,
        )
        object_content = s3_response['Body'].read()

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


def SaveDelimiter(body):
    """This method is for saving the delimiter in database"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_id = body.get('job_id', None)
        file_name = body.get('filename', None)
        job_int_id, created_by = getJobDetails(job_id)
        if job_id is not None and file_name is not None:
            file_key = "sample-files/" + job_id + "/" + file_name

            delimiter = get_delimiter(file_key, job_id)
            print(f"The delimiter detected for the file : {delimiter}")
            update_query = f"""
                                UPDATE jobs
                                SET delimiter = %s
                                WHERE uuid = %s
                              """
            cursor.execute(update_query, (delimiter, job_id))
            conn.commit()

            # It is for checking if the record in source_file_config table for give job_id
            query = """
                    SELECT sfc.uuid FROM source_file_config as sfc 
                    JOIN jobs as j ON sfc.job_id = j.id
                    WHERE j.uuid = %s
                """
            cursor.execute(query, (job_id,))
            data = cursor.fetchone()
            print("SaveDelimiter::data", data)
            if data is None:
                # creating a record in the source-file-config table
                insert_into_sfc = """
                                INSERT INTO source_file_config(uuid, created_by, job_id)
                                 VALUES(%s, %s, %s)
                                """
                values = (
                    str(uuid.uuid4()),
                    created_by,
                    job_int_id
                )
                cursor.execute(insert_into_sfc, values)
                conn.commit()
                if cursor.rowcount == 1:
                    print("Record inserted successfully")
                else:
                    print("Record insertion failed")
            return response(200, f"The delimiter: {delimiter} saved successfully", None)
        else:
            return response(400, "Please provide the valid job_id", None)

    except csv.Error as csv_error:
        return response(500, 'The job onboarding failed due to invalid file format or delimiter', str(csv_error))
    except PermissionError as error:
        return response(403, "Access to the file is denied", str(error))
    except FileNotFoundError as error:
        return response(404, "File not found in S3 bucket", str(error))
    except ValueError as error:
        return response(500, f"Value Error: {str(error)}", str(error))
    except UnicodeDecodeError as error:
        return response(500, "Error decoding file", str(error))
    except psycopg2.DatabaseError as e:
        print(f"Couldn't save the delimiter in the table: {e}")
        return response(500, 'Unable to save delimiter to database', str(e))
    except Exception as e:
        print(f"SaveDelimiter:: caught unknown Error: {e}")
        return response(500, 'Unknown error caught while saving delimiter to database', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def delete_files_in_s3(objs):
    try:
        s3 = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        files_to_delete = [{'Key': item['Key']} for item in objs]

        # Delete the specified files from the S3 bucket
        s3_response = s3.delete_objects(
            Bucket=s3_bucket,
            Delete={
                'Objects': files_to_delete,
                'Quiet': False
            }
        )
        print("delete_files_in_s3::s3_response ", s3_response)

    except Exception as e:
        print(f"delete_files_in_s3:: Unknown error caught: {e}")
        return response(500, 'Post Job configuration details failed', str(e))


def getFilesFromS3Bucket(uuid):
    """This method is getting file from S3"""
    bucket_name = s3_bucket
    folder_path = 'sample-files/' + uuid + "/"
    s3_client = boto3.client('s3', config=client.Config(signature_version='s3v4'))

    try:
        s3_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        objects = s3_response.get('Contents', [])
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'])
        print("getFilesFromS3Bucket:: sorted_objects ", sorted_objects)
        sorted_objects.pop()
        return sorted_objects

    except Exception as e:
        print(f"getFilesFromS3Bucket:: Unknown error caught: {e}")
        return response(500, 'Get Job configuration details failed', str(e))


def response(status_code, message, data):
    """This is response method"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data})
    }
