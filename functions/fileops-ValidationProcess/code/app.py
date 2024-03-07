"""This module is for validating process"""
import csv
import json
import logging

import boto3
import pandas as pd
import psycopg2
import uuid

from dbconnection import pool, getConnection
from .fetch_job_config import fetch_job_config, NoJobConfigError
from .error_saving import error_storing_db
from .file_validations import get_file_validations
from .forming_errors import forming_error
from .logs import printlogs
from .run_job_detail import run_job_detail_fun
from .schema_validations import getSchemaValidations
from .utils.utils import get_job_validation_status, get_current_date_time, invoke_file_converter_lambda
# JobRunLogger
from .utils.loggers import job_run_logger, log_file_validations, log_job_metadata, log_job_run_timestamp, \
    log_job_validation_status, log_schema_validations
from .utils.store_logs import store_logs_db, store_logs_s3
from datetime import datetime

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("validation-process.lambda.handler")
lambda_client = boto3.client('lambda')


# update_job_run_time = log_job_run_timestamp()


def replace_the_pattern_with_date(date_expression_string):
    date_pattern = r'\${(.*?)}'
    import re
    print("date_expression_string", date_expression_string)
    match = re.search(date_pattern, date_expression_string)
    date = datetime.now().date()
    if match:
        formatted_date = date.strftime(match.group(1))
        formatted_string = date_expression_string.replace(f"{match.group()}", formatted_date)
    # Added support for locations and paths without ${} group blocks
    elif date_expression_string is not None and date_expression_string.find("%") != -1:
        formatted_string = date.strftime(date_expression_string)
    else:
        print("No date format found in the input string.")
        formatted_string = date_expression_string
    return formatted_string


def lambda_handler(event, context):
    """This method is for handling validation  process"""
    conn, cursor = None, None
    # JobRunLogger - Updating Start Time
    update_job_run_time = log_job_run_timestamp()
    start_time = update_job_run_time.__next__()
    logger.info(start_time)
    print("Received Event: ", str(event))

    event_s3_trigger_record = None
    is_s3_event_trigger = False

    if event.get("Records") is not None:
        is_s3_event_trigger = True
        event_s3_trigger_record = event.get("Records")[0]

    if (is_s3_event_trigger and
            (event_s3_trigger_record['eventSource'] == 'aws:s3' and
             event_s3_trigger_record['eventName'] == 'ObjectCreated:Put')):
        event = s3_trigger_event(event_s3_trigger_record)
    try:
        conn, cursor = getConnection()
        query_params = event.get('queryStringParameters', None)
        if query_params is not None and "business_validate_id" in query_params:
            query_params = event['queryStringParameters']
            business_validate_id = query_params['business_validate_id']
            logger.info(
                "job_validation_process  ========   " + business_validate_id + 'date_time----  '
                + get_current_date_time()
            )
            # job configuration fetched
            record = fetch_job_config(business_validate_id)
            logger.info(f"Job configuration details {record}")

            # define and initialize variables
            job_uuid = record['job_uuid']
            job_int_id = record['job_id']
            file_name = record['file_name']
            job_run_record = {}
            job_validation_status = {}
            schema_validations_data = {}
            num_of_schema_validation_errors = [0]
            file_validations_data = {}
            file_errors = [0]

            # Job Run Logger - Business Process Id
            job_run_logger.business_process_id = record.get('business_process_uuid', None)
            printlogs(record, logger)

            if job_int_id is not None:
                if 'job_type' in query_params and query_params['job_type'] == "Scheduled":
                    query_params['location_pattern'] = replace_the_pattern_with_date(query_params['location_pattern'])
                    query_params['file_name_pattern'] = replace_the_pattern_with_date(query_params['file_name_pattern'])
                on_error_setting = record['standard_validations']['on_error'] if "on_error" in record[
                    'standard_validations'] else None
                terminate_flag = True if on_error_setting == "terminated" else False
                file_errors, file_validations_data, source_file_location = get_file_validations(record,
                                                                                                business_validate_id,
                                                                                                query_params,
                                                                                                terminate_flag)

                # Job Run Logger - Update File Validations
                log_file_validations(file_name, file_errors, file_validations_data)
                if file_errors[0] > 0:
                    if terminate_flag:
                        logger.info("File validation errors occurred and termination enabled on error")
                        error_list = forming_error(file_validations_data)
                        job_validation_status = get_job_validation_status(file_errors, [0])
                        job_run_record = error_storing_db(error_list, record, file_validations_data,
                                                          business_validate_id, query_params, job_validation_status,
                                                          source_file_location)
                        logger.info("Saving run logs " + get_current_date_time())
                        return prepare_job_run_results(job_uuid, job_run_record, job_validation_status,
                                                       update_job_run_time)
                logger.info("Source schema validation kicks in...")
                num_of_schema_validation_errors, schema_validations_data = getSchemaValidations(
                    business_validate_id, record,
                    logger, query_params)
                log_schema_validations(num_of_schema_validation_errors, schema_validations_data)
                # merge both file and schema validations
                total_validation_results = {**schema_validations_data, **file_validations_data}
                error_list = forming_error(total_validation_results)
                job_validation_status = get_job_validation_status(file_errors, num_of_schema_validation_errors)

                job_run_record = error_storing_db(error_list, record, total_validation_results,
                                                  business_validate_id, query_params, job_validation_status,
                                                  source_file_location)
                logger.info("Saving run logs " + get_current_date_time())

                job_run_uuid = job_run_record[1]

                invoke_file_converter_lambda(job_uuid=job_uuid, job_run_id=job_run_uuid, file_path=source_file_location,
                                             file_name=file_name)

                return prepare_job_run_results(job_uuid, job_run_record, job_validation_status, update_job_run_time,
                                               start_time)
            else:
                return response_body(400, "Provide valid job_id", None)
        else:
            return response_body(400, "Business_validate_id not found", None)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response_body(500, 'File validation failed due to database errors', str(e))
    except NoJobConfigError as e:
        return response_body(e.code, e.args[0], "No Job configuration found")
    except csv.Error as csv_error:
        return response_body(500, "The job validation failed due to invalid file format or delimiter", str(csv_error))
    except pd.errors.ParserError as pd_error:
        return response_body(500, "The job validation failed while parsing the file", str(pd_error))
    except Exception as error:
        print(f"Unknown error caught: {error}")
        return response_body(500, "The job validation failed due to unknown runtime error", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def response_body(statuscode, message, body):
    """This is response method"""
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


def trigger_logs_lambda(job_run_id):
    import os
    # split_character = "-"
    # split_list = os.environ['AWS_LAMBDA_FUNCTION_NAME'].split(split_character)
    # print("checking on ",split_list)
    lambda_client.invoke(
        FunctionName='fileops-Logs' + "-" + os.environ.get('Environment'),
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps({'function': os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                            "job_run_id": job_run_id,
                            "log_stream_name": os.environ['AWS_LAMBDA_LOG_STREAM_NAME']})
    )


def prepare_job_run_results(job_uuid, stored_job_run_record, job_validation_status, update_job_run_time, start_time):
    get_run_job = run_job_detail_fun(stored_job_run_record, job_validation_status)
    log_job_validation_status(job_validation_status)
    log_job_metadata(get_run_job[0])
    end_time = update_job_run_time.__next__()
    job_run_id = stored_job_run_record[1]
    job_run_logger.job_id = job_uuid
    job_run_logger.job_run_id = job_run_id
    # JobRunLogger - Store Logs
    job_run_logs_json = job_run_logger.to_json()
    store_logs_db(stored_job_run_record[1], job_run_logs_json, start_time, end_time)

    if start_time is not None and end_time is not None:
        start_time_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%f")
        end_time_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%f")

        get_run_job[0]['start_time'] = start_time_dt.strftime('%H:%M:%S')
        get_run_job[0]['end_time'] = end_time_dt.strftime('%H:%M:%S')

        duration=end_time_dt-start_time_dt
        duration_dt = datetime.strptime(str(duration), '%H:%M:%S.%f')

        get_run_job[0]['run_duration'] = duration_dt.strftime('%H:%M:%S')
    else:
        get_run_job[0]['start_time'] = None
        get_run_job[0]['end_time'] = None
        get_run_job[0]['run_duration'] = None


    store_logs_s3(job_uuid, job_run_id, job_run_logs_json)
    return response_body(200, "Retrieved Successfully", get_run_job)


if __name__ == "__main__":
    dateValue = replace_the_pattern_with_date("s3://filecentral/%m/%d/%Y")
    print(dateValue)


def s3_trigger_event(event_s3_trigger_record):
    conn, cursor = getConnection()
    s3_bucket_details = event_s3_trigger_record['s3']
    file_name = s3_bucket_details['object']['key'].split('/')[-1]
    file_uploaded_url = (s3_bucket_details['bucket']['name'] + '/' + s3_bucket_details['object']['key'])
    id_s_query = """
            SELECT j.id AS job_id,j.business_process_id  FROM jobs j WHERE j.uuid = %s;
            """
    cursor.execute(id_s_query, (s3_bucket_details['configurationId'],))
    conn.commit()
    inserted_row = cursor.fetchone()
    job_id = inserted_row[0]
    business_process_id = inserted_row[1]
    qurey = """
                       INSERT INTO business_user_validate_process(uuid, business_process_id,file_name, file_location_path,job_id)
                       VALUES(%s, %s, %s, %s,%s)
                       RETURNING *
                    """
    values = (str(uuid.uuid4()), business_process_id, file_name, file_uploaded_url, job_id)
    cursor.execute(qurey, values)
    conn.commit()
    inserted_row = cursor.fetchone()
    business_process_validate_id = inserted_row[1]
    location_pattern = file_uploaded_url.replace(f'/{file_name}', '')
    event = {
        'queryStringParameters': {
            'business_validate_id': business_process_validate_id,
            'job_type': 's3_trigger',
            'location_pattern': location_pattern,
            'file_name_pattern': file_name.split('.csv')[0],
            'key': s3_bucket_details['object']['key'],
            'bucket_name': s3_bucket_details['bucket']['name']
        }
    }
    print("S3 trigger Event", event)
    return event
