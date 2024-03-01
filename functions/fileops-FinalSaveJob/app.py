import psycopg2
from dbconnection import pool, getConnection
import json
import boto3
import os
from botocore.exceptions import ClientError
import pandas as pd
from redshift_connector import redshift_connection, get_secret
import uuid

env = os.environ.get('Environment')
redshift_role = os.environ.get('Redshift_Role')
scheduler_sns_topic = os.environ.get('Scheduler_Sns_Topic')
email_notification_sns_topic = os.environ.get('Email_Notification_Sns_Topic')

# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

Account = os.environ.get('Account')
role_arn = f'arn:aws:iam::{Account}:role/FileOps_Role'


def lambda_handler(event, handler):
    print("Received Event: ", str(event))
    query_params = event.get('queryStringParameters', None)
    if event['resource'] == "/job/final-save" and event['httpMethod'] == 'POST':
        if 'body' in event:
            body = json.loads(event['body'])
            return businessProcessStatusUpdate(body)
    elif event['resource'] == "/job/final-save" and event['httpMethod'] == 'GET':
        return sourceConfigDetails(query_params)


def businessProcessStatusUpdate(body):
    if body is not None and "business_process_id" in body:
        business_process_id = body['business_process_id']
        job_id = body['job_id']
        conn, cursor = None, None
        try:
            conn, cursor = getConnection()
            jobs_query = """
                UPDATE jobs
                SET status = 'configured'
                where uuid = %s
                """
            values = (job_id,)
            cursor.execute(jobs_query, values)
            conn.commit()
            query = """
                UPDATE business_process
                SET status = sub.status
                from(
                select CASE WHEN count(*) >= bp.no_of_files THEN 'Active' ELSE 'Draft' END status
                from jobs as j join business_process as bp on bp.id = j.business_process_id
                where bp.uuid = %s and j.status = 'configured' group by bp.id )
                sub where uuid = %s
                """
            values = (business_process_id, business_process_id)
            cursor.execute(query, values)
            conn.commit()
            jobs_query1 = """
                              SELECT j.schedule_json, j.job_name, sc.file_config_details,sc2.location_pattern,j.job_type,
                                   j.id, tc2.location_pattern as target_location_pattern, tc2.secret_name, sc.target_metadata, tc2.connectivity_type
                                  FROM jobs as j
    				              JOIN source_file_config as sc on j.id = sc.job_id
    				              left join source_config sc2 on sc2.id = sc.source_id
    				              left join target_config tc2 on tc2.id = sc.target_id
                                  WHERE j.uuid = %s
                              """
            values = (job_id,)
            cursor.execute(jobs_query1, values)
            record = cursor.fetchone()

            if record[9] == 'redshift':
                # create table in external schema of Redshift
                job_name = record[1].replace(' ', '-')
                job_int_id = record[5]
                redshift_ext_table_raw = str(job_name) + "_" + str(job_int_id)
                redshift_ext_table = redshift_ext_table_raw
                target_location_pattern = record[6]
                secret_name = 'fdo536T-dev' #record[7]
                target_schema = record[8]
                create_ext_table_redshift(redshift_ext_table, target_location_pattern, secret_name, target_schema)

            print(record[2])
            if record[4] == 's3_trigger':
                creating_S3_trigger(job_id, record[3])

            if record is not None and record[0] is not None:

                file_name_pattern = record[2]['file_name_pattern']
                location_pattern = record[3]
                sns_data = {
                    "job_id": job_id,
                    "business_process_id": business_process_id,
                    "schedule_json": record[0],
                    "job_name": record[1],
                    "env": env,
                    "action": "create",
                    "file_name_pattern": file_name_pattern,
                    "location_pattern": location_pattern
                }

                snsnotification(scheduler_sns_topic, sns_data)

            bp_details_query = """SELECT j.job_name, j.job_type, j.file_name,j.file_size, sfc.dqrules,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                          sfc.jira_incident_to_be_raised_for_failures, 
                                          bp."name" as business_name, bp.email_notification as email_recipient_list, bp.jira_incident_to_be_raised_for_failures as jira_recipient_list,
                                          sc."name" as source_name,sc.absolute_file_path as source_absolute_file_path,
                                          tc."name" as target_name,tc.absolute_file_path as target_absolute_file_path
                                          FROM jobs AS j
                                          JOIN business_process as bp on bp.id = j.business_process_id
                                          JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                          left join target_config as tc on sfc.target_id = tc.id
                                          left join source_config as sc on sc.id = sfc.source_id
                                          WHERE bp.uuid =  %s 
                                      """
            cursor.execute(bp_details_query, (business_process_id,))
            bp_data = cursor.fetchall()
            if len(bp_data) > 0:
                bp_data_record = bp_data[-1]
                if bp_data_record[7] == True:
                    sns_record = {
                        "record_type": "JOB_ONBOARD_EMAIL_NOTIFICATION",
                        "job_name": bp_data_record[0],
                        "job_type": bp_data_record[1],
                        "file_name": bp_data_record[2],
                        "file_size": bp_data_record[3],
                        "column_validation": bp_data_record[4],
                        "standard_validations": bp_data_record[5],
                        "file_config_details": bp_data_record[6],
                        "email_notification_enablement": bp_data_record[7],
                        "jira_notification_enablement": bp_data_record[8],
                        "business_process_name": bp_data_record[9],
                        "email_recipient_list": bp_data_record[10],
                        "jira_recipient_list": bp_data_record[11],
                        "source_name": bp_data_record[12],
                        "source_absolute_file_path": bp_data_record[13],
                        "target_name": bp_data_record[14],
                        "target_absolute_file_path": bp_data_record[15]
                    }

                    print("sns_record :", sns_record)

                    snsnotification(email_notification_sns_topic, sns_record)
            return response_body(200, "Updated Successfully", "")
        except psycopg2.Error as error:
            # Handle the error
            print("Error executing update query:", error)
            return response_body(400, 'error', str(error))
        except Exception as error:
            print("Error executing update query:", error)
            return response_body(400, str(error), None)
        finally:
            cursor.close()
            pool.putconn(conn)
    else:
        return response_body(400, "Business_process_id not found", "")

def create_ext_table_redshift(table_name,target_location,secret_name,schema):
    secret_string = get_secret(secret_name)
    secret_dict = json.loads(secret_string)
    database = secret_dict['database']
    create_schema_query = f"CREATE EXTERNAL SCHEMA IF NOT EXISTS filecentral_ext_schema FROM DATA CATALOG DATABASE '{database}' IAM_ROLE '{redshift_role}' CREATE EXTERNAL DATABASE IF NOT EXISTS;"
    redshift_connection(create_schema_query)
    print("external schema created")
    create_table_query = f"create external table filecentral_ext_schema.{table_name}(" + schema + ")" + f" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{target_location}' TABLE PROPERTIES ('skip.header.line.count'='1');"

    table_exists = redshift_connection("SELECT EXISTS(SELECT * FROM svv_external_tables WHERE tablename = %s);",
                                       (table_name,))
    print(table_exists)
    # if table_exists[0][0] == True:
    if table_exists is not None:
        print(f"table '{table_name}' already existing")
    else:
        redshift_connection(create_table_query)
        print(f"The '{table_name}' table is created")
    # select_query = f'select * from ext_schema.{table_name} limit 10'
    # redshift_connection(select_query)

def sourceConfigDetails(body):
    conn, cursor = None, None
    try:
        if body is not None and "business_process_id" in body:
            conn, cursor = getConnection()
            business_process_id = body["business_process_id"]
            get_business_process_details = """SELECT j.file_name,j.job_type,j.job_name,sfc.standard_validations,sfc.file_config_details,sfc.email_notification,
                                          sfc.file_format,sfc.jira_incident_to_be_raised_for_failures,j.id, 
                                          j.schedule_json as job_schedule, j.delimiter as delimiter_value, bp.no_of_files,bp."name" as business_name,
                                          tc."name" as target_name,tc.host as target_host,tc.user_name as target_user_name,tc.secret_name,
                                          tc.connectivity_type as target_connectivity_type,tc.location_pattern as target_location_pattern,
                                          sc."name" as source_name,tc.host as source_host,tc.user_name as source_user_name,
                                          sc.connectivity_type as source_connectivity_type,sc.location_pattern as source_location_pattern,
                                          concat(j.job_name, '_', j.id) as redshift_table_name
                                          FROM jobs AS j
                                          JOIN business_process as bp on bp.id = j.business_process_id
                                          JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                          left join target_config as tc on sfc.target_id = tc.id
                                          left join source_config as sc on sc.id = sfc.source_id
                                          WHERE bp.uuid =  %s and j.status != 'Deleted'
                                      """
            cursor.execute(get_business_process_details, (business_process_id,))
            get_business_process_details_data = cursor.fetchall()
            cols = list(map(lambda x: x[0], cursor.description))
            df = pd.DataFrame(get_business_process_details_data, columns=cols)
            records = df.to_dict(orient="records")
            return response_body(200, "Retrieved Successfully", records)

        else:
            return response_body(400, "provide business_process_id", "")
    except psycopg2.Error as error:
        print("Error occurred:", error)
        return response_body(400, str(error), None)
    except Exception as error:
        print(error)
        return response_body(500, str(error), None)
    finally:
        cursor.close()
        pool.putconn(conn)


def response_body(statuscode, message, body):
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


def snsnotification(sns_topic, data):
    try:
        sns = boto3.client('sns')
        payload = {
            'message': 'Sending payload to SNS from final Save Job ',
            'data': data
        }
        response = sns.publish(
            TopicArn=sns_topic,
            Message=json.dumps(payload)
        )
        print(f"Message published successfully with message ID: {response['MessageId']}")
    except ClientError as e:
        # Handle SNS client errors
        if e.response['Error']['Code'] == 'InvalidParameter':
            print(f"Invalid parameter error: {e}")
        elif e.response['Error']['Code'] == 'AuthorizationError':
            print(f"Authorization error: {e}")
        else:
            print(f"An unexpected error occurred: {e}")
    except Exception as e:
        # Handle unexpected errors
        print(f"An unexpected error occurred: {e}")


def creating_S3_trigger(job_id, location_pattern):
    s3_client = boto3.client('s3')
    print("locationnn", location_pattern)
    s3_bucket_path =location_pattern.split('$')
    s3_bucket_pattern = s3_bucket_path[0].replace("s3://", '').replace("S3://", '')
    s3_bucket_name_1 = s3_bucket_pattern.split('/')
    s3_bucket_name = s3_bucket_name_1[0]
    s3_prefix = s3_bucket_path[0].replace(f'S3://{s3_bucket_name}/', '').replace(f's3://{s3_bucket_name}/', '')
    s3_prefix=s3_prefix.split('/')[0]
    s3_prefix_final = s3_prefix + "/"
    print("prefixxx", s3_prefix_final)
    lambda_function_arn = f'arn:aws:lambda:us-east-2:{Account}:function:fileops-ValidationProcess-{env}'

    lambda_function_configurations = []

    s3_event_lambda_configuration = {
        'Id': job_id,
        'LambdaFunctionArn': lambda_function_arn,
        'Events': ['s3:ObjectCreated:Put'],
        'Filter': {
            'Key': {
                'FilterRules': [
                    {
                        'Name': 'prefix',
                        'Value': s3_prefix_final
                    }
                ]
            }
        }
    }
    lambda_function_configurations.append(s3_event_lambda_configuration)

    s3_events_configuration = s3_client.get_bucket_notification_configuration(Bucket=s3_bucket_name)

    if isinstance(s3_events_configuration, dict) and s3_events_configuration.get(
            'LambdaFunctionConfigurations') is not None:
        lambda_function_configurations += s3_events_configuration['LambdaFunctionConfigurations']

    response = s3_client.put_bucket_notification_configuration(
        Bucket=s3_bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': lambda_function_configurations
        }
    )
    print("response", response)
