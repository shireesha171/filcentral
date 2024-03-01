import boto3
from dbconnection import pool, getConnection
import psycopg2
import json


# Get log streams
def lambda_handler(event, context):
    print("Received Event: ", str(event))
    if 'queryStringParameters' in event and 'job_run_id' in event['queryStringParameters']:
        query_string_params = event['queryStringParameters']
        job_run_id = query_string_params['job_run_id']
        print(f"Getting logs for job run id : {job_run_id}")
        return get_logs(job_run_id)
    else:
        return storing_logs(event)


def storing_logs(event):
    # Initialize AWS SDK
    client = boto3.client('logs')
    conn, cursor = None, None
    log_group_name = '/aws/lambda/' + event['function']
    job_run_id = event['job_run_id']
    log_stream_name = event['log_stream_name']
    print(f"log_group : {log_group_name}, log_stream_name : {log_stream_name}, job_run_id : {job_run_id}")

    # Get log events
    log_response = client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name
    )
    print(f"log events information {str(log_response['events'])}")
    try:
        conn, cursor = getConnection()
        update_query = """
            UPDATE job_runs
            SET logs = %s
            WHERE uuid = %s
        """
        values = (json.dumps(log_response['events']), job_run_id)
        cursor.execute(update_query, values)
        conn.commit()
        return response_body(200, f"Logs are saved successfully for {job_run_id}:", True)
    except psycopg2.Error as e:
        print(f"DB error occurred while saving logs for the job run id {job_run_id}:", str(e))
        return response_body(500,
                             f"DB error occurred while saving logs for the job run id {job_run_id}:", str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_logs(job_run_id):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_query = """
        select j.logs from job_runs as j where uuid = %s
         """
        values = (job_run_id,)
        cursor.execute(job_query, values)
        data = cursor.fetchone()
        return response_body(200, f'''Logs are retrieved successfully for
                                 job run id : {job_run_id}''', data)
    except psycopg2.Error as e:
        print(f"DB error occurred while retrieving logs for the job run id {job_run_id}:", str(e))
        return response_body(500,
                             f'''DB error occurred while retrieving logs for 
                             the job run id {job_run_id}:''', str(e))
    except Exception as error:
        return response_body(400,
                             f'''Request failed due to processing error while retrieving 
                             logs for job run id : {job_run_id}''', str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def response_body(status_code, message, body):
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
