"""This module is for creating business processes, jobs and uploading file to S3"""
import json
import uuid
import boto3
from dbconnection import pool, getConnection
import psycopg2
import os
from botocore import client
env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    """This handler is for creating and editing businesses process and jobs"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                return create_business_process(body)
            else:
                return response(400, 'missing or invalid payload', None)

        elif event['resource'] == "/job" and event['httpMethod'] == 'PUT':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return edit_email_and_jira_list(body)
            else:
                return response(400, 'missing or invalid payload', None)

        elif event['resource'] == "/job/file_upload" and event['httpMethod'] == 'POST':
            body = json.loads(event['body'])
            # body = event['body']
            if body is not None:
                job_type = "job"
                s3_file = "sample-files/" + body['job_id'] + "/" + body['s3_filename']
                return get_pre_signed_url(s3_file, job_type, body)
            else:
                return response(400, 'missing or invalid payload', None)

        # elif event['resource'] == "/jira/upload" and event['httpMethod'] == 'GET':
        #     filepath = prepare_s3_key(event)
        #     job_type = "jira"
        #     return get_pre_signed_url(filepath, job_type, 'null')

        elif event['resource'] == "/job" and event['httpMethod'] == 'DELETE':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return delete_job(body)
            else:
                return response(400, 'missing or invalid payload', None)

        else:
            return response(404, 'resource or endpoint not found', None)
    except Exception as e:
        print(f"Unknown error caught in file upload lambda handler : {e}")
        return response(500, 'Unknown error caught in file upload lambda handler', str(e))


def create_business_process(body):
    """This method is for creating the business process and jobs"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        user_uuid = body["user_id"]
        business_process_id = body.get('business_process_id', None)
        # Calling getUserDetails method
        user_intid = get_user_details(user_uuid)
        business_status = "Draft"
        if user_intid is None:
            return response(400, f"Invalid user id{user_uuid}", None)

        else:
            if business_process_id is None:
                if "group_id" in body and body.get("group_id") != "" and body.get("group_id") != "undefined":
                    group_id = body['group_id']
                else:
                    print("group_id is missing in payload")
                    return response(400, "group_id is missing in payload", None)
                # Confirming whether the Business Process name is unique
                business_process_name = business_process_name_unique(body)
                if business_process_name != "unique":
                    return response(409,
                                    f"""The business process already exists with the given name, 
                                    please provide a unique name""",
                                    None)
                else:
                    insert_to_business_process = """
                                                  INSERT INTO business_process(uuid, name, created_by, status, 
                                                  email_notification, no_of_files, jira_service_management)
                                                  VALUES(%s, %s, %s, %s, %s, %s, %s)
                                                  RETURNING *
                                                 """
                    values = (
                        str(uuid.uuid4()),
                        body["business_process_name"],
                        user_intid,
                        business_status,
                        body.get('email_notification', []),
                        body.get("no_of_files"),
                        json.dumps(body.get('jira_service_management', {}))
                    )
                    cursor.execute(insert_to_business_process, values)
                    conn.commit()
                    inserted_row = cursor.fetchone()
                    data = {
                        'user_id': user_intid,
                        'business_process_int_id': inserted_row[0],
                        "business_process_id": inserted_row[1]
                    }
                    # adding the new business process to the user group
                    bp_uuid = inserted_row[1]
                    new_business_process = json.dumps(
                        {bp_uuid: body["business_process_name"]})
                    update_group_query = """UPDATE groups SET business_process_list = business_process_list || 
                    %s WHERE uuid = %s"""
                    cursor.execute(update_group_query,
                                   (new_business_process, group_id))
                    conn.commit()
                    # Calling createJob method
                    return create_job(body, data)
            else:
                # This block is for creating multiple jobs
                business_process_int_id = business_details(business_process_id)
                data = {
                    "business_process_int_id": business_process_int_id,
                    "user_id": user_intid,
                    "business_process_id": business_process_id
                }
                return create_job(body, data)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while creating businessProcess/job failed : {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def create_job(body, data):
    job_name = body["job_name"]
    user_id = data['user_id']
    business_process_id = data['business_process_int_id']
    """This method is for creating the jobs"""
    if is_job_duplicate(job_name, business_process_id):
        return response(409,
                        f"""This job name already exist under the business process, 
                                    please provide a unique name""",
                        None)
    conn, cursor = None, None
    status = "Draft"
    try:
        conn, cursor = getConnection()
        insert_into_jobs = """
                                INSERT INTO jobs(uuid, job_name, created_by, status, business_process_id)
                                VALUES(%s, %s, %s, %s, %s)
                                RETURNING *
                               """
        values = (
            str(uuid.uuid4()),
            job_name,
            user_id,
            status,
            business_process_id
        )
        cursor.execute(insert_into_jobs, values)
        conn.commit()
        job_data = cursor.fetchone()
        response_data = {
            "business_process_id": data['business_process_id'],
            "job_id": job_data[1]
        }

        # Updating the no_of_files column of the business process table
        update_query = """
                        UPDATE business_process
                        SET status= %s ,no_of_files = %s
                        WHERE id = %s
                        """
        update_values = (status, body.get('no_of_files', ''), data['business_process_int_id'])
        cursor.execute(update_query, update_values)
        conn.commit()
        return response(200, "Job created successfully", response_data)
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while creating businessProcess/job failed: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def edit_email_and_jira_list(body):
    """ This method is for updating emails list and jira status """
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        bp_uuid = body.get('business_process_id', '')
        if bp_uuid != '':
            update_business_process_table = f"""
                                            UPDATE business_process 
                                            SET email_notification = %s,
                                                jira_service_management = %s
                                            WHERE uuid = '{bp_uuid}'
                                            """
            values = (body.get('email_notification', ''), json.dumps(body.get('jira_service_management', {})))
            cursor.execute(update_business_process_table, values)
            conn.commit()
            return response(200, "update email and jira list success", None)
        else:
            return response(400, "please provide valid uuid", None)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'update email and jira list failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while update email : {e}")
        return response(500, 'update email and jira list failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_pre_signed_url(s3_file, entity_type, body):
    """This method is for generating Pre-SignedURL"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        s3_client = boto3.client('s3', config=client.Config(signature_version='s3v4'))
        # Generating the pre-signed URL
        pre_signed_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': s3_bucket, 'Key': s3_file},
            ExpiresIn=600
        )
        # Here calling a method to update the top_ten_rows column
        update_sample_preview_data(body['job_id'])

        if entity_type == "job":
            # Updating the file_name and size in jobs table
            update_query = """
                           UPDATE jobs
                           SET file_name = %s,
                               file_size = %s
                           WHERE uuid = %s
                        """
            update_values = (body.get('s3_filename', ''), body.get('file_size', ''), body['job_id'])
            cursor.execute(update_query, update_values)
            conn.commit()

            data = {
                "presignedURL": pre_signed_url,
                "job_id": body['job_id'],
                "business_process_id": body['business_process_id']
            }
        else:
            data = {
                "presignedURL": pre_signed_url,
                "filepath": s3_file
            }
        return response(200, "The pre-signed-url is ready", data)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'file upload failed', str(e))
    except Exception as e:
        print(f"Unknown error caught file upload failed : {e}")
        return response(500, 'file upload failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_job_details(job_uuid):
    """This method is for getting the job details"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        query = f"SELECT * FROM jobs WHERE uuid = '{job_uuid}'"
        cursor.execute(query)
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'file upload failed', str(e))
    except Exception as e:
        print(f"Unknown error caught file upload failed: {e}")
        return response(500, 'file upload failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def update_sample_preview_data(job_uuid):
    """This method is for updating the top_ten_rows column in the source-file-config table to empty"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_int_id = get_job_details(job_uuid)

        update_query = f"""
                           UPDATE source_file_config
                           SET top_ten_rows = %s
                           WHERE job_id = '{job_int_id}'
                        """
        update_values = (None,)
        cursor.execute(update_query, update_values)
        conn.commit()

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'file upload failed', str(e))
    except Exception as e:
        print(f"Unknown error caught during file upload failed: {e}")
        return response(500, 'file upload failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def change_business_process_status(job_uuid):
    """
    This method is for changing the status of the business process.
    """
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        status = None
        query = """
                    SELECT j.status FROM jobs j 
                    WHERE business_process_id = (SELECT business_process_id FROM jobs WHERE uuid = %s) 
                    AND j.status != 'Deleted'
                """
        cursor.execute(query, (job_uuid,))
        list_of_tuples_job_statuses = cursor.fetchall()
        print("change_business_process_status::tuple_of_job_status ", list_of_tuples_job_statuses)
        for item in list_of_tuples_job_statuses:
            if 'Draft' in item:
                status = 'Draft'
                return status
        else:
            status = 'Active'
            return status
    except Exception as error:
        print("change_business_process_status::Error while changing the status of the business process ", str(error))
        raise error


def delete_job(body):
    """This method is for soft deleting the job"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_id = body['job_id']

        # the below statement sets the status of the job id as deleted
        query = "UPDATE jobs SET status = %s WHERE uuid = %s"
        values = ('Deleted', job_id)
        cursor.execute(query, values)
        conn.commit()
        status = change_business_process_status(job_id)
        # the below statement updates the no_of_files of the business_process table
        update_no_of_files_query = """UPDATE business_process 
                    SET no_of_files = no_of_files-1,
                        status = %s
                    WHERE id = (SELECT
                    business_process_id FROM jobs 
                    WHERE uuid = %s) 
                    and no_of_files > 0"""
        cursor.execute(update_no_of_files_query, (status, job_id))
        conn.commit()

        return response(200, "Job deleted successfully", None)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'deleting a job has failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while deleting a job has failed: {e}")
        return response(500, 'deleting a job has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


# def prepare_s3_key(event):
#     user_id = "afac5bdf-700c-49fe-bcab-18c1059960ec"
#     # or
#     # event['requestContext']['authorizer']['claims']['sub']
#     return f"pemfiles/{user_id}/privatekey.pem"


def business_process_name_unique(body):
    """This method is for checking the given business process name is unique or not"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        business_process_name = body["business_process_name"]
        business_process_name_query = "SELECT * FROM business_process WHERE status != 'Deleted' and name = %s"
        cursor.execute(business_process_name_query, (business_process_name,))
        data = cursor.fetchone()
        if data is None:
            result = "unique"
            return result
        else:
            result = "not unique"
            return result

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while creating businessProcess/job failed: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def business_details(business_process_id):
    """This method is for getting the business process details"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        get_business_process_details = (
            "SELECT id FROM business_process WHERE uuid = %s"
        )
        cursor.execute(get_business_process_details, (business_process_id,))
        data = cursor.fetchone()
        if data is not None:
            business_process_intid = data[0]
            return business_process_intid
        else:
            business_process_intid = None
            return business_process_intid

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'fetching business process details failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while fetching business process details failed: {e}")
        return response(500, 'fetching business process details failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_user_details(user_uuid):
    """This method is for getting user details"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        get_int_user_id = "SELECT id FROM users WHERE uuid = %s"
        cursor.execute(get_int_user_id, (user_uuid,))
        user_int_id = cursor.fetchone()
        if user_int_id is not None:
            return user_int_id[0]
        else:
            user_int_id = None
            return user_int_id
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    except Exception as e:
        print(f"Unknown error caught file upload failed while creating businessProcess/job failed: {e}")
        return response(500, 'creating businessProcess/job failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def is_job_duplicate(job_name, business_process_id):
    """
    @param job_name: name of the job created by the user
    @param business_process_id: business process id
    @return: True if there is already a job exist under the business process with same name, else False
    """
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        get_job_name_count = "select count(*) FROM jobs WHERE job_name = %s and business_process_id = %s and status != 'Deleted'"
        cursor.execute(get_job_name_count, (job_name, business_process_id))
        job_name_count = cursor.fetchone()
        if job_name_count is not None and job_name_count[0] > 0:
            return True
        else:
            return False
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'Duplicate check for job name failed', str(e))
    except Exception as e:
        print(f"Unknown error caught while duplicate check for job name: {e}")
        return response(500, 'Unknown error thrown while duplicate check for job name', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


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
        "body": json.dumps({"message": message, "data": data}),
    }

