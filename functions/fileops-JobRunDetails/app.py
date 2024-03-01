from dbconnection import pool, getConnection
import pandas as pd
import json
from functools import reduce
import boto3
import os
from datetime import datetime

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    conn, cursor = None, None
    print("Received Event: \n", str(event))
    try:
        conn, cursor = getConnection()
        if event['resource'] == "/job-runs" and event['httpMethod'] == 'GET':
            query_params = event.get("queryStringParameters", None)
            records_per_page = query_params['recordsperpage']
            offset = query_params['offset']
            pagination_query = f"offset {offset} ROWS FETCH next {records_per_page} ROWS ONLY"

            if query_params is not None and "group_id" in query_params and query_params.get(
                    "group_id") != "" and query_params.get("group_id") != "undefined" and query_params.get("role_id",
                                                                                                           None) != "0":
                group_id = query_params['group_id']
            elif query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
                job_query = f"""
                  select  j.file_name,j.job_name,jr.uuid as job_run_id,bp."name" as business_process_name ,
                  bup.file_name,sc2.location_pattern as source_file_location_pattern, jr.created_at,jr.status,
                  jr.job_type,us.first_name,us.last_name, count(*) OVER() AS full_count
                  from  job_runs as jr
                  join  jobs as j on j.id = jr.job_id
                  join source_file_config as sc on sc.job_id = j.id
                  join business_user_validate_process as bup on bup.id = jr.business_user_validate_process_id
                  join business_process as bp on bp.id  = j.business_process_id
                  join users as us on us.id = bp.created_by
                  left join source_config sc2 on sc.source_id = sc2.id
                  where bp.status = 'Active'
                  ORDER BY jr.id DESC {pagination_query}
                  """
                cursor.execute(job_query)
                data = cursor.fetchall()
                cols = list(map(lambda x: x[0], cursor.description))
                df = pd.DataFrame(data, columns=cols)
                records = df.to_dict(orient="records")
                for item in records:
                    item['created_at'] = str(item['created_at'])
                return response_body(200, "Job run records are retrieved successfully", records)
            else:
                return response_body(400, "group_id is missing in the request", None)

            group_associated_bp_list = get_group_associated_business_process(group_id)
            if group_associated_bp_list:
                job_query = f"""
                select  j.file_name,j.job_name,jr.uuid as job_run_id,bp."name" as business_process_name ,
                bup.file_name,sc2.location_pattern as source_file_location_pattern, jr.created_at,jr.status,
                jr.job_type,us.first_name,us.last_name, count(*) OVER() AS full_count
                from  job_runs as jr
                join  jobs as j on j.id = jr.job_id
                join source_file_config as sc on sc.job_id = j.id
                join business_user_validate_process as bup on bup.id = jr.business_user_validate_process_id
                join business_process as bp on bp.id  = j.business_process_id
                join users as us on us.id = bp.created_by
                left join source_config sc2 on sc.source_id = sc2.id
                where bp.status = 'Active' AND bp.uuid IN %s
                ORDER BY jr.id DESC {pagination_query}
                """
                cursor.execute(job_query, (tuple(group_associated_bp_list),))
                data = cursor.fetchall()
                cols = list(map(lambda x: x[0], cursor.description))
                df = pd.DataFrame(data, columns=cols)
                records = df.to_dict(orient="records")
                for item in records:
                    item['created_at'] = str(item['created_at'])
                return response_body(200, "Job run records are retrieved successfully", records)
            else:
                print("No business process was found under the user assigned group", group_id)
                return response_body(404, "No business process was found under the user assigned group", None)
        else:
            query_params = event['queryStringParameters']
            if query_params is not None and "job_run_id" in query_params:
                job_run_id = query_params['job_run_id']
                print("Retrieving the job run details : ", job_run_id)
                job_query = """
                    select 
                    jr.error_records,
                    sfc.dqrules,
                    jr.errors,
                    j.uuid, 
                    jr.status, 
                    jr.job_validation_status, 
                    tc.uuid as target_id,   
                    sfc.standard_validations, 
                    j.file_name, 
                    j.file_size, 
                    j.job_name ,
                    jr.start_time ,
                    jr.end_time,
                    jr.job_type,     
                    j.schedule_json,
                    tc.absolute_file_path,
                    tc.connectivity_type,
                    buvp.file_name as source_file_name,
                    sc.absolute_file_path as source_absolute_file_path,
                    tc.absolute_file_path as target_absolute_file_path
                    from job_runs jr join source_file_config sfc on jr.job_id = sfc.job_id
                    join jobs j on j.id = jr.job_id 
                    left join target_config tc on tc.id = sfc.target_id 
                    left join business_user_validate_process buvp on buvp.job_id =jr.job_id
                    left join source_config sc on sc.id = sfc.source_id  
                    where jr.uuid = %s
                      """
                cursor.execute(job_query, (job_run_id,))
                data = cursor.fetchall()
                print("job run information : ", data)
                cols = list(map(lambda x: x[0], cursor.description))
                df = pd.DataFrame(data, columns=cols)
                records = df.to_dict(orient="records")
                print("job run errors / dq-checks : ", records)
                for record in records:
                    if record['error_records'] is not None and record['dqrules'] is not None:
                        errors_record_filter = [item for item in record['error_records'].values() if len(item) > 0]
                        if len(errors_record_filter) > 0:
                            error_records = reduce(lambda x, y: x + y, errors_record_filter)
                            for item in error_records:
                                if item["column_name"] in record['dqrules']:
                                    item['dqrule'] = record['dqrules'][item['column_name']]
                    del record['dqrules']

                    if record['start_time'] is not None and record['end_time'] is not None:
                        start_time = datetime.strptime(record['start_time'], "%Y-%m-%d %H:%M:%S.%f")
                        end_time = datetime.strptime(record['end_time'], "%Y-%m-%d %H:%M:%S.%f")
                        duration = end_time - start_time
                        record['Run_Duration'] = str(duration)
                    else:
                        record['Run_Duration'] = None
                    if len(records) > 0:
                        record = records[0]
                        # print(record['source_and_target'])
                        if record is not None and record['source_absolute_file_path'] is not None:
                            source_data = downloads3(record['source_file_name'],record['source_absolute_file_path'] ,record['uuid'], 'source')
                            if data is not None:
                                record['source_pre_signed_url_information'] = source_data
                        if record is not None and record['target_id'] is not None:
                            target_data = downloads3("data.csv", record['target_absolute_file_path'] ,record['uuid'],'target')
                            if data is not None:
                                record['target_pre_signed_url_information'] = target_data

                return response_body(200, "Job run record is retrieved successfully", records)
    except Exception as error:
        return response_body(500, "Job runs / Job run detail couldn't be fetched", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_group_associated_business_process(group_id):
    conn, cursor = None, None
    try:
        print("Business process fetched for group : ", group_id)
        conn, cursor = getConnection()
        get_group_details = "SELECT group_name, business_process_list FROM groups WHERE uuid = %s"
        cursor.execute(get_group_details, (group_id,))
        group_data = cursor.fetchone()
        if group_data is None:
            print("There is no business process in this group : ", group_id)
            return []
        elif group_data[1] is not None:
            business_process_list = group_data[1]
            bp_ids_list = list(business_process_list.keys())
            return bp_ids_list
    except Exception as error:
        print("Error occurred while fetching the associated business processes for the group : ", group_id)
        raise error
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


def downloads3(s3_filename, file_path,job_uuid,type):
    """This method is for uploding the validating file to S3"""
    if type=='target':
        print("locate target file for the job_id : ", job_uuid)
        s3_key = file_path.replace("S3://","").replace("s3://",'')
        s3_file = "target-files/" + s3_key +"/"+job_uuid + "/" + s3_filename
    if type=='source':
        print("locate source file for the job_id : ", job_uuid)
        s3_key = file_path.replace("S3://","").replace("s3://",'')
        s3_file = "patterns/" +s3_key+"/" + s3_filename
    try:
        s3_client = boto3.client('s3')
        print("generating pre-signed url from s3 file : ", s3_bucket + s3_file)
        # Generate the pre-signed URL
        pre_signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_file},
            ExpiresIn=600  # URL expiration time in seconds (adjust as needed)
        )
        print("pre_signed_url : " + pre_signed_url)
        data = {
            "presignedURL": pre_signed_url,
            "job_id": job_uuid
        }
        return data
    except Exception as error:
        print("check", error)
        return response_body(500, 'generating Pre Signed URL failed', str(error))
