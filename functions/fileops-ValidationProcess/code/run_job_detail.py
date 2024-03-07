import os
from functools import reduce

import boto3
import pandas as pd
import psycopg2
import pytz

from dbconnection import pool, getConnection

env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'
from datetime import datetime


def run_job_detail_fun(run_job_id, job_validation_status):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()

        job_query = """
           select j.uuid as job_uuid,tc.uuid as target_id, j.job_name,jr.uuid as job_run_id,bp."name" as business_process_name,
          jr.created_at,jr.status,jr.job_validation_status,jr.errors,jr.job_type,us.first_name,us.last_name, jr.error_records,sfc.dqrules,sfc.standard_validations
          ,j.file_name as sample_file_name ,j.file_size ,jr.start_time ,jr.end_time,jr.job_type, j.schedule_json,sc.connectivity_type as source_connectivity_type, tc.connectivity_type as target_connectivity_type ,bup.file_name as source_file_name,
          tc.absolute_file_path  as target_absolute_file_path,
          jr.source_file_location as source_absolute_file_path
          from  job_runs as jr
          join  jobs as j on j.id = jr.job_id
          join source_file_config as sfc on sfc.job_id = j.id
          left join source_config sc on sc.id = sfc.source_id 
          join business_user_validate_process as bup on bup.id = jr.business_user_validate_process_id
          join business_process as bp on bp.id  = j.business_process_id
          left join target_config tc on tc.id = sfc.target_id
          join users as us on us.id = bp.created_by
          where jr.uuid = %s
           """
        cursor.execute(job_query, (run_job_id[1],))
        data = cursor.fetchall()
        cols = list(map(lambda x: x[0], cursor.description))
        df = pd.DataFrame(data, columns=cols)
        records = df.to_dict(orient="records")
        print(run_job_id[1])
        if records:
            for record in records:
                record['created_at'] = str(record['created_at'])
                if record['error_records'] is not None and record['dqrules'] is not None:
                    errors_record_filter = [item for item in record['error_records'].values() if len(item) > 0]
                    if len(errors_record_filter) > 0:
                        error_records = reduce(lambda x, y: x + y, errors_record_filter)
                        for item in error_records:
                            if item["column_name"] in record['dqrules']:
                                item['dqrule'] = record['dqrules'][item['column_name']]
                del record['dqrules']
            records[0]["job_validation_status"] = job_validation_status.value
            record = records[0]
            if record is not None and record['target_id'] is not None:
                s3_key = record['target_absolute_file_path'].replace("S3://", '').replace("s3://", '')
                s3_file_target_location =  s3_key +"/"+ record['job_uuid'] + "/" + "data.csv"
                query = "UPDATE job_runs SET target_file_location = %s WHERE uuid = %s"
                values = (s3_file_target_location, run_job_id[1])
                cursor.execute(query, values)
            files_locations_query = "SELECT source_file_location,target_file_location from job_runs WHERE uuid=%s"
            cursor.execute(files_locations_query, (run_job_id[1],))
            result = cursor.fetchone()
            record['target_absolute_file_path']=result[1]
            conn.commit()
            # print(record['source_and_target'])
            if record is not None and record['source_absolute_file_path'] is not None:
                source_data = downloads3(result[0],record['job_uuid']
                                        )
                if source_data is not None:
                    record['source_pre_signed_url_information'] = source_data
            if record is not None and record['target_id'] is not None and job_validation_status.value == 'PASSED':
                # s3_filename, file_path,job_uuid,type
                target_data = downloads3(result[1],record['job_uuid'])
                if target_data is not None:
                    record['target_pre_signed_url_information'] = target_data
            else:
                record['target_pre_signed_url_information'] = None
        return records
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return str(e)
    except Exception as error:
        return str(error)
    finally:
        cursor.close()
        pool.putconn(conn)


def downloads3( s3_key,job_uuid):
    """This method is for generating presigned URL for S3 file"""
    try:
        s3_client = boto3.client('s3')
        # Generate the presigned URL
        pre_signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_key},
            ExpiresIn=600  # URL expiration time in seconds (adjust as needed)
        )
        data = {
            "presignedURL": pre_signed_url,
            "job_id": job_uuid
        }
        return data

    except Exception as error:
        print("check", error)
        return (500, str(error), None)


def pst_time(utc_time):
    # Set the UTC timezone
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(utc_time)

    # Convert to PST timezone
    pst_timezone = pytz.timezone('America/Los_Angeles')  # 'America/Los_Angeles' is the timezone ID for PST
    pst_time = utc_time.astimezone(pst_timezone)
    return pst_time.strftime('%Y-%m-%d %H:%M:%S')
