import json
import os

import psycopg2

from dbconnection import pool, getConnection

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'


def lambda_handler(event, context):
    print("Received Event: ", str(event))
    try:

        if event['resource'] == "/job/transformation" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                target_data = body['target_data']
                job_id = body['job_id']

                update_metadata_record(target_data, job_id)

                return response(200, "The transformation data has been saved successfully", None)
            else:
                return response(400, "Missing or invalid payload", None)

        elif event['resource'] == "/job/transformation" and event['httpMethod'] == 'GET':
            query_params = event.get('queryStringParameters', None)
            job_id = query_params.get('job_id', None)
            if query_params is not None and job_id is not None and job_id != '':
                return get_transformations(job_id)
            else:
                return response(400, f"Invalid query parameters{job_id}", None)

        else:
            return response(404, 'resource or endpoint not found', None)
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
        return response(500, 'Transformation API has failed due to an Unexpected Error', str(e))


def update_metadata_record(target_metadata, job_id):
    """This method is for updating Metadata Record"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        sql = """
                  UPDATE source_file_config  SET target_metadata = %s from jobs as j
                  WHERE j.id  = job_id and j.uuid = %s
              """
        data = [
            (json.dumps(target_metadata), job_id)
        ]
        cursor.executemany(sql, data)
        conn.commit()
        return None
    except psycopg2.DatabaseError as e:
        print(f"updateMetadataRecord::Database error: {e}")
        return response(500, 'Saving the transformation data has failed', str(e))
    except Exception as e:
        print(f"updateMetadataRecord::Unknown error caught: {e}")
        return response(500, 'Saving the transformation data has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_transformations(job_id):
    """This method is for getting the transformations applied on columns"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        transformation_data_query = """
                                    SELECT sfc.target_metadata, sfc.target_file_config_details
                                    FROM source_file_config as sfc
                                    JOIN jobs as j ON sfc.job_id = j.id
                                    where j.uuid  = %s;
                                   """
        cursor.execute(transformation_data_query, (job_id,))
        data = cursor.fetchone()

        if data is None:
            return response(404, "No data is found for the given job_id", None)
        else:

            target_file_config_details = json.loads(data[1]) if data[1] is not None else {}

            target_file_partitions = target_file_config_details.get('parquet_partitions_schema', None)

            if target_file_partitions is not None:
                for partition in target_file_partitions:
                    column = partition.get('column', None)

                    if column is not None:
                        target_column_metadata = next((index, partition.get('order', None)) for
                                                      index, target_column_schema in enumerate(data[0]) if
                                                      target_column_schema.get('source_column') == column)

                        target_column_metadata_index, target_column_parquet_partition_order = target_column_metadata

                        data[0][target_column_metadata_index][
                            'parquet_partition_order'] = target_column_parquet_partition_order

            return response(200, "The transformation data is ready", data[0])

    except psycopg2.DatabaseError as e:
        print(f"get_transformations::Database error: {e}")
        return response(500, 'Fetching the transformation data has failed', str(e))
    except Exception as e:
        print(f"get_transformations::Unknown error caught: {e}")
        return response(500, 'Fetching the transformation data has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def response(status_code, message, data):
    """This is response handling method"""
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
