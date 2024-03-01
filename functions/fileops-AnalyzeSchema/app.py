"""This module is for analysing the schema"""
import copy
import json
from dbconnection import pool, getConnection
import psycopg2


def lambda_handler(event, context):
    """This method is for handling the analyse schema functionality"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/job/analyze-schema" and event['httpMethod'] == 'POST':
            if 'body' in event:
                body = json.loads(event['body'])
                # body = event['body']
                return UpdateSchema(body)
            else:
                return response(400, 'Missing or invalid payload', None)
        elif event['resource'] == "/job/analyze-schema" and event['httpMethod'] == 'GET':
            job_id = event["queryStringParameters"]["jobId"]
            return EditSchema(job_id)
        else:
            return response(404, 'resource or endpoint not found', None)
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
        return response(500, 'Analyze Schema API has failed due to an Unexpected Error', str(e))


def response(stautsCode, message, data):
    """This is response handling method"""
    return {
        "statusCode": stautsCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data})
    }


def preparing_dq_rules(schema_data):
    """
    This method is for preparing the dq_rules post data
    """
    try:
        dq_rules = {}
        for item in schema_data:
            column_name = item.get('column', "")
            status = item.get('dqCheck', None)
            if status:
                if len(item['range_list_value']) > 0 and 'value' in item['range_list_value'][0]:
                    dq_rules[column_name] = {
                        'list_of_value_check': [item['value'] for item in item['range_list_value'] if item['selected']]
                    }

                elif len(item['range_list_value']) > 0 and 'lowerBound' in item['range_list_value'][0]:
                    dq_rules[column_name] = {
                        'range_check': [item['range_list_value'][0]['lowerBound'],
                                        item['range_list_value'][0]['upperBound']]
                    }
        print("preparing_dq_rules:: dq_rules ", dq_rules)
        return dq_rules
    except Exception as error:
        print("preparing_dq_rules::Error during the preparing the dq rules data ", str(error))
        raise error


def UpdateSchema(payload):
    """This is updateSchema method"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        schema = payload['schema']
        schema_copy = copy.deepcopy(schema)
        job_id = payload.get('job_id', None)
        # Calling preparing_dq_rules method data to save to database.
        dq_rules = preparing_dq_rules(schema_copy)
        if job_id is not None:
            query = """
                        UPDATE source_file_config  
                        SET metadata = %s,
                            dqrules = %s
                        FROM jobs as j
                        WHERE j.id  = job_id AND j.uuid = %s
                    """
            data = [(json.dumps(schema)), json.dumps(dq_rules), job_id]
            cursor.execute(query, data)
            conn.commit()
            return response(200, "file schema updated successfully", None)
        else:
            return response(400, "Provide valid job_id", None)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'updating file schema failed', str(e))
    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'updating file schema failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def EditSchema(job_id):
    """This is editSchema method"""
    conn, cursor = None, None
    data = {}
    try:
        conn, cursor = getConnection()
        query_job_id = "select id from jobs where uuid = %s"
        cursor.execute(query_job_id, (job_id,))
        job_details = cursor.fetchone()

        if job_details and len(job_details) > 0:
            query = "select job_id, metadata, target_metadata, uuid, target_file_config_details from source_file_config where job_id = %s"
            cursor.execute(query, (job_details[0],))
            rows = cursor.fetchone()
            if rows is not None:
                data['sourceSchema'] = rows[1]
                data['targetSchema'] = rows[2]

                target_file_config_details = json.loads(rows[4]) if rows[4] is not None else {}

                target_file_partitions = target_file_config_details.get('parquet_partitions_schema', None)

                if target_file_partitions is not None:
                    for partition in target_file_partitions:
                        column = partition.get('column', None)

                        if column is not None:
                            target_column_metadata = next((index, partition.get('order', None)) for
                                                          index, target_column_schema in enumerate(data['targetSchema'])
                                                          if
                                                          target_column_schema.get('source_column') == column)

                            target_column_metadata_index, target_column_parquet_partition_order = target_column_metadata

                            data['targetSchema'][target_column_metadata_index][
                                'parquet_partition_order'] = target_column_parquet_partition_order

                            # Update target_metadata with parquet_partition_order fields
                            update_target_metadata_query = """UPDATE source_file_config SET target_metadata = %s WHERE job_id = %s"""

                            target_metadata_json = json.dumps(data['targetSchema'])

                            cursor.execute(update_target_metadata_query, (target_metadata_json, job_details[0]))
                            conn.commit()

                return response(200, 'File Schema retrieved successfully ', data)
            else:
                return response(200, f'file schema not found for job_id {job_id}', None)
        else:
            return response(200, f'unable to find job details with job_id {job_id}', data)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'file schema fetch failed', str(e))
    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'file schema fetch failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)
