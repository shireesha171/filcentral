from dbconnection import pool, getConnection
import json
import psycopg2


def response(status_code, message, data):
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


def lambda_handler(event, context):
    print("Received Event: ", str(event))
    if event['resource'] == "/job/dq-rules" and event['httpMethod'] == 'POST':
        return post_dataquality_rules(event)
    elif event['resource'] == "/job/dq-rules" and event['httpMethod'] == 'GET':
        return get_dataquality_rules(event)
    else:
        return response(404, 'resource or endpoint not found', None)


# This method is get the DQRules from Source_file_config table @ 'DQRules' column
def get_dataquality_rules(event):
    print("Received event : \n", event)
    conn, cursor = None, None
    query_params = event.get('queryStringParameters', None)
    if query_params is not None and "job_id" in query_params and query_params.get('job_id') != "":
        query_params = event['queryStringParameters']
        job_uuid = query_params['job_id']
        print("job_id : ", job_uuid)
    else:
        return response(400, "job_id or job_id value is not found", None)
    try:
        conn, cursor = getConnection()
        select_query = """
                        SELECT sf.DQRules
                        FROM source_file_config sf
                        JOIN jobs j ON sf.job_id = j.id
                        WHERE j.uuid = %s
                       """
        cursor.execute(select_query, (job_uuid,))
        dq_rules_list = cursor.fetchone()
        if len(dq_rules_list) > 0 and isinstance(dq_rules_list[0], dict):
            dq_rules = dq_rules_list[0]
            dq_rules_response = []
            for key in dq_rules.keys():
                value = dq_rules[key]
                value['column_name'] = key
                dq_rules_response.append(value)
            return response(200, 'DQRules retrieved successfully for this job', dq_rules_response)
        else:
            return response(200, f'No DQRules setup for this Job', [])
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'DQ Rules fetch failed', str(e))
    except Exception as e:
        print(f"Unknown error caught : {e}")
        return response(500, 'DQ Rules fetch failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


# This method is to post the DQRules in Source_file_config table @ 'DQRules' column
def post_dataquality_rules(event):
    print("Received event : \n", event)
    conn, cursor = None, None
    body = json.loads(event['body'])
    dq_rules = body['dq_rules']
    column_name = body['column_name']
    job_uuid = body['job_id']
    dq_r_fdb = get_dq_rules(job_uuid)
    dq_rules_json = {}
    if dq_r_fdb is not None and dq_r_fdb[0] is not None:
        dq_rules_json = dq_r_fdb[0]
    dq_rules_json[column_name] = dq_rules
    dq_rules_json = json.dumps(dq_rules_json)
    try:
        conn, cursor = getConnection()
        query = "SELECT id FROM jobs WHERE uuid = %s"
        cursor.execute(query, (job_uuid,))
        job_integer_id = cursor.fetchone()[0]
        update_query = f"""
                UPDATE source_file_config
                SET dqrules = %s
                WHERE job_id = %s
                """
        values = (dq_rules_json, job_integer_id)
        cursor.execute(update_query, values)
        conn.commit()
        return response(200, 'DQ Rules updated successfully.', None)
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'DQ Rules update failed', str(e))
    except Exception as e:
        print(f"Unknown error caught : {e}")
        return response(500, 'DQ Rules update failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def get_dq_rules(job_uuid):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        select_query = """
                        SELECT sf.dqrules
                        FROM source_file_config sf
                        JOIN jobs j ON sf.job_id = j.id
                        WHERE j.uuid = %s
                       """
        cursor.execute(select_query, (job_uuid,))
        dq_rules_list = cursor.fetchone()
        return dq_rules_list
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'DQ Rules fetch failed', str(e))
    except Exception as e:
        print(f"Unknown error caught : {e}")
        return response(500, 'DQ Rules fetch failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)
