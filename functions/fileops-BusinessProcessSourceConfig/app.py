import json
import psycopg2
from dbconnection import pool, getConnection
import pandas as pd


def lambda_handler(event, context):
    conn, cursor = None, None
    print("Get Business Process Job Details request received\n")
    print("Received Event: \n", str(event))
    try:
        conn, cursor = getConnection()
        query_params = event["queryStringParameters"]
        if query_params is not None and "business_process_id" in query_params:
            business_process_id = query_params["business_process_id"]
            print(f"Business process id : {business_process_id}")
            get_business_process_details = """SELECT j.job_name,sfc.standard_validations,sfc.file_config_details,
                                            sfc.email_notification,
                                            sfc.file_format,sfc.jira_incident_to_be_raised_for_failures,j.id, 
                                            j.schedule_json as job_schedule,bp.no_of_files,bp."name" as business_name,
                                            tc."name" as target_name,tc.host as target_host,
                                            tc.user_name as target_user_name,
                                            tc.connectivity_type as target_connectivity_type,
                                            tc.location_pattern as target_location_pattern,
                                            sc."name" as source_name,tc.host as source_host,
                                            tc.user_name as source_user_name,
                                            tc.connectivity_type as source_connectivity_type,tc.location_pattern as 
                                            source_location_pattern
                                            FROM jobs AS j
                                            JOIN business_process as bp on bp.id = j.business_process_id
                                            JOIN source_file_config AS sfc ON j.id = sfc.job_id
                                            left join target_config as tc on sfc.target_id = tc.id
                                            left join source_config as sc on sc.id = sfc.source_id
                                            WHERE bp.uuid =  %s 
                                        """
            cursor.execute(get_business_process_details, (business_process_id,))
            get_business_process_details_data = cursor.fetchall()
            cols = list(map(lambda x: x[0], cursor.description))
            df = pd.DataFrame(get_business_process_details_data, columns=cols)
            records = df.to_dict(orient="records")
            return response(200, "Business process job details retrieved successfully", records)
        else:
            return response(400, "Invalid request payload : business_process_id is missing", [])
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return response(500, 'Retrieving Business process job details failed due to DB error', str(e))
    except Exception as e:
        print(f"Unknown error caught: {e}")
        return response(500, 'Retrieving Business process job details failed due to unknown exception', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def response(status_code, message, data):
    """
    This is a response method
    """
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
