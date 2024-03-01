"""
In this module we are trying to retrive lists of various funtionalities
"""
import json
from dbconnection import pool, getConnection
from datetime import datetime
import psycopg2

import pandas as pd


def lambda_handler(event, context):
    """ Getting lists"""
    print("Received Event: ", str(event))
    try:
        query_params = event.get("queryStringParameters", None)
        list_type = query_params.get("list_type", None)

        if query_params is not None and list_type is not None:
            if list_type == "business_process":
                return business_process_list(query_params)
            if list_type == "business_process_all":
                return business_process_list_all(query_params)
            elif list_type == "source_config":
                return source_config_list(query_params)
            elif list_type == "target_config":
                return target_config_list(query_params)
            elif list_type == "transformation":
                return transformation_list()
            elif list_type == "jobs":
                return jobs_list()
        else:
            return response(400, "please provide valid queryStringParameters", None)
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
        return response(500, 'Get List API has failed due to an Unexpected Error', str(e))


def business_process_list(query_params):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if query_params is not None and "group_id" in query_params and query_params.get("group_id") != "" and query_params.get("group_id") != "undefined" and query_params.get("role_id", None) != "0":
            group_id = query_params['group_id']
        elif query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
            # return response(400, "group_id is missing in request params", None)
            get_business_processes = "SELECT uuid, name FROM business_process WHERE status = %s order by id desc"
            cursor.execute(get_business_processes, ("Active",))
            business_process_data = cursor.fetchall()
            business_process_list = []
            for column in business_process_data:
                business_process_details = {}
                business_process_details = {
                    "name": column[1],
                    "business_process_id": column[0]
                }
                business_process_list.append(business_process_details)
            return response(200, "Business process list retrieved successfully", business_process_list)
        else:
            return response(400, "user id is missing in params", None)
        group_associated_bp_list = getGroupAssociatedBusinessProcess(group_id)
        if group_associated_bp_list:
            get_business_processes = "SELECT uuid, name FROM business_process WHERE status = %s AND uuid IN %s order by id desc"
            cursor.execute(get_business_processes, ("Active", tuple(group_associated_bp_list),))
            business_process_data = cursor.fetchall()
            business_process_list = []
            for column in business_process_data:
                business_process_details = {}
                business_process_details = {
                    "name": column[1],
                    "business_process_id": column[0]
                }
                business_process_list.append(business_process_details)
            return response(200, "Business process list retrieved successfully", business_process_list)
        else:
            print("No business processes are present in the group. Please contact File Central Administrator", group_id)
            return response(404, "No business processes are present in the group. Please contact File Central Administrator", None)
    
    except psycopg2.DatabaseError as e:
        print(f"business_process_list::Database error: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    except Exception as e:
        print(f"business_process_list::Unknown error caught: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)
    
def business_process_list_all(query_params):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
            get_business_processes = "SELECT uuid, name FROM business_process WHERE status != %s order by id desc"
            cursor.execute(get_business_processes, ("Deleted",))
            business_process_data = cursor.fetchall()
            business_process_list = []
            for column in business_process_data:
                business_process_details = {}
                business_process_details = {
                    "name": column[1],
                    "business_process_id": column[0]
                }
                business_process_list.append(business_process_details)
            return response(200, "Business process list retrieved successfully", business_process_list)
        else:
            return response(400, "role_id is missing in params", None)
    except psycopg2.DatabaseError as e:
        print(f"business_process_list_all::Database error: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    except Exception as e:
        print(f"business_process_list_all::Unknown error caught: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def getGroupAssociatedBusinessProcess(group_id):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        print(group_id)
        get_group_details = "SELECT group_name, business_process_list FROM groups WHERE uuid = %s"
        cursor.execute(get_group_details, (group_id,))
        group_data = cursor.fetchone()
        if group_data is None:
            print("No business process present in group", group_id)
            return []
        elif group_data[1] is not None:
            business_process_list = group_data[1]
            bp_ids_list = list(business_process_list.keys())
            return bp_ids_list
   
    except psycopg2.DatabaseError as e:
        print(f"getGroupAssociatedBusinessProcess::Database error: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    except Exception as e:
        print(f"getGroupAssociatedBusinessProcess::Unknown error caught: {e}")
        return response(500, 'Fetching Business Process list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def jobs_list():
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        jobs_query = "SELECT uuid, job_name, status FROM jobs where status like '%configured%'"
        cursor.execute(jobs_query)
        data = cursor.fetchall()

        fields = list(map(lambda x: x[0], cursor.description))
        jobs_data_df = pd.DataFrame(data, columns=fields)
        job_data = jobs_data_df.to_dict(orient="records")

        jobs_list = []

        for job in job_data:
            jobs = {
                "job_name": job['job_name'],
                "job_id": job['uuid'],
                "business_process_name": None,
                "job_description": None,
                "status": job['status'],
                "source": None,
                "file_name": None,
                "file_size": None
            }

            jobs_list.append(jobs)
        return response(200, "The list of jobs retrieved successfully", jobs_list)
    except psycopg2.DatabaseError as e:
        print(f"jobs_list::Database error: {e}")
        return response(500, 'Fetching jobs list has failed', str(e))
    except Exception as e:
        print(f"jobs_list::Unknown error caught: {e}")
        return response(500, 'Fetching jobs list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)

def source_config_list(query_params):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if query_params is not None and "group_id" in query_params and query_params.get("group_id") != "" and query_params.get("group_id") != "undefined" and query_params.get("role_id", None) != "0":
            group_id = query_params['group_id']
        elif query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
            #fetching all the source list for administrator
            get_source_configs = "SELECT uuid, name, location_pattern,connectivity_type FROM source_config WHERE status = %s order by id desc"
            cursor.execute(get_source_configs, ("draft",))
            get_source_configs_data = cursor.fetchall()
            source_config_list = []
            for column in get_source_configs_data:
                source_config_details = {}
                source_config_details = {
                    "name": column[1],
                    "uuid": column[0],
                    "location_pattern": get_location_pattern_format(column[2]),
                    "connectivity_type":column[3]
                }
                source_config_list.append(source_config_details)
            return response(200, "Source configuration list retrieved successfully", source_config_list)
        else:
            return response(400, "user id is missing in params", None)
        #fetching group associated source list
        group_associated_source_list = getGroupAssociatedConfigList(group_id, 'source_list')
        if group_associated_source_list:
            get_source_configs = "SELECT uuid, name, location_pattern ,connectivity_type FROM source_config WHERE status = %s AND uuid IN %s order by id desc"
            cursor.execute(get_source_configs, ("draft",tuple(group_associated_source_list),))
            get_source_configs_data = cursor.fetchall()
            source_config_list = []
            for column in get_source_configs_data:
                source_config_details = {}
                source_config_details = {
                    "name": column[1],
                    "uuid": column[0],
                    "location_pattern": get_location_pattern_format(column[2]),
                    "connectivity_type": column[3]
                }
                source_config_list.append(source_config_details)
            return response(200, "Source configuration list retrieved successfully", source_config_list)
        else:
            print("No Source list is present in the group.", group_id)
            return response(404, "No Source list is present in the group. Please contact File Central Administrator", None)

    except psycopg2.DatabaseError as e:
        print(f"source_config_list::Database error: {e}")
        return response(500, 'Fetching source configuration list has failed', str(e))
    except Exception as e:
        print(f"source_config_list::Unknown error caught: {e}")
        return response(500, 'Fetching source configuration list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)

def getGroupAssociatedConfigList(group_id, config_type):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        print(group_id)
        get_group_details = f"""SELECT group_name, {config_type} FROM groups WHERE uuid = %s"""
        cursor.execute(get_group_details, (group_id,))
        group_data = cursor.fetchone()
        if group_data is None:
            print("No config list present in group", group_id)
            return []
        elif group_data[1] is not None:
            config_list = group_data[1]
            config_ids_list = list(config_list.keys())
            return config_ids_list
   
    except psycopg2.DatabaseError as e:
        print(f"getGroupAssociatedConfigList::Database error: {e}")
        return response(500, 'Fetching config list list has failed', str(e))
    except Exception as e:
        print(f"getGroupAssociatedConfigList::Unknown error caught: {e}")
        return response(500, 'Fetching config list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def target_config_list(query_params):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if query_params is not None and "group_id" in query_params and query_params.get("group_id") != "" and query_params.get("group_id") != "undefined" and query_params.get("role_id", None) != "0":
            group_id = query_params['group_id']
        elif query_params is not None and "role_id" in query_params and query_params.get("role_id") == "0":
            get_target_configs = "SELECT uuid, name, location_pattern FROM target_config WHERE status = %s order by id desc"
            cursor.execute(get_target_configs, ("draft",))
            get_target_configs_data = cursor.fetchall()
            target_config_list = []
            for column in get_target_configs_data:
                target_config_details = {}
                target_config_details = {
                    "name": column[1],
                    "uuid": column[0],
                    "location_pattern": get_location_pattern_format(column[2])
                }
                target_config_list.append(target_config_details)
            return response(200, "Target configuration list retrieved successfully", target_config_list)
        else:
            return response(400, "user id is missing in params", None)
        group_associated_target_list = getGroupAssociatedConfigList(group_id, 'target_list')
        if group_associated_target_list:
            get_target_configs = "SELECT uuid, name, location_pattern FROM target_config WHERE status = %s AND uuid IN %s order by id desc"
            cursor.execute(get_target_configs, ("draft",tuple(group_associated_target_list),))
            get_target_configs_data = cursor.fetchall()
            target_config_list = []
            for column in get_target_configs_data:
                target_config_details = {}
                target_config_details = {
                    "name": column[1],
                    "uuid": column[0],
                    "location_pattern": get_location_pattern_format(column[2])
                }
                target_config_list.append(target_config_details)
            return response(200, "Target configuration list retrieved successfully", target_config_list)
        else:
            print("No Target list is present in the group.", group_id)
            return response(404, "No Target list is present in the group. Please contact File Central Administrator", None)
    except psycopg2.DatabaseError as e:
        print(f"target_config_list::Database error: {e}")
        return response(500, 'Fetching target configuration list has failed', str(e))
    except Exception as e:
        print(f"target_config_list::Unknown error caught: {e}")
        return response(500, 'Fetching target configuration list has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)


def transformation_list():
    try:
        transformation_list = [
            {
                "transformation_id": 'concatenation',
                "transformation_type": 'Concatenation',
                "dtype": "string"
            },
            {
                "transformation_id": 'derived_value',
                "transformation_type": 'Derived Value',
                "dtype": "number"

            }
        ]
        return response(200, "The transformation list is ready", transformation_list)
    except Exception as e:
        print(f"transformation_list::Unknown error caught: {e}")
        return response(500, 'Fetching transformation list has failed', str(e))


def get_location_pattern_format(location_pattern):
    try:
        current_date_time = datetime.now()
        start_char = "{"
        end_char = "}"
        start_index = location_pattern.find(start_char)
        end_index = location_pattern.find(end_char)

        # Check if both special characters are found in the string
        if start_index != -1 and end_index != -1:
            # Extract the substring between the special characters
            substring = location_pattern[start_index + 1:end_index]
            print(substring)
            formatted_date = current_date_time.strftime(substring)
            print(formatted_date)
            actual_string = location_pattern.split("$", len(location_pattern))
            if(len(actual_string) > 0):
                location_pattern = actual_string[0] + formatted_date
            return location_pattern
        else:
            return location_pattern
    except Exception as e:
        print(f"get_location_pattern_format::Unknown error caught: {e}")
        raise e

def response(stauts_code, message, data):
    """ This is a response method """
    return {
        "statusCode": stauts_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": data}),
    }
