"""This module is for source file configuration"""
import json
import uuid
from dbconnection import pool, getConnection
import pandas as pd
import psycopg2
import boto3
import os

def lambda_handler(event, context):
    """This method is for handling the source file configurations"""
    print("Received Event: ", str(event))

    if event['requestContext'] and not event['requestContext']['authorizer']['claims']:
        return response(400, "user is not authenticated", None)
    user_id = event['requestContext']['authorizer']['claims']['sub']

    try:
        if event['resource'] == "/source-target-config" and event['httpMethod'] == 'POST':
            if 'body' not in event:
                return response(400, 'please provide payload', None)
            # body = event['body']
            body = json.loads(event['body'])
            return createConfigurations(user_id, body)

        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'GET':
            return getConfigurationDetails(user_id, event)

        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'PUT':
            if 'body' not in event:
                return response(400, 'please provide update payload', None)
            body = json.loads(event['body'])
            # body = event['body']
            return updateConfiguration(user_id, body)

        elif event['resource'] == "/source-target-config" and event['httpMethod'] == 'DELETE':
            return deleteConfiguration(user_id, event)

        elif event['resource'] == "/source-target-config/list" and event['httpMethod'] == 'GET':
            return listOfConfigurations(user_id, event)
        else:
            return response(400, 'resource or endpoint not found', None)
    except Exception as error:
        print(error)
        return response(400, str(error), None)


def saveConfigurationsToDB(table_name, body, created_by):
    """
        This method is to save the source or target configurations based on the config type.
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()
        if body["connectivity_type"] and body["connectivity_type"] == "redshift":
            secret_name = create_secret(body)
            insert_into_sc = f"""
                    INSERT INTO {table_name}(uuid, name, host, user_name, connectivity_type,status, created_by, secret_name, create_table, location_pattern, absolute_file_path)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """
            values = (
                str(uuid.uuid4()),
                body["name"] if body["name"] else '',
                body["endpoint"] if body["endpoint"] else '',
                body["user_name"] if body["user_name"] else '',
                body["connectivity_type"] if body["connectivity_type"] else '',
                'draft',
                created_by,
                secret_name if secret_name else '',
                body["create_table"] if body["create_table"] else True,
                body["location_pattern"] if body["location_pattern"] else '',
                body["absolute_file_path"] if body["absolute_file_path"] else '',
            )
        else:
            secret_name = ''
            if body["connectivity_type"] and body["connectivity_type"] == "ftp":
                secret_name = create_secret(body)
            insert_into_sc = f"""
                            INSERT INTO {table_name}(uuid, name, host, user_name, location_pattern, absolute_file_path, connectivity_type,status, created_by, secret_name)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING *
                        """
            values = (
                str(uuid.uuid4()),
                body["name"] if body["name"] else '',
                body["host"] if body["host"] else '',
                body["user_name"] if body["user_name"] else '',
                body["location_pattern"] if body["location_pattern"] else '',
                body["absolute_file_path"] if body["absolute_file_path"] else '',
                body["connectivity_type"] if body["connectivity_type"] else '',
                'draft',
                created_by,
                secret_name
            )
        cursor.execute(insert_into_sc, values)
        data = cursor.fetchone()
        conn.commit()

        cols = list(map(lambda x: x[0], cursor.description))
        df = pd.DataFrame([data], columns=cols)
        record = df.to_dict(orient="records")[0]
        record['created_on'] = str(record['created_on'])
        print("saveConfigurationsToDB::record ", record)

        # adding the source/target to the user group
        if 'group_list' in body and len(body['group_list']) > 0:
            record_uuid = data[0]
            list_type = 'source_list' if body['config_type'] == 'source' else 'target_list'
            for group in body['group_list']:
                for group_uuid in group.keys():
                    new_config = json.dumps(
                        {record_uuid: body["name"]})
                    update_group_query = f"""UPDATE groups 
                            SET {list_type} = CASE 
                                                WHEN {list_type} IS NULL THEN %s 
                                                ELSE {list_type} || %s 
                                            END 
                            WHERE uuid = %s"""
                    values = (new_config, new_config, group_uuid)
                    cursor.execute(update_group_query, values)
                    affected_rows = cursor.rowcount  # Get the number of affected rows
                    if affected_rows == 0:
                        raise ValueError("No records found to update")
                    conn.commit()

        return record
    except psycopg2.DatabaseError as error:
        print("saveConfigurationsToDB::Error", error)
        return error
        # return response(500, "unable save the configurations", str(error))
    except Exception as error:
        print("saveConfigurationsToDB::Unknown error caught", error)
        return error
        # return response(500, "unable save the configurations", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def create_secret(body):
    try:
        session = boto3.session.Session()
        env = os.environ.get('Environment')
        region = os.environ.get('Region')
        # env = "dev"
        target_name = body['name']
        secret_name = f"{target_name}-{env}"
        secret_name = secret_name.replace(" ", "_")
        region_name = region
        client = session.client(service_name='secretsmanager', region_name=region_name)
        # Check if specific keys are not empty in the payload
        required_keys_redshift = ['endpoint', 'database', 'user_name', 'password', 'region', 'port', 's3_location']
        required_keys_ftp = ['host', 'user_name', 'password']

        if body["connectivity_type"] == "redshift":
            if all(body.get(key) for key in required_keys_redshift):
                secret_value = {key: body[key] for key in required_keys_redshift}
                secret_value_str = json.dumps(secret_value)
            else:
                raise ValueError("Missing redshift required params in payload")
        elif body["connectivity_type"] == "ftp":
            if all(body.get(key) for key in required_keys_ftp):
                secret_value = {key: body[key] for key in required_keys_ftp}
                secret_value_str = json.dumps(secret_value)
            else:
                raise ValueError("Missing ftp required params in payload")
        else:
            raise ValueError("Invalid connectivity type")

        response = client.create_secret(
            Name=secret_name,
            SecretString=secret_value_str
        )
        print("Created secret Name:", response['Name'])
        return response['Name'] if response['Name'] else ''
    
    except Exception as e:
        print(f"create_secret::Unknown error caught: {e}")
        raise e
    
def check_source_or_target_exits(table_name, body_update_values):
    """
    This method is to check the name of the source is unique or not
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        config_uuid = None
        if 'source_id' in body_update_values:
            config_uuid = body_update_values['source_id']
            query = f"SELECT * FROM {table_name} where name=%s and status !=%s and uuid != %s"
            config_name = body_update_values['name']
            cursor.execute(query, (config_name, "deleted", config_uuid))
            data = cursor.fetchone()

        elif 'target_id' in body_update_values:
            config_uuid = body_update_values['target_id']
            query = f"SELECT * FROM {table_name} where name=%s and status !=%s and uuid != %s"
            config_name = body_update_values['name']
            cursor.execute(query, (config_name, "deleted", config_uuid))
            data = cursor.fetchone()
        else:
            query = f"SELECT * FROM {table_name} where name=%s and status != %s"
            config_name = body_update_values['name']
            cursor.execute(query, (config_name, "deleted"))
            data = cursor.fetchone()

        print("check_source_or_target_exits::data", data)

        if data is None:
            return False
        return True

    except psycopg2.DatabaseError as error:
        print("check_source_or_target_exits::Error", error)
        return response(500, "unable to update the source or target config", str(error))
    except Exception as error:
        print("check_source_or_target_exits::Unknown error caught", error)
        return response(500, "unable to update the source or target config", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def createConfigurations(user_id, body):
    """
        This method is to redirect to respective methods for creating source or target configurations.
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        query = "SELECT id FROM users where uuid = %s"
        cursor.execute(query, (user_id,))
        created_by = cursor.fetchone()
        if body.get('config_type', None) is None:
            return response(404, "Unable to save save configurations", "No config_type in body")

        if body['config_type'] == 'source':
            source_table = 'source_config'

            # Calling check_source_or_target_exits method to check if the given source name exists already.
            if check_source_or_target_exits(source_table, body):
                return response(400, str("Source name already exists"), None)

            # Calling saveConfigurationsToDB method to save the source_config details to DB.
            body = saveConfigurationsToDB(source_table, body, created_by)

        elif body['config_type'] == 'target':
            target_table = 'target_config'

            # Calling check_source_or_target_exits method to check if the given target name exists already.
            if check_source_or_target_exits(target_table, body):
                return response(400, str("Target name already exists"), None)

            # Calling saveConfigurationsToDB method to save the target_config details to DB.
            body = saveConfigurationsToDB(target_table, body, created_by)

        return response(200, "The data inserted successfully", body)

    except psycopg2.DatabaseError as error:
        print("createConfigurations::Error", error)
        return response(500, "unable update the source", str(error))
    except Exception as error:
        print("createConfigurations::Unknown error caught", error)
        return response(500, "unable update the source", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def getConfigurationDetails(user_id, event):
    """
        This method is for editing the configurations
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        if 'queryStringParameters' not in event:
            return response(400, "Please provide Query parameters", None)

        params = event['queryStringParameters']

        if 'source_id' in params:
            id = params['source_id']
            query = f"""
                SELECT 
                    s.*,
                    COALESCE((
                        SELECT jsonb_agg(jsonb_build_object(g1.uuid, g1.group_name))
                        FROM groups g1
                        WHERE jsonb_exists(g1.source_list, s.uuid::text)
                    ), '[]'::jsonb) AS assigned_groups
                FROM 
                    source_config s
                WHERE 
                    s.uuid = %s 
                GROUP BY 
                    s.id
                ORDER BY 
                    s.id DESC;
            """
        elif 'target_id' in params:
            id = params['target_id']
            query = f"""
                SELECT 
                    t.*,
                    COALESCE((
                        SELECT jsonb_agg(jsonb_build_object(g1.uuid, g1.group_name))
                        FROM groups g1
                        WHERE jsonb_exists(g1.target_list, t.uuid::text)
                    ), '[]'::jsonb) AS assigned_groups
                FROM 
                    target_config t
                WHERE 
                    t.uuid = %s 
                GROUP BY 
                    t.id
                ORDER BY 
                    t.id DESC;
            """

        else:
            return response(400, "Unable to get the configuration details", "Invalid source_id or target_id")

        cursor.execute(query, (id,))
        column_names = [desc[0] for desc in cursor.description]
        column_names.append('full_count')
        res = cursor.fetchone()

        obj = {}
        for index, item in enumerate(res):
            obj[column_names[index]] = res[index]
        print("getConfigurationDetails::obj ", obj)
        del obj['created_on']
        #fetching secret details
        if obj['secret_name'] and obj['secret_name'] != '':
            get_secret = get_secret_details(obj['secret_name'])
            secret_dict = json.loads(get_secret)
            if obj['connectivity_type'] == 'redshift':
                obj['host'] = secret_dict['endpoint']
                obj['endpoint'] = secret_dict['endpoint']
                obj['database'] = secret_dict['database']
                obj['user_name'] = secret_dict['user_name']
                obj['password'] = secret_dict['password']
                obj['region'] = secret_dict['region']
                obj['port'] = secret_dict['port']
                obj['s3_location'] = secret_dict['s3_location']
            if obj['connectivity_type'] == 'ftp':
                obj['host'] = secret_dict['host']
                obj['user_name'] = secret_dict['user_name']
                obj['password'] = secret_dict['password']
        return response(200, "The data fetched successfully", obj)

    except psycopg2.DatabaseError as error:
        print("getConfigurationDetails::Error", error)
        return response(500, "Unable to get the configuration details", str(error))
    except Exception as error:
        print("getConfigurationDetails::Unknown error caught", error)
        return response(500, "Unable to get the configuration details", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)

def get_secret_details(secret_name):
    try:
        region = os.environ.get('Region')
        region_name = region
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        return secret_string
    except Exception as error:
        print("get_secret_details::Unable to get connection details secret manager", str(error))
        raise error
 
def deleteConfiguration(user_id, event):
    """
        This method is to delete the configurations
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        if 'queryStringParameters' not in event:
            return response(400, "Please provide Query parameters", None)
        params = event['queryStringParameters']

        if 'source_id' in params:
            id = params['source_id']
            query = "update source_config set status=%s where uuid = %s"
            get_query = "SELECT secret_name FROM source_config where uuid = %s"
        elif 'target_id' in params:
            id = params['target_id']
            query = "update target_config set status=%s where uuid = %s"
            get_query = "SELECT secret_name FROM target_config where uuid = %s"
        else:
            return response(400, "Unable to delete the configurations", "Invalid source_id or target_id")
        print("deleteConfiguration::query, id ", query, id)
        cursor.execute(query, ('deleted', id,))
        conn.commit()

        #deleting config associated secret from secrets manager
        cursor.execute(get_query, (id,))
        data = cursor.fetchone()
        if data[0] is not None and data[0] != '':
            delete_secret_details(data[0])
        # deleting config in groups table
        list_type = 'source_list' if 'source_id' in params else 'target_list'
        update_groups = f"""
            UPDATE groups 
            SET {list_type} = {list_type} - %s
                WHERE {list_type} ? %s
        """
        values = (
            id,
            id
        )
        cursor.execute(update_groups, values)
        conn.commit()

        return response(200, "deleted successfully", id)
    except psycopg2.DatabaseError as error:
        print("deleteConfiguration::Error", error)
        return response(500, "Unable to delete the configurations", str(error))
    except Exception as error:
        print("deleteConfiguration::Unknown error caught", error)
        return response(500, "Unable to delete the configurations", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def updateConfiguration(user_id, body):
    """
    This method is to update the configuration details.
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        body_update_values = body['update_values']

        set_clause = ', '.join(
            f"{column} = %s" for column in body_update_values.keys())
        if 'source_id' in body or body.get("config_type") == 'source':
            source_table = 'source_config'
            source_id = body.get('source_id')
            secret_name = ''
            if body.get("connectivity_type") == 'ftp':
                secret_name = body.get('secret_name')
                secret_details = {
                    "host": body_update_values["host"],
                    "user_name": body_update_values["user_name"],
                    "password": body_update_values["password"]
                }
                keys_to_remove = ["password"]
                for key in keys_to_remove:
                    body_update_values.pop(key, None)
                set_clause = ', '.join(
                    f"{column} = %s" for column in body_update_values.keys())
            
            body_update_values['source_id'] = source_id
            if check_source_or_target_exits(source_table, body_update_values):
                return response(400, "source name already exists", "unable to update the configuration details")

            query = f"UPDATE source_config SET {set_clause} WHERE uuid = %s"
            values = tuple(body_update_values.values())
            print("updateConfiguration::values ", values)
            cursor.execute(query, values)

            if secret_name != '':
                update_secret_details(secret_name, secret_details)
        elif 'target_id' in body or body.get("config_type") == 'target':
            target_table = 'target_config'
            target_id = body.get('target_id')
            secret_name = ''
            secret_details = {}
            if body.get("connectivity_type") == 'redshift':
                secret_name = body.get('secret_name')
                secret_details = {
                    "endpoint": body_update_values["endpoint"],
                    "user_name": body_update_values["user_name"],
                    "database": body_update_values["database"],
                    "password": body_update_values["password"],
                    "port": body_update_values["port"],
                    "region": body_update_values["region"],
                    "s3_location": body_update_values["s3_location"]
                }
                body_update_values["host"] = body_update_values["endpoint"]
                keys_to_remove = ["endpoint", "database", "password", "port", "region", "s3_location"]
                for key in keys_to_remove:
                    body_update_values.pop(key, None)
                set_clause = ', '.join(
                    f"{column} = %s" for column in body_update_values.keys())

            elif body.get("connectivity_type") == 'ftp':
                secret_name = body.get('secret_name')
                secret_details = {
                    "host": body_update_values["host"],
                    "user_name": body_update_values["user_name"],
                    "password": body_update_values["password"]
                }
                keys_to_remove = ["password"]
                for key in keys_to_remove:
                    body_update_values.pop(key, None)
                set_clause = ', '.join(
                    f"{column} = %s" for column in body_update_values.keys())
            
            body_update_values['target_id'] = target_id
            if check_source_or_target_exits(target_table, body_update_values):
                return response(400, "Target name already exists", "unable to update the configuration details")

            query = f"UPDATE target_config SET {set_clause} WHERE uuid = %s"
            values = tuple(body_update_values.values())
            print("updateConfiguration::values ", values)
            cursor.execute(query, values)
            if secret_name != '':
                update_secret_details(secret_name, secret_details)
        else:
            return response(400, "Please provide source_id or target_id", None)

        # removing the source/target from the user group
        list_type = 'source_list' if body['config_type'] == 'source' else 'target_list'
        record_uuid = body_update_values['source_id'] if body[
            'config_type'] == 'source' else body_update_values['target_id']
        update_groups = f"""
            UPDATE groups 
            SET {list_type} = {list_type} - %s
                WHERE {list_type} ? %s
        """
        values = (
            record_uuid,
            record_uuid
        )
        cursor.execute(update_groups, values)
        conn.commit()
        # adding the source/target to the user group
        if 'group_list' in body and len(body['group_list']) > 0:
            record_uuid = body_update_values['source_id'] if body['config_type'] == 'source' else body_update_values[
                'target_id']
            list_type = 'source_list' if body['config_type'] == 'source' else 'target_list'
            for group in body['group_list']:
                for group_uuid in group.keys():
                    new_config = json.dumps(
                        {record_uuid: body_update_values["name"]})
                    update_group_query = f"""UPDATE groups 
                            SET {list_type} = CASE 
                                                WHEN {list_type} IS NULL THEN %s 
                                                ELSE {list_type} || %s 
                                            END 
                            WHERE uuid = %s"""
                    values = (new_config, new_config, group_uuid)
                    cursor.execute(update_group_query, values)
                    affected_rows = cursor.rowcount  # Get the number of affected rows
                    if affected_rows == 0:
                        raise ValueError("No records found to update")
                    conn.commit()

        conn.commit()
        return response(200, "Configuration details updated successfully", body)

    except psycopg2.DatabaseError as error:
        print("check_source_or_target_exits::Error", error)
        return response(500, "unable to update the source_or_target", str(error))
    except Exception as error:
        print("check_source_or_target_exits::Unknown error caught", error)
        return response(500, "unable update the source_or_target", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def update_secret_details(secret_name, secret_details):
    try:
        region = os.environ.get('Region')
        region_name = region
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        response = client.update_secret(
            SecretId = secret_name,
            SecretString = json.dumps(secret_details)
        )
        print("Secret updated successfully")
    except Exception as e:
        print("Error updating secret:", e)
        raise e
    
def delete_secret_details(secret_name):
    try:
        region = os.environ.get('Region')
        region_name = region
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        response = client.delete_secret(
            SecretId = secret_name        )
        print("Secret deleted successfully")
    except Exception as e:
        print("Error updating secret:", e)
        raise e
    
def listOfConfigurations(user_id, event):
    """
        This method is to get the list of configurations
    """

    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        if 'queryStringParameters' not in event:
            return response(400, "Please provide Query parameters", None)
        params = event['queryStringParameters']

        recordsperpage = params['recordsperpage']
        offset = params['offset']
        pagination_query = f"offset {offset} ROWS FETCH next {recordsperpage} ROWS ONLY"
        if params is not None and "group_id" in params and params.get("group_id") != "" and params.get(
                "group_id") != "undefined" and params.get("role_id") != "0":
            group_id = params['group_id']
        elif params is not None and "role_id" in params and params.get("role_id") == "0":
            if 'config_type' in params and params["config_type"] == 'source':
                query = f"""select *,count(*) OVER() AS full_count from source_config where status !='deleted'
                        ORDER BY id DESC {pagination_query}"""
            elif 'config_type' in params and params["config_type"] == 'target':
                query = f"""select *, count(*) OVER() AS full_count from target_config where status !='deleted'
                        ORDER BY id DESC {pagination_query}"""
            else:
                return response(400, "Please provide config_type", None)

            cursor.execute(query)
            column_names = [desc[0] for desc in cursor.description]
            res = cursor.fetchall()
            print("listOfConfigurations::res ", res)
            final = []
            for each in res:
                obj = {}
                for index, item in enumerate(each):
                    obj[column_names[index]] = each[index]
                del obj['created_on']
                final.append(obj)

            return response(200, "The data fetched successfully", final)
        else:
            return response(400, "group_id is missing in params", None)
        # If user is not an Admin
        list_type = 'source_list' if params['config_type'] == 'source' else 'target_list'
        group_associated_config_list = getGroupAssociatedConfigList(
            group_id, list_type)
        if group_associated_config_list:
            if list_type == 'source_list':
                query = f"""select *,count(*) OVER() AS full_count from source_config where status !='deleted' and uuid IN %s
                        ORDER BY id DESC {pagination_query}"""
            elif list_type == 'target_list':
                query = f"""select *, count(*) OVER() AS full_count from target_config where status !='deleted' and uuid IN %s
                        ORDER BY id DESC {pagination_query}"""
            else:
                return response(400, "Please provide config_type", None)

            cursor.execute(query, (tuple(group_associated_config_list),))
            column_names = [desc[0] for desc in cursor.description]
            res = cursor.fetchall()
            print("listOfConfigurations::res ", res)
            final = []
            for each in res:
                obj = {}
                for index, item in enumerate(each):
                    obj[column_names[index]] = each[index]
                del obj['created_on']
                final.append(obj)

            return response(200, "The data fetched successfully", final)
        else:
            print(
                f"No {params['config_type']} details are found. Please contact File Central Administrator", group_id)
            return response(404, f"No {params['config_type']} details are found. Please contact File Central Administrator", None)
    except psycopg2.DatabaseError as error:
        print("listOfConfigurations::Error", error)
        return response(500, "unable to get the list of configurations", str(error))
    except Exception as error:
        print("listOfConfigurations::Unknown error caught", error)
        return response(500, "unable to get the list of configurations", str(error))
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
    except psycopg2.DatabaseError as error:
        print("getGroupAssociatedConfigList::Error", error)
        return response(500, "unable to get the list of configurations in group", str(error))
    except Exception as error:
        print("getGroupAssociatedConfigList::Unknown error caught", error)
        return response(500, "unable to get the list of configurations in group", str(error))
    finally:
        cursor.close()
        pool.putconn(conn)


def response(stauts_code, message, data):
    """This is response method"""
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
