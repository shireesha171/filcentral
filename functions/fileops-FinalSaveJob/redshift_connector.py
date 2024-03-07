import boto3
import psycopg2
import json
import os
from psycopg2 import pool

def get_secret(secret_name):
    env = os.environ.get('Environment')
    region = os.environ.get('Region')
    #env = "dev"
    region_name = region
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    return secret_string

def redshift_connection(secret_dict, sql_query):
    try:
        #secret_string = get_secret(secrete_name)
        #secret_dict = json.loads(secret_string)
        conn = psycopg2.connect(
            host=secret_dict['endpoint'],
            port=secret_dict['port'],
            database=secret_dict['database'],
            user=secret_dict['user_name'],
            password=secret_dict['password']
        )

        conn.autocommit = True
        redshift_cursor = conn.cursor()
        print("connected successfully")

        redshift_cursor.execute(sql_query)
        conn.commit()
        return True
    except psycopg2.Error:
        # logger.exception('Failed to open database connection.')
        print("Failed to connect Redshift DB")
        return False
    finally:
        if conn.status:
            redshift_cursor.close()
            conn.close()

def redshift_connection_select(secret_dict, sql_query):
    try:
        #secret_string = get_secret(secrete_name)
        #secret_dict = json.loads(secret_string)
        conn = psycopg2.connect(
            host=secret_dict['endpoint'],
            port=secret_dict['port'],
            database=secret_dict['database'],
            user=secret_dict['user_name'],
            password=secret_dict['password']
        )

        conn.autocommit = True
        redshift_cursor = conn.cursor()
        print("connected successfully")

        redshift_cursor.execute(sql_query)
        record = redshift_cursor.fetchone()[0]
        conn.commit()

        if record is True:
            return True
        else:
            return False
    except psycopg2.Error:
        # logger.exception('Failed to open database connection.')
        print("Failed to connect Redshift DB")
    finally:
        if conn.status:
            redshift_cursor.close()
            conn.close()















