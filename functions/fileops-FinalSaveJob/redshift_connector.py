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

def redshift_connection(secrete_name, sql_query, val=None, many=None):
    try:
        secret_string = get_secret(secrete_name)
        secret_dict = json.loads(secret_string)
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
        if many:
            redshift_cursor.executemany(sql_query, val)
        else:
            redshift_cursor.execute(sql_query, val)

        conn.commit()
    except psycopg2.Error:
        # logger.exception('Failed to open database connection.')
        print("Failed to connect Redshift DB")
    finally:
        if conn.status:
            redshift_cursor.close()
            conn.close()















