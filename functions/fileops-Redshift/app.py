"""
In this module we are trying to retrive the test the connection of Redshift.
"""
import json
import psycopg2
from dbconnection import cursor, conn
import os

def lambda_handler(event, context):
    """ Getting Redshift test connection and their details"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/redshift/test-connection" and event['httpMethod'] == 'POST':
            if 'body' not in event:
                return response(400, 'INVALID_PAYLOAD', None)
            body = json.loads(event['body'])
            print(body)
            #body = event['body']
            host = body['endpoint']
            port = body['port']
            database = body['database']
            user = body['user']
            password = body['password']
            # Check if port is provided and valid
            redshift_port = os.environ.get('RedshiftPort')
            if port is None or port != redshift_port:
                return response(400, "Missing or invalid connection details", "failed")
            try:
                conn = psycopg2.connect(
                    dbname=database,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )
                print("Connected to Redshift successfully!")
                print(conn)
                return response(200, "Connected to Redshift successfully!", "success")
                # Create a cursor object using the connection
            except Exception as e:
                print("Error connecting to Redshift:", e)
                return response(400, "Missing or invalid connection details", "failed")
    except Exception as e:
        print(f"Unknown error caught in Redshift lambda handler : {e}")
        return response(500, 'Unexpected Error', str(e))


def response(stauts_code, message, data):
    """
    This is a response method
    """
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
