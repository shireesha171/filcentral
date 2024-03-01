"""This module is for Error Codes"""
import json
from dbconnection import pool, getConnection
import psycopg2


def lambda_handler(event, context):
    """This method is for handling the error codes"""
    print("Received Event: ", str(event))
    try:
        if event['resource'] == "/error-codes" and event['httpMethod'] == 'GET':
            return getAllErrorCodes()
        else:
            return response(400, 'resource or endpoint not found', None)
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
        return response(500, 'Error Codes API has failed due to an Unexpected Error', str(e))



def response(stauts_code, message, data):
    """This is response method"""
    return {
        "statusCode": stauts_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
            "Access-Control-Allow-Headers":"*",
            },
            "body":json.dumps({"message": message, "data":data})
    }

def getAllErrorCodes():
    """This method is for getting the error codes"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        query = "select uuid, errorcode, error_desc from status_codes"
        cursor.execute(query)
        rows = cursor.fetchall()
        error_code_list = []
        for row in rows:
            data = {}
            data['uuid']  = row[0]
            data['errorcode']  = row[1]
            data['error_desc']  = row[2]
            data['severity']  = 1
            data['impact']  = 1
            data['priority']  = 1
            error_code_list.append(data)
        return response(200,'Successfully fetched', error_code_list)
    except psycopg2.DatabaseError as e:
        print(f"getAllErrorCodes::Database error: {e}")
        return response(500, 'Fetching Error Codes has failed', str(e))
    except Exception as e:
        print(f"getAllErrorCodes::Unknown error caught: {e}")
        return response(500, 'Fetching Error Codes has failed', str(e))
    finally:
        cursor.close()
        pool.putconn(conn)
