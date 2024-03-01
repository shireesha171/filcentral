"""This module is for authenticating the user from the database"""
import json
from dbconnection import pool, getConnection


def lambda_handler(event, context):
    """This method is for handling the authentication a user"""
    conn, cursor = None, None
    print("Received Event :", str(event))
    params_dict = json.loads(event['body'])
    email = params_dict['email']
    password = params_dict['password']
    try:
        conn, cursor = getConnection()
        get_query = """
                        SELECT users.first_name, users.uuid, users.last_login, roles.id, roles.role_name
                        FROM users as users
                        INNER JOIN roles as roles ON users.role_id = roles.id
                        WHERE users.email =  %s AND users.password =  %s;
                    """
        cursor.execute(get_query, (email, password))
        user_data = cursor.fetchone()
        if user_data is None:
            return response(401, 'Invalid email or password', None)
        else:
            user_dict = {
                'first_name': user_data[0],
                'uuid': user_data[1],
                'last_login': user_data[2].strftime("%m/%d/%Y, %H:%M:%S"),
                'role_id': user_data[3],
                "role_name": user_data[4]
            }
            return response(200, 'LoggedIn Successfully', user_dict)
    except Exception as error:
        print(f"Unknown error occurred while authenticating the user : {error}")
        return response(500, f"Unknown error occurred while authenticating the user : {error}", error)
    finally:
        cursor.close()
        pool.putconn(conn)


def response(status_code, message, data):
    """This is a response method"""
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
