import psycopg2
import json
from dbconnection import pool, getConnection
import os
import boto3
import botocore.exceptions

def lambda_handler(event, context):
    print("Received Event: ", str(event))
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        if (
                (
                    event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                    event['request']['userAttributes']['cognito:user_status'] == 'EXTERNAL_PROVIDER'
                # user who comes from okta
                )
                or
                (
                    event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                    event['request']['userAttributes']['cognito:user_status'] == 'CONFIRMED'
                # user who signed up them self using cognito
                )
                or
                (
                    event['triggerSource'] == 'PostAuthentication_Authentication' and
                    event['request']['userAttributes']['cognito:user_status'] == 'FORCE_CHANGE_PASSWORD'
                # user who is created by admin
                )
        ):

            if 'email' not in event['request']['userAttributes']:
                print('email')
                raise Exception("email is required")
            if 'family_name' not in event['request']['userAttributes']:
                print('family_name')
                raise Exception("lastName is required")
            if 'given_name' not in event['request']['userAttributes']:
                print('given_name')
                raise Exception("firstName is required")
            if 'custom:userType' not in event['request']['userAttributes']:
                event['request']['userAttributes']['custom:userType'] = 'Creator'
                print('userType')
            # if 'phone_number' not in event['request']['userAttributes']:
            #     print('phone_number')
            #     raise Exception("mobilePhone is required")
            #     raise Exception("userType is required")
            # if 'name' not in event['request']['userAttributes']:
            #     raise Exception("displayName is required")

            email = event['request']['userAttributes']['email']
            family_name = event['request']['userAttributes']['family_name']
            given_name = event['request']['userAttributes']['given_name']
            user_type = event['request']['userAttributes']['custom:userType']
            sub = event['request']['userAttributes']['sub']
            if user_type == 'Administrator':
                role_id = 0
            elif user_type == 'Creator':
                role_id = 1
            else:
                role_id = 2
            created_from = 'okta' if (
                event['triggerSource'] == 'PostConfirmation_ConfirmSignUp' and
                event['request']['userAttributes']['cognito:user_status'] == 'EXTERNAL_PROVIDER'
                # user who comes from okta
            ) else 'cognito'

            role_status = 'active' if role_id == 0 else None
            query = "INSERT INTO users (uuid, first_name, last_name, email, role_id, created_from, role_status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (
                sub,
                given_name,
                family_name,
                email,
                role_id,
                created_from,
                role_status
            )
            cursor.execute(query, values)
            conn.commit()
            print(f"New user {email} inserted into users table")
            update_cognito_user_attributes(event, created_from, user_type)
        return event
    except psycopg2.DatabaseError as e:
        print(f"lambda_handler::Database error: {e}")
    except Exception as e:
        print(f"lambda_handler::Unknown error caught: {e}")
    finally:
        cursor.close()
        pool.putconn(conn)


def update_cognito_user_attributes(event, created_from, user_type):
    try:
        email = event['request']['userAttributes']['email']
        if 'identities' in event['request']['userAttributes']:
            # Extract userId from the 'identities' array
            identities = event['request']['userAttributes']['identities']
            user_id = json.loads(identities)[0]['userId']
            if user_id[0].isupper():
                email = user_id
        try:
            client = boto3.client('cognito-idp', region_name=os.environ.get('Region'))
            user_pool_id = os.environ.get('user_pool_id')
            if created_from == 'okta':
                okta_prefix = os.environ.get('okta_prefix')
                email = okta_prefix + email

            # Specify the updated attributes
            updated_attributes = [{
                'Name': 'custom:userType',
                'Value': user_type
            }]

            # Update user attributes in cognito pool
            client.admin_update_user_attributes(
                UserPoolId=user_pool_id,
                Username=email,
                UserAttributes=updated_attributes
            )

            print(
                f"User attributes updated successfully to cognito: {updated_attributes}")
        except botocore.exceptions.ClientError as e:
            print(f"Failed to update user attributes: {email}", str(e))
    except botocore.exceptions.ClientError as e:
        print(f"Failed to update user attributes: {email}", str(e))