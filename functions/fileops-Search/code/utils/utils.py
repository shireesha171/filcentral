import json

from enum import Enum


class HTTP_STATUS_CODES(Enum):
    """
        Enum representing commonly used HTTP status codes.
    """

    SUCCESS = 200
    FAILED = 500


class API_MESSAGES(Enum):
    """
        Enum representing custom API messages for different scenarios.
    """

    NOT_A_VALID_EVENT = "NOT_A_VALID_EVENT"
    INVALID_QUERY_PARAMS = "INVALID_QUERY_PARAMS"
    LAMBDA_ERROR = "LAMBDA_ERROR"
    JOB_RUN_RECORDS_RETRIEVE_SUCCESS = "JOB_RUN_RECORDS_RETRIEVE_SUCCESS"
    REQUEST_NOT_SUPPORTED = "REQUEST_NOT_SUPPORTED"


class QUERY_RESOURCES(Enum):
    """
        Enum representing query resources
    """

    JOB_RUNS = "job_runs"

def response_body(statuscode, message, body):
    """
       Generates a standardized HTTP response body for API requests.

       Parameters:
       - statuscode (int): HTTP status code indicating the result of the request.
       - message (str): A custom message describing the result or error of the request.
       - body (dict): Additional data to include in the response body.

       Returns:
       - dict: Standardized HTTP response body in JSON format.
   """

    response = {
        "statusCode": statuscode.value,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message.value, "data": body})
    }

    return response
