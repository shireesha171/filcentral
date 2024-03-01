import boto3

import json

from .constants import account, region, env
from .enums import JobValidationTypes
import datetime

def get_current_date_time():
  message = "Current datetime: " + str(datetime.datetime.now())
  return message

def get_job_validation_status(file_errors, num_of_schema_validation_errors):
    """
        Get the Job Validation Status based on file validation and schema validation errors
    """

    number_of_file_errors = file_errors[0]

    job_validation_status = None

    if number_of_file_errors > 0 or num_of_schema_validation_errors[0] > 0:
        job_validation_status = JobValidationTypes.FAILED
    else:
        job_validation_status = JobValidationTypes.PASSED

    return job_validation_status


def invoke_file_converter_lambda(**kwargs):
    lambda_client = boto3.client('lambda')

    payload = {
        "job_uuid": kwargs.get("job_uuid", None),
        "job_run_uuid": kwargs.get("job_run_id", None),
        "source_file_path": kwargs.get("file_path", None),
        "source_file_name": kwargs.get("file_name", None)
    }

    payload_json = json.dumps(payload)

    print("invoke_file_converter_lambda::utils", f"Sending Payload to FileConverter lambda: {payload_json}")

    response = lambda_client.invoke(
        FunctionName=f'arn:aws:lambda:{region}:{account}:function:fileops-FileConverter-{env}',
        InvocationType='Event',
        Payload=payload_json,
        LogType='Tail'
    )

    print("invoke_file_converter_lambda::utils", response)

    return response
