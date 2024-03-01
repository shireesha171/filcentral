import os
from enum import Enum

from functools import wraps


class FILE_EXTENSIONS(Enum):
    XLSX = "xlsx"
    CSV = "csv"
    JSON = "json"


class CONSTANTS(Enum):
    # Generic
    ENVIRONMENT = os.environ.get("Environment", "dev")

    # S3
    S3_STORAGE = os.environ.get("S3FileStorage", "")
    S3_TARGET_FILE_FOLDER_PATH = "target-files"
    S3_TARGET_FILE_NAME = "data.csv"
    S3_PARQUET_FILE_FOLDER_PATH = "parquet_files"

    # PARQUET
    TEMP_FOLDER_NAME = "tmp"
    PARQUET_OUTPUT_FOLDER_NAME = "parquet-output"


def get_s3_bucket_name():
    return f'{CONSTANTS.S3_STORAGE.value}-{CONSTANTS.ENVIRONMENT.value}'


def get_s3_target_path(job_run_uuid):
    return f"{get_s3_bucket_name()}/{CONSTANTS.S3_PARQUET_FILE_FOLDER_PATH.value}/{job_run_uuid}"


# Decorators
def handle_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)

            return result
        except Exception as e:
            print(f"{func.__name__}", f"Error: {e}")

    return wrapper
