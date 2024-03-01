from .utils.loggers import log_file_metadata
from .utils.utils import get_current_date_time
from chardet.universaldetector import UniversalDetector
from datetime import datetime
import csv
import math
import os
import re

import logging
import boto3

env = os.environ.get('Environment')
# s3_bucket = f'fileops-storage-{env}'
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

file_validation_data = {}
# JobRunLogger

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("validation-process.file_validations")


# JobRunLogger - FileValidation Class


def fetchFileContent(file_name, business_validate_id, query_params):
    logger.info(f"query params : {query_params}")
    s3 = boto3.client('s3')
    bucket_name = s3_bucket
    if 'job_type' in query_params and query_params['job_type'] == 'Scheduled':
        # location_pattern = query_params['location_pattern'].replace("S3://", '').replace("s3://", '')
        # s3_object_key = location_pattern + query_params['file_name_pattern']
        s3_object_key = query_params['location_pattern'] + \
            query_params['file_name_pattern']
        bucket_name = re.search(r"s3://([^/]+)/", s3_object_key).group(1)
        s3_object_key = re.sub(r"s3://[^/]+/", "", s3_object_key)
        if "{" in s3_object_key and "}" in s3_object_key:
            s3_object_key = s3_object_key.replace("{", "").replace("}", "")
        s3_object_key = datetime.now().strftime(s3_object_key)
        logger.info(f"The job source file is present in {s3_object_key}")
        # location_pattern = query_params['location_pattern'].replace("S3://", '').replace("s3://", '')
        # source_file_location = location_pattern + "/" + query_params['file_name_pattern']
        source_file_location = s3_object_key
    elif 'job_type' in query_params and query_params['job_type'] == "s3_trigger":
        s3_object_key = query_params['key']
        bucket_name = query_params['bucket_name']
        source_file_location = query_params['key']
    else:
        s3_object_key = 'sample-files/' + business_validate_id + "/" + file_name
        s3_object_key = datetime.now().strftime(s3_object_key)
        # source_file_location = 'sample-files/' + business_validate_id + "/" + query_params['file_name_pattern']
        source_file_location = s3_object_key
    # Use the list_objects_v2 method to retrieve object names
    print("Folder Prefix for listing the filename", s3_object_key)

    response = s3.get_object(
        Bucket=bucket_name,
        Key=s3_object_key
    )
    if response is not None:
        # JobRunLogger - Update File name
        log_file_metadata(file_name=file_name, file_path=s3_object_key, bucket_name=bucket_name,
                          folder_prefix=s3_object_key)
        local_file_name = "/tmp/" + file_name
        s3.download_file(bucket_name, s3_object_key, local_file_name)
        return local_file_name, source_file_location
    else:
        return None, source_file_location


def get_file_validations(record, business_validate_id, query_params, terminate_flag):
    file_errors = [0]
    src_file_name = record["file_name"]
    try:
        source_file, source_file_location = fetchFileContent(
            src_file_name, business_validate_id, query_params)
        if source_file is None:
            file_validation_data['file_not_exists'] = forming_values(
                ["FAIL"], "Failure")
            return file_errors[0]+1, file_validation_data
        file_validation_data['file_exists'] = forming_values(
            ["PASS"], "Passed")
        # # calling get file encodings of the file
        get_file_encodings(record, file_errors, source_file, terminate_flag)
        # calling get_file_size method
        get_file_size(record, file_errors, source_file, terminate_flag)
        # calling get delimiter of the file
        get_delimiter_of_file(record, file_errors, source_file, terminate_flag)
        # # calling get file extension method
        get_file_extension(record, file_errors, source_file, terminate_flag)
        return file_errors, file_validation_data, source_file_location
    except FileEncodingCheckError as e:
        logger.error(
            f"File encoding check failed for this source file : {src_file_name} validation_errors : {file_validation_data}")
        return file_errors, file_validation_data
    except FileSizeCheckError as e:
        logger.error(
            f"File size check failed for this source file : {src_file_name} validation_errors : {file_validation_data}")
        return file_errors, file_validation_data
    except FileDelimiterCheckError as e:
        logger.error(
            f"File Delimiter check failed for this source file : {src_file_name} validation_errors : {file_validation_data}")
        return file_errors, file_validation_data
    except Exception as e:
        print("get_file_validations::Error ", e)
        raise e


def get_file_size(record, file_errors, localfile, terminate_flag):
    """This method is for checking the file size """
    try:
        file_size = os.path.getsize(localfile)
        file_size_max_str = record['file_config_details']['file_size_max']
        file_size_min_str = record['file_config_details']['file_size_min']
        if file_size_min_str != "" and file_size_max_str != "":
            max_value, unit_of_data = extract_numerical_and_non_numerical_parts(
                file_size_max_str)
            min_value, unit_of_data = extract_numerical_and_non_numerical_parts(
                file_size_min_str)

            if unit_of_data == "Bytes":
                if min_value <= file_size <= max_value:
                    message = f"The file size is in range: [{min_value}Bytes, {max_value}Bytes] i.e {file_size} Bytes"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)

                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size) + "Bytes", "PASS", str(min_value) +
                         ' Bytes', str(max_value) + ' Bytes'],
                        "Passed")
                else:
                    message = f"This file size is not in the range: [{min_value}Bytes, {max_value}Bytes]"
                    print(message)
                    logger.info(message + "  " + get_current_date_time())
                    file_errors[0] += 1
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size) + "Bytes", "FAIL", str(min_value) +
                         ' Bytes', str(max_value) + ' Bytes'],
                        "Failure")
                    if terminate_flag:
                        raise FileSizeCheckError(
                            f"FileSizeCheck failed for file : {record['file_name']} {message}")

            elif unit_of_data == "KB":
                if min_value <= file_size / 1024 <= max_value:
                    message = f"The file size is in range: [{min_value}KB, {max_value}KB] i.e {math.ceil(file_size / (1024) * 100) / 100} KB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // 1024) + "KB", "PASS",
                         str(min_value) + ' KB', str(max_value) + ' KB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range: [{min_value}KB, {max_value}KB]"
                    print(message)
                    logger.info(message + "  " + get_current_date_time())
                    file_errors[0] += 1
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // 1024) + "KB", "FAIL",
                         str(min_value) + ' KB', str(max_value) + ' KB'],
                        "Failure")
                    if terminate_flag:
                        raise FileSizeCheckError(
                            f"FileSizeCheck failed for file : {record['file_name']} {message}")

            elif unit_of_data == "MB":
                if min_value <= file_size / (1024 ** 2) <= max_value:
                    message = f"The file size is in range: [{min_value}MB, {max_value}MB] i.e {math.ceil(file_size / (1024 ** 2) * 100) / 100} MB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // (1024 ** 2)) + "MB", "PASS",
                         str(min_value) + ' MB', str(max_value) + ' MB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}MB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size // (1024 ** 2)) + "MB", "FAIL",
                         str(min_value) + ' MB', str(max_value) + ' MB'],
                        "Failure")
                    file_errors[0] += 1
                    if terminate_flag:
                        raise FileSizeCheckError(
                            f"FileSizeCheck failed for file : {record['file_name']} {message}")

            elif unit_of_data == "GB":
                if min_value <= file_size / (1024 ** 3) <= max_value:
                    message = f"The file size is in range: [{min_value}GB, {max_value}GB] i.e {math.ceil(file_size / (1024 ** 3) * 100) / 100} GB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 3)) + "GB", "PASS",
                         str(min_value) + ' GB', str(max_value) + ' GB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}GB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 3)) + "GB", "FAIL",
                         str(min_value) + ' GB', str(max_value) + ' GB'],
                        "Failure")
                    file_errors[0] += 1
                    if terminate_flag:
                        raise FileSizeCheckError(
                            f"FileSizeCheck failed for file : {record['file_name']} {message}")
            elif unit_of_data == "TB":
                if min_value <= file_size / (1024 ** 4) <= max_value:
                    message = f"The file size is in range: [{min_value}GB, {max_value}GB] i.e {math.ceil(file_size / (1024 ** 4) * 100) / 100} TB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 4)) + "TB", "PASS",
                         str(min_value) + ' TB', str(max_value) + ' TB'],
                        "Passed")
                else:
                    message = f"This file size is not in the range{min_value} and {max_value}TB"
                    logger.info(message + "  " + get_current_date_time())
                    print(message)
                    file_validation_data['file_size_range'] = forming_values(
                        [str(file_size / (1024 ** 4)) + "TB", "FAIL",
                         str(min_value) + ' TB', str(max_value) + ' TB'],
                        "Failure")
                    file_errors[0] += 1
                    if terminate_flag:
                        raise FileSizeCheckError(
                            f"FileSizeCheck failed for file : {record['file_name']} {message}")
    except Exception as e:
        logger.error(str(e) + "  " + get_current_date_time())
        print("Exception Found", e)
        raise e


def detect_delimiter(local_file):
    """This method is to get the delimiter of the file"""
    try:
        with open(local_file, 'r') as csv_data:
            sniffer = csv.Sniffer()
            sample = csv_data.read(4096)  # Read a sample of the file's content
            delimiter = sniffer.sniff(sample, delimiters='|,:;\t').delimiter
            print(f"detect_delimiter::The file is ({delimiter}) separated")
            return delimiter
    except csv.Error as csv_error:
        raise csv_error
    except Exception as error:
        raise error


def check_delimiter_consistency(localfile, expected_delimiter):
    """This method is for getting the delimiter consistency"""
    try:
        obj = {'status': True}
        actual_delimiter = detect_delimiter(localfile)
        # to check if the file has the expected_delimiter (configured one).
        if actual_delimiter != expected_delimiter:
            obj['status'] = False
            obj['actual_delimiter'] = actual_delimiter
            obj['expected_delimiter'] = expected_delimiter
            return obj
        else:
            with open(localfile, "r") as f:
                delimiter_effected_rows = []
                # 1-based indexing
                for line_number, line in enumerate(f, start=1):
                    if line_number == 1:
                        no_of_cols = len(line.split(expected_delimiter))
                        print(
                            "check_delimiter_consistency::No of columns in the file ", no_of_cols)
                    elif len(line.split(expected_delimiter)) != no_of_cols:
                        delimiter_effected_rows.append(line_number)
                obj['delimiter_validation_result'] = delimiter_effected_rows
                print("check_delimiter_consistency::Rows with unexpected delimiters ",
                      delimiter_effected_rows)
                return obj
    except Exception as error:
        print("check_delimiter_consistency ", error)
        raise error


def get_delimiter_of_file(record, file_errors, local_file, terminate_flag):
    """This method is for checking the file delimiter """
    try:
        if record['standard_validations'] and record['standard_validations']['delimiter'] == "Yes":
            expected_delimiter = record['delimiter']
            delimiter_validation_result_obj = check_delimiter_consistency(
                local_file, expected_delimiter)
            if delimiter_validation_result_obj['status']:
                del delimiter_validation_result_obj['status']
                logger.info(
                    f"This file has a valid delimiter: {expected_delimiter}" + "  " + get_current_date_time())
                file_validation_data['file_delimiter'] = forming_values(
                    [delimiter_validation_result_obj, "PASS"], "Passed")
            else:
                del delimiter_validation_result_obj['status']
                logger.info("This file doesn't have a valid delimiter" +
                            "  " + get_current_date_time())
                file_validation_data['file_delimiter'] = forming_values(
                    [delimiter_validation_result_obj, "FAIL"], "Failure")
                file_errors[0] += 1
                if terminate_flag:
                    raise FileDelimiterCheckError(
                        f"File Delimiter check failed for this file : {record['file_name']}")
    except Exception as e:
        print("get_delimiter_of_file::Error ", e)
        logger.error(
            f"Error retrieving file delimiter from S3: {e}" + '   ' + get_current_date_time())
        raise e


def get_file_encodings(record, file_errors, local_src_file, terminate_flag):
    """This method is for checking the file encoding """
    try:
        file_config_encoding = record['standard_validations']['encoding'] \
            if record.get('standard_validations') and record['standard_validations'].get('encoding') is not None and \
            record['standard_validations']['encoding'] != "" else None
        if file_config_encoding is not None:
            detector = UniversalDetector()
            local_src_file_handle = open(local_src_file, 'rb')
            for line in local_src_file_handle.readlines():
                detector.feed(line)
                if detector.done:
                    break
            detector.close()
            local_src_file_handle.close()
            result = detector.result
            src_file_encoding = result['encoding']
            src_file_encoding = src_file_encoding.upper()
            logger.info("Job config - File encoding : " + file_config_encoding)
            logger.info(
                f"Detected encoding for the file {local_src_file} : {src_file_encoding}")
            if file_config_encoding == src_file_encoding:
                logger.info(
                    f"This file has a valid file encoding: {src_file_encoding}")
                print(
                    f"This file has a valid file encoding: {src_file_encoding}")
                file_validation_data['file_encoding'] = forming_values(
                    [src_file_encoding, "PASS", file_config_encoding], "Passed")
            else:
                logger.error(
                    f"The file has a different encoding than expected. Actual : {src_file_encoding}, Expected : {file_config_encoding}")
                file_validation_data['file_encoding'] = forming_values(
                    [src_file_encoding, "FAIL", record['standard_validations']['encoding']], "Failure")
                file_errors[0] += 1
                if terminate_flag:
                    raise FileEncodingCheckError("File encoding check failed")
        else:
            logger.info("No File encoding check for this job : " +
                        str(record['job_id']))
    except Exception as e:
        logger.error(f"Error in getting file encoding file from S3: {e}")
        print("get_file_encodings::Error ", e)
        raise e


def get_file_extension(record, file_errors, local_file, terminate_flag):
    """This method is for checking the file file extension """
    try:
        split_tup = os.path.splitext(local_file)
        file_extension = split_tup[1].replace(".", "")
        file_extension = file_extension.upper()
        file_type = record['file_config_details']['file_type']
        if file_type == file_extension:
            logger.info(
                f"This file has a valid file extension: {file_extension}")
            print(f"This file has a valid file extension: {file_extension}")
            file_validation_data['file_extension'] = forming_values(
                [file_extension, "PASS"], "Passed")
        else:
            logger.info("This file doesn't have a valid file extension")
            file_validation_data['file_extension'] = forming_values(
                [file_extension, "FAIL"], "Failure")
            file_errors[0] += 1
            if terminate_flag:
                raise FileExtensionCheckError(
                    f"File extension failed for this file. Expected : {file_type}, found : {file_extension}")
    except Exception as e:
        logger.error(f"Error getting file extension from S3: {e}")
        print("get_file_extension::Error ", e)
        raise e


def extract_numerical_and_non_numerical_parts(word):
    """This method is for extracting the numerical and non-numerical part"""
    parts = re.split(r'(\d+)', word)
    size_value = parts[1] if len(parts) > 1 else None
    unit_of_data = parts[2] if len(parts) > 0 else None
    return int(size_value), unit_of_data


def forming_values(value, status_type):
    if isinstance(value, list):
        value = tuple(value)

    return {"value": value, "status_type": status_type}


class FileEncodingCheckError(Exception):
    """Custom exception class"""

    def __init__(self, message):
        super().__init__(message)


class FileSizeCheckError(Exception):
    """Custom exception class"""

    def __init__(self, message):
        super().__init__(message)


class FileExtensionCheckError(Exception):
    """Custom exception class"""

    def __init__(self, message):
        super().__init__(message)


class FileDelimiterCheckError(Exception):
    """Custom exception class"""

    def __init__(self, message):
        super().__init__(message)

# if __name__ == "__main__":
#     get_file_encodings(None,None,"./app.py")
