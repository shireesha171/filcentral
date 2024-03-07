"""This module is for validating process"""
import csv
import json
import os
import boto3
import chardet
import numpy as np
import pandas as pd
import logging

from datetime import datetime
from functools import reduce
from io import BytesIO
from urllib.parse import urlparse

from .data_validations import mandatoryComparision
from .utils.utils import get_current_date_time, check_date_time

env = os.environ.get('Environment')
s3_storage = os.environ.get('S3FileStorage')
s3_bucket = f'{s3_storage}-{env}'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("validation-process.schema_validations")


def getSchemaValidations(business_validate_id, record, logger1, query_params):
    """This method is for handling validation process"""

    if record is not None and record['metadata'] is not None:
        metadata = record['metadata']
        metadata_df = pd.json_normalize(metadata)
        metadata_df.set_index('column', inplace=True)
    else:
        logger.error("metaData not found   ", + get_current_date_time())
        print("metaData not found")

    if 'job_type' in query_params and query_params['job_type'] == "Scheduled":
        print("inside schema validation")
        parsed_s3_path = urlparse(query_params['location_pattern'])
        # bucket_name = s3_bucket
        bucket_name = parsed_s3_path.netloc
        object_key = parsed_s3_path.path.lstrip('/')
        folders = object_key

        # folder_path = folders + '/' + query_params['file_name_pattern']
        date_prefix = folders.split("/")[1]
        if "{" in date_prefix and "}" in date_prefix:
            date_prefix = date_prefix.replace("{", "").replace("}", "")
        else:
            print("Date prefix error", date_prefix)

        print("folder_prefix", datetime.now().strftime(date_prefix))
        # location_pattern = query_params['location_pattern'].replace("S3://", '').replace("s3://", '')
        # folder_path = location_pattern + query_params['file_name_pattern']
        # folder_path = f"patterns/" + datetime.now().strftime(date_prefix) + "/" + \
        #               query_params['file_name_pattern']
        folder_path = folders + query_params['file_name_pattern']

        # folder_path = f"patterns/" + datetime.now().strftime(date_prefix) + "/" + \
        #               query_params['file_name_pattern']
    elif 'job_type' in query_params and query_params['job_type'] == "s3_trigger":
        folder_path = query_params['key']
        bucket_name = query_params['bucket_name']
    else:
        folder_path = 'sample-files/' + business_validate_id + "/"
        bucket_name = s3_bucket

    s3_object = getfilefromS3(folder_path, bucket_name)

    if s3_object is None:
        logger.error(
            "No Sample file found neither format not support  " + get_current_date_time())
        return response_body(400, "", body="No Sample file found neither format not support")

    source_df = creatingDataframe(s3_object['file'], s3_object['Body'])
    num_of_schema_validation_errors, data = actualData(
        source_df, metadata_df, record)
    data["mandatory_column_data"] = mandatoryComparision(
        source_df, metadata_df)
    # data["mandatory_columns"] = data_validations.checkingMandatoryColumns(source_df, metadata_df)

    return num_of_schema_validation_errors, data


#  creating actual validation part
def actualData(source_df, metadata_df, record):
    """This method is for actual data"""

    num_of_schema_validation_errors = [0]  # Pass by reference

    try:
        dq_rules = record['dqrules']
        standard_validation = record['standard_validations']
        profile_column = source_df.columns
        metadata_column = metadata_df.index
        emptyvalues = source_df.isnull().sum()

        obj = {}
        obj['total_columns'] = forming_values(
            len(profile_column), None, num_of_schema_validation_errors)
        logger.info("getting total column  " + get_current_date_time())
        # checking  additinal columns
        additional_columns = list(set(profile_column) - set(metadata_column))
        obj["additional_columns"] = forming_values(
            additional_columns, "Failure" if len(additional_columns) > 0 else "Passed", num_of_schema_validation_errors)
        # obj["additional_columns"] = list(set(profile_column) - set(metadata_column))
        logger.info("validating additional columns  " +
                    get_current_date_time())
        # checking missing columns
        missing_columns = list(set(metadata_column) - set(profile_column))
        obj["missing_columns"] = forming_values(
            missing_columns, "Failure" if len(additional_columns) > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating missing columns  " + get_current_date_time())
        # checking missing content
        missing_content = len(list(filter(lambda x: x > 0, emptyvalues)))
        obj["missing_content"] = forming_values(
            missing_content, "Failure" if missing_content > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating missing_content  " + get_current_date_time())
        # checking total null count
        total_null_count = reduce(lambda x, y: x + y, emptyvalues)
        obj["total_null_count"] = forming_values(
            total_null_count, "Failure" if total_null_count > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating total_null_count  " + get_current_date_time())
        # checking duplicate_records_by_row
        duplicate_records_by_row = check_duplicate_records(
            source_df, standard_validation)
        obj["duplicate_records_by_row"] = forming_values(
            duplicate_records_by_row, "Failure" if duplicate_records_by_row > 0 else "Passed",
            num_of_schema_validation_errors)
        logger.info("validating duplicate_records_by_row  " +
                    get_current_date_time())
        data_check = data_type_checks(source_df, metadata_df)
        obj['data_check_passed'] = forming_values(data_check.get(
            'Passed'), "Passed" if len(data_check.get('Passed')) > 0 else "Failure", num_of_schema_validation_errors)
        logger.info("validating data_check_passed  " + get_current_date_time())
        obj['data_check_failure'] = forming_values(data_check.get(
            'failed'), "Failure" if len(data_check.get('failed')) > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating data_check_failure  " +
                    get_current_date_time())
        range_rows = range_check_main(source_df, dq_rules, metadata_df)
        obj['range_rows'] = forming_values(
            range_rows, "Failure" if len(range_rows) > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating range_rows  " + get_current_date_time())
        list_value_rows = list_value_check_main(
            source_df, dq_rules, metadata_df)
        obj['list_value_rows'] = forming_values(
            list_value_rows, "Failure" if len(list_value_rows) > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating list_value_rows  " + get_current_date_time())
        Number_of_range_mismatch = [{"column_name": item['column_name'], "count": len(
            item['data'])} for item in obj["range_rows"]['value']]
        range_mismatch_status_type = (
            x for x in Number_of_range_mismatch if x['count'] > 0)
        range_count = next(range_mismatch_status_type, None)
        obj['Number_of_range_mismatch'] = forming_values(
            Number_of_range_mismatch, "Failure" if range_count is not None else "Passed",
            num_of_schema_validation_errors)
        logger.info("validating Number_of_range_mismatch  " +
                    get_current_date_time())
        number_of_list_value_error = [{"column_name": item['column_name'], "count": len(
            item['data'])} for item in obj["list_value_rows"]['value']]
        number_of_list_value_status_type = (
            x for x in number_of_list_value_error if x['count'] > 0)
        list_count = next(number_of_list_value_status_type, None)
        obj['number_of_list_value_error'] = forming_values(
            number_of_list_value_error, "Failure" if list_count is not None else "Passed",
            num_of_schema_validation_errors)
        logger.info("validating number_of_list_value_error " +
                    get_current_date_time())
        obj['number_of_range_mismatch_success'] = forming_values(len(
            [item for item in obj['Number_of_range_mismatch']['value'] if item['count'] == 0]), None,
            num_of_schema_validation_errors)
        logger.info(
            "validating number_of_range_mismatch_success  " + get_current_date_time())
        obj['number_of_list_value_success'] = forming_values(len(
            [item for item in obj['number_of_list_value_error']['value'] if item['count'] == 0]), None,
            num_of_schema_validation_errors)
        logger.info("validating number_of_list_value_success  " +
                    get_current_date_time())
        obj['range_mismatch_count'] = forming_values(sum(data_dict.get(
            'count', 0) for data_dict in obj['Number_of_range_mismatch']['value']), None,
                                                     num_of_schema_validation_errors)
        logger.info("validating range_mismatch_count  " +
                    get_current_date_time())
        obj['list_value_count'] = forming_values(sum(data_dict.get(
            'count', 0) for data_dict in obj['number_of_list_value_error']['value']), None,
                                                 num_of_schema_validation_errors)
        logger.info("validating list_value_count  " + get_current_date_time())
        null_rows = getting_null_rows(source_df)
        obj['null_rows'] = forming_values(
            null_rows, "Failure" if len(null_rows) > 0 else "Passed", num_of_schema_validation_errors)
        logger.info("validating null_rows  " + get_current_date_time())
        return num_of_schema_validation_errors, obj
    except Exception as error:
        print("actualData::Error ", error)
        raise error


def creatingDataframe(key, body):
    """This method is creatingDataframe"""
    if key.endswith("csv"):
        try:
            object_content = body.read()
            delimiter = getDelimiter(object_content)
            print("creatingDataframe:Delimiter ", delimiter)
            return pd.read_csv(BytesIO(object_content), delimiter=delimiter, on_bad_lines='skip')
        except pd.errors.ParserError as pd_error:
            raise pd_error
    elif key.endswith("xlsx"):
        return pd.read_excel(body)
    else:
        return None


def getfilefromS3(folder_path, bucket_name):
    """This method is getting file from S3"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_path)
        for obj in response['Contents']:
            if obj['Key'].endswith(".csv"):
                s3_object = s3_client.get_object(
                    Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'csv'}
            elif obj['Key'].endswith(".xlsx"):
                s3_object = s3_client.get_object(
                    Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'xlsx'}
            elif obj['Key'].endswith(".json"):
                s3_object = s3_client.get_object(
                    Bucket=bucket_name, Key=obj['Key'])
                return {"Body": s3_object['Body'], "file": 'json'}
        return None
    except Exception as error:
        print("The Error:", str(error))
        return str(error)


def decodeContent(object_content_bytes):
    """This method is for decoding the s3 file content with appropriate file encoding"""
    try:
        detected_encoding = chardet.detect(object_content_bytes)['encoding']
        decoded_file_content = object_content_bytes.decode(detected_encoding)
    except UnicodeDecodeError:
        fallback_encoding = 'utf-8'
        decoded_file_content = object_content_bytes.decode(fallback_encoding)
    return decoded_file_content


def getDelimiter(object_content_bytes):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = decodeContent(object_content_bytes)

        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        file_delimiter = sniffer.sniff(
            sample, delimiters='|,:;\t').delimiter
        return file_delimiter
    except csv.Error:
        return ','


def data_type_checks(source_df, metadata_df):
    try:
        meta_columns = metadata_df.index
        condition = metadata_df['dtype'] == ("string")
        metadata_df.loc[condition, 'dtype'] = 'object'
        check_obj = {}
        check_obj['Passed'] = []
        check_obj['failed'] = []
        for item in meta_columns:
            item_data_type = source_df[item].dtype

            is_date_time = None

            if item_data_type.name == 'object':
                is_date_time = check_date_time(source_df, item)

            if item in source_df.columns and (
                    (metadata_df.loc[item]['dtype'] == source_df[item].dtype) or is_date_time):
                check_obj['Passed'].append(item)
            else:
                check_obj['failed'].append(item)
        return check_obj
    except Exception as error:
        print("data_type_checks::Error ", error)


def range_check_fun(source_df, range_check, column):
    return source_df[(source_df[column] < range_check[0]) | (source_df[column] > range_check[1])]


def list_value_check_fun(source_df, list_value, column):
    return source_df[~source_df[column].isin(list_value)]


def list_value_check_main(source_df, dq_rules, metadata_df):
    try:
        if dq_rules is not None:
            dqrules = dq_rules
            keys = list(dqrules.keys())
            column_mismatch = []
            for item in keys:
                if item in source_df.columns and item in metadata_df.index:
                    if metadata_df.loc[item]['dtype'] == "object":
                        data = list_value_check_fun(source_df, dq_rules.get(item)[
                            'list_of_value_check'], item)
                        obj = {"column_name": item,
                               "data": data.to_dict(orient='records')}
                        column_mismatch.append(obj)
            return column_mismatch

        else:
            return []

    except Exception as error:
        print("list_value_check_main::Error ", error)
        raise error


def range_check_main(source_df, dq_rules, metadata_df):
    try:
        if dq_rules is not None:
            dqrules = dq_rules
            keys = list(dqrules.keys())
            column_mismatch = []
            for item in keys:
                if item in source_df.columns and item in metadata_df.index:
                    if metadata_df.loc[item]['dtype'] == "int64":
                        data = range_check_fun(source_df, dq_rules.get(item)[
                            'range_check'], item)

                        if data.size > 0:
                            obj = {"column_name": item,
                                   "data": data.to_dict(orient='records')}
                            column_mismatch.append(obj)
            return column_mismatch
        else:
            return []

    except Exception as error:
        print("range_check_main::Error ", error)


def check_duplicate_records(source_df, standard_validation):
    try:
        column_list = source_df.columns.tolist()
        if "ignore_columns" in standard_validation:
            ignore_columns = standard_validation['ignore_columns']
            column_list = list(set(column_list) - set(ignore_columns))
        duplicate_rows = source_df[source_df.duplicated(
            subset=column_list, keep="first")]
        return len(duplicate_rows)
    except Exception as error:
        print("check_duplicate_records::Error", error)
        raise error


def getting_null_rows(source_df):
    try:
        null_columns_data = []
        null_rows = source_df.loc[:, source_df.isnull().any()]
        for item in null_rows.columns:
            df = source_df.loc[source_df[item].isnull(), :]
            df_filled = df.replace({np.nan: None})
            null_columns_data.append(
                {"column_name": item, "data": df_filled.to_dict(orient='records')})
        return null_columns_data

    except Exception as error:
        print("getting_null_rows::Error ", error)
        raise error


def forming_values(value, status_type, num_of_schema_validation_errors):
    if isinstance(value, list):
        value = tuple(value)

    if status_type == "Failure":
        num_of_schema_validation_errors[0] += 1

    return {"value": value, "status_type": status_type}


def response_body(statuscode, message, body):
    """This is response method"""
    response = {
        "statusCode": statuscode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps({"message": message, "data": body})
    }
    return response
