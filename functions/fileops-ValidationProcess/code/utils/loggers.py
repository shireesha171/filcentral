from datetime import datetime

from ..libs.job_run_logger.data_classes import FileValidation, SchemaValidation
from ..libs.job_run_logger.enums import FileValidationTypes, ValidationResultTypes, SchemaValidationTypes, TriggerTypes
from .utils import get_job_validation_status
from ..libs.job_run_logger.logger import JobRunLogger

# Job Run Logger
job_run_logger = JobRunLogger()


def _map_validation_type(status_type):
    if status_type == "Passed":
        return ValidationResultTypes.PASS
    elif status_type == "Failure":
        return ValidationResultTypes.FAIL


def _map_trigger_type(trigger_type):
    if trigger_type == "Scheduled":
        return TriggerTypes.SCHEDULED
    elif trigger_type == "Ad hoc":
        return TriggerTypes.MANUAL
    elif trigger_type == 's3_trigger':
        return TriggerTypes.S3_TRIGGER


def log_file_validations(file_name,file_errors, file_validation_data):
    job_run_logger.number_of_file_validation_errors = file_errors[0]
    file_validation_arr = []
    for file_validation_type in file_validation_data.keys():
        file_validation_item = file_validation_data.get(file_validation_type)
        file_validation_obj = None
        status_type = file_validation_item.get("status_type")
        mapped_status_type = _map_validation_type(status_type)
        if file_validation_type == "file_exists":
            file_validation_obj = FileValidation(FileValidationTypes.FILE_EXIST_CHECK, file_name, mapped_status_type, datetime.now().isoformat() )
            file_validation_arr.append(file_validation_obj)
        elif file_validation_type == "file_encoding":
            file_validation_obj = FileValidation(FileValidationTypes.FILE_ENCODE_CHECK, file_name, mapped_status_type,  datetime.now().isoformat())
        elif file_validation_type == "file_size_range":
            file_validation_obj = FileValidation(FileValidationTypes.FILE_SIZE_CHECK, file_name, mapped_status_type,  datetime.now().isoformat())
        elif file_validation_type == "file_delimiter":
            file_validation_obj = FileValidation(FileValidationTypes.FILE_DELIMITER_CHECK, file_name, mapped_status_type,
                                                 datetime.now().isoformat())
        elif file_validation_type == "file_extension":
            file_validation_obj = FileValidation(FileValidationTypes.FILE_EXTENSION_CHECK, file_name, mapped_status_type,
                                                 datetime.now().isoformat())
        if file_validation_obj is not None:
            file_validation_arr.append(file_validation_obj)
    job_run_logger.file_validations = file_validation_arr


def log_schema_validations(number_of_schema_validation_errors, schema_validation_data):
    # The error count is in the 0th index of the number_of_schema_validation_errors array
    job_run_logger.number_of_schema_validation_errors = number_of_schema_validation_errors[0]

    schema_validation_arr = []

    for schema_validation_type in schema_validation_data.keys():
        schema_validation_item = schema_validation_data.get(schema_validation_type)

        if type(schema_validation_item) is not dict:
            continue

        status_type = schema_validation_item.get("status_type")
        mapped_status_type = _map_validation_type(status_type)

        value = schema_validation_item.get("value", None)

        schema_validation_obj = None

        if schema_validation_type == "total_columns":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.TOTAL_COLUMNS, value, mapped_status_type)
        elif schema_validation_type == "additional_columns":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.ADDITIONAL_COLUMNS_CHECK, value,
                                                     mapped_status_type)
        elif schema_validation_type == "missing_columns":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.MISSING_COLUMNS_CHECK, value,
                                                     mapped_status_type)
        elif schema_validation_type == "missing_content":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.MISSING_CONTENT_CHECK, value,
                                                     mapped_status_type)
        elif schema_validation_type == "total_null_count":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.TOTAL_NULL_COUNT, value, mapped_status_type)
        elif schema_validation_type == "duplicate_records_by_row":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.DUPLICATE_RECORDS_BY_ROW, value,
                                                     mapped_status_type)
        elif schema_validation_type == "data_check_failure":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.DATA_CHECK_FAILURE, value,
                                                     mapped_status_type)
        elif schema_validation_type == "range_rows":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.RANGE_ROWS_CHECK, value, mapped_status_type)
        elif schema_validation_type == "list_value_rows":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.LIST_VALUES_ROWS_CHECK, value,
                                                     mapped_status_type)
        elif schema_validation_type == "Number_of_range_mismatch":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.NUMBER_OF_RANGE_MISMATCH, value,
                                                     mapped_status_type)
        elif schema_validation_type == "number_of_list_value_error":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.NUMBER_OF_LIST_VALUE_ERROR, value,
                                                     mapped_status_type)
        elif schema_validation_type == "number_of_range_mismatch_success":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.NUMBER_OF_RANGE_MISMATCH_SUCCESS, value,
                                                     mapped_status_type)
        elif schema_validation_type == "number_of_list_value_success":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.NUMBER_OF_LIST_VALUE_SUCCESS, value,
                                                     mapped_status_type)
        elif schema_validation_type == "range_mismatch_count":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.RANGE_MISMATCH_COUNT, value,
                                                     mapped_status_type)
        elif schema_validation_type == "list_value_count":
            schema_validation_obj = SchemaValidation(SchemaValidationTypes.LIST_VALUE_COUNT, value, mapped_status_type)

        if schema_validation_obj is not None:
            schema_validation_arr.append(schema_validation_obj)

    job_run_logger.schema_validations = schema_validation_arr


def log_job_validation_status(job_validation_status):
    """
    Updates the Job Validation Status based on file validation and schema validation errors
    """

    job_run_logger.job_validation_status = job_validation_status


def log_job_metadata(job_run_data):
    job_run_logger.business_process_name = job_run_data.get("business_process_name", None)
    job_run_logger.job_name = job_run_data.get("job_name", None)
    job_run_logger.trigger_by = f"{job_run_data.get('first_name', None)} {job_run_data.get('last_name', None)}"
    job_type = job_run_data.get("job_type", None)
    mapped_job_type = _map_trigger_type(job_type)
    job_run_logger.trigger_type = mapped_job_type
    job_run_logger.job_status = job_run_data.get("status", None)


def log_job_run_timestamp():
    job_run_logger.started_time = datetime.now().isoformat()
    yield f"{job_run_logger.started_time}"
    job_run_logger.completed_time = datetime.now().isoformat()
    yield f"{job_run_logger.completed_time}"


def log_file_metadata(**file_metadata):
    job_run_logger.file_name = file_metadata.get("file_name", None)
    job_run_logger.file_path = file_metadata.get("file_path", None)
    job_run_logger.bucket_name = file_metadata.get("bucket_name", None)
    job_run_logger.folder_prefix = file_metadata.get("folder_prefix", None)
