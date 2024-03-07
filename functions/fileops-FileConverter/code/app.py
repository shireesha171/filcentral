"""This module is for File Converter"""

from .interfaces.source_file_config import TargetFileConfigDetails, TargetFileTypes
from .interfaces.target_config import TargetConfig
from .services.db_data_service import get_target_file_config_details, get_target_config_data
from .services.parquet_service import create_parquet_bytes, get_partitions
from .services.s3_service import S3Service, DataTransformationS3Service
from .services.transform_data_service import transform_data
from .utils.utils import get_s3_bucket_name, handle_errors, parse_location_pattern


@handle_errors
def lambda_handler(event, context):
    """This handler is for creating and getting the source file configurations"""
    print("lambda_handler::app", "Received Event: ", str(event))

    # Fetch job_uuid, job_run_uuid, file_location, file_name
    job_uuid = event['job_uuid']
    source_file_path = event["source_file_path"]
    source_file_name = event["source_file_name"]

    # Fetch S3 Bucket Name
    s3_bucket_name = get_s3_bucket_name()

    # Fetch target_file_config_details
    target_file_config_details: TargetFileConfigDetails = get_target_file_config_details(job_uuid)

    # Fetch target config data
    target_config_data: TargetConfig = get_target_config_data(job_uuid)

    # Parse target config data
    target_bucket_name, target_object_path = parse_location_pattern(target_config_data.location_pattern)

    # Transform Data
    target_metadata, transformed_df = transform_data(job_uuid=job_uuid,
                                                     file_path=source_file_path,
                                                     file_name=source_file_name,
                                                     s3_bucket_name=s3_bucket_name)

    # Write transformed df to S3

    # For now we have CSV. Need to extend it to other file types.
    if target_file_config_details.target_file_type == TargetFileTypes.CSV:
        DataTransformationS3Service.write_target_file_to_s3(bucket_name=target_bucket_name,
                                                            object_path=target_object_path,
                                                            target_df=transformed_df)

    # Convert DataFrame to Parquet format with partitions
    if target_file_config_details.target_file_type == TargetFileTypes.PARQUET:
        parquet_partitions = get_partitions(target_metadata)

        # Write parquet to S3
        s3_filesystem = S3Service.get_s3_bucket_filesystem(target_bucket_name)

        parquet_target_path = f"{target_bucket_name}/{target_object_path}"

        create_parquet_bytes(df=transformed_df, partitions=parquet_partitions, file_system=s3_filesystem,
                             target_path=parquet_target_path,
                             )

    return {
        'statusCode': 200,
        'body': f"{__name__} exited successfully"
    }
