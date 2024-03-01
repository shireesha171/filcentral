"""This module is for source file configuration"""
import json

from .services.transform_data_service import transform_data
from .services.parquet_service import create_parquet_bytes, get_parquet_partition_data, get_partitions, \
    write_parquet_to_s3
from .services.s3_service import S3Service, DataTransformationS3Service

from .utils.utils import get_s3_bucket_name, get_s3_target_path, handle_errors


@handle_errors
def lambda_handler(event, context):
    """This handler is for creating and getting the source file configurations"""
    print("lambda_handler::app", "Received Event: ", str(event))

    # Fetch job_uuid, job_run_uuid, file_location, file_name
    job_uuid = event['job_uuid']
    job_run_uuid = event['job_run_uuid']
    source_file_path = event["source_file_path"]
    source_file_name = event["source_file_name"]

    # Fetch S3 Bucket Name
    s3_bucket_name = get_s3_bucket_name()

    # Transform Data
    target_metadata, transformed_df = transform_data(job_uuid=job_uuid, file_path=source_file_path,
                                                     file_name=source_file_name,
                                                     s3_bucket_name=s3_bucket_name)

    # Write transformed df to S3

    DataTransformationS3Service.write_target_file_to_s3(bucket_name=s3_bucket_name, job_uuid=job_uuid,
                                                        job_run_uuid=job_run_uuid,
                                                        target_df=transformed_df)

    # Convert DataFrame to Parquet format with partitions

    parquet_partitions = get_partitions(target_metadata)

    # Write parquet to S3
    parquet_target_path = get_s3_target_path(job_run_uuid)

    s3_filesystem = S3Service.get_s3_bucket_filesystem(s3_bucket_name)

    create_parquet_bytes(df=transformed_df, partitions=parquet_partitions, file_system=s3_filesystem,
                         target_path=parquet_target_path,
                         )

    return {
        'statusCode': 200,
        'body': f"{__name__} exited successfully"
    }
