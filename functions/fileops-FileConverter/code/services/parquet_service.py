import os

import pyarrow as pa
import pyarrow.parquet as pq

from .s3_service import S3Service


def create_parquet_bytes(df, partitions, file_system, target_path):

    print("create_parquet_bytes::parquet_service", "Creating Parquet Bytes")

    # Write DataFrame to a Parquet file in memory
    try:

        table = pa.Table.from_pandas(df)

        pq.write_to_dataset(table, root_path=target_path, partition_cols=partitions, compression='snappy',
                            filesystem=file_system, use_threads=True)
    except Exception as e:
        print("create_parquet_bytes::parquet_service", f"Failed to create a parquet file in memory: {e}")
        raise e


def get_partitions(target_metadata):
    print("get_partitions::parquet_service", "Fetching partitions")

    partitions = [partition for partition in target_metadata if

                  partition.get("parquet_partition_order") is not None]

    sorted_partitions = sorted(partitions, key=lambda x: x['parquet_partition_order'])

    partition_target_columns = [partition.get("target_column") for partition in sorted_partitions]

    return partition_target_columns


def write_parquet_to_s3(s3_bucket_name, temp_file_path, s3_folder_path):
    for root, dirs, files in os.walk(temp_file_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.join(s3_folder_path, os.path.relpath(local_file_path, temp_file_path))

            S3Service.upload_file_s3(bucket_name=s3_bucket_name, local_file_path=local_file_path, key=s3_key)
