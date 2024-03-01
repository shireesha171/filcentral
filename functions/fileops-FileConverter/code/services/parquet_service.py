import os

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from dbconnection import pool, getConnection
from .s3_service import S3Service


def create_parquet_bytes(df, partitions, file_system, target_path):
    # Write DataFrame to a Parquet file in memory
    try:

        table = pa.Table.from_pandas(df)

        pq.write_to_dataset(table, root_path=target_path, partition_cols=partitions, compression='snappy',
                            filesystem=file_system, use_threads=True)
    except Exception as e:
        print("create_parquet_bytes::parquet_service", f"Failed to create a parquet file in memory: {e}")
        raise e


def get_partitions(target_metadata):
    partitions = [partition for partition in target_metadata if

                  partition.get("parquet_partition_order") is not None]

    sorted_partitions = sorted(partitions, key=lambda x: x['parquet_partition_order'])

    partition_target_columns = [partition.get("target_column") for partition in sorted_partitions]

    return partition_target_columns


def get_parquet_partition_data(job_uuid):
    """This method is used for fetching parquet related metadata from db"""
    conn, cursor = None, None

    try:

        conn, cursor = getConnection()

        get_source_file_configuration_details = """SELECT sfc.target_file_config_details
                                                   FROM jobs j LEFT JOIN source_file_config sfc ON j.id = sfc.job_id 
                                                   WHERE j.uuid = %s      
                                                """

        cursor.execute(get_source_file_configuration_details, (job_uuid,))

        data = cursor.fetchone()

        # data[0] contains target_file_config_details
        return data[0]

    except psycopg2.DatabaseError as e:
        print("get_parquet_partition_data::parquet_service", f"Database error: {e}")
        raise e
    except Exception as e:
        print("get_parquet_partition_data::parquet_service", f"Unknown error caught: {e}")
        raise e
    finally:
        cursor.close()
        pool.putconn(conn)


def write_parquet_to_s3(s3_bucket_name, temp_file_path, s3_folder_path):
    for root, dirs, files in os.walk(temp_file_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.join(s3_folder_path, os.path.relpath(local_file_path, temp_file_path))

            S3Service.upload_file_s3(bucket_name=s3_bucket_name, local_file_path=local_file_path, key=s3_key)
