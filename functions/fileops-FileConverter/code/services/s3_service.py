from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from s3fs import S3FileSystem

from ..utils.utils import FILE_EXTENSIONS, CONSTANTS


class S3Service:

    @staticmethod
    def read_file_s3(bucket_name, key):
        """Read file to S3"""
        try:
            s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            object_content = response['Body'].read()

            if object_content is not None:

                return object_content
            else:
                return None
        except ClientError as e:
            print("read_file_s3::S3Service::s3_service", f"Reading data from S3::error caught while connecting s3: {e}")
            raise e

    @staticmethod
    def write_file_s3(bucket_name, key, content):
        """Write file to S3"""
        try:
            s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
            s3_client.put_object(Body=content, Bucket=bucket_name, Key=key)

            print("write_file_s3::S3Service::s3_service",
                  f"File uploaded to S3 bucket '{bucket_name}' with key '{key}'.")
        except ClientError as e:
            print("write_file_s3::S3Service::s3_service", f"Error uploading Parquet file to S3: {e}")
            raise e

    @staticmethod
    def upload_file_s3(bucket_name, local_file_path, key):
        """Upload a file to an S3 bucket"""
        try:
            s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
            s3_client.upload_file(local_file_path, bucket_name, key)

            print("upload_file_s3::S3Service::s3_service",
                  f"File uploaded to S3 bucket '{bucket_name}' with key '{key}'.")
        except ClientError as e:
            print("upload_file_s3::S3Service::s3_service", f"Error uploading Parquet file to S3: {e}")
            raise e

    @staticmethod
    def get_s3_bucket_filesystem(bucket_name):
        """Get S3 bucket filesystem"""

        s3 = S3FileSystem()

        try:
            s3.ls(bucket_name)
        except:
            raise Exception("get_s3_bucket_filesystem::s3_service", f"Bucket '{bucket_name}' is not found")

        return s3


class DataTransformationS3Service:

    @staticmethod
    def get_source_file_from_s3(bucket_name, file_path, file_name):
        """This method is used for fetching the file from S3"""

        try:
            if file_path is not None:

                content = S3Service.read_file_s3(bucket_name, file_path)

                if file_name.endswith(f".{FILE_EXTENSIONS.CSV.value}"):
                    return {"Body": content, "file": FILE_EXTENSIONS.CSV.value}
                elif file_name.endswith(f".{FILE_EXTENSIONS.XLSX.value}"):
                    return {"Body": content, "file": FILE_EXTENSIONS.XLSX.value}
                elif file_name.endswith(f".{FILE_EXTENSIONS.JSON.value}"):
                    return {"Body": content, "file": FILE_EXTENSIONS.JSON.value}
            return None
        except Exception as e:
            print("get_source_file_from_s3::DataTransformationS3Service::s3_service", f"Unknown error caught: {e}")
            raise e

    @staticmethod
    def write_target_file_to_s3(bucket_name, object_path, target_df):
        """This method is used for writing target file (transformed) to S3"""

        file_key = object_path + "/" + CONSTANTS.S3_TARGET_FILE_NAME.value

        if isinstance(target_df, dict):
            target_df = pd.DataFrame.from_dict(target_df)

        csv_buffer = StringIO()
        target_df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        S3Service.write_file_s3(bucket_name=bucket_name, key=file_key, content=csv_data)
