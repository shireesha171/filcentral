import csv
import os
import urllib.parse as parse
from io import BytesIO
import boto3
import chardet
import pandas as pd
from botocore.exceptions import ClientError
from ydata_profiling import ProfileReport

s3 = boto3.client('s3')


def get_file_from_s3(s3_bucket_name, s3_filepath):
    """This method is to get the file s3 bucket"""
    try:
        s3_response = s3.get_object(Bucket=s3_bucket_name, Key=s3_filepath)
        return s3_response
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise FileNotFoundError(f"File {s3_filepath} not found in bucket {s3_bucket_name}.")
        elif e.response['Error']['Code'] == "403":
            raise PermissionError(f"Access denied to file {s3_filepath} in bucket {s3_bucket_name}.")
    except Exception as error:
        print("get_file_from_s3:: Unknown error occurred during the file reading from s3")
        raise error


def decode_content(object_content_bytes):
    """This method is for decoding the s3 file content with appropriate file encoding"""
    try:
        detected_encoding = chardet.detect(object_content_bytes)['encoding']
        if detected_encoding is None:
            raise ValueError("Could not detect the encoding of the file content.")
        decoded_file_content = object_content_bytes.decode(detected_encoding)
        return decoded_file_content
    except Exception as e:
        print(f"decode_content::Error decoding file {e}")
        raise e


def get_delimiter(object_content_bytes):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = decode_content(object_content_bytes)

        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        file_delimiter = sniffer.sniff(sample, delimiters='|\t,:;').delimiter
        return file_delimiter
    except csv.Error as csv_error:
        raise csv_error
    except Exception as error:
        print(f"getDelimiter::Unknown error occurred during the detection of delimiter for the file {str(error)}")
        raise error


try:
    # Define the output file names
    html_file_name = 'y_data_profile.html'
    json_file_name = 'y_data_profile.json'

    if not os.environ['file_path']:
        raise FileNotFoundError("unable to get the path from airflow")

    filepath = os.environ['file_path']
    bucket_name = filepath.split('__init__')[0]
    filepath = filepath.split('__init__')[1]

    print("Bucket name: ", bucket_name)
    print("filepath: ", filepath)

    job_id = str()
    if filepath is not None:
        filepath = parse.unquote(filepath)
        job_id = filepath.split('/')[1]

    response = get_file_from_s3(bucket_name, filepath)

    # Object Content is of type bytes
    object_content = response['Body'].read()

    # calling getDelimiter method to get the delimiter
    delimiter = get_delimiter(object_content)
    print(f"The delimiter detected for the file : {delimiter}")

    df = pd.read_csv(BytesIO(object_content), delimiter=delimiter, on_bad_lines='skip')

    profile = ProfileReport(df, title="Profiling_Report")

    # Generate HTML report and save to S3
    html_output = profile.to_html()
    s3.put_object(Body=html_output, Bucket=bucket_name, Key=f"profiling/{job_id}/{html_file_name}",
                  ContentType='text/html')

    # Generate JSON report and save to S3
    json_output = profile.to_json()
    s3.put_object(Body=json_output, Bucket=bucket_name, Key=f"profiling/{job_id}/{json_file_name}",
                  ContentType='application/json')
except csv.Error as csv_error:
    print("The job onboarding failed due to invalid file format or delimiter ", str(csv_error))
except ValueError as value_error:
    print(f"value error: {str(value_error)}")
except FileNotFoundError as file_not_found_error:
    print(f"File not found in S3 bucket: {str(file_not_found_error)}")
except Exception as error:
    print("Unexpected error occurred", error)
