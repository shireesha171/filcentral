import csv
from io import BytesIO

import chardet
import pandas as pd
import psycopg2

from dbconnection import pool, getConnection
from .s3_service import DataTransformationS3Service
from ..utils.utils import FILE_EXTENSIONS
from ..utils.errors import FileNotFoundError


def get_transformation_metadata(job_id):
    """This method is for getting the transformations applied on columns"""
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()

        target_metadata_query = """
                                    SELECT sfc.target_metadata
                                    FROM source_file_config as sfc
                                    JOIN jobs as j ON sfc.job_id = j.id
                                    where j.uuid  = %s;
                                   """

        cursor.execute(target_metadata_query, (job_id,))
        data = cursor.fetchone()

        return data[0]
    except psycopg2.DatabaseError as e:
        print("get_transformation_metadata::transform_data_service", f"Database Error: {e}")
        return e
    except Exception as e:

        print("get_transformation_metadata::transform_data_service", f"Unknown error caught: {e}")
        return e
    finally:
        cursor.close()
        pool.putconn(conn)


def transform_data(**kwargs):
    """This method is for getting the transformations applied on data"""

    job_uuid = kwargs["job_uuid"]
    s3_bucket_name = kwargs["s3_bucket_name"]
    file_path = kwargs["file_path"]
    file_name = kwargs["file_name"]

    # Get Target Metadata
    target_metadata = get_transformation_metadata(job_uuid)

    target_metadata_values = sorted((item for item in target_metadata if item['status']),
                                    key=lambda x: x['order'])

    target_metadata_df = form_target_df(
        target_metadata_values)  # Creating df with for target file (with status = True columns)

    s3_object = DataTransformationS3Service.get_source_file_from_s3(s3_bucket_name, file_path, file_name)

    if s3_object is None:
        print("transform_data::transform_data_service",
              f"s3_object: {s3_bucket_name}/{file_path}/{file_name} is not found")

        raise FileNotFoundError("transform_data::transform_data_service", "s3_object: {s3_bucket_name}/{"

                                                                          "file_location} is not found")

    # Transform Data
    source_data_df = create_data_frame(s3_object['file'], s3_object['Body'])

    target_data_df = adding_data_df(target_metadata_df, source_data_df, target_metadata_values)

    transformed_data_df = apply_transformation(target_data_df, target_metadata_values)

    return target_metadata, transformed_data_df


def form_target_df(target_values):
    try:
        df = pd.DataFrame(columns=[item['target_column'] for item in target_values])
        return df
    except Exception as e:
        print(f"forming_target_df::transform_data_service", f"Unknown error caught: {e}")
        raise e


def create_data_frame(key, body):
    """This method is for creating a data frame from body"""
    try:
        if key.endswith(FILE_EXTENSIONS.CSV.value):
            delimiter = get_delimiter(body)
            return pd.read_csv(BytesIO(body), delimiter=delimiter)

        elif key.endswith(FILE_EXTENSIONS.XLSX.value):
            return pd.read_excel(body)

        elif key.endswith(FILE_EXTENSIONS.JSON.value):
            return pd.read_json(body)
        else:
            return None
    except Exception as e:
        print("create_data_frame::transform_data_service", f"Unknown error caught:", str(e))
        raise e


def get_delimiter(object_content_bytes):
    """This method is for the delimiter of the file"""
    try:
        # Here calling method to get the file from s3
        file_content = decode_content(object_content_bytes)

        sniffer = csv.Sniffer()
        sample = file_content[:4096]
        file_delimiter = sniffer.sniff(sample, delimiters=[',', ';', ':', '|']).delimiter
        return file_delimiter
    except csv.Error:
        return ','


def decode_content(object_content_bytes):
    """This method is for decoding the s3 file content with appropriate file encoding"""
    try:
        detected_encoding = chardet.detect(object_content_bytes)['encoding']
        decoded_file_content = object_content_bytes.decode(detected_encoding)
    except UnicodeDecodeError:
        fallback_encoding = 'utf-8'
        decoded_file_content = object_content_bytes.decode(fallback_encoding)
    return decoded_file_content


def adding_data_df(target_df, source_df, target_values):
    """adding Data to Target DataFrame"""
    try:
        count_new_cols = 0
        for item in target_values:
            if "column_type" in item and item['column_type'] == 'New':
                count_new_cols += 1
                new_source_column = f"New Column {count_new_cols}"
                # item['source_column'] = item['target_column']
                item['source_column'] = new_source_column
                source_df.loc[0:(len(item['values']) - 1) if 'values' in item else -1, new_source_column] = item[
                    'values'] if 'values' in item else []  # This is to select no.of rows.

        target_columns = [item['target_column'] for item in target_values]
        source_columns = [item['source_column'] for item in target_values]
        print("adding_data_df::target_df.columns", target_df.columns.to_list())
        print("adding_data_df::source.columns", source_df.columns.to_list())

        target_df[target_columns] = source_df[source_columns].copy()
        return target_df
    except Exception as e:
        print("adding_data_df::transform_data_service", f"Unknown error caught: {e}")
        raise e


def apply_transformation(target_df, target_values):
    try:
        for item in target_values:
            if "transformation" in item and item['transformation'] is not None:
                if "transformation_type" in item['transformation'] and item['transformation'][
                    'transformation_type'] == "concatenation":
                    target_df = concatenation_of_columns(item, target_df)
                elif "transformation_type" in item['transformation'] and item['transformation'][
                    'transformation_type'] == "derived_value":
                    target_df = derive_value_of_columns(item, target_df)
        return target_df
    except Exception as e:
        print("apply_transformation::transform_data_service", f"applyingTransformation::Unknown error caught: {e}")
        raise e


def concatenation_of_columns(item, df):
    """This method is for concatenating the source columns and adding the new column and returning the updated DF."""
    try:
        transformation = item['transformation']
        source_columns = transformation['source_columns']
        separator = transformation['seperator']
        target_column = item['target_column']

        for col in source_columns:
            df[col] = df[col].astype(str).fillna('')

        df[target_column] = df[source_columns].apply(lambda x: separator.join(x), axis=1)
        return df
    except Exception as e:
        print(f"concatenation_of_columns::transform_data_service", f"Unknown error caught: {e}")
        raise e


def derive_value_of_columns(item, df):
    """This method is for creating a new column with derived values of existing columns."""
    try:
        transformation = item['transformation']
        source_columns = transformation['source_columns']
        operators = transformation['operator']
        target_column = item['target_column']

        new_column = df[source_columns[0]]
        for operator, column in zip(operators, source_columns[1:]):
            if operator == "+":
                new_column = new_column + df[column]
            elif operator == "-":
                new_column = new_column - df[column]
            elif operator == "*":
                new_column = new_column * df[column]

        df[target_column] = new_column
        return df
    except Exception as e:
        print(f"derive_value_of_columns::Unknown error caught: {e}")
        raise e
