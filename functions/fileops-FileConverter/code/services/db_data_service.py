import json

from dbconnection import pool, getConnection

from ..interfaces.source_file_config import TargetFileConfigDetails, map_parquet_partition_type, map_target_file_type

from ..interfaces.target_config import TargetConfig


def get_target_file_config_details(job_uuid: str) -> TargetFileConfigDetails:
    """This method is used for fetching target_file_config_details"""

    conn, cursor = getConnection()

    target_file_config_details_query = """SELECT sfc.target_file_config_details
                                                   FROM jobs j LEFT JOIN source_file_config sfc ON j.id = sfc.job_id 
                                                   WHERE j.uuid = %s      
                                                """

    cursor.execute(target_file_config_details_query, (job_uuid,))

    data = cursor.fetchone()

    cursor.close()
    pool.putconn(conn)

    # data[0] contains target_file_config_details

    parsed_data = json.loads(data[0])

    target_file_type = parsed_data.get("target_file_type")
    target_filename_pattern = parsed_data.get("target_filename_pattern")
    partition_type = parsed_data.get("parquet_partition_type")
    partition_columns = parsed_data.get("parquet_partitions_schema")

    mapped_target_file_type = map_target_file_type(target_file_type)
    mapped_partition_type = map_parquet_partition_type(partition_type)

    target_file_config_details = TargetFileConfigDetails(target_file_type=mapped_target_file_type,
                                                         target_filename_pattern=target_filename_pattern,
                                                         parquet_partition_type=mapped_partition_type,
                                                         parquet_partitions_schema=partition_columns)

    return target_file_config_details


def get_target_config_data(job_uuid: str) -> TargetConfig:
    """This method is used for fetching target_config_details"""

    conn, cursor = getConnection()

    target_config_details_query = """select tc."uuid", tc.location_pattern from target_config tc
                                     left join source_file_config sfc on sfc.target_id = tc.id
                                     left join jobs j on sfc.job_id = j.id
                                     where j."uuid" = %s
                                   """

    cursor.execute(target_config_details_query, (job_uuid,))

    data = cursor.fetchone()

    cursor.close()
    pool.putconn(conn)
    
    target_config_uuid = data[0]
    target_config_location_pattern = data[1]
    
    return TargetConfig(uuid=target_config_uuid, location_pattern=target_config_location_pattern)
    
    
