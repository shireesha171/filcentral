
import psycopg2
import pandas as pd

from dbconnection import pool, getConnection


def fetch_job_config(business_validate_id):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()
        job_query = """
                  SELECT sc.metadata,bvp.file_location_path, bvp.business_process_id, bvp.job_id, bvp.mailing_list, sc.dqrules,
                  bvp.file_name as file_name, bvp.id as business_validate_process_id,j.uuid AS job_uuid, j.delimiter as delimiter, sc.file_config_details, 
                  sc.standard_validations, tc."name" as target_name,tc.location_pattern as target_location_pattern,
                  tc.connectivity_type as target_connectivity_type, tc.absolute_file_path as target_absolute_file_path,
                  sc2."name" as source_name,sc2.location_pattern as source_location_pattern,
                  sc2.absolute_file_path as source_absolute_file_path, sc2.connectivity_type as source_connectivity_type,
                  bp.uuid as business_process_uuid
                  FROM business_user_validate_process AS bvp join
                  source_file_config as sc on bvp.job_id = sc.job_id
                  join jobs AS j on j.id = bvp.job_id
                  join business_process bp on bp.id = bvp.business_process_id
                  left join target_config tc on tc.id = sc.target_id
                  left join source_config sc2   on sc2.id = sc.source_id
                  WHERE bvp.uuid = %s
                   """
        cursor.execute(job_query, (business_validate_id,))
        data = cursor.fetchone()
        if data is None:
            raise NoJobConfigError("No Job configurations found for business validation id : "+business_validate_id,400)
        cols = list(map(lambda x: x[0], cursor.description))
        df = pd.DataFrame([data], columns=cols)
        record = df.to_dict(orient="records")[0]
        return record
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        raise e
    except Exception as e:
        print(f"Unknown error caught while fetching job configuration : {e}")
        raise e
    finally:
        cursor.close()
        pool.putconn(conn)


class NoJobConfigError(Exception):
    """Custom exception class"""
    def __init__(self, message,code):
        super().__init__(message)
        self.code = code