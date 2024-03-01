import psycopg2
import boto3

from dbconnection import pool, getConnection
from .constants import s3_fileops_storage_bucket, job_run_logs_folder


def store_logs_db(job_run_id: str, logs: str):
    print("store_logs::store_logs", f"Logs: {logs}")

    conn, cursor = None, None

    try:
        conn, cursor = getConnection()

        update_query = """
        UPDATE job_runs
        SET logs = %s
        WHERE uuid = %s
    """
        values = (logs, job_run_id)
        cursor.execute(update_query, values)
        conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as error:
        print("The Error: ", error)
    finally:
        cursor.close()
        pool.putconn(conn)


def store_logs_s3(job_id: str, job_run_id: str, logs: str):
    print("store_logs_s3::store_logs", f"Logs: {logs}")

    s3 = boto3.resource('s3')
    s3_obj = s3.Object(s3_fileops_storage_bucket, f"{job_run_logs_folder}/{job_id}/{job_run_id}_logs.json")
    s3_obj.put(Body=logs)
