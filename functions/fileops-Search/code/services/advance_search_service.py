"""
    Advanced Search Service

    This module provides a set of advanced search functions for efficiently querying and retrieving information from
    various data sources.

    It is designed to enhance the search capabilities beyond basic keyword matching, offering more sophisticated and
    customizable search options.
"""

import pandas as pd

from dbconnection import pool, getConnection


def get_job_runs(query_params, multi_value_query_string_params):
    conn, cursor = None, None
    try:
        conn, cursor = getConnection()

        business_process_uuids = query_params.get('business_process_uuid', None)
        job_uuids = multi_value_query_string_params.get('job_uuid', None)
        from_date = query_params.get('from_date', None)
        to_date = f"{query_params.get('to_date', None)} 23:59:59+00000"
        job_status = multi_value_query_string_params.get('job_status', None)

        business_process_ids_data = None

        # Business Process UUID
        if business_process_uuids is not None:
            business_process_query = "select id from business_process WHERE uuid = %s"
            cursor.execute(business_process_query, (business_process_uuids,))
            business_process_ids_data = cursor.fetchone()

        # Job UUIDs
        jobs_ids_data = None

        if job_uuids is not None:
            placeholders_job_uuids = ', '.join(['%s' for _ in job_uuids])

            jobs_query = f"SELECT id FROM jobs WHERE uuid IN ({placeholders_job_uuids})"

            jobs_query_data = (*job_uuids,)

            cursor.execute(jobs_query, jobs_query_data)
            jobs_ids_data = cursor.fetchall()

        # Job Runs
        # We need business_process_id from the jobs table for the job runs
        job_runs_query = '''select j.job_name, jr."uuid" as job_run_id, buvp.file_name, bp."name" as business_process_name,
                            j.job_type , jr.status, jr.created_at, u.first_name, u.last_name
                            from job_runs jr left join jobs j on j.id = jr.job_id 
                            left join business_process bp on j.business_process_id  = bp.id
                            left join business_user_validate_process buvp on buvp.id = jr.business_user_validate_process_id
                            left join users u ON u.id = buvp.created_by'''

        conditions = []

        job_runs_data = ()

        if business_process_ids_data is not None:
            conditions.append("j.business_process_id = %s")
            job_runs_data += (business_process_ids_data[0],)

        if jobs_ids_data is not None:
            placeholders_job_ids = ', '.join(['%s' for _ in jobs_ids_data])
            conditions.append(f"jr.job_id IN ({placeholders_job_ids})")
            job_runs_data += (*jobs_ids_data,)

        if job_status is not None:
            placeholders_job_status = ', '.join(['%s' for _ in job_status])

            # We have status in both job runs and jobs table
            conditions.append(f"jr.status IN ({placeholders_job_status})")
            job_runs_data += (*job_status,)

        # From Date, To Date
        if from_date is not None and to_date is not None:
            conditions.append("jr.created_at::text >= %s AND jr.created_at::text <= %s")
            job_runs_data += (from_date, to_date)

        elif from_date is not None:
            conditions.append("jr.created_at::text >= %s")
            job_runs_data += (from_date,)

        elif to_date is not None:
            conditions.append("jr.created_at::text <= %s")
            job_runs_data += (to_date,)

        # Dynamically forming the SQL statement
        for index, condition in enumerate(conditions):
            if index == 0:
                job_runs_query += " where " + condition
            else:
                job_runs_query += " and " + condition

        cursor.execute(job_runs_query, job_runs_data)
        data = cursor.fetchall()

        print("lambda_handler::app", f"Length of job runs: {len(data)}")

        # Formatting
        field_names = list(map(lambda x: x[0], cursor.description))

        job_runs_df = pd.DataFrame(data, columns=field_names)

        # Converting all timestamps in created_at column to str
        job_runs_df['created_at'] = job_runs_df['created_at'].astype(str)

        job_runs = job_runs_df.to_dict(orient="records")

        return job_runs

    except Exception as error:
        print("get_job_runs::advance_search_service", "Error occurred while fetching the job runs details")

        raise error
    finally:
        pool.putconn(conn)
