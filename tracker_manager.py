import os
import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Name for the single table
ALL_JOBS_TABLE_NAME = config.BIGQUERY_TABLE

# BigQuery client initialization
def get_bigquery_client():
    try:
        creds = service_account.Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_PATH
        )
        client = bigquery.Client(credentials=creds, project=config.BIGQUERY_PROJECT)
        return client
    except FileNotFoundError:
        logger.error(f"Google API credentials file not found at '{config.GOOGLE_CREDENTIALS_PATH}'.")
        logger.error("Please ensure the file exists and the path in config.py is correct.")
        return None
    except Exception as e:
        logger.error(f"Error initializing BigQuery client: {e}")
        return None

def get_table_ref():
    return f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_TABLE}"

def clear_all_jobs_table():
    """
    Clears the BigQuery jobs table or creates it if it doesn't exist.
    Writes only the header row (schema).
    """
    logger.info(f"Preparing to clear/initialize table: {ALL_JOBS_TABLE_NAME}.")
    client = get_bigquery_client()
    if not client:
        logger.error("Could not connect to BigQuery. Aborting clear operation.")
        return False

    table_ref = get_table_ref()
    schema = [
        bigquery.SchemaField(field, "STRING") for field in config.JOB_FIELDS
    ]
    try:
        # Try to delete the table if it exists
        client.delete_table(table_ref, not_found_ok=True)
        logger.info(f"Deleted table {table_ref} if it existed.")
    except Exception as e:
        logger.error(f"Error deleting table {table_ref}: {e}")
        return False
    try:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"Created table {table_ref} with schema.")
        return True
    except Exception as e:
        logger.error(f"Error creating table {table_ref}: {e}")
        return False

def load_existing_job_ids():
    """
    Load existing job IDs from the BigQuery jobs table.
    Returns a set of job IDs.
    """
    client = get_bigquery_client()
    if not client:
        return set()
    table_ref = get_table_ref()
    existing_job_ids = set()
    try:
        query = f"SELECT id FROM `{table_ref}`"
        query_job = client.query(query)
        for row in query_job:
            job_id = str(row.get('id', ''))
            if job_id:
                existing_job_ids.add(job_id)
        logger.info(f"Loaded {len(existing_job_ids)} job IDs from table: {ALL_JOBS_TABLE_NAME}.")
        return existing_job_ids
    except Exception as e:
        logger.error(f"Error loading existing job IDs from BigQuery: {str(e)}")
        return set()

def update_jobs_tracker(new_jobs):
    """
    Insert only new jobs (by id) into the BigQuery jobs table.
    """
    logger.info(f"Updating jobs tracker for table: {ALL_JOBS_TABLE_NAME}.")
    client = get_bigquery_client()
    if not client:
        logger.error("Could not connect to BigQuery. Aborting update.")
        return
    table_ref = get_table_ref()
    existing_job_ids = load_existing_job_ids()
    jobs_to_insert = []
    for job in new_jobs:
        job_id_str = str(job.get('id'))
        if not job_id_str:
            logger.warning(f"Skipping job due to missing id: {job.get('job_title', 'N/A')}")
            continue
        if job_id_str in existing_job_ids:
            continue
        # Prepare job dict with only the fields in config.JOB_FIELDS
        job_row = {field: str(job.get(field, '')) for field in config.JOB_FIELDS}
        jobs_to_insert.append(job_row)
    if not jobs_to_insert:
        logger.info("No new unique jobs to add.")
        return
    try:
        errors = client.insert_rows_json(table_ref, jobs_to_insert)
        if errors:
            logger.error(f"Errors occurred while inserting rows: {errors}")
        else:
            logger.info(f"Inserted {len(jobs_to_insert)} new jobs into table '{ALL_JOBS_TABLE_NAME}'.")
    except Exception as e:
        logger.error(f"Error inserting jobs into table {ALL_JOBS_TABLE_NAME}: {e}")
        return

def update_job_in_tracker(job_id_to_update: str, job_data: dict):
    """
    Updates a single job identified by job_id_to_update in the BigQuery table with new job_data.
    """
    logger.info(f"Attempting to update job ID: {job_id_to_update} in table: {ALL_JOBS_TABLE_NAME}.")
    client = get_bigquery_client()
    if not client:
        logger.error("Could not connect to BigQuery. Aborting update for job ID: {job_id_to_update}.")
        return False

    table_ref_str = get_table_ref()

    # Ensure all field values in job_data are strings and conform to JOB_FIELDS
    update_values = {}
    set_clauses = []
    param_index = 0
    query_params = []

    for field in config.JOB_FIELDS:
        if field == 'id': # ID should not be in the SET clause, it's for the WHERE clause
            continue
        value = job_data.get(field)
        if value is not None:
            # Ensure date fields are correctly formatted if they are part of job_data for update
            if field in ['publishing_date', 'date_added'] and isinstance(value, str) and len(value) == 10:
                # If only date is provided, append a default time. BigQuery expects YYYY-MM-DD HH:MM:SS
                value_str = f"{value} 00:00:00"
            elif isinstance(value, datetime.datetime):
                value_str = value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                value_str = str(value)
            
            set_clauses.append(f"{field} = @param_{param_index}")
            query_params.append(bigquery.ScalarQueryParameter(f"param_{param_index}", "STRING", value_str))
            param_index += 1
        # If you want to set fields to NULL if not in job_data, add an else clause here.
        # Otherwise, fields not in job_data will remain unchanged.

    if not set_clauses:
        logger.warning(f"No valid fields to update for job ID: {job_id_to_update}. Nothing to do.")
        return False

    # Add the job_id for the WHERE clause as the last parameter
    query_params.append(bigquery.ScalarQueryParameter(f"param_{param_index}", "STRING", str(job_id_to_update)))

    query = f"UPDATE `{table_ref_str}` SET {', '.join(set_clauses)} WHERE id = @param_{param_index}"

    logger.debug(f"Executing update query: {query} with params: {[(p.name, p.value) for p in query_params]}")

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params
        )
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete

        if query_job.errors:
            logger.error(f"Errors occurred while updating job ID {job_id_to_update}: {query_job.errors}")
            return False
        if query_job.num_dml_affected_rows > 0:
            logger.info(f"Successfully updated {query_job.num_dml_affected_rows} row(s) for job ID: {job_id_to_update}.")
            return True
        else:
            logger.info(f"No rows updated for job ID: {job_id_to_update}. It might not exist or values were the same.")
            return False # Or True if no change is not an error for you

    except Exception as e:
        logger.error(f"Error updating job ID {job_id_to_update} in BigQuery table {ALL_JOBS_TABLE_NAME}: {e}")
        return False

def save_jobs_to_file(jobs, filename=config.JSON_OUTPUT_PATH):
    """
    Saves a list of job dictionaries to a JSON file.
    Ensures 'Country' and 'work_type_name' are present.
    """
    import json
    processed_jobs = []
    for job in jobs:
        if 'Country' not in job and 'search_location' in job: job['Country'] = job['search_location']
        elif 'Country' not in job: job['Country'] = 'Unknown'

        if 'work_type_name' not in job:
            raw_work_type = job.get('work_type')
            if isinstance(raw_work_type, int): job['work_type_name'] = config.WORK_TYPE_NAMES.get(raw_work_type, "Any")
            elif isinstance(raw_work_type, str) and raw_work_type in config.WORK_TYPES: job['work_type_name'] = raw_work_type
            elif str(raw_work_type).isdigit(): job['work_type_name'] = config.WORK_TYPE_NAMES.get(int(raw_work_type), "Any")
            else: job['work_type_name'] = str(raw_work_type) if raw_work_type else "Any"
        
        if 'search_keyword_job_title' not in job:
            job['search_keyword_job_title'] = job.get('search_keywords', 'Unknown') 

        processed_jobs.append(job)

    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(processed_jobs, f, ensure_ascii=False, indent=4)
        logger.info(f"Successfully saved {len(processed_jobs)} jobs to {filename}")
    except Exception as e:
        logger.error(f"Error saving jobs to JSON file {filename}: {e}") 