import logging
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import config
from tracker_manager import get_bigquery_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_llm_data_table_ref():
    return f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_TABLE_LLM_DATA}"


def load_existing_llm_job_ids():
    client = get_bigquery_client()
    if not client:
        return set()
    table_ref = get_llm_data_table_ref()
    existing_job_ids = set()
    try:
        query = f"SELECT job_id FROM `{table_ref}`"
        query_job = client.query(query)
        for row in query_job:
            job_id = str(row.get('job_id', ''))
            if job_id:
                existing_job_ids.add(job_id)
        logger.info(f"Loaded {len(existing_job_ids)} job_ids from table: {table_ref}.")
        return existing_job_ids
    except Exception as e:
        logger.error(f"Error loading job_ids from llm_data: {str(e)}")
        return set()

def insert_llm_data(rows):
    if not rows:
        return 0
    client = get_bigquery_client()
    if not client:
        logger.error("Could not connect to BigQuery. Aborting llm_data insert.")
        return 0
    table_ref = get_llm_data_table_ref()
    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"Errors occurred while inserting LLM rows: {errors}")
            return 0
        return len(rows)
    except Exception as e:
        logger.error(f"Error inserting LLM data into table {table_ref}: {e}")
        return 0

def query_llm_data_with_filters(filters=None):
    client = get_bigquery_client()
    if not client:
        logger.error("Could not connect to BigQuery. Aborting llm_data query.")
        return []
    table_ref = get_llm_data_table_ref()
    where_clause = f"WHERE {filters}" if filters else ""
    query = f"SELECT * FROM `{table_ref}` {where_clause}"
    try:
        query_job = client.query(query)
        rows = [dict(row) for row in query_job]
        logger.info(f"Queried {len(rows)} rows from table: {table_ref} with filters: {filters}")
        return rows
    except Exception as e:
        logger.error(f"Error querying llm_data with filters '{filters}': {e}")
        return [] 