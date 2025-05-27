import logging
import time
import random
import os
import config
import linkedin_scraper
import tracker_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reverse mapping for WORK_TYPES from name to code for the scraper
WORK_TYPE_CODES = {name: code for code, name in config.WORK_TYPES.items()}

def get_failed_job_entries():
    client = tracker_manager.get_bigquery_client() # Use client from tracker_manager
    if not client:
        return []

    table_ref = tracker_manager.get_table_ref()
    # Select fields needed to re-initiate the scrape
    # Assuming 'country' stores the original search location name and 'search_keyword_job_title' the original job title string
    query = f"""
        SELECT 
            id, 
            search_keyword_job_title, 
            country, 
            work_type
        FROM `{table_ref}` 
        WHERE job_title = 'None' OR job_title IS NULL
    """
    logger.info(f"Querying for failed jobs: {query}")
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        failed_jobs = []
        for row in results:
            failed_jobs.append({
                'id': str(row.id),
                'search_keyword_job_title': str(row.search_keyword_job_title) if row.search_keyword_job_title else None,
                'country': str(row.country) if row.country else None, # This is the location name like 'Italia'
                'work_type_name': str(row.work_type) if row.work_type else None # This is the string like 'Remote'
            })
        logger.info(f"Found {len(failed_jobs)} job entries to re-scrape.")
        return failed_jobs
    except Exception as e:
        logger.error(f"Error fetching failed job entries: {e}")
        return []

def main():
    logger.info("Starting retry process for failed job scrapes...")
    failed_entries = get_failed_job_entries()

    if not failed_entries:
        logger.info("No failed job entries found to retry.")
        return

    successful_updates = 0
    failed_updates = 0

    for entry in failed_entries:
        job_id = entry.get('id')
        original_job_title_search = entry.get('search_keyword_job_title')
        country_search = entry.get('country') # This is the location for the search
        work_type_name_search = entry.get('work_type_name')

        if not all([job_id, original_job_title_search, country_search, work_type_name_search]):
            logger.warning(f"Skipping job ID {job_id} due to missing original search parameters (title, country, or work_type_name).")
            failed_updates += 1
            continue

        logger.info(f"Retrying job ID: {job_id} (Searched as: {original_job_title_search} in {country_search}, {work_type_name_search})")

        work_type_code = WORK_TYPE_CODES.get(work_type_name_search)
        if work_type_code is None and work_type_name_search.lower() != "any": # "Any" might map to None in WORK_TYPE_CODES if config handles it
             logger.warning(f"Could not find work type code for '{work_type_name_search}' for job ID {job_id}. Skipping.")
             failed_updates += 1
             continue

        # Use a slightly longer, more random delay for retries
        time.sleep(random.uniform(2.0, 5.0))

        # Re-scrape the job details
        # Note: linkedin_scraper.fetch_job_details expects the location name as 'country' parameter
        job_details, html_content = linkedin_scraper.fetch_job_details(
            job_id=job_id,
            work_type=work_type_code, 
            country=country_search, # Pass original search location
            search_keyword_job_title=original_job_title_search # Pass original search job title
        )

        if job_details:
            logger.info(f"Successfully re-scraped job ID: {job_id}. New title: {job_details.get('job_title')}")
            
            # Prepare data for update, ensuring all fields from config.JOB_FIELDS are present
            # Values extracted by clean_job_html (like publishing_date, date_added) will be used
            # country and search_keyword_job_title are already set by clean_job_html via fetch_job_details
            update_data = {field: job_details.get(field) for field in config.JOB_FIELDS if field != 'id'}
            
            if tracker_manager.update_job_in_tracker(job_id, update_data):
                successful_updates += 1
            else:
                logger.error(f"Failed to update job ID: {job_id} in BigQuery after re-scrape.")
                failed_updates += 1
        else:
            logger.warning(f"Failed to re-scrape job ID: {job_id}. No details found.")
            # Optionally, save html_content for debugging failed re-scrapes
            debug_html_dir = os.path.join("output", "debug_html_retry")
            os.makedirs(debug_html_dir, exist_ok=True)
            html_file_path = os.path.join(debug_html_dir, f"retry_debug_html_{job_id}.html")
            try:
                with open(html_file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content if html_content else "Error: HTML content was None.")
                logger.info(f"HTML for failed retry of job ID {job_id} saved to: {html_file_path}")
            except Exception as e_write:
                logger.error(f"Could not write HTML for job ID {job_id} to {html_file_path}: {e_write}")
            failed_updates += 1
    
    logger.info(f"Retry process finished. Successful updates: {successful_updates}, Failed updates: {failed_updates}.")

if __name__ == "__main__":
    main() 