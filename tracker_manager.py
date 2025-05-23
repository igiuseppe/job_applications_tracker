import os
import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
# from openpyxl.styles import Font, PatternFill, Alignment, Border, Side # Not used for GSheets directly
import logging
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the scope for Google Sheets API
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file' # if you need to create new sheets or manage files
]

# Name for the single sheet
ALL_JOBS_SHEET_NAME = "All Jobs"

def get_gspread_client():
    """Initializes and returns a gspread client."""
    try:
        creds = Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_PATH, scopes=SCOPES
        )
        client = gspread.authorize(creds)
        return client
    except FileNotFoundError:
        logger.error(f"Google API credentials file not found at '{config.GOOGLE_CREDENTIALS_PATH}'.")
        logger.error("Please ensure the file exists and the path in config.py is correct.")
        return None
    except Exception as e:
        logger.error(f"Error initializing gspread client: {e}")
        return None

def load_existing_jobs_from_tracker(sheet_id=config.GOOGLE_SHEET_ID):
    """
    Load existing jobs from the "All Jobs" sheet in the Google Sheet tracker.
    Returns a set of job IDs and a list of all existing job records.
    """
    client = get_gspread_client()
    if not client:
        return set(), []

    existing_job_ids = set()
    all_existing_jobs = []

    try:
        spreadsheet = client.open_by_key(sheet_id)
        try:
            worksheet = spreadsheet.worksheet(ALL_JOBS_SHEET_NAME)
            logger.info(f"Loading jobs from sheet: {ALL_JOBS_SHEET_NAME}")
            records = worksheet.get_all_records()  # Assumes first row is header
            
            if records:
                for record in records:
                    job_id = str(record.get('job_id', ''))
                    if job_id:
                        existing_job_ids.add(job_id)
                    all_existing_jobs.append(dict(record))
            logger.info(f"Loaded {len(all_existing_jobs)} jobs from sheet: {ALL_JOBS_SHEET_NAME}, {len(existing_job_ids)} unique job IDs.")
        
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' not found. Assuming no existing jobs.")
            # No need to return error, just means no jobs loaded.

        return existing_job_ids, all_existing_jobs

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{sheet_id}' not found or permission denied.")
        return set(), []
    except Exception as e:
        logger.error(f"Error loading existing jobs from Google Sheet: {str(e)}")
        return set(), []

def update_jobs_tracker(new_jobs, sheet_id=config.GOOGLE_SHEET_ID, mode='default'):
    """
    Update the "All Jobs" sheet in the Google Sheet tracker with new job listings.
    - 'deep' mode: Clears and overwrites the "All Jobs" sheet.
    - 'default' mode: Appends new jobs to the "All Jobs" sheet, avoiding duplicates.
    """
    logger.info(f"Updating jobs tracker in '{mode}' mode for sheet: {ALL_JOBS_SHEET_NAME}.")
    client = get_gspread_client()
    if not client:
        logger.error("Could not connect to Google Sheets. Aborting update.")
        return

    try:
        spreadsheet = client.open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{sheet_id}' not found. Please create it or check the ID.")
        return
    except Exception as e:
        logger.error(f"Error opening Google Sheet: {e}")
        return

    existing_jobs_in_sheet = []
    current_sheet_job_ids = set()

    try:
        worksheet = spreadsheet.worksheet(ALL_JOBS_SHEET_NAME)
        logger.info(f"Accessing sheet: {ALL_JOBS_SHEET_NAME}")
        if mode == 'deep':
            logger.info(f"Deep mode: Sheet '{ALL_JOBS_SHEET_NAME}' will be cleared.")
            # existing_jobs_in_sheet and current_sheet_job_ids remain empty
        else: # default mode, load existing
            logger.info(f"Default mode: Loading existing jobs from '{ALL_JOBS_SHEET_NAME}'.")
            records = worksheet.get_all_records()
            existing_jobs_in_sheet = [dict(rec) for rec in records]
            current_sheet_job_ids = {str(job.get('job_id', '')) for job in existing_jobs_in_sheet if job.get('job_id')}
            logger.info(f"Loaded {len(existing_jobs_in_sheet)} existing jobs with {len(current_sheet_job_ids)} unique IDs.")

    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' not found. Creating it.")
        try:
            worksheet = spreadsheet.add_worksheet(title=ALL_JOBS_SHEET_NAME, rows="1", cols=str(len(config.JOB_FIELDS)))
            # New sheet, so no existing jobs to load.
            # existing_jobs_in_sheet and current_sheet_job_ids remain empty
        except Exception as e:
            logger.error(f"Error creating sheet {ALL_JOBS_SHEET_NAME}: {e}")
            return
    except Exception as e:
        logger.error(f"Error accessing sheet {ALL_JOBS_SHEET_NAME}: {e}")
        return
    
    added_count = 0
    updated_count = 0 # Not strictly tracking updates vs new, but count of jobs processed

    for job in new_jobs:
        job_id_str = str(job.get('job_id'))
        
        # Add 'Country' and 'work_type_name' if not already present (main.py should add them)
        if 'Country' not in job:
            job['Country'] = job.get('search_location', 'Unknown') 
        if 'work_type_name' not in job: # Should be added in main.py run_search
             # Attempt to derive it if missing - placeholder logic
            raw_work_type = job.get('work_type') 
            if isinstance(raw_work_type, int):
                 job['work_type_name'] = config.WORK_TYPE_NAMES.get(raw_work_type, "Any")
            elif isinstance(raw_work_type, str) and raw_work_type in config.WORK_TYPES: # if it's "Remote", "Hybrid"
                 job['work_type_name'] = raw_work_type
            else: # if it's the actual code like "2" but as string
                 job['work_type_name'] = config.WORK_TYPE_NAMES.get(int(raw_work_type), "Any") if str(raw_work_type).isdigit() else "Any"
        
        # Ensure 'search_keyword_job_title' is present (main.py should add this)
        if 'search_keyword_job_title' not in job:
            job['search_keyword_job_title'] = job.get('search_keywords', 'Unknown') # Fallback, though main.py should provide original_job_title

        if mode == 'deep' or job_id_str not in current_sheet_job_ids:
            if job_id_str not in current_sheet_job_ids: # New job
                 job['status'] = 'New'
                 job['notes'] = ''
                 job['date_added'] = datetime.datetime.now().strftime("%Y-%m-%d")
                 added_count +=1
            else: # Job exists, but deep mode means we re-add it (effectively an update)
                 # Preserve status/notes if already there? For now, deep mode re-adds as "New" if it was previously scraped
                 # For simplicity in deep mode, treat as new add for now.
                 # More sophisticated update logic could be added here if needed for deep mode.
                 job['status'] = 'New' # Or try to find and merge?
                 job['notes'] = ''
                 job['date_added'] = datetime.datetime.now().strftime("%Y-%m-%d") # Or keep old one?
                 updated_count +=1 # Or consider this an "added" in deep mode context


            existing_jobs_in_sheet.append(job)
            current_sheet_job_ids.add(job_id_str) # Add to set to handle duplicates within the new_jobs batch itself

    if added_count > 0 :
        logger.info(f"Added {added_count} new jobs to '{ALL_JOBS_SHEET_NAME}'.")
    if updated_count > 0 and mode == 'deep': # only log "updated" in sense of re-processing for deep mode
        logger.info(f"Re-processed {updated_count} jobs in 'deep' mode for '{ALL_JOBS_SHEET_NAME}'.")
    if not new_jobs:
        logger.info("No new jobs provided to update.")
    elif added_count == 0 and updated_count == 0 and mode !='deep':
        logger.info(f"No new unique jobs to add to '{ALL_JOBS_SHEET_NAME}'.")


    # Sort all jobs (existing + new unique ones, or all if deep mode)
    # Ensure publishing_date is a string for sorting, handle missing dates
    def get_sort_key(x):
        date_str = x.get('publishing_date', '1970-01-01')
        if isinstance(date_str, datetime.date):
            return date_str.isoformat()
        if isinstance(date_str, datetime.datetime):
            return date_str.date().isoformat()
        return str(date_str)

    sorted_jobs_for_sheet = sorted(
        existing_jobs_in_sheet,
        key=get_sort_key,
        reverse=True
    )

    headers = config.JOB_FIELDS # JOB_FIELDS now includes 'Country'
    data_to_write = [headers] + [
        [str(job.get(field, '')) for field in headers] for job in sorted_jobs_for_sheet
    ]

    try:
        worksheet.clear() 
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        worksheet.format("A1:Z1", {"textFormat": {"bold": True}}) # Format header
        # Auto-resize columns - this might be slow or fail for very large sheets/many columns.
        # gspread itself doesn't have a direct auto-resize. This needs to be done via Google Sheets API batchUpdate.
        # For simplicity, skipping auto-resize or doing it manually in GSheets is often easier.
        logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' updated with {len(sorted_jobs_for_sheet)} total jobs.")
    except Exception as e:
        logger.error(f"Error writing data to sheet {ALL_JOBS_SHEET_NAME}: {e}")
        return

    logger.info(f"Tracker data update attempt finished for Google Sheet ID: {sheet_id}")

def save_jobs_to_file(jobs, filename=config.JSON_OUTPUT_PATH):
    """
    Saves a list of job dictionaries to a JSON file.
    Ensures 'Country' and 'work_type_name' are present.
    """
    import json
    processed_jobs = []
    for job in jobs:
        # Ensure 'Country' is present, derived from 'search_location' if needed
        if 'Country' not in job and 'search_location' in job:
            job['Country'] = job['search_location']
        elif 'Country' not in job:
            job['Country'] = 'Unknown' # Default if not found

        # Ensure 'work_type_name' is present
        if 'work_type_name' not in job:
            raw_work_type = job.get('work_type')
            if isinstance(raw_work_type, int):
                 job['work_type_name'] = config.WORK_TYPE_NAMES.get(raw_work_type, "Any")
            elif isinstance(raw_work_type, str) and raw_work_type in config.WORK_TYPES: # if it's "Remote", "Hybrid"
                 job['work_type_name'] = raw_work_type # It's already the name
            elif str(raw_work_type).isdigit():
                 job['work_type_name'] = config.WORK_TYPE_NAMES.get(int(raw_work_type), "Any")
            else: # Fallback if it's some other string or None
                 job['work_type_name'] = str(raw_work_type) if raw_work_type else "Any"
        
        # Ensure 'search_keyword_job_title' is present
        if 'search_keyword_job_title' not in job:
            # Fallback: use 'search_keywords' if 'original_job_title' wasn't propagated
            # config.py has JOB_TITLES which maps readable to URL-safe, we want readable here.
            # This fallback might not be perfect if search_keywords is the URL-encoded version.
            # Ideally, main.py correctly populates 'search_keyword_job_title' from 'original_job_title'.
            job['search_keyword_job_title'] = job.get('search_keywords', 'Unknown') 

        processed_jobs.append(job)

    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(processed_jobs, f, ensure_ascii=False, indent=4)
        logger.info(f"Successfully saved {len(processed_jobs)} jobs to {filename}")
    except Exception as e:
        logger.error(f"Error saving jobs to JSON file {filename}: {e}")

# Provide backward compatibility
load_existing_jobs_from_crm = load_existing_jobs_from_tracker
update_jobs_crm = update_jobs_tracker 