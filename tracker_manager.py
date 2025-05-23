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

def clear_all_jobs_sheet(sheet_id=config.GOOGLE_SHEET_ID):
    """
    Clears the "All Jobs" sheet or creates it if it doesn't exist.
    Writes only the header row.
    """
    logger.info(f"Preparing to clear/initialize sheet: {ALL_JOBS_SHEET_NAME}.")
    client = get_gspread_client()
    if not client:
        logger.error("Could not connect to Google Sheets. Aborting clear operation.")
        return False

    try:
        spreadsheet = client.open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{sheet_id}' not found. Cannot clear/initialize.")
        return False
    except Exception as e:
        logger.error(f"Error opening Google Sheet: {e}")
        return False

    worksheet = None
    try:
        worksheet = spreadsheet.worksheet(ALL_JOBS_SHEET_NAME)
        logger.info(f"Found existing sheet: '{ALL_JOBS_SHEET_NAME}'. Clearing it.")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' not found. Creating it.")
        try:
            worksheet = spreadsheet.add_worksheet(title=ALL_JOBS_SHEET_NAME, rows="1", cols=str(len(config.JOB_FIELDS)))
        except Exception as e:
            logger.error(f"Error creating sheet {ALL_JOBS_SHEET_NAME}: {e}")
            return False
    except Exception as e:
        logger.error(f"Error accessing or clearing sheet {ALL_JOBS_SHEET_NAME}: {e}")
        return False

    if worksheet:
        try:
            headers = config.JOB_FIELDS
            worksheet.update('A1', [headers], value_input_option='USER_ENTERED')
            worksheet.format("A1:Z1", {"textFormat": {"bold": True}}) # Format header
            logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' cleared and header written.")
            return True
        except Exception as e:
            logger.error(f"Error writing headers to sheet {ALL_JOBS_SHEET_NAME}: {e}")
            return False
    return False

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
            # Get all records, which gspread interprets as list of dicts
            # Skips the header row by default if headers=1 (which is default for get_all_records)
            records = worksheet.get_all_records() 
            
            if records:
                for record in records:
                    job_id = str(record.get('job_id', ''))
                    if job_id: # Ensure job_id is not empty
                        existing_job_ids.add(job_id)
                    # Convert all cell values to string to match how we write them, avoids type mismatches later
                    all_existing_jobs.append({k: str(v) for k, v in record.items()})

            logger.info(f"Loaded {len(all_existing_jobs)} jobs from sheet: {ALL_JOBS_SHEET_NAME}, {len(existing_job_ids)} unique job IDs.")
        
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' not found during load. Assuming no existing jobs. It might be created by clear_all_jobs_sheet or during update.")
            # This is not an error if the sheet is meant to be created/cleared by another function first.

        return existing_job_ids, all_existing_jobs

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{sheet_id}' not found or permission denied during load.")
        return set(), []
    except Exception as e:
        logger.error(f"Error loading existing jobs from Google Sheet: {str(e)}")
        return set(), []

def update_jobs_tracker(new_jobs, sheet_id=config.GOOGLE_SHEET_ID):
    """
    Update the "All Jobs" sheet by loading existing jobs, appending new unique ones,
    sorting, and then rewriting the entire sheet.
    """
    logger.info(f"Updating jobs tracker for sheet: {ALL_JOBS_SHEET_NAME}.")
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

    # Load existing jobs from the sheet
    # This function now returns (set_of_ids, list_of_job_dicts)
    current_sheet_job_ids, existing_jobs_in_sheet = load_existing_jobs_from_tracker(sheet_id)
    
    if not isinstance(existing_jobs_in_sheet, list): # Safety check
        logger.error("Failed to load existing jobs correctly, aborting update.")
        existing_jobs_in_sheet = []
        current_sheet_job_ids = set()


    worksheet = None
    try:
        worksheet = spreadsheet.worksheet(ALL_JOBS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Sheet '{ALL_JOBS_SHEET_NAME}' not found during update. Attempting to create it.")
        try:
            worksheet = spreadsheet.add_worksheet(title=ALL_JOBS_SHEET_NAME, rows="1", cols=str(len(config.JOB_FIELDS)))
            # If created here, existing_jobs_in_sheet and current_sheet_job_ids would be empty from load_existing_jobs_from_tracker
            # or the load would have logged "not found" and returned empty sets/lists.
        except Exception as e:
            logger.error(f"Error creating sheet {ALL_JOBS_SHEET_NAME} during update: {e}")
            return
    except Exception as e:
        logger.error(f"Error accessing sheet {ALL_JOBS_SHEET_NAME} during update: {e}")
        return
    
    if not worksheet:
        logger.error(f"Failed to get or create worksheet '{ALL_JOBS_SHEET_NAME}'. Aborting update.")
        return

    added_count = 0
    
    for job in new_jobs:
        job_id_str = str(job.get('job_id'))
        
        # Ensure required fields are present (main.py should handle this)
        if 'Country' not in job: job['Country'] = job.get('search_location', 'Unknown') 
        if 'work_type_name' not in job:
            raw_work_type = job.get('work_type') 
            if isinstance(raw_work_type, int): job['work_type_name'] = config.WORK_TYPE_NAMES.get(raw_work_type, "Any")
            elif isinstance(raw_work_type, str) and raw_work_type in config.WORK_TYPES: job['work_type_name'] = raw_work_type
            else: job['work_type_name'] = config.WORK_TYPE_NAMES.get(int(raw_work_type), "Any") if str(raw_work_type).isdigit() else "Any"
        if 'search_keyword_job_title' not in job: job['search_keyword_job_title'] = job.get('search_keywords', 'Unknown')

        if job_id_str and job_id_str not in current_sheet_job_ids: # Add if ID exists and is new
            job['status'] = 'New'
            job['notes'] = ''
            job['date_added'] = datetime.datetime.now().strftime("%Y-%m-%d")
            
            # Ensure all values are strings for consistency before appending, matching load_existing_jobs_from_tracker
            stringified_job = {k: str(v) for k, v in job.items()}
            existing_jobs_in_sheet.append(stringified_job)
            current_sheet_job_ids.add(job_id_str)
            added_count +=1
        elif not job_id_str:
            logger.warning(f"Skipping job due to missing job_id: {job.get('job_title', 'N/A')}")


    if added_count > 0 :
        logger.info(f"Added {added_count} new unique jobs to the list for '{ALL_JOBS_SHEET_NAME}'.")
    elif not new_jobs:
        logger.info("No new jobs provided in this batch.")
    else:
        logger.info(f"No new unique jobs to add from this batch to '{ALL_JOBS_SHEET_NAME}'. All {len(new_jobs)} were duplicates or already present.")

    # Sort all jobs (existing loaded + newly added unique ones)
    def get_sort_key(x):
        date_str = x.get('publishing_date', '1970-01-01')
        if isinstance(date_str, datetime.date): return date_str.isoformat()
        if isinstance(date_str, datetime.datetime): return date_str.date().isoformat()
        return str(date_str)

    # Filter out any potential placeholder dicts if worksheet.get_all_records() returned empty rows as dicts of Nones
    # Though get_all_records usually handles this well.
    # And ensure all job dicts have at least a job_id to be considered valid for sorting/writing
    valid_jobs_for_sheet = [j for j in existing_jobs_in_sheet if j.get('job_id')]


    sorted_jobs_for_sheet = sorted(
        valid_jobs_for_sheet,
        key=get_sort_key,
        reverse=True
    )

    headers = config.JOB_FIELDS
    data_to_write = [headers] + [
        [job.get(field, '') for field in headers] for job in sorted_jobs_for_sheet # Use job.get with default for safety
    ]

    try:
        worksheet.clear() 
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        worksheet.format("A1:Z1", {"textFormat": {"bold": True}}) 
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

# Provide backward compatibility
load_existing_jobs_from_crm = load_existing_jobs_from_tracker
update_jobs_crm = update_jobs_tracker 