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
    Load existing jobs from the Google Sheet tracker.
    Returns a dictionary mapping sheet names to sets of job IDs, and a list of all existing job records.
    """
    client = get_gspread_client()
    if not client:
        return {}, [] 

    existing_job_ids_by_sheet = {} 
    all_existing_jobs = [] 

    try:
        spreadsheet = client.open_by_key(sheet_id)
        worksheets = spreadsheet.worksheets()
        
        for worksheet in worksheets:
            sheet_name = worksheet.title
            # No longer skipping 'Summary' here; if it exists and has job data, it will be loaded.
            # The decision to not process it as a job data sheet is handled in update_jobs_tracker.
            logger.info(f"Loading jobs from sheet: {sheet_name}")
            try:
                records = worksheet.get_all_records()  # Assumes first row is header
            except Exception as e:
                logger.warning(f"Could not get records from sheet: {sheet_name}. Error: {e}. Skipping this sheet.")
                continue

            sheet_job_ids = set()
            sheet_jobs_list = []
            if records: # Ensure records is not empty
                for record in records:
                    job_id = str(record.get('job_id', '')) 
                    if job_id:
                        sheet_job_ids.add(job_id)
                    sheet_jobs_list.append(dict(record))
            
            existing_job_ids_by_sheet[sheet_name] = sheet_job_ids
            all_existing_jobs.extend(sheet_jobs_list)
            logger.info(f"Loaded {len(sheet_jobs_list)} jobs from sheet: {sheet_name}, {len(sheet_job_ids)} unique job IDs.")

        total_loaded_jobs = sum(len(jobs) for jobs in existing_job_ids_by_sheet.values())
        logger.info(f"Loaded {len(all_existing_jobs)} total existing jobs from tracker across {len(existing_job_ids_by_sheet)} sheets. ({total_loaded_jobs} unique job IDs found)")
        return existing_job_ids_by_sheet, all_existing_jobs
    
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{sheet_id}' not found or permission denied.")
        return {}, []
    except Exception as e:
        logger.error(f"Error loading existing jobs from Google Sheet: {str(e)}")
        return {}, []

def update_jobs_tracker(new_jobs, sheet_id=config.GOOGLE_SHEET_ID, mode='default'):
    """
    Update the Google Sheet tracker with new job listings.
    Behavior depends on the mode:
    - 'deep' mode: Clears and overwrites sheets. Creates new sheets if they don't exist.
    - 'default' mode: Only updates existing sheets. Skips job groups if sheet doesn't exist.
    """
    logger.info(f"Updating jobs tracker in '{mode}' mode.")
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

    new_job_groups = {}
    for job in new_jobs:
        keywords = job.get('search_keywords', 'Unknown')
        location = job.get('search_location', 'Unknown')
        work_type_from_job = job.get('work_type', None) # Note: work_type in job might be int code
        group_key = f"{keywords}_{location}_{work_type_from_job}"
        sheet_name_for_group = create_sheet_name(keywords, location, work_type_from_job)

        if group_key not in new_job_groups:
            new_job_groups[group_key] = {
                'jobs': [],
                'sheet_name': sheet_name_for_group
            }
        new_job_groups[group_key]['jobs'].append(job)

    total_added_globally = 0
    all_sheet_names_in_spreadsheet = [ws.title for ws in spreadsheet.worksheets()]
    processed_sheet_names = set()

    for group_key, group_data in new_job_groups.items():
        target_sheet_name = group_data['sheet_name']
        new_jobs_for_this_group = group_data['jobs']
        processed_sheet_names.add(target_sheet_name)

        if mode == 'default' and target_sheet_name not in all_sheet_names_in_spreadsheet:
            logger.info(f"Default mode: Sheet '{target_sheet_name}' does not exist. Skipping {len(new_jobs_for_this_group)} jobs for this group.")
            continue

        try:
            worksheet = spreadsheet.worksheet(target_sheet_name)
            logger.info(f"Updating existing sheet: {target_sheet_name}")
            if mode == 'deep':
                logger.info(f"Deep mode: Clearing sheet '{target_sheet_name}' before writing.")
                existing_jobs_in_sheet = []
                current_sheet_job_ids = set()
            else: # default mode, load existing
                existing_records = worksheet.get_all_records() 
                existing_jobs_in_sheet = [dict(rec) for rec in existing_records]
                current_sheet_job_ids = {str(job.get('job_id', '')) for job in existing_jobs_in_sheet if job.get('job_id')}
        except gspread.exceptions.WorksheetNotFound:
            if mode == 'deep':
                logger.info(f"Deep mode: Creating new sheet: {target_sheet_name}")
                worksheet = spreadsheet.add_worksheet(title=target_sheet_name, rows="1", cols=str(len(config.JOB_FIELDS)))
                existing_jobs_in_sheet = []
                current_sheet_job_ids = set()
            else: # Should have been caught by the check above, but as a safeguard
                logger.warning(f"Default mode: Sheet '{target_sheet_name}' not found unexpectedly. Skipping.")
                continue 
        except Exception as e:
            logger.error(f"Error accessing or creating sheet {target_sheet_name}: {e}")
            continue 

        added_to_this_sheet_count = 0
        for job in new_jobs_for_this_group:
            # In deep mode, current_sheet_job_ids is empty, so all jobs are "new" to the sheet
            # In default mode, we check against loaded IDs
            if str(job.get('job_id')) not in current_sheet_job_ids:
                job['status'] = 'New'
                job['notes'] = ''
                job['date_added'] = datetime.datetime.now().strftime("%Y-%m-%d")
                existing_jobs_in_sheet.append(job) # Add to the list that will be sorted and written
                if mode == 'default': # Only count as "added" if it wasn't there before in default mode
                    current_sheet_job_ids.add(str(job.get('job_id')))
                added_to_this_sheet_count += 1
        
        total_added_globally += added_to_this_sheet_count
        if added_to_this_sheet_count > 0:
            logger.info(f"Added/processed {added_to_this_sheet_count} jobs for sheet: {target_sheet_name}")

        sorted_jobs_for_sheet = sorted(
            existing_jobs_in_sheet, 
            key=lambda x: x.get('publishing_date', '1970-01-01'), 
            reverse=True
        )

        headers = config.JOB_FIELDS
        data_to_write = [headers] + [
            [str(job.get(field, '')) for field in headers] for job in sorted_jobs_for_sheet
        ]

        worksheet.clear() # Clear before writing in both modes (deep mode clears, default mode re-writes with merged)
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
        worksheet.format("A1:Z1", {"textFormat": {"bold": True}})
        logger.info(f"Sheet '{target_sheet_name}' updated with {len(sorted_jobs_for_sheet)} total jobs.")

    # Re-sort any existing sheets not touched by the new job batch
    # This part is more relevant for default mode, but can run in deep mode too.
    if mode == 'default': # Only re-sort other sheets in default mode if they weren't processed by new jobs
        for sheet_name in all_sheet_names_in_spreadsheet:
            if sheet_name not in processed_sheet_names:
                logger.info(f"Default mode: Re-sorting existing sheet not in current batch: {sheet_name}")
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    existing_records = worksheet.get_all_records()
                    if not existing_records:
                        logger.info(f"Sheet {sheet_name} is empty or not a job data sheet. Skipping re-sort.")
                        continue
                    existing_jobs_in_sheet = [dict(rec) for rec in existing_records]
                    
                    if not existing_jobs_in_sheet or not all(header in existing_jobs_in_sheet[0] for header in config.JOB_FIELDS[:3]):
                        logger.warning(f"Sheet {sheet_name} does not appear to be a job data sheet. Skipping re-sort.")
                        continue

                    sorted_jobs_for_sheet = sorted(
                        existing_jobs_in_sheet, 
                        key=lambda x: x.get('publishing_date', '1970-01-01'), 
                        reverse=True
                    )
                    headers = config.JOB_FIELDS
                    data_to_write = [headers] + [
                        [str(job.get(field, '')) for field in headers] for job in sorted_jobs_for_sheet
                    ]
                    worksheet.clear()
                    worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')
                    worksheet.format("A1:Z1", {"textFormat": {"bold": True}})
                    logger.info(f"Sheet '{sheet_name}' re-sorted.")
                except Exception as e:
                    logger.error(f"Error re-sorting sheet {sheet_name} in default mode: {e}")
    
    if total_added_globally == 0 and not any(group_data['jobs'] for group_data in new_job_groups.values()):
        logger.info("No new jobs to process or add to tracker.")
    else:
        logger.info(f"Total new/processed jobs for relevant sheets: {total_added_globally}")
    logger.info(f"Tracker data update attempt finished for Google Sheet ID: {sheet_id}")

def create_sheet_name(keywords, location, work_type_code_or_name):
    """Create a simplified sheet name from search parameters."""
    # work_type_code_or_name can be the integer code from the job data or the string name from create_search_param
    if isinstance(work_type_code_or_name, int):
        work_type_name = config.WORK_TYPE_NAMES.get(work_type_code_or_name, "Any")
    else: # It's already a name or None
        work_type_name = config.WORK_TYPE_NAMES.get(config.WORK_TYPES.get(work_type_code_or_name), work_type_code_or_name if work_type_code_or_name else "Any")

    keywords = str(keywords).replace('%2B', '+')
    location = str(location)
    
    sheet_name_parts = []
    if keywords and keywords != 'Unknown': sheet_name_parts.append(keywords)
    if location and location != 'Unknown': sheet_name_parts.append(location)
    if work_type_name != "Any": sheet_name_parts.append(work_type_name)

    sheet_name = "-".join(sheet_name_parts)
    if not sheet_name: sheet_name = "Default_Sheet"

    invalid_chars = [':', '\\', '/', '?', '*', '[', ']']
    for char in invalid_chars:
        sheet_name = sheet_name.replace(char, '')
    
    return sheet_name[:100] # Google Sheets have a 100 char limit for sheet names

def save_jobs_to_file(jobs, filename=config.JSON_OUTPUT_PATH):
    """Save jobs to a JSON file, overwriting if it exists."""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Convert to DataFrame for easier JSON export if needed, or just use json.dump
        # For simplicity, using pandas to handle potential complexities like date formatting
        df = pd.DataFrame(jobs)
        df.to_json(filename, orient='records', indent=4, date_format='iso')
        logger.info(f"Successfully saved {len(jobs)} jobs to {filename}")
    except Exception as e:
        logger.error(f"Error saving jobs to {filename}: {e}")

# Provide backward compatibility
load_existing_jobs_from_crm = load_existing_jobs_from_tracker
update_jobs_crm = update_jobs_tracker 