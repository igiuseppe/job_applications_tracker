import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration variables for the job applications tracker

# Work Type filters
WORK_TYPE_REMOTE = 2  # Remote jobs
WORK_TYPE_HYBRID = 3  # Hybrid jobs
WORK_TYPE_ONSITE = 1  # On-site jobs
WORK_TYPE_ANY = None  # No filter

# Work type descriptions for display and sheet naming
WORK_TYPE_NAMES = {
    WORK_TYPE_REMOTE: "Remote",
    WORK_TYPE_HYBRID: "Hybrid",
    WORK_TYPE_ONSITE: "On-site",
    WORK_TYPE_ANY: "Any"
}

# Work type codes by name (for search)
WORK_TYPES = {
    "Remote": WORK_TYPE_REMOTE,
    "Hybrid": WORK_TYPE_HYBRID,
    "On-site": WORK_TYPE_ONSITE,
}

# Scraping settings
DELAY_BETWEEN_SEARCHES = 1  # seconds

# File paths
OUTPUT_DIR = "output"
JSON_OUTPUT_PATH = f"{OUTPUT_DIR}/linkedin_jobs.json"
OUTREACH_OUTPUT_DIR = f"{OUTPUT_DIR}/outreach"
STATE_DIR = f"{OUTPUT_DIR}/state"
PROCESSED_IDS_PATH = f"{STATE_DIR}/processed_job_ids.json"

# Common field definitions for job data
JOB_FIELDS = [
    'id', 'job_title', 'search_keyword_job_title', 'country', 'work_type', 'company', 'location', 'publishing_date',
    'posted_time_ago', 'seniority_level', 'employment_type', 'job_function',
    'industries', 'date_added', 'job_link', 'company_link', 'job_description'
]

# Outreach CSV columns (final output per run)
OUTREACH_CSV_COLUMNS = [
    'job title',
    'description',
    'company name',
    'company linkedin url',
    'job url',
    'upload date',
    'hiring manager name',
    'hiring manager linkedin url',
    'fit',
    'message',
]

# BigQuery settings
BIGQUERY_PROJECT="decent-era-411512"
BIGQUERY_DATASET="jobs_tracker"
BIGQUERY_TABLE="jobs"
BIGQUERY_TABLE_LLM_DATA = "llm_data"
GOOGLE_CREDENTIALS_PATH = "credentials.json"  # Path to your Google API service account credentials, defaults to credentials.json

# LinkedIn URL templates
CONTRACT_TYPE_CODES = {
    "Full-time": "F",
    "Contract": "C",
    "Part-time": "P",
    "Temporary": "T",
    "Internship": "I",
    "Other": "O",
}

# Time posted filter codes for f_TPR
# Empty string means no filter
TIME_POSTED_CODES = {
    "Any": "",
    "Past 24 hours": "r86400",
    "Past Week": "r604800",
    "Past Month": "r2592000",
}

# Note: {contract_param} should be either an empty string or like 'f_JT=F%2CC'
LINKEDIN_JOB_LIST_URL_TEMPLATE = (
    "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
    "keywords={keywords}&location={location}&geoId={geoId}&f_WT={work_type}"
    "{contract_param}{time_param}&start={start_position}"
)
LINKEDIN_JOB_DETAIL_URL_TEMPLATE = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}" 

# Geo IDs (countries)
GEO_IDS = {
    "Italia": "103350119",
    "Francia": "105015875",
    "Germania": "101282230",
    "Spagna": "105646813",
    "Portogallo": "100364837",
    "Paesi Bassi": "102890719",
    "Svizzera": "106693272",
    "Regno Unito": "101165590",
    "Irlanda": "104738515",
    "Svezia": "105117694",
    "Danimarca": "104514075",
    "Finlandia": "100456013",
    "Norvegia": "103819153",
    "Austria": "103883259",
    # English names used in new configs
    "Germany": "101282230",
    "Switzerland": "106693272",
    "France": "105015875",
    "Italy": "103350119",
}