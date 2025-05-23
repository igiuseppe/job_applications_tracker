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

# Scraping settings
DELAY_BETWEEN_REQUESTS = 1  # seconds

# File paths
OUTPUT_DIR = "output"
JSON_OUTPUT_PATH = f"{OUTPUT_DIR}/linkedin_jobs.json"
TRACKER_EXCEL_PATH = f"{OUTPUT_DIR}/jobs_tracker.xlsx"

# Tracker settings
TRACKER_STATUSES = ["New", "Applied", "Interview", "Rejected", "Offer"]

# Common field definitions for job data
JOB_FIELDS = [
    'job_id', 'job_title', 'company_name', 'location', 'publishing_date',
    'posted_time_ago', 'seniority_level', 'employment_type', 'job_function',
    'industries', 'status', 'notes', 'date_added', 'job_link', 'company_link', 
    'work_type_name', 'job_description'
]

# LinkedIn URL templates
# Note: f_WT parameter is now handled separately in the scraper code
LINKEDIN_JOB_LIST_URL_TEMPLATE = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&geoId={geoId}&f_E=2%2C3&f_WT={work_type}&start={start_position}"
LINKEDIN_JOB_DETAIL_URL_TEMPLATE = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}" 