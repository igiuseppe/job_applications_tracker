import time
import datetime
import argparse
import logging
from tracker_manager import query_jobs_with_filters
from enhancer_manager import (
    load_existing_llm_job_ids,
    insert_llm_data
)
import json
from utils import call_llm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_prompt(job_row):
    job_description = job_row.get("job_description")
    prompt=f"""
You are my personal job hunter assistant. I need you to extract all the needed information from the following job description:
{job_description}
"""
    return prompt

def build_response_format():
    from scrape_jobs import JOB_TITLES
    job_titles = list(JOB_TITLES.keys())
    job_titles+=["Other"]
    response_format = {
        "type": "json_schema",
        "json_schema": 
        {
        "name": "job_details",
        "schema": {
            "type": "object",
            "properties": {
            "years_of_experience": {
                "type": "number",
                "description": "Number of years of experience required for the job, if there is a range put the minimum value. If not specified, put 0."
            },
            "company_type": {
                "type": "string",
                "description": "Type of the company offering the job.",
                "enum": ["Startup", "Large company", "Medium company", "Small company", "Consulting company"]
            },
            "selection_process": {
                "type": "string",
                "description": "Details of the selection process for the job. If not specified, put 'None'."
            },
            "language_requirements": {
                "description": "Language requirements for the job. Put here only the hard requirements. If other languages not specified here are mandatory put 'Other'.",
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["English", "Italian","French", "German", "Spanish", "Portuguese", "Other"]
                }
            },
            "technical_requirements": {
                "type": "string",
                "description": "Technical requirements for the job. Put here only technical ones, not related to languages or soft skills."
            },
            "real_job_title": {
                "type": "string",
                "description": "A concise job title that reflects the job description. If possible, select the most appropriate job title from the following list: {job_titles}. If none of the titles are a good match, put 'Other'.",
                "enum": job_titles
            },
            "salary": {
                "type": "integer",
                "description": "Salary for the job (monthly, in Euro), if there is a range put the average value. If they talk about yearly, convert it to monthly. If not present put 0."
            }},
            "required": [
            "years_of_experience",
            "company_type",
            "selection_process",
            "language_requirements",
            "technical_requirements",
            "salary",
            "real_job_title"
            ],
            "additionalProperties": False
            },
        "strict": True
        }
    }
    return response_format

def modify_lang(lang_list):
    if "Other" in lang_list or "French" in lang_list or "German" in lang_list or "Spanish" in lang_list or "Portuguese" in lang_list:
        lang="Other"
    elif "English" in lang_list:
        lang="English"
    elif "Italian" in lang_list:
        lang="Italian"
    else:
        lang=str(lang_list)
    return lang

def enhance_job_with_llm(job_row):
    prompt = build_prompt(job_row)
    response_format = build_response_format()
    try:
        response_str,input_tokens,output_tokens = call_llm(prompt=prompt, response_format=response_format)
        job_details = json.loads(response_str)
        job_details["input_tokens"] = input_tokens
        job_details["output_tokens"] = output_tokens
        job_details["language_requirements"] = modify_lang(job_details["language_requirements"])
        return job_details
    except Exception as e:
        logger.error(f"Error enhancing job {job_row.get('id')}: {e}")
        return None

def main(filters=None):
    logger.info("Starting LLM enhancement process for jobs data...")
    existing_llm_job_ids = load_existing_llm_job_ids()
    jobs = query_jobs_with_filters(filters)
    jobs_to_process = [job for job in jobs if str(job.get("id")) not in existing_llm_job_ids]
    logger.info(f"Total jobs fetched: {len(jobs)}. To enhance: {len(jobs_to_process)}.")
    enhanced_rows = []
    total_inserted = 0
    for i, job in enumerate(jobs_to_process):
        logger.info(f"Enhancing job {i+1}/{len(jobs_to_process)}: {job.get('id')}")
        enhanced = enhance_job_with_llm(job)
        if enhanced:
            enhanced["job_id"]=job.get("id")
            enhanced["created_at"]=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            enhanced_rows.append(enhanced)
        if len(enhanced_rows) >= 100:
            num_inserted = insert_llm_data(enhanced_rows)
            total_inserted += num_inserted
            logger.info(f"Inserted {num_inserted} rows into llm_data (batch of 100). Total inserted so far: {total_inserted}.")
            enhanced_rows = []
        time.sleep(1)  # avoid rate limits
    # Insert any remaining rows
    if enhanced_rows:
        num_inserted = insert_llm_data(enhanced_rows)
        total_inserted += num_inserted
        logger.info(f"Inserted {num_inserted} rows into llm_data (final batch). Total inserted: {total_inserted}.")
    logger.info(f"LLM enhancement complete. {total_inserted} new rows inserted into llm_data.")

default_filters="""
seniority_level in ('Associate','Entry level') 
and  publishing_date >= (CURRENT_TIMESTAMP() - INTERVAL 7 DAY)
order by id
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhance jobs data with LLM.")
    parser.add_argument('--filters', type=str, default=default_filters, help='Optional SQL WHERE clause to filter jobs (e.g., "country = \'Italia\'")')
    args = parser.parse_args()
    main(args.filters) 