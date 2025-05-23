import requests
from bs4 import BeautifulSoup
import time
import json
import csv
import os
import datetime
import re
from pathlib import Path
import pandas as pd
import logging
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_job_description(job_public_url):
    """Fetch a job description from a public LinkedIn URL"""
    response = requests.get(job_public_url)
    soup = BeautifulSoup(response.text, "html.parser")
    description = soup.find('div', class_='description__text description__text--rich').text.strip()  # Example: Job title
    return description

# Helper function to safely get text
def get_text_or_none(element):
    return element.get_text(strip=True) if element else None

def get_href_or_none(element):
    return element['href'] if element and element.has_attr('href') else None

def clean_job_html(html_content, work_type=None):
    """Extract structured job data from LinkedIn job HTML"""
    soup = BeautifulSoup(html_content, 'lxml')

    # --- Top Card Information ---
    job_title_element = soup.find('h2', class_='top-card-layout__title')
    job_title = get_text_or_none(job_title_element)

    company_name_element = soup.find('a', class_='topcard__org-name-link')
    company_name = get_text_or_none(company_name_element)

    location_element = soup.find('span', class_='topcard__flavor--bullet')
    location = get_text_or_none(location_element) # This might need refinement if there are multiple such spans

    posted_time_ago_element = soup.find('span', class_='posted-time-ago__text')
    posted_time_ago = get_text_or_none(posted_time_ago_element)

    num_applicants_note_element = soup.find('figcaption', class_='num-applicants__caption')
    num_applicants_note = get_text_or_none(num_applicants_note_element)

    see_who_hired_text_element = soup.find('p', class_='face-pile__text')
    see_who_hired_text = get_text_or_none(see_who_hired_text_element)

    # --- Recruiter Information ---
    recruiter_message_element = soup.select_one('.message-the-recruiter > p')
    recruiter_message = get_text_or_none(recruiter_message_element)

    recruiter_name_element = soup.find('h3', class_='base-main-card__title--link')
    recruiter_name = get_text_or_none(recruiter_name_element)

    recruiter_tagline_element = soup.find('h4', class_='base-main-card__subtitle')
    recruiter_tagline = get_text_or_none(recruiter_tagline_element)


    # --- Job Description ---
    job_description_container = soup.find('div', class_='show-more-less-html__markup')
    if job_description_container:
        job_description_paragraphs = job_description_container.find_all(['p', 'ul'])
        job_description_parts = []
        for item in job_description_paragraphs:
            if item.name == 'ul':
                list_items = [li.get_text(separator=' ', strip=True) for li in item.find_all('li')]
                job_description_parts.append("\n".join(["- " + li for li in list_items]))
            else:
                job_description_parts.append(item.get_text(separator=' ', strip=True))
        job_description = "\n\n".join(job_description_parts)
    else:
        job_description = None

    # --- Job Criteria ---
    seniority_level = None
    employment_type = None
    job_function = None
    industries = None

    criteria_items = soup.find_all('li', class_='description__job-criteria-item')
    for item in criteria_items:
        header = item.find('h3', class_='description__job-criteria-subheader')
        text_element = item.find('span', class_='description__job-criteria-text--criteria')
        if header and text_element:
            header_text = get_text_or_none(header)
            criteria_text = get_text_or_none(text_element)
            if header_text == 'Seniority level':
                seniority_level = criteria_text
            elif header_text == 'Employment type':
                employment_type = criteria_text
            elif header_text == 'Job function':
                job_function = criteria_text
            elif header_text == 'Industries':
                industries = criteria_text

    # --- Referral Information ---
    referral_text_element = soup.select_one('.find-a-referral__cta-container > p')
    referral_text = get_text_or_none(referral_text_element)

    referral_cta_element = soup.select_one('a.find-a-referral__cta')
    referral_cta_text = get_text_or_none(referral_cta_element)

    job_title_anchor_element = soup.find('a', class_='topcard__link', attrs={'data-tracking-control-name': 'public_jobs_topcard-title'})
    job_link = get_href_or_none(job_title_anchor_element)

    company_name_anchor_element = soup.find('a', class_='topcard__org-name-link', attrs={'data-tracking-control-name': 'public_jobs_topcard-org-name'})
    company_link = get_href_or_none(company_name_anchor_element)

    recruiter_profile_anchor_element = soup.select_one('div.message-the-recruiter a.base-card__full-link')
    # This selector targets the main link covering the recruiter's card.
    # An alternative, more specific to the recruiter's name if the structure is consistent:
    # recruiter_profile_anchor_element = soup.select_one('h3.base-main-card__title--link a') # if the <a> was inside the <h3>
    # However, in your HTML, the <a> wraps the content.
    recruiter_link = get_href_or_none(recruiter_profile_anchor_element)

    # Extract the publishing date from posted_time_ago
    publishing_date = extract_publishing_date(posted_time_ago)
    
    # Get work type name if specified
    work_type_name = config.WORK_TYPE_NAMES.get(work_type, "Unknown")

    # Create and return a dictionary with all extracted variables
    job_data = {
        "job_title": job_title,
        "company_name": company_name,
        "location": location,
        "posted_time_ago": posted_time_ago,
        "publishing_date": publishing_date,
        "num_applicants_note": num_applicants_note,
        "recruiter_message": recruiter_message,
        "recruiter_name": recruiter_name,
        "recruiter_tagline": recruiter_tagline,
        "job_description": job_description,
        "seniority_level": seniority_level,
        "employment_type": employment_type,
        "job_function": job_function,
        "industries": industries,
        "job_link": job_link,
        "company_link": company_link,
        "recruiter_link": recruiter_link,
        "work_type": work_type,
        "work_type_name": work_type_name
    }
    
    return job_data

def extract_publishing_date(posted_time_ago):
    """
    Extract an approximate publishing date from the 'posted X time ago' text
    """
    if not posted_time_ago:
        return datetime.datetime.now().strftime("%Y-%m-%d")
    
    today = datetime.datetime.now()
    
    # Handle different time formats
    if "minute" in posted_time_ago or "hour" in posted_time_ago:
        return today.strftime("%Y-%m-%d")
    elif "day" in posted_time_ago:
        match = re.search(r'(\d+)', posted_time_ago)
        if match:
            days = int(match.group(1))
            date = today - datetime.timedelta(days=days)
            return date.strftime("%Y-%m-%d")
    elif "week" in posted_time_ago:
        match = re.search(r'(\d+)', posted_time_ago)
        if match:
            weeks = int(match.group(1))
            date = today - datetime.timedelta(weeks=weeks)
            return date.strftime("%Y-%m-%d")
    elif "month" in posted_time_ago:
        match = re.search(r'(\d+)', posted_time_ago)
        if match:
            months = int(match.group(1))
            # Approximate months as 30 days
            date = today - datetime.timedelta(days=months*30)
            return date.strftime("%Y-%m-%d")
    
    # Default to today if we can't parse
    return today.strftime("%Y-%m-%d")

def get_job_list_page(keywords, location, geoId, start_position, work_type):
    """
    Fetches a page of job listings from LinkedIn
    
    Args:
        keywords: Search keywords
        location: Location to search in
        geoId: LinkedIn GeoID for the location
        start_position: Starting position for pagination
        work_type: Work type filter (None, 1=on-site, 2=remote, 3=hybrid)
    """

    
    list_url = config.LINKEDIN_JOB_LIST_URL_TEMPLATE.format(
        keywords=keywords,
        location=location,
        geoId=geoId,
        work_type=work_type,
        start_position=start_position
    )
    
    logger.info(f"Fetching URL: {list_url}")  # Debugging output
    
    try:
        response = requests.get(list_url)
        soup = BeautifulSoup(response.text, "html.parser")
        page_jobs = soup.find_all("li")
        return page_jobs
    except Exception as e:
        logger.error(f"Error fetching job list page: {str(e)}")
        return []

def extract_job_id(job_element):
    """Extracts job ID from a job listing element"""
    base_card_div = job_element.find("div", {"class": "base-card"})
    if not base_card_div or not base_card_div.get("data-entity-urn"):
        return None
    
    return base_card_div.get("data-entity-urn").split(":")[-1]

def fetch_job_details(job_id, work_type=None):
    """Fetches and processes details for a specific job"""
    job_url = config.LINKEDIN_JOB_DETAIL_URL_TEMPLATE.format(job_id=job_id)
    
    try:
        job_response = requests.get(job_url)
        html_content = job_response.text
        job_details = clean_job_html(html_content, work_type)
        
        # Add job_id to the details
        job_details["job_id"] = job_id
        return job_details
    except Exception as e:
        logger.error(f"Error scraping job {job_id}: {str(e)}")
        return None

def save_jobs_to_file(jobs, filename=config.JSON_OUTPUT_PATH):
    """Saves job data to a JSON file"""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    logger.info(f"Job data saved to {filename}")

def scrape_linkedin_jobs(keywords, location, geoId, work_type, jobs_per_page=25, max_pages=1):
    """
    Scrapes LinkedIn jobs for the specified search criteria
    
    Args:
        keywords: Search keywords
        location: Location to search in
        geoId: LinkedIn GeoID for the location
        work_type: Work type filter (None, 1=on-site, 2=remote, 3=hybrid)
        jobs_per_page: Number of jobs per page (default is 25)
        max_pages: Maximum number of pages to scrape
        
    Returns:
        List of job dictionaries
    """
    logger.info(f"Starting LinkedIn job scrape for keywords: '{keywords}', location: '{location}', work_type: {work_type}")
    
    all_jobs = []
    processed_job_ids = set()

    for page_num in range(max_pages):
        start_position = page_num * jobs_per_page
        logger.info(f"Scraping page {page_num + 1} (start_position: {start_position})...")
        
        page_job_elements = get_job_list_page(keywords, location, geoId, start_position, work_type)
        
        if not page_job_elements:
            logger.info("No more job elements found on the page. Ending scrape for this search.")
            break # Stop if no jobs are found on the page

        job_count_on_page = 0
        for job_element in page_job_elements:
            job_id = extract_job_id(job_element)
            
            if job_id and job_id not in processed_job_ids:
                logger.info(f"Processing job ID: {job_id}")
                job_details = fetch_job_details(job_id, work_type)
                if job_details:
                    # Add search parameters to job_details for context
                    job_details['search_keywords'] = keywords
                    job_details['search_location'] = location
                    job_details['search_geo_id'] = geoId
                    # work_type is already in job_details from fetch_job_details
                    
                    all_jobs.append(job_details)
                    processed_job_ids.add(job_id)
                    job_count_on_page += 1
                    logger.info(f"Successfully processed job: {job_details.get('job_title', 'N/A')} at {job_details.get('company_name', 'N/A')}")
                else:
                    logger.warning(f"Failed to fetch details for job ID: {job_id}")
            elif job_id in processed_job_ids:
                logger.debug(f"Job ID: {job_id} already processed. Skipping.")
            # else: job_id is None, so it's not a job card we can process

        logger.info(f"Found {job_count_on_page} new jobs on page {page_num + 1}.")
        
        if job_count_on_page == 0 and page_num > 0: # If not the first page and no new jobs found
             logger.info("No new jobs found on this page, and it's not the first page. Assuming end of results.")
             break

        # Add a delay between pages to be respectful to the server
        if page_num < max_pages - 1: # Don't sleep after the last page
            time.sleep(config.DELAY_BETWEEN_SEARCHES) 
            
    logger.info(f"Scrape finished for keywords: '{keywords}', location: '{location}'. Found {len(all_jobs)} jobs.")
    return all_jobs

def main():
    """Main function to test the scraper with example parameters."""
    logger.info("Starting LinkedIn Scraper Test...")

    # --- Configuration ---
    # Test Search Parameters (more examples below)
    keywords = "Business%2BAnalyst"
    location = "Italia"
    geoId = "103350119"
    max_pages = 1
    
    # Scrape jobs
    all_jobs = scrape_linkedin_jobs(keywords, location, geoId, max_pages=max_pages)
    
    # Save results to JSON
    save_jobs_to_file(all_jobs)

if __name__ == "__main__":
    main()
