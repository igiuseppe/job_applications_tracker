import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
from pathlib import Path
import pandas as pd
import logging
import config
import os
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Common headers to mimic a browser
COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

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

def clean_job_html(html_content, work_type=None, country=None, search_keyword_job_title=None):
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
    date_added = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get work type name if specified
    work_type_name = config.WORK_TYPE_NAMES.get(work_type, "Unknown")

    # Create and return a dictionary with all extracted variables
    job_data = {
        "job_title": job_title,
        "company": company_name,
        "location": location,
        "posted_time_ago": posted_time_ago,
        "publishing_date": publishing_date,
        "date_added": date_added,
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
        "work_type": work_type_name,
        "country": country,
        "search_keyword_job_title": search_keyword_job_title
    }
    
    return job_data

def extract_publishing_date(posted_time_ago):
    """
    Extract an approximate publishing date from the 'posted X time ago' text.
    Returns a string in the format 'YYYY-MM-DD HH:MM:SS', using the current time minus the posted_time_ago.
    """
    import re
    now = datetime.datetime.now()
    if not posted_time_ago:
        return now.strftime("%Y-%m-%d %H:%M:%S")
    posted_time_ago = posted_time_ago.lower()
    match = re.search(r'(\d+)\s*(minute|hour|day|week|month)', posted_time_ago)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == 'minute':
            dt = now - datetime.timedelta(minutes=value)
        elif unit == 'hour':
            dt = now - datetime.timedelta(hours=value)
        elif unit == 'day':
            dt = now - datetime.timedelta(days=value)
        elif unit == 'week':
            dt = now - datetime.timedelta(weeks=value)
        elif unit == 'month':
            dt = now - datetime.timedelta(days=value*30)
        else:
            dt = now
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    # If no match, fallback to now
    return now.strftime("%Y-%m-%d %H:%M:%S")

def get_job_list_page(keywords, location, geoId, start_position, work_type, contract_types=None, time_posted_code: str = ""):
    """
    Fetches a page of job listings from LinkedIn
    
    Args:
        keywords: Search keywords
        location: Location to search in
        geoId: LinkedIn GeoID for the location
        start_position: Starting position for pagination
        work_type: Work type filter (None, 1=on-site, 2=remote, 3=hybrid)
    """

    
    # Build optional contract types param f_JT=F%2CC (if provided)
    contract_param = ""
    if contract_types:
        if isinstance(contract_types, (list, tuple, set)):
            joined = "%2C".join(contract_types)
        else:
            joined = str(contract_types)
        contract_param = f"&f_JT={joined}"

    time_param = ""
    if time_posted_code:
        time_param = f"&f_TPR={time_posted_code}"

    list_url = config.LINKEDIN_JOB_LIST_URL_TEMPLATE.format(
        keywords=keywords,
        location=location,
        geoId=geoId,
        work_type=work_type,
        contract_param=contract_param,
        time_param=time_param,
        start_position=start_position
    )
    
    logger.info(f"Fetching URL: {list_url}")  # Debugging output
    
    try:
        response = requests.get(list_url, headers=COMMON_HEADERS)
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

def fetch_job_details(job_id, work_type=None, country=None, search_keyword_job_title=None):
    """Fetches and processes details for a specific job"""
    job_url = config.LINKEDIN_JOB_DETAIL_URL_TEMPLATE.format(job_id=job_id)
    
    try:
        job_response = requests.get(job_url, headers=COMMON_HEADERS)
        html_content = job_response.text
        job_details = clean_job_html(html_content, work_type, country, search_keyword_job_title)
        
        if job_details:
            job_details["id"] = job_id
        return job_details, html_content
    except Exception as e:
        logger.error(f"Error scraping job {job_id}: {str(e)}")
        return None, None

def scrape_linkedin_jobs(keywords, location, geoId, work_type, jobs_per_page=25, max_pages=1, search_keyword_job_title=None, contract_types=None, time_posted_code: str = ""):
    """
    Scrapes LinkedIn jobs for the specified search criteria
    Returns: List of job dictionaries
    """
    all_jobs = []
    processed_job_ids = set()
    debug_html_dir = os.path.join("output", "debug_html")
    os.makedirs(debug_html_dir, exist_ok=True)

    for page_num in range(max_pages):
        start_position = page_num * jobs_per_page
        page_job_elements = get_job_list_page(keywords, location, geoId, start_position, work_type, contract_types, time_posted_code)
        if not page_job_elements:
            break
        for job_element in page_job_elements:
            job_id = extract_job_id(job_element)
            if job_id and job_id not in processed_job_ids:
                time.sleep(random.uniform(1.0, 2.5))
                job_details, html_content = fetch_job_details(job_id, work_type, location, search_keyword_job_title)
                if job_details:
                    job_title = job_details.get('job_title')
                    company_name = job_details.get('company')
                    if not job_title or not company_name:
                        job_link = config.LINKEDIN_JOB_DETAIL_URL_TEMPLATE.format(job_id=job_id)
                        html_file_path = os.path.join(debug_html_dir, f"debug_html_{job_id}.html")
                        try:
                            with open(html_file_path, 'w', encoding='utf-8') as f:
                                f.write(html_content if html_content else "")
                            logger.warning(f"Missing title/company for job ID: {job_id}. Link: {job_link}. HTML saved to: {html_file_path}")
                        except Exception as e_write:
                            logger.error(f"Could not write HTML for job ID {job_id} to {html_file_path}: {e_write}")
                    job_details['search_keywords'] = keywords
                    job_details['search_location'] = location
                    job_details['search_geo_id'] = geoId
                    all_jobs.append(job_details)
                    processed_job_ids.add(job_id)
                else:
                    logger.warning(f"Failed to fetch details for job ID: {job_id}")
            # else: job_id is None, so it's not a job card we can process
        if len(page_job_elements) == 0:
            break
        if page_num < max_pages - 1:
            time.sleep(config.DELAY_BETWEEN_SEARCHES)
    logger.info(f"Scrape finished for keywords: '{keywords}', location: '{location}'. Found {len(all_jobs)} jobs.")
    return all_jobs

def fetch_public_profile(profile_url):
    """Fetch minimal public profile info from a LinkedIn profile URL (unauthenticated, best-effort)."""
    try:
        resp = requests.get(profile_url, headers=COMMON_HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, 'lxml')
        # Best-effort selectors across public profiles
        # Headline fallback to <title>
        title_tag = soup.find('title')
        headline = title_tag.get_text(strip=True) if title_tag else None

        # Try to capture a visible name element
        name = None
        h1 = soup.find('h1')
        if h1:
            name = h1.get_text(strip=True)

        # Try to capture a subtitle/headline block
        subtitle = None
        possible_classes = [
            'text-body-medium',
            'pv-text-details__left-panel',
            'pv-top-card--list',
        ]
        for cls in possible_classes:
            el = soup.find(class_=cls)
            if el:
                subtitle = el.get_text(separator=' ', strip=True)
                break

        location = None
        loc_candidates = soup.find_all('span')
        for span in loc_candidates[:100]:
            txt = span.get_text(strip=True)
            if txt and any(k in txt.lower() for k in ["location", "based", "milan", "london", "remote"]):
                location = txt
                break

        return {
            'profile_headline': headline,
            'profile_name': name,
            'profile_subtitle': subtitle,
            'profile_location': location,
        }
    except Exception as e:
        logger.error(f"Error fetching public profile {profile_url}: {e}")
        return {
            'profile_headline': None,
            'profile_name': None,
            'profile_subtitle': None,
            'profile_location': None,
        }

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
    # save_jobs_to_file(all_jobs)

if __name__ == "__main__":
    main()
