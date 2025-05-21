import requests
from bs4 import BeautifulSoup
import time
import json

def get_job_description(job_public_url):
    response=requests.get(job_public_url)
    soup=BeautifulSoup(response.text, "html.parser")
    description = soup.find('div', class_='description__text description__text--rich').text.strip()  # Example: Job title
    return description

# Helper function to safely get text
def get_text_or_none(element):
    return element.get_text(strip=True) if element else None

def get_href_or_none(element):
    return element['href'] if element and element.has_attr('href') else None

def clean_job_html(html_content):
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

    # Create and return a dictionary with all extracted variables
    job_data = {
        "job_title": job_title,
        "company_name": company_name,
        "location": location,
        "posted_time_ago": posted_time_ago,
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
        "recruiter_link": recruiter_link
    }
    
    return job_data

def get_job_list_page(keywords, location, geoId, start_position):
    """Fetches a page of job listings from LinkedIn"""
    list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&geoId={geoId}&start={start_position}"
    
    try:
        response = requests.get(list_url)
        soup = BeautifulSoup(response.text, "html.parser")
        page_jobs = soup.find_all("li")
        return page_jobs
    except Exception as e:
        print(f"Error fetching job list page: {str(e)}")
        return []

def extract_job_id(job_element):
    """Extracts job ID from a job listing element"""
    base_card_div = job_element.find("div", {"class": "base-card"})
    if not base_card_div or not base_card_div.get("data-entity-urn"):
        return None
    
    return base_card_div.get("data-entity-urn").split(":")[-1]

def fetch_job_details(job_id):
    """Fetches and processes details for a specific job"""
    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    
    try:
        job_response = requests.get(job_url)
        html_content = job_response.text
        job_details = clean_job_html(html_content)
        
        # Add job_id to the details
        job_details["job_id"] = job_id
        return job_details
    except Exception as e:
        print(f"Error scraping job {job_id}: {str(e)}")
        return None

def save_jobs_to_file(jobs, filename="output/linkedin_jobs.json"):
    """Saves job data to a JSON file"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"Job data saved to {filename}")

def scrape_linkedin_jobs(keywords, location, geoId, jobs_per_page=10, max_pages=2):
    """Main function to scrape LinkedIn jobs with pagination"""
    all_jobs = []
    page = 0
    
    while page < max_pages:
        start_position = page * jobs_per_page
        print(f"Scraping page {page+1}, jobs {start_position+1}-{start_position+jobs_per_page}")
        
        page_jobs = get_job_list_page(keywords, location, geoId, start_position)
        
        # If no jobs are found, we've reached the end
        if not page_jobs:
            print(f"No more jobs found after {len(all_jobs)} jobs")
            break
        
        for job in page_jobs:
            job_id = extract_job_id(job)
            if not job_id:
                continue
            
            job_details = fetch_job_details(job_id)
            if job_details:
                all_jobs.append(job_details)
                print(f"Scraped: {job_details['job_title']} at {job_details['company_name']}")
            
            # Optional: add a delay to avoid rate limiting
            time.sleep(1)
        
        page += 1
    
    print(f"Total jobs scraped: {len(all_jobs)}")
    return all_jobs

def main():
    # Search parameters
    keywords = "Business%2BAnalyst"
    location = "Italia"
    geoId = "103350119"
    max_pages = 1
    
    # Scrape jobs
    all_jobs = scrape_linkedin_jobs(keywords, location, geoId, max_pages=max_pages)
    
    # Save results
    save_jobs_to_file(all_jobs)

if __name__ == "__main__":
    main()
