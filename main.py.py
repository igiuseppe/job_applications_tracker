import os
import time
from linkedin_scraper import scrape_linkedin_jobs
from tracker_manager import update_jobs_crm, save_jobs_to_file

# Define common geo IDs for popular locations
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
    "Austria": "103883259"
}

# Common job titles with URL-safe formatting
JOB_TITLES = {
    "Business Analyst": "Business%2BAnalyst",
    "Data Scientist": "Data%2BScientist",
    "Data Analyst": "Data%2BAnalyst",
    "Data Engineer": "Data%2BEngineer",
    "ML Engineer": "ML%2BEngineer",
    "AI Engineer": "AI%2BEngineer",
    "Software Engineer": "Software%2BEngineer",
    "Product Manager": "Product%2BManager",
    "Project Manager": "Project%2BManager",
    "Sales Engineer": "Sales%2BEngineer",
}

# Work type definitions
WORK_TYPES = {
    "Remote": 2,
    "Hybrid": 3,
    "On-site": 1
}

def create_search_param(job_title, location, work_type, max_pages=1):
    """Helper function to create a search parameter dictionary"""
    # Convert string inputs to their corresponding codes
    job_title_code = JOB_TITLES.get(job_title, job_title)
    geo_id = GEO_IDS.get(location, location)
    work_type_code = WORK_TYPES.get(work_type, work_type)
    
    return {
        "name": f"{job_title} - {location} - {work_type}",
        "keywords": job_title_code,
        "location": location,
        "geo_id": geo_id,
        "work_type": work_type_code,
        "max_pages": max_pages
    }

def run_search(search_params):
    """Run a LinkedIn job search with the specified parameters"""
    print("-" * 50)
    print(f"Running search: {search_params['name']}")
    print(f"Keywords: {search_params['keywords']}")
    print(f"Location: {search_params['location']} (GeoID: {search_params['geo_id']})")
    print(f"Work type: {work_type_to_name(search_params['work_type'])}")
    print("-" * 50)
    
    jobs = scrape_linkedin_jobs(
        keywords=search_params['keywords'],
        location=search_params['location'],
        geoId=search_params['geo_id'],
        work_type=search_params['work_type'],
        jobs_per_page=search_params.get('jobs_per_page', 10),
        max_pages=search_params.get('max_pages', 1)
    )
    
    print(f"Found {len(jobs)} jobs for {search_params['name']}")
    return jobs

def work_type_to_name(work_type):
    """Convert work type value to name"""
    work_type_names = {
        1: "On-site",
        2: "Remote",
        3: "Hybrid",
        None: "Any"
    }
    return work_type_names.get(work_type, "Unknown")

def italy_jobs_routine(max_pages=1):
    """
    Search routine for all job titles in Italy with both remote and hybrid options
    
    Args:
        max_pages: Number of pages to scrape per search
    """
    searches = []
    
    # All job titles
    job_titles = list(JOB_TITLES.keys())
    
    # Work types for Italy
    work_types = ["Remote", "Hybrid"]
    
    # Create search combinations for Italy
    for job_title in job_titles:
        for work_type in work_types:
            searches.append(create_search_param(job_title, "Italia", work_type, max_pages=max_pages))
    
    return searches

def europe_remote_jobs_routine(max_pages=1):
    """
    Search routine for all job titles in European countries (excluding Italy) with remote only
    
    Args:
        max_pages: Number of pages to scrape per search
    """
    searches = []
    
    # All job titles
    job_titles = list(JOB_TITLES.keys())
    
    # European countries excluding Italy
    countries = [
        "Francia",
        "Germania",
        "Spagna",
        "Portogallo",
        "Paesi Bassi",
        "Svizzera",
        "Regno Unito",
        "Irlanda",
        "Svezia",
        "Danimarca",
        "Finlandia",
        "Norvegia",
        "Austria"
    ]
    
    # Work type for Europe (remote only)
    work_type = "Remote"
    
    # Create search combinations for Europe
    for job_title in job_titles:
        for country in countries:
            searches.append(create_search_param(job_title, country, work_type, max_pages=max_pages))
    
    return searches

def main():
    """Main function to run search routines"""
    print("LinkedIn Job Search Routines")
    print("=" * 50)
    
    # Output settings
    output_dir = "output"
    json_path = f"{output_dir}/linkedin_jobs.json"
    crm_path = f"{output_dir}/jobs_tracker.xlsx"
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Select which search strategy to use (uncomment one)
    search_strategy = "both"  # Run both Italy and Europe routines
    # search_strategy = "italy"  # Run only Italy routine
    # search_strategy = "europe"  # Run only Europe routine
    
    # Settings for page depth
    max_pages_italy = 1
    max_pages_europe = 1
    
    # Collect search combinations based on strategy
    search_combinations = []
    
    if search_strategy in ["both", "italy"]:
        italy_searches = italy_jobs_routine(max_pages=max_pages_italy)
        search_combinations.extend(italy_searches)
        print(f"Added {len(italy_searches)} Italy searches")
    
    if search_strategy in ["both", "europe"]:
        europe_searches = europe_remote_jobs_routine(max_pages=max_pages_europe)
        search_combinations.extend(europe_searches)
        print(f"Added {len(europe_searches)} Europe searches")
    
    print(f"Running {len(search_combinations)} searches in total")
    
    # Optional: Limit the number of searches to run (for testing)
    # max_searches = 10
    # if len(search_combinations) > max_searches:
    #     print(f"Limiting to first {max_searches} searches")
    #     search_combinations = search_combinations[:max_searches]
    
    # Run all searches and collect results
    all_jobs = []
    
    for i, search_params in enumerate(search_combinations[-2:-1]):
        # Show progress
        print(f"\nSearch {i+1} of {len(search_combinations)}")
        
        # Run the search
        jobs = run_search(search_params)
        all_jobs.extend(jobs)
        
        # Pause between searches to avoid rate limiting
        if search_params != search_combinations[-1]:  # Skip delay after last search
            print("Pausing between searches...")
            time.sleep(5)  # 5 second pause
    
    # Save all jobs to JSON
    if all_jobs:
        save_jobs_to_file(all_jobs, json_path)
        print(f"Saved {len(all_jobs)} jobs to {json_path}")
        
        # Update CRM with all jobs
        update_jobs_crm(all_jobs, crm_path)
        print(f"Updated CRM at {crm_path}")
    else:
        print("No jobs found across all searches")
    
    print("=" * 50)
    print(f"Search routine completed. Ran {len(search_combinations)} searches and found {len(all_jobs)} jobs.")

if __name__ == "__main__":
    main() 