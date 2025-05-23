import os
import time
import datetime
import argparse
from linkedin_scraper import scrape_linkedin_jobs
from tracker_manager import update_jobs_tracker, save_jobs_to_file
import config

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
    "Product Manager": "Product%2BManager",
    "Sales Engineer": "Sales%2BEngineer",
    "Business Analyst": "Business%2BAnalyst",
    "ML Engineer": "ML%2BEngineer",
    "AI Engineer": "AI%2BEngineer",
    "Data Scientist": "Data%2BScientist",
    "Data Analyst": "Data%2BAnalyst",
    "Data Engineer": "Data%2BEngineer",
    "Software Engineer": "Software%2BEngineer",
    "Project Manager": "Project%2BManager",
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

def run_search(search_params, output_dir, mode):
    """Run a LinkedIn job search with the specified parameters and save intermediate results"""
    search_name = search_params['name']
    search_id = search_name.replace(" ", "_").replace("-", "_").lower()
    
    # Start timing
    start_time = time.time()
    
    print("-" * 50)
    print(f"Running search: {search_name}")
    print(f"Keywords: {search_params['keywords']}")
    print(f"Location: {search_params['location']} (GeoID: {search_params['geo_id']})")
    print(f"Work type: {work_type_to_name(search_params['work_type'])}")
    print(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    jobs = scrape_linkedin_jobs(
        keywords=search_params['keywords'],
        location=search_params['location'],
        geoId=search_params['geo_id'],
        work_type=search_params['work_type'],
        jobs_per_page=search_params.get('jobs_per_page', 10),
        max_pages=search_params.get('max_pages', 1)
    )
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    print(f"Found {len(jobs)} jobs for {search_name}")
    print(f"Search completed in {elapsed_time:.2f} seconds")
    
    # Save intermediate results
    if jobs:
        # Create a timestamped filename for this search
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{output_dir}/intermediate/{search_id}_{timestamp}.json"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(json_filename), exist_ok=True)
        
        # Save to file
        save_jobs_to_file(jobs, json_filename)
        print(f"Saved intermediate results to {json_filename}")
        
        # Update tracker with this batch of jobs
        google_sheet_id = config.GOOGLE_SHEET_ID
        update_jobs_tracker(jobs, google_sheet_id, mode)
        print(f"Updated tracker at Google Sheet ID: {google_sheet_id} with latest search results (Mode: {mode})")
    
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

def main(mode_arg):
    """Main function to run search routines"""
    print(f"LinkedIn Job Search Routines - Mode: {mode_arg}")
    print("=" * 50)

    # Determine max_pages based on mode
    if mode_arg == 'deep':
        max_pages = 5
        print("Deep mode activated: scraping up to 5 pages per search.")
    else: # default mode
        max_pages = 1
        print("Default mode activated: scraping up to 1 page per search.")
    
    # Start overall timing
    total_start_time = time.time()
    start_datetime = datetime.datetime.now()
    print(f"Started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Output settings
    output_dir = "output"
    json_path = f"{output_dir}/linkedin_jobs.json"
    google_sheet_id = config.GOOGLE_SHEET_ID
    
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(f"{output_dir}/intermediate", exist_ok=True)

    # Optional: Limit the number of searches to run (for testing)
    max_searches = 0
    
    # Select which search strategy to use (uncomment one)
    search_strategy = "both"  # Run both Italy and Europe routines
    # search_strategy = "italy"  # Run only Italy routine
    # search_strategy = "europe"  # Run only Europe routine
    
    # Settings for page depth (use the determined max_pages)
    max_pages_italy = max_pages
    max_pages_europe = max_pages
    
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

    if max_searches>0 and len(search_combinations) > max_searches:
        print(f"Limiting to first {max_searches} searches")
        search_combinations = search_combinations[:max_searches]
    
    print(f"Running {len(search_combinations)} searches in total")
    
    # Run all searches and collect results
    all_jobs = []
    successful_searches = 0
    failed_searches = 0
    
    for i, search_params in enumerate(search_combinations):
        # Show progress
        print(f"\nSearch {i+1} of {len(search_combinations)}")
        
        try:
            # Run the search
            jobs = run_search(search_params, output_dir, mode_arg)
            all_jobs.extend(jobs)
            successful_searches += 1
            
        except Exception as e:
            print(f"ERROR in search {search_params['name']}: {str(e)}")
            failed_searches += 1
        
        # Pause between searches to avoid rate limiting
        if search_params != search_combinations[-1]:  # Skip delay after last search
            pause_time = 1
            print(f"Pausing for {pause_time} seconds between searches...")
            time.sleep(pause_time)
    
    # Save all jobs to JSON
    if all_jobs:
        print(f"Saving all {len(all_jobs)} jobs to {json_path} (final output)")
        save_jobs_to_file(all_jobs, json_path)
        
        # Update tracker with all jobs
        update_jobs_tracker(all_jobs, google_sheet_id, mode_arg)
        print(f"Updated tracker at Google Sheet ID: {google_sheet_id} (Mode: {mode_arg})")
    else:
        print("No jobs found across all searches")
    
    # End overall timing
    total_end_time = time.time()
    end_datetime = datetime.datetime.now()
    total_elapsed_time = total_end_time - total_start_time
    
    print("=" * 50)
    print("LinkedIn Job Search Completed")
    print(f"Finished at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
    print(f"Total successful searches: {successful_searches}")
    print(f"Total failed searches: {failed_searches}")
    print(f"Total jobs collected: {len(all_jobs)}")
    print("=" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run LinkedIn job scraping routines.")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=['default', 'deep'],
        default='default',
        help="Scraping mode: 'default' (1 page per search, for daily updates) or 'deep' (5 pages per search, for initial population). Default is 'default'."
    )
    args = parser.parse_args()
    
    main(args.mode) 