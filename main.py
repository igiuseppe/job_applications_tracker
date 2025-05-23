import os
import time
import datetime
import argparse
import logging
from linkedin_scraper import scrape_linkedin_jobs
from tracker_manager import update_jobs_tracker, save_jobs_to_file
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        "original_job_title": job_title,
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
    
    logger.info("-" * 50)
    logger.info(f"Running search: {search_name}")
    logger.info(f"Keywords: {search_params['keywords']}")
    logger.info(f"Location: {search_params['location']} (GeoID: {search_params['geo_id']})")
    logger.info(f"Work type: {work_type_to_name(search_params['work_type'])}")
    logger.info(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("-" * 50)
    
    scraped_jobs_list = scrape_linkedin_jobs(
        keywords=search_params['keywords'],
        location=search_params['location'],
        geoId=search_params['geo_id'],
        work_type=search_params['work_type'],
        jobs_per_page=search_params.get('jobs_per_page', 10),
        max_pages=search_params.get('max_pages', 1)
    )
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    logger.info(f"Found {len(scraped_jobs_list)} jobs for {search_name}")
    logger.info(f"Search completed in {elapsed_time:.2f} seconds")
    
    # Add search_location (Country) and work_type_name to each job
    processed_jobs = []
    if scraped_jobs_list:
        for job in scraped_jobs_list:
            job['search_location'] = search_params['location'] # This will be used as 'Country'
            job['Country'] = search_params['location'] # Explicitly add 'Country'
            job['work_type_name'] = work_type_to_name(search_params['work_type'])
            job['search_keyword_job_title'] = search_params['original_job_title']
            # Ensure other necessary fields from search_params are in the job if needed by tracker/saver
            job['search_keywords'] = search_params['keywords'] # Keep for context if useful
            processed_jobs.append(job)

    # Save intermediate results
    if processed_jobs:
        # Create a timestamped filename for this search
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{output_dir}/intermediate/{search_id}_{timestamp}.json"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(json_filename), exist_ok=True)
        
        # Save to file
        save_jobs_to_file(processed_jobs, json_filename)
        logger.info(f"Saved intermediate results to {json_filename}")
        
        # Update tracker with this batch of jobs
        google_sheet_id = config.GOOGLE_SHEET_ID
        update_jobs_tracker(processed_jobs, google_sheet_id, mode)
        logger.info(f"Updated tracker at Google Sheet ID: {google_sheet_id} with latest search results (Mode: {mode})")
    
    return processed_jobs

def work_type_to_name(work_type_code):
    """Convert work type value to name"""
    work_type_names = {
        1: "On-site",
        2: "Remote",
        3: "Hybrid",
        None: "Any"
    }
    return work_type_names.get(work_type_code, "Unknown")

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
    logger.info(f"LinkedIn Job Search Routines - Mode: {mode_arg}")
    logger.info("=" * 50)

    # Determine max_pages based on mode
    if mode_arg == 'deep':
        max_pages = 5
        logger.info("Deep mode activated: scraping up to 5 pages per search.")
    else: # default mode
        max_pages = 1
        logger.info("Default mode activated: scraping up to 1 page per search.")
    
    # Start overall timing
    total_start_time = time.time()
    start_datetime = datetime.datetime.now()
    logger.info(f"Started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
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
        logger.info(f"Added {len(italy_searches)} Italy searches")
    
    if search_strategy in ["both", "europe"]:
        europe_searches = europe_remote_jobs_routine(max_pages=max_pages_europe)
        search_combinations.extend(europe_searches)
        logger.info(f"Added {len(europe_searches)} Europe searches")

    if max_searches>0 and len(search_combinations) > max_searches:
        logger.info(f"Limiting to first {max_searches} searches")
        search_combinations = search_combinations[:max_searches]
    
    logger.info(f"Running {len(search_combinations)} searches in total")
    
    # Run all searches and collect results
    all_jobs = []
    successful_searches = 0
    failed_searches = 0
    
    for i, search_params in enumerate(search_combinations):
        # Show progress
        logger.info("-" * 30)
        logger.info(f"Processing search {i+1}/{len(search_combinations)}: {search_params['name']}")
        
        try:
            jobs = run_search(search_params, output_dir, mode_arg)
            if jobs:
                all_jobs.extend(jobs)
                successful_searches += 1
                logger.info(f"Successfully processed: {search_params['name']}")
            else:
                logger.warning(f"No jobs found for {search_params['name']}")
                failed_searches +=1
            
        except Exception as e:
            logger.error(f"Error processing {search_params['name']}: {e}")
            failed_searches +=1
            
        # Optional: Add a delay between searches to avoid rate limiting
        time.sleep(config.DELAY_BETWEEN_SEARCHES)
    
    # Save all collected jobs to a single JSON file
    if all_jobs:
        save_jobs_to_file(all_jobs, json_path)
        logger.info(f"Saved all {len(all_jobs)} jobs to {json_path}")
    else:
        logger.info("No jobs collected in this run.")

    # Final summary
    total_elapsed_time = time.time() - total_start_time
    end_datetime = datetime.datetime.now()
    
    logger.info("=" * 50)
    logger.info("LinkedIn Job Search Summary")
    logger.info("=" * 50)
    logger.info(f"Mode: {mode_arg}")
    logger.info(f"Total searches attempted: {len(search_combinations)}")
    logger.info(f"Successful searches: {successful_searches}")
    logger.info(f"Failed searches: {failed_searches}")
    logger.info(f"Total jobs collected: {len(all_jobs)}")
    logger.info(f"Started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Ended at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total duration: {total_elapsed_time:.2f} seconds")
    logger.info("=" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run LinkedIn job scraper.")
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['default', 'deep'], 
        default='default', 
        help='Scraping mode: "default" for 1 page, "deep" for up to 5 pages per search.'
    )
    args = parser.parse_args()
    
    main(args.mode) 