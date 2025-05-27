import time
import datetime
import argparse
import logging
from linkedin_scraper import scrape_linkedin_jobs
from tracker_manager import update_jobs_tracker
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
    "Solution Architect": "Solution%2BArchitect",
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

def run_search(search_params):
    """Run a LinkedIn job search with the specified parameters and save intermediate results"""
    search_name = search_params['name']
    start_time = time.time()

    scraped_jobs_list = scrape_linkedin_jobs(
        keywords=search_params['keywords'],
        location=search_params['location'],
        geoId=search_params['geo_id'],
        work_type=search_params['work_type'],
        jobs_per_page=search_params.get('jobs_per_page', 10),
        max_pages=search_params.get('max_pages', 1),
        search_keyword_job_title=search_params['original_job_title']
    )

    elapsed_time = time.time() - start_time
    num_found = len(scraped_jobs_list) if scraped_jobs_list else 0

    # Add search_location (Country) and work_type_name to each job
    processed_jobs = []
    if scraped_jobs_list:
        for job in scraped_jobs_list:
            job_out = {}
            for field in config.JOB_FIELDS:
                for k in job:
                    if k.lower() == field.lower():
                        job_out[field] = job[k]
                        break
                else:
                    job_out[field] = ''
            processed_jobs.append(job_out)

    num_added = 0
    if processed_jobs:
        num_added = update_jobs_tracker(processed_jobs)

    logger.info(f"[SEARCH] {search_name}: Found {num_found} jobs, {num_added} added to DB. (Elapsed: {elapsed_time:.2f}s)")
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
    return [create_search_param(j, "Italia", w, max_pages) for j in JOB_TITLES for w in ["Remote", "Hybrid"]]

def europe_remote_jobs_routine(max_pages=1):
    """
    Search routine for all job titles in European countries (excluding Italy) with remote only
    
    Args:
        max_pages: Number of pages to scrape per search
    """
    countries = [k for k in GEO_IDS if k != "Italia"]
    return [create_search_param(j, c, "Remote", max_pages) for j in JOB_TITLES for c in countries]

def main(mode_arg):
    """Main function to run search routines"""
    logger.info(f"LinkedIn Job Search Routines - Mode: {mode_arg}")
    logger.info("=" * 50)

    # Determine max_pages based on mode
    max_pages = 5 if mode_arg == 'deep' else 1
    logger.info(f"{'Deep' if mode_arg == 'deep' else 'Default'} mode: scraping up to {max_pages} page(s) per search.")
    
    # Start overall timing
    total_start_time = time.time()
    start_datetime = datetime.datetime.now()
    logger.info(f"Started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Collect search combinations based on strategy
    search_combinations = italy_jobs_routine(max_pages) + europe_remote_jobs_routine(max_pages)
    
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
            jobs = run_search(search_params)
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

    if all_jobs:
        logger.info(f"Collected {len(all_jobs)} jobs in this run.")
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