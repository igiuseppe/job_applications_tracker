import os
from linkedin_scraper import scrape_linkedin_jobs
from crm_manager import save_jobs_to_file, update_jobs_crm

def main():
    """Main entry point for the application"""
    print("LinkedIn Job Scraper and CRM")
    print("-" * 50)
    
    # Define all variables directly in main.py
    # Search parameters
    keywords = "Business%2BAnalyst"
    location = "Italia"
    geo_id = "103350119"
    
    # Work Type settings (1=on-site, 2=remote, 3=hybrid)
    work_type = 1
    
    # Scraping settings
    max_pages = 1
    jobs_per_page = 10
    
    # Output settings
    output_dir = "output"
    json_path = f"{output_dir}/linkedin_jobs.json"
    crm_path = f"{output_dir}/jobs_crm.xlsx"
    
    # Behavior flags
    skip_json = False  # Set to True to skip saving JSON
    skip_crm = False   # Set to True to skip updating CRM
    multiple_searches = False  # Set to True to search all work types
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Display current configuration
    print(f"Searching for: {keywords}")
    print(f"Location: {location} (GeoID: {geo_id})")
    print(f"Work type: {work_type_to_name(work_type)}")
    print(f"Max pages: {max_pages}, Jobs per page: {jobs_per_page}")
    print(f"Output directory: {output_dir}")
    print("-" * 50)
    
    all_jobs = []
    
    if multiple_searches:
        # Run searches for all work types
        work_types = [
            (1, "On-site"),  # On-site
            (2, "Remote"),   # Remote
            (3, "Hybrid")    # Hybrid
        ]
        
        for work_type_value, work_type_name in work_types:
            print(f"\nRunning search for {work_type_name} jobs...")
            jobs = scrape_linkedin_jobs(
                keywords=keywords,
                location=location,
                geoId=geo_id,
                work_type=work_type_value,
                jobs_per_page=jobs_per_page,
                max_pages=max_pages
            )
            all_jobs.extend(jobs)
            print(f"Found {len(jobs)} {work_type_name} jobs")
    else:
        # Run a single search with the specified work type
        all_jobs = scrape_linkedin_jobs(
            keywords=keywords,
            location=location,
            geoId=geo_id,
            work_type=work_type,
            jobs_per_page=jobs_per_page,
            max_pages=max_pages
        )
    
    if not all_jobs:
        print("No jobs found!")
        return
    
    # Save to JSON
    if not skip_json:
        save_jobs_to_file(all_jobs, json_path)
    
    # Update CRM
    if not skip_crm:
        update_jobs_crm(all_jobs, crm_path)
    
    print("-" * 50)
    print("Job scraping completed successfully!")

def work_type_to_name(work_type):
    """Convert work type value to name"""
    work_type_names = {
        1: "On-site",
        2: "Remote",
        3: "Hybrid",
        None: "Any"
    }
    return work_type_names.get(work_type, "Unknown")

if __name__ == "__main__":
    main()
