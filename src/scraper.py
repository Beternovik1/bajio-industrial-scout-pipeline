import logging
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
import os

# 1. Setup logging
# this leps debugging when and why something fails
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def scout_jobs(search_term= 'Ingeniero Industrial', location='Irapuato, Guanajuato', results_limit=20):
    logging.info(f"Starting scout for: '{search_term}' in '{location}'")
    try:
        # 2. Scraper
        # We start with Indeed and Glassdor as linkedin is stricter
        jobs = scrape_jobs(
            # site_name=['indeed', 'glassdoor', 'linkedin'],
            site_name=['indeed', 'glassdoor'],
            search_term=search_term,
            location=location,
            distance=40,
            country_indeed='mexico',
            results_wanted=results_limit,
            hours_old=72, # Last 3 days
            verbose=0
        )

        logging.info(f'Extraction completed. Found {len(jobs)} raw jobs')
        return jobs
    except Exception as e:
        logging.error(f'Scrapper failed: {e}')
        return pd.DataFrame() # It returns an empty df so the pipeline doesn't crash
    
if __name__ == '__main__':
    # testing mode
    # 1. running a small test scrape
    df = scout_jobs(search_term="Ingeniero Industrial", results_limit=5)

    if not df.empty:
        # displaying the whole output
        pd.set_option('display.max_columsn', None) # Show all columns
        pd.set_option('display.max_colwidth', None) # Don't cut off long text
        pd.set_option('display.width', 1000) # Use the full with of the screen
        # 2. saving the results 
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw_jobs_{timestamp}.csv"

        df.to_csv(filename, index=False)
        print("Scout Report")
        print(f"data saved to: {filename}")
        print(df[['title', 'company', 'location', 'date_posted']].head(3))
    else:
        print("No jobs found")

# $ source venv/Scripts/activate