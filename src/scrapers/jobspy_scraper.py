#src/jobspy_scraper.py
import logging
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
import os
import time
import random

# 1. Setup logging
# this leps debugging when and why something fails
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Agregamos el parámetro country_code con 'MX' por defecto
def scout_jobs(search_term, location, results_limit, include_linkedin=False, country_code='MX'):
        
    logging.info(f"Starting scout for: '{search_term}' in '{location}' ({country_code})")

    sites = ['indeed']
    if include_linkedin:
        sites.append('linkedin')

    # Diccionario de mapeo para que JobSpy entienda a qué país ir según el código
    country_mapping = {
        'MX': 'mexico',
        'CO': 'colombia',
        'PE': 'peru',
        'CL': 'chile',
        'AR': 'argentina'
    }
    
    # Obtenemos el nombre del país para la librería, si no existe usamos mexico por defecto
    jobspy_country = country_mapping.get(country_code, 'mexico')

    try:
        # humanization: adding delay to requests pre-scraping
        delay = random.uniform(5, 15)
        logging.info(f"Sleeping for {delay:.2f} seconds before request...")
        time.sleep(delay)

        # 2. Scraper
        jobs = scrape_jobs(
            site_name=sites,
            search_term=search_term,
            location=location,
            country_indeed=jobspy_country, # Ahora es dinámico
            results_wanted=results_limit, 
            hours_old=72,
            verbose=0
        )

        # delay post-scraping
        time.sleep(random.uniform(2,5))

        # 3. TRANSFORMACIÓN DE ESQUEMA (Alineación con Supabase)
        if not jobs.empty:
            # Inyectamos el country_code explícitamente en el DataFrame
            jobs['country_code'] = country_code
            
            # (Opcional por ahora) Aseguramos que la vieja columna tenga algo 
            # para no romper 
            jobs['country'] = 'México' if country_code == 'MX' else country_code

        logging.info(f'Extraction completed. Found {len(jobs)} raw jobs')
        return jobs
        
    except Exception as e:
        logging.error(f'Scrapper failed: {e}')
        return pd.DataFrame() # It returns an empty df so the pipeline doesn't crash
# if __name__ == '__main__':
#     # testing mode
#     # 1. running a small test scrape
#     df = scout_jobs(search_term="Ingeniero Industrial", results_limit=5)

#     if not df.empty:
#         # displaying the whole output
#         pd.set_option('display.max_columsn', None) # Show all columns
#         pd.set_option('display.max_colwidth', None) # Don't cut off long text
#         pd.set_option('display.width', 1000) # Use the full with of the screen
#         # 2. saving the results 
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = f"data/raw_jobs_{timestamp}.csv"

#         df.to_csv(filename, index=False)
#         print("Scout Report")
#         print(f"data saved to: {filename}")
#         print(df[['title', 'company', 'location', 'date_posted']].head(3))
#     else:
#         print("No jobs found")

# $ source venv/Scripts/activate