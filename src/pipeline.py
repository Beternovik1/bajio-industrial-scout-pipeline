import pandas as pd
from sqlalchemy.orm import sessionmaker
from scraper import scout_jobs
from models import db_connect, create_tables, Job

# Extract
def extract_data(search_term, location="Irapuato, Guanajuato"):
    print(f"(EXTRACT) Starting scraper for '{search_term} in {location}'...")
    df = scout_jobs(search_term=search_term, location=location, results_limit=10)
    if df.empty:
        print("No data found.")
    return df

# TRANSFORM
def transform_data(df):
    """
    Converts raw df row into clean job objects
    """
    print(f"(TRANSFORM) Cleaning {len(df)} raw jobs")
    transformed_jobs = []

    for index, row in df.iterrows():
        # Salary logic
        # Salary handling
        # JobSpy library gives me 'min_amount' and max_amount' if it is available
        # i need to handle cases where they might be NaN
        salary_min = row.get('min_amount')
        salary_max = row.get('max_amount')
        currency = row.get('currency')

        # If pandas gives me NaN force it to None for SQL
        if pd.isna(salary_min): salary_min = None
        if pd.isna(salary_max): salary_max = None
        if pd.isna(currency): currency = None

        company_name = row.get('company')
        if pd.isna(company_name):
            company_name = "Empresa confidencial"

        location_name = row.get('location')
        if pd.isna(location_name):
            location_name = "Ubicacion desconocida"

        # Creating the object
        job_obj = Job(
            site = row.get('site', 'unknown'),
            job_url = row['job_url'],
            title = row['title'].upper(),
            company = company_name,
            location = location_name,
            date_posted = row.get('date_posted'),
            description = row.get('description'),
            salary_min = salary_min,
            salary_max = salary_max,
            currency = currency
        )
        transformed_jobs.append(job_obj)
    return transformed_jobs

# LOAD
def load_data(session, job_objects):
    """
    Takes the job_objects lists and saves them if they are new to prevent duplicates
    """
    print("(LOAD) Saving to Database")
    new_count = 0
    
    for job in job_objects:
        exists = session.query(Job).filter_by(job_url = job.job_url).first()

        if not exists:
            session.add(job)
            new_count += 1
    
    try:
        session.commit()
        print(f"Success !, saved {new_count} new jobs")
        print(f"Ignored {len(job_objects) - new_count} duplicates")
    except Exception as err:
        session.rollback()
        print(f"Database Error: {err}")

def run_pipeline():
    # Setup
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Pipeline flow
    try:
        # Extract
        raw_df = extract_data("Ingeniero Industrial", location="Irapuato, Guanajuato")

        if not raw_df.empty:
            # Transform
            clean_jobs = transform_data(raw_df)

            # Load
            load_data(session, clean_jobs)
    finally:
        session.close()

if __name__ == "__main__":
    run_pipeline()
