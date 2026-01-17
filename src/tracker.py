print("SOY JOTO ALAVERGA")
import pandas as pd
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)        # Stop breaking rows into new lines
pd.set_option('display.max_colwidth', None)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.sql import func
from models import db_connect, Job

# Connecting to the DB
engine = db_connect()
Session = sessionmaker(bind=engine) 

def show_to_do_list():
    print("Calculating priorities...")

    query = """
        WITH base AS (
            SELECT 
                id, company, title, status,
                COALESCE(last_updated, applied_date) as last_activity_on,
                applied_date,
                CAST(julianday(date('now')) - julianday(COALESCE(last_updated, applied_date)) AS INTEGER) as days_since_update,
                CAST(julianday(date('now')) - julianday(applied_date) AS INTEGER) as days_since_applied
            FROM jobs
            WHERE status IN ('APPLIED', 'SCREENING', 'INTERVIEW')
        )
        SELECT 
            id, company, title, status, days_since_update,
            (
                (CASE WHEN status IN ('APPLIED', 'SCREENING') THEN 5 ELSE 0 END) +
                (CASE WHEN days_since_update >= 14 THEN 5
                    WHEN days_since_update >= 7 THEN 3
                    WHEN days_since_update >= 3 THEN 1
                    ELSE 0 END) +
                (CASE WHEN days_since_applied >= 21 THEN 3
                    WHEN days_since_applied >= 14 THEN 2
                    WHEN days_since_applied >= 7 THEN 1
                    ELSE 0 END)
            ) AS followup_score
        FROM base
        ORDER BY followup_score DESC
        """
    # Use engine.connect() for reading
    with engine.connect() as conn:
        try:
            df = pd.read_sql(text(query), conn)
            if df.empty:
                print("No active applications to follow up on yet")
            else:
                print(df.to_string(index=False))
        except Exception as err:
            print(f"Error: {err}")

def mark_applied(job_id):
    # Use Session() for writing
    with Session() as session:
        job = session.query(Job).filter(Job.id == job_id).first()

        if job:
            job.status = 'APPLIED'
            job.applied_date = func.now()

            try:
                session.commit() # Fixed typo: seesion -> session
                print(f"Marked '{job.title}' at {job.company} as APPLIED !")
            except Exception as err:
                session.rollback()
                print(f"Database Error: {err}")
        else:
            print("Job ID not found")
        
def run_tracker():
    while True:
        print("--------------- JOB TRACKER -----------------")
        print("1. Show New Jobs")
        print("2. Mark Job as Applied")
        print("3. Show Follow-up priorities")
        print("q.Quit")

        choice = input("Select: ")

        if choice == '1':
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT id,
                            title,
                            company,
                            location,
                            site,
                            salary_min,
                            salary_max,
                            job_url,
                            date_scraped
                        FROM jobs
                        WHERE status = 'NEW' 
                        LIMIT 20          
                """), conn)
                if df.empty:
                    print("No new jobs found")
                else:
                    for index, row in df.iterrows():
                        print("------------------------------------------------")
                        print(f"TITLE:   {row['title']}")
                        print(f"COMPANY: {row['company']}")
                        print(f"LOCATION: {row['location']}")
                        print(f"SITE: {row['site']}")
                        print(f"SALARY:  {row['salary_min']} - {row['salary_max']}")
                        print(f"LINK:    {row['job_url']}")
                        print("------------------------------------------------\n")
        elif choice == '2':
            job_id = input("Enter Job ID to mark as 'APPLIED': ")
            if job_id.isdigit():
                mark_applied(int(job_id))
            else:
                print("Invalid ID, please enter a number")
        elif choice == '3':
            show_to_do_list()
        elif choice == 'q':
            print("Adios PIKAO")
            break
        else:
            print("Invalid choice, try again...")
    
if __name__ == "__main__":
    run_tracker()