import pandas as pd
from database.models import db_connect
import os
from datetime import datetime
from sqlalchemy import text

def export_jobs():
    engine = db_connect()

    with engine.connect() as conn:
        query = """
                    SELECT id, title, company, 
                        raw_location, state, job_type,
                        salary_min, salary_max, site, job_url,
                        date_posted, 
                        date_scraped AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' AS local_date_scraped
                    FROM jobs
                    WHERE status IN ('NEW', 'APPLIED')
                    ORDER BY local_date_scraped DESC
                """

        df = pd.read_sql(text(query), conn)

        reports_folder = "reports"
        if not os.path.exists(reports_folder):
            os.makedirs(reports_folder)
            print(f"Crated folder: {reports_folder}")

        if df.empty:
            print("No new or applied jobs to export")
            return
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{reports_folder}/job_report_{today}.csv" 
        # saving interesting jobs into a csv
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        # utf-8-sig fixes accents in excel
        print("Csv with interesting jobs was created successfully !")

if __name__ == "__main__":
    export_jobs()
