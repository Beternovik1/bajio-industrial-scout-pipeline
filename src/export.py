import pandas as pd
from models import db_connect
import os
from datetime import datetime
from sqlalchemy import text

def export_jobs():
    engine = db_connect()

    with engine.connect() as conn:
            # I only want 'new' or 'applied' jobs, not rejected ones
        query = """
            SELECT id,
                    title,
                    company,
                    location,
                    salary_min, 
                    salary_max,
                    site,
                    job_url,
                    date_posted,
                    date_scraped
                FROM jobs
                WHERE status IN ('NEW', 'APPLIED')
                ORDER BY date_scraped DESC
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
        filename = f"../{reports_folder}/job_report_{today}.csv" 
        # saving interesting jobs into a csv
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        # utf-8-sig fixes accents in excel
        print("Csv with interesting jobs was created successfully !")

if __name__ == "__main__":
    export_jobs()
