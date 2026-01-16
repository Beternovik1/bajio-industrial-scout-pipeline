import pandas as pd
from sqlalchemy import text
from models import db_connect

def run_audit():
    engine = db_connect()
    print("Checking if the pipeline is working well...")

    with engine.connect() as conn:
        # 1. Check the total count
        # Pandas returns a table with 1 row and 1 column
        # index     count
        #   0        500
        # with iloc (integer location) with index 0 and column 'count'
        total = pd.read_sql(text("""
            SELECT COUNT(*) as count
                FROM jobs
        """), conn).iloc[0]['count']
        print(f'Total jobs in db: {total}')

        # 2. Check for null values in title and job_url, because
        # if we have a null value on any of those columns the data is garbage
        nulls = pd.read_sql(text("""
            SELECT COUNT(*) as count
                FROM jobs 
                WHERE title IS NULL OR job_url IS NULL
        """), conn).iloc[0]['count']

        if nulls > 0:
            print(f"WARNING: Found {nulls} rows with missing Title of URL")
        else:
            print("title and job_url are good")

        # Checking for duplicates
        duplicates = pd.read_sql(text("""
            SELECT job_url, 
                    COUNT(*) as count
                FROM jobs 
                GROUP BY job_url
                HAVING count > 1
        """), conn)
        if not duplicates.empty:
            print(f"WARNING: Found {len(duplicates)} URLs")
        else:
            print("No duplicates found")

        # 4. Salary
        salaries = pd.read_sql(text("""
                SELECT COUNT(*) as count
                    FROM jobs
                    WHERE salary_min IS NOT NULL
            """), conn).iloc[0]['count']
        # Normal way
        # if total > 0:
        #     percentage = (salaries / total*100)
        # else:
        #     percentage = 0

        # Ternary operator way
        percentage = (salaries / total*100) if total > 0 else 0
        print(f"Salary info availability: {percentage:.1f}% of jobs have salary data")

if __name__ == "__main__":
    run_audit()