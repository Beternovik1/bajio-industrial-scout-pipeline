from pipeline import run_pipeline
from reporting.csv_export import export_jobs
from config import get_search_config, SCRAPER_CONFIG, include_linkedin_today
from datetime import datetime

def main():
    print("Iniciando Bajio Industrial Scout...")
    
    search_config = get_search_config()
    results_limit = SCRAPER_CONFIG["results_limit"]
    include_linkedin = include_linkedin_today()

    print("--- 1. Ejecutando ETL Pipeline ---")
    run_pipeline(
        search_config=search_config,
        results_limit=results_limit,
        include_linkedin=include_linkedin
    )
    print("Exporting jobs to csv...")
    export_jobs()

    # print("Generating pdf file...")
    # pdf_path = generate_pdf_report()

    # print("Sending mail with pdf...")
    # if pdf_path:
    #     send_email(pdf_path)
    #     print("Updating Database status...")
    #     mark_jobs_as_notified()
    # else:
    #     print("Pdf could not be generated :(")
    #     print("There's no any new jobs to send")
    
    print("Pipeline was successfully completed !")

if __name__ == "__main__":
    main()