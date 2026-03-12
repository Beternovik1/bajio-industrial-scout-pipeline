from pipeline import run_pipeline
from reporting.csv_export import export_jobs
from reporting.pdf_report import generate_pdf_report
from reporting.email_sender import send_email
from database.operations import mark_jobs_as_notified
from datetime import datetime

def main():
    today = datetime.now().weekday()
    is_monday = (today == 0)

    print("Starting pipeline...")
    run_pipeline(
            include_linkedin=is_monday, 
            search_terms=["Desarrollador Backend", "Python Backend", "Java Backend"],
            location="México"
        )
    print("Exporting jobs to csv...")
    export_jobs()

    print("Generating pdf file...")
    pdf_path = generate_pdf_report()

    print("Sending mail with pdf...")
    if pdf_path:
        send_email(pdf_path)
        print("Updating Database status...")
        mark_jobs_as_notified()
    else:
        print("Pdf could not be generated :(")
        print("There's no any new jobs to send")
    
    print("Pipeline was successfully completed !")

if __name__ == "__main__":
    main()