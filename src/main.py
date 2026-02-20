from pipeline import run_pipeline
from export import export_jobs
from reporter import generate_pdf_report
from send_report import send_email
from datetime import datetime

def main():
    today = datetime.now().weekday()
    is_monday = (today == 0)

    print("Starting pipeline...")
    run_pipeline(include_linkedin=is_monday)

    print("Exporting jobs to csv...")
    export_jobs()

    print("Generating pdf file...")
    pdf_path = generate_pdf_report()

    print("Sending mail with pdf...")
    if pdf_path:
        send_email(pdf_path)
    else:
        print("Pdf could not be generated :(")
    
    print("Pipeline was successfully completed !")

if __name__ == "__main__":
    main()