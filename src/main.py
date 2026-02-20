from pipeline import run_pipeline
from export import export_jobs
from reporter import generate_pdf_report
from send_report import send_email

def main():
    print("Starting pipeline...")
    run_pipeline()

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