import smtplib
import os
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_email(pdf_filename):
    print(f"Setting up the mail file: {pdf_filename}")

    #credenciales
    EMAIL_USER = os.environ.get('EMAIL_USER')
    EMAIL_PASS = os.environ.get('EMAIL_PASS')
    DESTINATION_EMAIL = 'Christianalfonsoalfarohernande@gmail.com'

    if not EMAIL_USER or not EMAIL_PASS:
        print("Error !. Credentials could not be found :(")
        return 
    
    msg = EmailMessage()
    msg['Subject'] = 'Vacantes pa mi pikao'
    msg['From'] = EMAIL_USER
    msg['To'] = DESTINATION_EMAIL
    msg.set_content('Buenas mi pikao, aqui te van unos jales con todo !')

    try:
        with open(pdf_filename, 'rb') as f:
            pdf_data = f.read()
            pdf_name = os.path.basename(pdf_filename)
        msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=pdf_name)
    except FileNotFoundError:
        print("Error !. The file could not be found :(")
        return
    
    # sending the mail using gmail
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
            print("The report was sent successfully !")
    except Exception as e:
        print(f"Error !. The mail could not be sent")

if __name__ == "__main__":
    send_email("../reports/test1.pdf")