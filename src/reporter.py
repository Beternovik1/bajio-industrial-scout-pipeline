import pandas as pd
from fpdf import FPDF
from models import db_connect
from datetime import datetime
import pytz
import os
from sqlalchemy import text
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

class JobPDF(FPDF):
    def __init__(self, timezone_name):
        # Initializing the parent
        super().__init__()
        # Encapsulating
        self.timezone = pytz.timezone(timezone_name)

    def header(self):
        header_time = datetime.now(self.timezone)

        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'Busqueda de trabajos pal pikao', border=False, ln=True, align='C')
        self.set_font('Arial', 'I', 10)
        self.cell(0, 10, f'Reporte diario - {header_time.strftime("%Y-%m-%d %H:%M")}', border=False, ln=True, align='C')
        self.ln(10)
    
    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')

def clean_text(text):
    """
    Fixing accents
    """
    if text:
        text = text.replace('“', '"').replace('”', '"').replace('’', "'").replace('–', '-')
        return text.encode('latin-1', 'replace').decode('latin-1')
    return ""

def generate_pdf_report():
    print("Generating pdf report ...")
    engine = db_connect()

    tz_name = 'America/Mexico_City'

    # Get only NEW jobs
    query = """
        SELECT title, 
                company, 
                location,
                salary_min,
                salary_max, 
                job_url,
                site,
                description
            FROM jobs
            WHERE status = 'NEW' 
            ORDER BY date_scraped DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        print("No hay trabajos nuevos mi pikao")
        return None
    
    # setup the pdf
    pdf = JobPDF(tz_name)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    for index, row in df.iterrows():
        # Title
        pdf.set_font('Helvetica', 'B', 12)
        pdf.set_text_color(0, 51, 102)
        # Using clean_text() to fix special characters
        title_text =  f"{clean_text(row['title'])} ({row['site'].capitalize()})"
        pdf.multi_cell(0, 6, title_text)

        # Company and location
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(255, 255, 255)
        company_val = clean_text(row['company']) if row['company'] else "Empresa confidencial"
        location_val = clean_text(row['location']) if row['location'] else "Zona Bajio"
        company_location_text = f"{company_val} | {location_val}"
        pdf.cell(0, 6, company_location_text, ln=True)


        # Company and location
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 0, 0)
        company_val = clean_text(row['company']) if row['company'] else "Empresa confidencial"
        location_val = clean_text(row['location']) if row['location'] else "Zona Bajio"
        company_location_text = f"{company_val} | {location_val}"
        pdf.cell(0, 6, company_location_text, ln=True)


        # Salary
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(34, 139, 34)
        salary_text = "Salario no mostrado"
        if row['salary_min']:
            salary_text = f"${row['salary_min']:, .0f}"
            if row['salary_max']:
                salary_text += f" - ${row['salary_max']:, .0f}"
        pdf.cell(0, 6, clean_text(salary_text), ln=True)

        # Description
        pdf.ln(2)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(50, 50, 50)
        # Getting the first 300 characters of description
        raw_desc = row['description'] if row['description'] else "Sin descripcion disponible"
        short_desc = clean_text(raw_desc)[:400] + "..."
        pdf.multi_cell(0, 5, short_desc)

        # link
        pdf.ln(2)
        pdf.set_font('Helvetica', 'U', 9)
        pdf.set_text_color(0, 0, 255)
        pdf.cell(0, 6, ">> Picale aqui mi pikaooo !", link=row['job_url'], ln=True)

        # Divider
        pdf.ln(4)
        pdf.set_draw_color(200, 200, 200)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(6)

    # Save
    if not os.path.exists('reports'):
        os.makedirs('reports')
    
    current_time = datetime.now(pytz.timezone(tz_name))
    filename = f"reports/jobs{current_time.strftime('%Y-%m-%d_%H-%M')}.pdf"

    pdf.output(filename)
    print(f"pdf saved: {filename} !!")
    return filename

if __name__ == "__main__":
    generate_pdf_report()
