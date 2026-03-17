import os
from datetime import datetime

WORKER_NAME = os.getenv("WORKER_NAME", "Local_Test")

SCRAPER_CONFIG = {
    "results_limit": 25,       
    "hours_old": 72,           
}

BUSINESS_RULES = {
    "linkedin_on_mondays": True,
}

def include_linkedin_today() -> bool:
    is_monday = datetime.now().weekday() == 0
    return is_monday and BUSINESS_RULES["linkedin_on_mondays"]

def get_search_config():
    """
    Retorna: [(search_term, location, industry_niche), ...]
    """
    
    if WORKER_NAME == "GitHub_Actions":
        return [
            ("Data Scientist", "México", "Inteligencia Artificial"),
            ("Machine Learning Engineer", "México", "Inteligencia Artificial"),
            ("AI Engineer", "México", "Inteligencia Artificial"),
            ("MLOps Engineer", "México", "Inteligencia Artificial") 
        ]
        
    elif WORKER_NAME == "Edgar":
        return [
            ("Data Engineer", "México", "Ingeniería de Datos"),
            ("Data Analyst", "México", "Ingeniería de Datos"),
            ("Analytics Engineer", "México", "Ingeniería de Datos"),
            ("Database Administrator", "México", "Ingeniería de Datos")
        ]
        
    elif WORKER_NAME == "Fercho":
        return [
            ("Cloud Engineer", "México", "Infraestructura Cloud"),
            ("DevOps Engineer", "México", "Infraestructura Cloud"),
            ("Site Reliability Engineer", "México", "Infraestructura Cloud"),
            ("Arquitecto AWS", "México", "Infraestructura Cloud")
        ]
        
    elif WORKER_NAME == "Alexa":
        return [
            ("Desarrollador Frontend", "México", "Desarrollo de Software"),
            ("React Developer", "México", "Desarrollo de Software"),
            ("iOS Developer", "México", "Desarrollo Móvil"),
            ("Android Developer", "México", "Desarrollo Móvil")
        ]
        
    elif WORKER_NAME == "Mazuca":
        return [
            ("Desarrollador Backend", "México", "Desarrollo de Software"),
            ("Python Developer", "México", "Desarrollo de Software"),
            ("Java Backend", "México", "Desarrollo de Software"),
            ("C# Backend", "México", "Desarrollo de Software")
        ]
        
    # NODO 6: Geraldine (Análisis de Negocio y Producto)
    elif WORKER_NAME == "Geraldine":
        return [
            ("Cybersecurity Engineer", "México", "Ciberseguridad"),
            ("QA Automation", "México", "Testing"),
            ("Business Intelligence", "México", "Analítica de Datos"),
            ("Product Manager", "México", "Gestión Tech")
        ]
        
    else:
        raise ValueError(f"Error !! Trabajador '{WORKER_NAME}' no autorizado.")