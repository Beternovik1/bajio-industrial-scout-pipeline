#src/shared/config.py
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

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
            ("Data Engineer", "México", "Ingeniería de Datos"),
            ("Data Analyst", "México", "Ingeniería de Datos"),
            ("Analytics Engineer", "México", "Ingeniería de Datos"),
            ("Cloud Engineer", "México", "Infraestructura Cloud"),
            ("DevOps Engineer", "México", "Infraestructura Cloud"),
            ("Python Developer", "México", "Desarrollo de Software"),
            ("React Developer", "México", "Desarrollo de Software"),
            ("Cybersecurity Engineer", "México", "Ciberseguridad"),
            ("Business Intelligence", "México", "Analítica de Datos"),
        ]
        # return [
        #     ("Cloud Engineer", "México", "Infraestructura Cloud"),
        #     ("DevOps Engineer", "México", "Infraestructura Cloud"),
        #     ("Site Reliability Engineer", "México", "Infraestructura Cloud"),
        #     ("Arquitecto AWS", "México", "Infraestructura Cloud")
        # ]        
        # return [
        #     ("Data Scientist", "México", "Inteligencia Artificial"),
        #     ("Machine Learning Engineer", "México", "Inteligencia Artificial"),
        #     ("AI Engineer", "México", "Inteligencia Artificial"),
        #     ("MLOps Engineer", "México", "Inteligencia Artificial") 
        # ]
        
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
        logger.warning(f"Unknown WORKER_NAME '{WORKER_NAME}', defaulting to GitHub_Actions config")
        return [
            ("Data Engineer", "México", "Ingeniería de Datos"),
            ("Cloud Engineer", "México", "Infraestructura Cloud"),
        ]


def get_keywords_from_config(search_config: list) -> list:
    """Extract unique search terms for use with Computrabajo/OCC scrapers."""
    return list(set(term for term, location, niche in search_config))


NEW_SCRAPERS_CONFIG = {
    "computrabajo": {
        "max_results": int(os.getenv("COMPUTRABAJO_MAX_RESULTS", "100")),
        "countries": os.getenv("COMPUTRABAJO_COUNTRIES", "MX").split(","),
        "is_remote": os.getenv("COMPUTRABAJO_REMOTE", "false").lower() == "true",
        "enabled": os.getenv("COMPUTRABAJO_ENABLED", "true").lower() == "true",
    },
    "occ": {
        "max_results": int(os.getenv("OCC_MAX_RESULTS", "100")),
        "is_remote": os.getenv("OCC_REMOTE", "false").lower() == "true",
        "enabled": os.getenv("OCC_ENABLED", "true").lower() == "true",
    },
}

ENRICHMENT_CONFIG = {
    "daily_limit": int(os.getenv("DAILY_ENRICHMENT_LIMIT", "100")),
    "batch_size": int(os.getenv("ENRICHMENT_BATCH_SIZE", "20")),
    "cooldown_seconds": int(os.getenv("ENRICHMENT_COOLDOWN", "60")),
    "circuit_breaker_threshold": 3,
    "circuit_breaker_sleep": 300,
}