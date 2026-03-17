import os
import logging
import pandas as pd
import re

logger = logging.getLogger(__name__)

# CONSTANTES
STATE_MAP = {
    "guanajuato": "Guanajuato",
    "gto": "Guanajuato",
    "irapuato": "Guanajuato",
    "leon": "Guanajuato",
    "celaya": "Guanajuato",

    "queretaro": "Querétaro",
    "qro": "Querétaro",

    "jalisco": "Jalisco",
    "guadalajara": "Jalisco",
}

REMOTE_KEYWORDS = [
    "remote",
    "remoto",
    "home office"
]

HYBRID_KEYWORDS = [
    "hybrid",
    "hibrido",
    "híbrido"
]

def clean(value, default=None):
    if pd.isna(value):
        return default
    return value


def normalize_text(value):
    if value is None:
        return ""
    return str(value).lower()

def detect_job_type(title: str, location: str) -> str:
    text = f"{title} {location}".lower()
    if any(word in text for word in REMOTE_KEYWORDS):
        return "remoto"

    if any(word in text for word in HYBRID_KEYWORDS):
        return "hibrido"
    return "presencial"


def detect_state(raw_location: str):
    location = normalize_text(raw_location)
    for key, state in STATE_MAP.items():
        if key in location:
            return state
    return None

def extract_salary_from_text(description):
    """
    Busca formatos como "$12,000 a $13,000" o "$15,000" dentro del texto
    """
    if not description or pd.isna(description):
        return None, None
        
    text = str(description)
    # Regex que busca el signo de $, un espacio opcional, y números con comas
    pattern = r'\$\s?(\d{1,3}(?:,\d{3})+)'
    matches = re.findall(pattern, text)
    
    if matches:
        # Convertimos los textos encontrados (ej. '12,000') a números (12000.0)
        salaries = [float(m.replace(',', '')) for m in matches]
        
        if len(salaries) == 1:
            return salaries[0], salaries[0]
        elif len(salaries) >= 2:
            return min(salaries), max(salaries)
            
    return None, None

# TRANSFORM
def transform_data(df: pd.DataFrame, search_term: str, industry_niche: str):
    logger.info(f"(TRANSFORM) Procesando {len(df)} registros")
    worker_name = os.getenv("WORKER_NAME", "default_worker")
    records = []

    for row in df.to_dict("records"):
        job_url = row.get("job_url")
        if not job_url:
            continue
        raw_location = clean(row.get("location"), "Ubicación desconocida")

        min_sal = clean(row.get("min_amount"))
        max_sal = clean(row.get("max_amount"))

        # Si jobspy falla por el formato mexicano, 
        # entramos a la funcion
        if min_sal is None and max_sal is None:
            min_sal, max_sal = extract_salary_from_text(row.get("description"))

        record = {
            "site": clean(row.get("site"), "unknown"),
            "job_url": job_url,

            "title": str(clean(row.get("title"), "")).upper(),
            "company": clean(row.get("company"), "Empresa confidencial"),

            # ubicación
            "raw_location": raw_location,
            "country": "México",
            "state": detect_state(raw_location),

            # negocio
            "career": search_term,
            "scraped_by": worker_name,
            "industry_niche": industry_niche,
            "is_premium": False,

            # modalidad
            "job_type": detect_job_type(
                str(row.get("title", "")),
                raw_location
            ),

            # salario
            "salary_min": min_sal,
            "salary_max": max_sal,
            "currency": clean(row.get("currency"), "MXN"),

            # texto
            "description": clean(row.get("description")),
            "date_posted": clean(row.get("date_posted")),
        }

        records.append(record)

    logger.info(f"(TRANSFORM) {len(records)} registros listos")
    return records
