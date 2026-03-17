import logging
from scraper.jobspy_scraper import scout_jobs

logger = logging.getLogger(__name__)

def extract_data(search_term, location, results_limit, include_linkedin=False):
    logger.info(f"(EXTRACT) Buscando '{search_term}' en '{location}'")

    df = scout_jobs(
        search_term=search_term,
        location=location,
        results_limit=results_limit, 
        include_linkedin=include_linkedin
    )

    if df.empty:
        logger.warning("(EXTRACT) No se encontraron vacantes")
    return df