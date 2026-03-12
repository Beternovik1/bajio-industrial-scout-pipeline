import logging
from scraper.jobspy_scraper import scout_jobs

logger = logging.getLogger(__name__)

# EXTRACT
def extract_data(search_term, location="Irapuato, Guanajuato", include_linkedin=False):
    logger.info(f"(EXTRACT) Buscando '{search_term}' en '{location}'")

    df = scout_jobs(
        search_term=search_term,
        location=location,
        results_limit=1,
        include_linkedin=include_linkedin
    )

    if df.empty:
        logger.warning("(EXTRACT) No se encontraron vacantes")
    return df
