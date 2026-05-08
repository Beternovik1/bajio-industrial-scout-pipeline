import logging
from sqlalchemy.orm import sessionmaker
from database.models import db_connect, create_tables
from etl.extract import extract_data
from etl.transform import transform_data
from etl.loaders import load_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline(search_config, results_limit, include_linkedin=False):
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Desempaquetamos la configuración directamente del config.py
        for search_term, location, industry_niche in search_config:
            logger.info(f"==> Extrayendo: {search_term} en {location} (Nicho: {industry_niche})")
                    
            # EXTRACT
            raw_df = extract_data(
                search_term=search_term,
                location=location,
                results_limit=results_limit,
                include_linkedin=include_linkedin
            )
                    
            if raw_df.empty:
                logger.warning(f"No hay datos para: {search_term}. Saltando...")
                continue

            # TRANSFORM
            records = transform_data(raw_df, search_term, industry_niche)
            # LOAD
            load_data(session, records)
            
    except Exception as error:
        logger.error(f"(PIPELINE) Falló: {error}")
        raise
    finally:
        session.close()
        logger.info("Conexión de base de datos cerrada.")