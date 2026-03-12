import logging
from sqlalchemy.orm import sessionmaker

from database.models import db_connect, create_tables
from etl.extract import extract_data
from etl.transform import transform_data
from etl.loaders import load_data

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# PIPELINE
def run_pipeline(include_linkedin=False, search_terms=["Data Engineer"], location="México"):
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    # search_term = "Ingeniero Industrial"
    # location = "Irapuato, Guanajuato"
    try:
        # EXTRACT
        for search_term in search_terms:
            logger.info(f"Iniciando extracción para: {search_term} en {location}")
                    
            # EXTRACT
            raw_df = extract_data(
                search_term,
                location,
                include_linkedin
                )
                    
            # Si no hay datos, saltamos a la siguiente profesión en la lista
            if raw_df.empty:
                logger.warning(f"No hay datos crudos para: {search_term}. Saltando a la siguiente...")
                continue

            # TRANSFORM
            records = transform_data(raw_df, search_term)

            # LOAD
            load_data(session, records)
    except Exception as error:
        logger.error(f"(PIPELINE) Falló: {error}")
        raise
    finally:
        session.close()
        logger.info("Pipeline finalizado y conexion cerrada exitosamente !")


# MAIN
if __name__ == "__main__":
    # run_pipeline()
    run_pipeline(
        search_terms=["Ingeniero de Datos", "Data Engineer"], 
        location="Irapuato, Guanajuato"
    )