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
def run_pipeline(include_linkedin=False):
    engine = db_connect()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    search_term = "Ingeniero Industrial"
    location = "Irapuato, Guanajuato"
    try:
        # EXTRACT
        raw_df = extract_data(
            search_term,
            location,
            include_linkedin
        )
        if raw_df.empty:
            logger.info("Pipeline terminado temprano: No hay datos crudos")
            return
        
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
    run_pipeline()