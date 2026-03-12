import logging
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import Job

logger = logging.getLogger(__name__)

# LOAD
def load_data(session, records):
    if not records:
        logger.warning("(LOAD) No hay registros para insertar")
        return 0, 0
    try:
        stmt = pg_insert(Job).values(records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["job_url"]
        )
        result = session.execute(stmt)
        session.commit()
        inserted = result.rowcount if result.rowcount != -1 else len(records)
        skipped = len(records) - inserted
        logger.info(
            f"(LOAD) {inserted} insertados | {skipped} duplicados ignorados"
        )
        return inserted, skipped
    except Exception as error:
        session.rollback()
        logger.error(f"(LOAD) Error en base de datos: {error}")
        raise
