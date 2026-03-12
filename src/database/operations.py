from sqlalchemy import text
import logging
from database.models import db_connect

logger = logging.getLogger(__name__)

def mark_jobs_as_notified():
    """
    Cambia el estatus de las vacantes de NEW a NOTIFIED 
    después de mandar el reporte.
    """
    engine = db_connect()
    
    try:
        # Usamos engine.begin() para que haga el "commit" automáticamente
        with engine.begin() as conn:
            result = conn.execute(text("UPDATE jobs SET status = 'NOTIFIED' WHERE status = 'NEW'"))
            # Opcional: Saber cuántas actualizó
            filas_actualizadas = result.rowcount
            
        print(f"Base de datos actualizada: {filas_actualizadas} vacantes pasaron de NEW a NOTIFIED.")
    except Exception as e:
        logger.error(f"Error actualizando la base de datos: {e}")