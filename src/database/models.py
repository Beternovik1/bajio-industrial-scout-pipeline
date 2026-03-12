import os
from sqlalchemy import (
    create_engine, Column, BigInteger, String, DateTime,
    Text, Boolean, Numeric, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class Job(Base):
    __tablename__ = 'jobs'

    id = Column(BigInteger, primary_key=True)
    site = Column(String(50))
    job_url = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    company = Column(String, nullable=False)

    # Ubicación
    raw_location = Column(Text, nullable=False)
    country = Column(String(50), default='México')
    state = Column(String(50))

    # Negocio
    career = Column(String(100))
    scraped_by = Column(String(50))
    industry_niche = Column(String(50))
    is_premium = Column(Boolean, default=False)

    # Modalidad
    job_type = Column(String(20))

    # Salarios
    salary_min = Column(Numeric)
    salary_max = Column(Numeric)
    currency = Column(String(10), default='MXN')
    salary_source = Column(String(20))

    # NLP
    description = Column(Text)
    cleaned_description = Column(Text)
    keywords_json = Column(JSONB)

    date_posted = Column(String)

    # Tracker
    date_scraped = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    status = Column(String(20), default='NEW')

    __table_args__ = (
        Index("idx_niche_date", "industry_niche", "date_scraped"),
        Index("idx_geo_type", "country", "state", "job_type"),
        Index("idx_career_scope", "career", "job_type"),
        Index("idx_status", "status"),
    )

    def __repr__(self):
        return f"<Job(title='{self.title}', company='{self.company}', type='{self.job_type}')>"


def db_connect():
    database_url = os.getenv('DATABASE_URL')

    if not database_url:
        raise ValueError("DATABASE_URL no encontrada en .env")

    database_url = database_url.replace("postgres://", "postgresql://")

    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True
    )

    return engine


def create_tables(engine):
    Base.metadata.create_all(engine)
    print("Conexión exitosa con Supabase y ORM sincronizado.")

if __name__ == "__main__":
    # 1. Establecemos la conexión a Supabase
    engine = db_connect()
    # 2. Sincronizamos el modelo y mostramos el mensaje
    create_tables(engine)
    print("YA jalo jeje !!")