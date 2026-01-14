from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os

# 1. Fundation
Base = declarative_base()

# 2. Table design with ORM (Object-Relational Mapping)
class Job(Base):
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True)
    site = Column(String(50)) # indeed, glassdoor
    job_url = Column(String, unique=True, nullable=False) # Unique prevents duplicates
    title = Column(String, nullable=False)
    company = Column(String, nullable=False)
    location = Column(String, nullable=False)

    salary_min = Column(Float, nullable=True)
    salary_max = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)

    description = Column(Text, nullable=True)
    date_posted = Column(String, nullable=True)
    date_scraped = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Job(title='{self.title},' company='{self.company}')>"
    
# 3. Setup tools
def db_connect():
    """
    Connects to db with SQLite, later i will switch to PostgreSQL in Docker
    """
    # Create data folder if missing
    os.makedirs('data', exist_ok=True)

    # This creates a file 'jobs.db' in the data folder
    engine = create_engine('sqlite:///data/jobs.db')
    return engine

def create_tables(engine):
    """
    Builds the tables of the databased based on the job class
    """
    Base.metadata.create_all(engine)
    print("Database tables created successfully !")
    
if __name__ == "__main__":
    engine = db_connect()
    create_tables(engine)