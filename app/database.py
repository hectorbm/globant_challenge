from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = os.environ.get('POSTGRES_USER')
DB_PASSWORD = os.environ.get('POSTGRES_PWD')
DB_PORT = "5432"

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Set the metadata with the schema name
metadata = MetaData(schema="challenge_app")

Base = declarative_base(metadata=metadata)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()