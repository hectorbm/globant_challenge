from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# DB Configuration
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = os.environ.get('POSTGRES_USER')  # The DB username read from an environment variable
DB_PASSWORD = os.environ.get('POSTGRES_PWD')  # The DB password read from an environment variable
DB_PORT = "5432"

# Define the connection URL using the environment variables
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create a database engine with the defined connection URL
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create a SessionLocal class to manage database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Set the metadata with the schema name defined for the solution
metadata = MetaData(schema="challenge_app")

# Create a declarative base for the ORM
Base = declarative_base(metadata=metadata)

def get_db():
    """
    Function to get a database session.

    Yields: A database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
