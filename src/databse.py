from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


# Connection details - replace with your DBeaver credentials
DB_TYPE = 'postgresql'
DB_USER = os.getenv('DB_USER')     
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        # Create the connection string
        connection_string = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Test the connection
        with engine.connect() as connection:
            print("Database connection successful!")
        
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    
def load_query_to_df(query, engine):
    """Executes a SQL query and returns the results as a pandas DataFrame."""
    try:
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None