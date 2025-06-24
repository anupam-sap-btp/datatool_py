import psycopg2
import os
from dotenv import load_dotenv
from fastapi import HTTPException, status

# Load environment variables
load_dotenv()



# Database configuration from environment variables
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "mydatabase"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}


# Helper function for database connection
def get_db_connection():
    try:
        # print(DB_CONFIG)
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection error"
        )

# Dependency for database connection
def get_db():
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    finally:
        if conn:
            conn.close()