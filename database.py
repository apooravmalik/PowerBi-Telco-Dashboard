import os
import urllib
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database configuration for CustomerChurnAnalytics
DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'),
    'server': os.getenv('DB_SERVER', 'APOORAV_MALIK'),
    'database': os.getenv('DB_DATABASE', 'CustomerChurnAnalytics'),
    'username': os.getenv('DB_USERNAME', 'sa'),
    'password': os.getenv('DB_PASSWORD', ''),
    'trust_cert': os.getenv('DB_TRUST_CERT', 'yes'),
}


DB_SCHEMA = "dbo"

# Function to create connection string for CustomerChurnAnalytics
def create_connection_string():
    """Create a properly formatted connection string for MS SQL Server"""
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate={'yes' if DB_CONFIG['trust_cert'].lower() == 'yes' else 'no'};"
        f"Timeout=60;"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"



# Create engine for CustomerChurnAnalytics
engine = create_engine(
    create_connection_string(),
    echo=True,  # Set to False in production
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
)


# Event listener to create schema if it doesn't exist for CustomerChurnAnalytics
@event.listens_for(engine, 'connect')
def create_schema(dbapi_connection, connection_record):
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = '{DB_SCHEMA}'
            )
            BEGIN
                EXEC('CREATE SCHEMA {DB_SCHEMA}')
            END
        """)
        cursor.close()
        dbapi_connection.commit()
    except Exception as e:
        logger.error(f"Error creating schema: {e}")

# Session factory for CustomerPredictions
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)


# Dependency function for CustomerChurnAnalytics database session
def get_db():
    """Database session dependency for CustomerPredictions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Test connection for TEST database
def test_connection_CustomerPredictions_db():
    """Test connection to TEST database"""
    try:
        # Query the CustomerChurnAnalytics database
        with engine.connect() as connection:
            result = connection.execute(text("SELECT TOP 1 * FROM [CustomerChurnAnalytics].[dbo].[CustomerPredictions]"))
            for row in result:
                print(row)

        logger.info("Successfully connected to CustomerPredictions database!")
    except Exception as e:
        logger.error(f"Error connecting to CustomerPredictions database: {e}")
        return False

test_connection_CustomerPredictions_db()