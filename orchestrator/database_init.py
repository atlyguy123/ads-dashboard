#!/usr/bin/env python3
"""
Database initialization script for Heroku deployment
Creates database structure from schema.sql when databases don't exist
"""

import os
import sqlite3
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_path(db_name):
    """Get the database path, creating directory if needed"""
    # On Heroku, we'll store databases in the app directory
    if os.getenv('FLASK_ENV') == 'production':
        db_dir = '/app/database'
    else:
        db_dir = os.path.join(os.path.dirname(__file__), '..', 'database')
    
    # Create directory if it doesn't exist
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, db_name)

def initialize_database(db_path, schema_path):
    """Initialize a database from schema file"""
    logger.info(f"Initializing database: {db_path}")
    
    try:
        # Connect to database (creates if doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Read and execute schema
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Execute schema (split on semicolon for multiple statements)
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Database initialized successfully: {db_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error initializing database {db_path}: {e}")
        return False

def init_all_databases():
    """Initialize all required databases"""
    logger.info("🚀 Starting database initialization...")
    
    # Get schema path
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'schema.sql')
    
    if not os.path.exists(schema_path):
        logger.error(f"❌ Schema file not found: {schema_path}")
        return False
    
    # List of databases to initialize
    databases = [
        'mixpanel_data.db',
        'meta_analytics.db',
        'mixpanel_analytics.db'
    ]
    
    success_count = 0
    for db_name in databases:
        db_path = get_database_path(db_name)
        
        # Only initialize if database doesn't exist or is empty
        if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
            if initialize_database(db_path, schema_path):
                success_count += 1
        else:
            logger.info(f"📄 Database already exists: {db_path}")
            success_count += 1
    
    logger.info(f"✅ Database initialization complete: {success_count}/{len(databases)} databases ready")
    return success_count == len(databases)

if __name__ == '__main__':
    init_all_databases() 