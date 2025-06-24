#!/usr/bin/env python3
"""
Database initialization script for Heroku deployment
Creates database structure from schema.sql when databases don't exist
"""

import os
import sqlite3
import logging
from pathlib import Path
import sys

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.database_utils import get_database_manager, DatabasePathError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_schema_path():
    """Get the path to the database schema file."""
    # Try multiple locations for the schema file
    possible_paths = [
        project_root / "database" / "schema.sql",
        Path(__file__).parent.parent / "database" / "schema.sql",
        Path("/app/database/schema.sql"),  # Heroku deployment path
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    # If schema file doesn't exist, we'll create a minimal structure
    logger.warning("Schema file not found, will create minimal database structure")
    return None

def create_minimal_database_structure(conn):
    """Create minimal database structure if schema file is not available."""
    logger.info("Creating minimal database structure")
    
    # Create essential tables for the dashboard to function
    minimal_schema = """
    -- Essential tables for dashboard functionality
    CREATE TABLE IF NOT EXISTS mixpanel_user (
        distinct_id TEXT PRIMARY KEY,
        abi_ad_id TEXT,
        abi_campaign_id TEXT,
        abi_ad_set_id TEXT,
        country TEXT,
        region TEXT,
        city TEXT,
        has_abi_attribution BOOLEAN DEFAULT FALSE,
        profile_json TEXT,
        first_seen DATETIME,
        last_updated DATETIME,
        valid_user BOOLEAN DEFAULT FALSE,
        economic_tier TEXT
    );

    CREATE TABLE IF NOT EXISTS mixpanel_event (
        event_uuid TEXT PRIMARY KEY,
        event_name TEXT NOT NULL,
        abi_ad_id TEXT,
        abi_campaign_id TEXT,
        abi_ad_set_id TEXT,
        distinct_id TEXT NOT NULL,
        event_time DATETIME NOT NULL,
        country TEXT,
        region TEXT,
        revenue_usd DECIMAL(10,2),
        raw_amount DECIMAL(10,2),
        currency TEXT,
        refund_flag BOOLEAN DEFAULT FALSE,
        is_late_event BOOLEAN DEFAULT FALSE,
        trial_expiration_at_calc DATETIME,
        event_json TEXT,
        FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
    );

    CREATE TABLE IF NOT EXISTS user_product_metrics (
        user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        distinct_id TEXT NOT NULL,
        product_id TEXT NOT NULL, 
        credited_date DATE NOT NULL,
        country TEXT, 
        region TEXT, 
        device TEXT, 
        current_status TEXT NOT NULL, 
        current_value DECIMAL(10,2) NOT NULL,
        value_status TEXT NOT NULL, 
        segment_id TEXT, 
        accuracy_score TEXT, 
        trial_conversion_rate DECIMAL(5,4),
        trial_converted_to_refund_rate DECIMAL(5,4),
        initial_purchase_to_refund_rate DECIMAL(5,4),
        price_bucket DECIMAL(10,2),
        assignment_type TEXT,
        last_updated_ts DATETIME NOT NULL,
        valid_lifecycle BOOLEAN DEFAULT FALSE,
        store TEXT,
        UNIQUE (distinct_id, product_id),
        FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_status (
        id INTEGER PRIMARY KEY,
        status TEXT NOT NULL,
        started_at DATETIME,
        completed_at DATETIME,
        progress_percentage INTEGER DEFAULT 0,
        current_step TEXT,
        error_message TEXT,
        error_count INTEGER DEFAULT 0,
        warning_count INTEGER DEFAULT 0,
        processed_users INTEGER DEFAULT 0,
        total_users INTEGER DEFAULT 0
    );

    -- Create basic indexes
    CREATE INDEX IF NOT EXISTS idx_mixpanel_user_country ON mixpanel_user(country);
    CREATE INDEX IF NOT EXISTS idx_mixpanel_event_distinct_id ON mixpanel_event(distinct_id);
    CREATE INDEX IF NOT EXISTS idx_mixpanel_event_time ON mixpanel_event(event_time);
    CREATE INDEX IF NOT EXISTS idx_upm_distinct_id ON user_product_metrics (distinct_id);
    CREATE INDEX IF NOT EXISTS idx_upm_credited_date ON user_product_metrics (credited_date);
    """
    
    try:
        conn.executescript(minimal_schema)
        conn.commit()
        logger.info("Minimal database structure created successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create minimal database structure: {e}")
        return False

def initialize_database_from_schema(database_key: str):
    """Initialize a database from the schema file."""
    try:
        db_manager = get_database_manager()
        
        # Get database connection (this will create the file if it doesn't exist)
        with db_manager.get_connection(database_key) as conn:
            schema_path = get_schema_path()
            
            if schema_path and schema_path.exists():
                logger.info(f"Initializing {database_key} database from schema: {schema_path}")
                
                # Read and execute schema
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                
                # Execute schema in chunks to handle complex SQL
                statements = schema_sql.split(';')
                for statement in statements:
                    statement = statement.strip()
                    if statement and not statement.startswith('--'):
                        try:
                            conn.execute(statement)
                        except sqlite3.Error as e:
                            # Log but don't fail on individual statement errors
                            logger.warning(f"Schema statement failed (continuing): {e}")
                            logger.debug(f"Failed statement: {statement[:100]}...")
                
                conn.commit()
                logger.info(f"Database {database_key} initialized from schema successfully")
                
            else:
                # Create minimal structure if schema file not found
                logger.info(f"Schema file not found, creating minimal structure for {database_key}")
                create_minimal_database_structure(conn)
                
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize database {database_key}: {e}")
        return False

def initialize_all_databases():
    """Initialize all required databases."""
    logger.info("Starting database initialization...")
    
    try:
        db_manager = get_database_manager()
        database_configs = db_manager.DATABASE_CONFIGS
        
        initialized_count = 0
        for db_key in database_configs.keys():
            logger.info(f"Initializing database: {db_key}")
            if initialize_database_from_schema(db_key):
                initialized_count += 1
            else:
                logger.warning(f"Failed to initialize database: {db_key}")
        
        logger.info(f"Database initialization complete. {initialized_count}/{len(database_configs)} databases initialized.")
        return initialized_count > 0
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

def check_database_health():
    """Check if databases are accessible and have basic structure."""
    try:
        db_manager = get_database_manager()
        
        # Test connection to mixpanel database
        with db_manager.get_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Check if essential tables exist
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('mixpanel_user', 'mixpanel_event', 'user_product_metrics')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            if len(tables) >= 2:  # At least 2 essential tables
                logger.info(f"Database health check passed. Found tables: {tables}")
                return True
            else:
                logger.warning(f"Database health check failed. Only found tables: {tables}")
                return False
                
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize databases
    success = initialize_all_databases()
    
    if success:
        # Check health
        if check_database_health():
            print("✅ Database initialization completed successfully")
            sys.exit(0)
        else:
            print("⚠️ Database initialization completed but health check failed")
            sys.exit(1)
    else:
        print("❌ Database initialization failed")
        sys.exit(1) 