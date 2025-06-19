#!/usr/bin/env python3
"""
Module 2: Database Schema Setup & Validation
Initializes the Mixpanel analytics database with the authoritative schema and ensures
complete structural integrity before data ingestion begins.

MISSION-CRITICAL DATABASE SETUP:
• Creates SQLite database with production-grade optimizations
• Applies complete schema from database/schema.sql (18 tables, 43 indexes)
• Validates all table structures, data types, and constraints
• Enforces foreign key relationships and data integrity
• Optimizes performance with WAL mode, caching, and indexing
• Provides bulletproof error handling and transaction rollback
• Supports both fresh installation and existing database validation

DEPENDENCIES: Requires database/schema.sql
OUTPUTS: Fully initialized database/mixpanel_data.db ready for data ingestion
SUCCESS CODE: Returns 0 on success, 1 on failure
"""
import os
import sys
import sqlite3
import logging
import re
from typing import Dict, Any, List, Optional, Tuple, Set
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - Use centralized database path discovery
DATABASE_PATH = Path(get_database_path('mixpanel_data'))
# Find project root for schema path
project_root = DATABASE_PATH.parent.parent  # database is in project root, so go up one more level
SCHEMA_PATH = project_root / "database" / "schema.sql"

# Debug logging for path resolution
logger.info(f"Script location: {Path(__file__)}")
logger.info(f"Current working directory: {Path.cwd()}")
logger.info(f"Resolved project root: {project_root}")
logger.info(f"Schema path: {SCHEMA_PATH}")
logger.info(f"Database path: {DATABASE_PATH}")

# Expected schema structure for validation
EXPECTED_TABLES = {
    'mixpanel_user': {
        'distinct_id': 'TEXT',
        'abi_ad_id': 'TEXT',
        'country': 'TEXT',
        'region': 'TEXT',
        'city': 'TEXT',
        'has_abi_attribution': 'BOOLEAN',
        'profile_json': 'TEXT',
        'first_seen': 'DATETIME',
        'last_updated': 'DATETIME',
        'valid_user': 'BOOLEAN',
        'economic_tier': 'TEXT'
    },
    'mixpanel_event': {
        'event_uuid': 'TEXT',
        'event_name': 'TEXT',
        'abi_ad_id': 'TEXT',
        'abi_campaign_id': 'TEXT',
        'abi_ad_set_id': 'TEXT',
        'distinct_id': 'TEXT',
        'event_time': 'DATETIME',
        'country': 'TEXT',
        'region': 'TEXT',
        'revenue_usd': 'DECIMAL',
        'raw_amount': 'DECIMAL',
        'currency': 'TEXT',
        'refund_flag': 'BOOLEAN',
        'is_late_event': 'BOOLEAN',
        'trial_expiration_at_calc': 'DATETIME',
        'event_json': 'TEXT'
    },
    'user_product_metrics': {
        'user_product_id': 'INTEGER',
        'distinct_id': 'TEXT',
        'product_id': 'TEXT',
        'credited_date': 'DATE',
        'country': 'TEXT',
        'region': 'TEXT',
        'device': 'TEXT',
        'abi_ad_id': 'TEXT',
        'abi_campaign_id': 'TEXT',
        'abi_ad_set_id': 'TEXT',
        'current_status': 'TEXT',
        'current_value': 'DECIMAL',
        'value_status': 'TEXT',
        'segment_id': 'TEXT',
        'accuracy_score': 'TEXT',
        'trial_conversion_rate': 'DECIMAL',
        'trial_converted_to_refund_rate': 'DECIMAL',
        'initial_purchase_to_refund_rate': 'DECIMAL',
        'price_bucket': 'DECIMAL',
        'assignment_type': 'TEXT',
        'last_updated_ts': 'DATETIME',
        'valid_lifecycle': 'BOOLEAN',
        'store': 'TEXT'
    },
    'pipeline_status': {
        'id': 'INTEGER',
        'status': 'TEXT',
        'started_at': 'DATETIME',
        'completed_at': 'DATETIME',
        'progress_percentage': 'INTEGER',
        'current_step': 'TEXT',
        'error_message': 'TEXT',
        'error_count': 'INTEGER',
        'warning_count': 'INTEGER',
        'processed_users': 'INTEGER',
        'total_users': 'INTEGER'
    },
    'ad_performance_daily': {
        'ad_id': 'TEXT',
        'date': 'DATE',
        'adset_id': 'TEXT',
        'campaign_id': 'TEXT',
        'ad_name': 'TEXT',
        'adset_name': 'TEXT',
        'campaign_name': 'TEXT',
        'spend': 'DECIMAL',
        'impressions': 'INTEGER',
        'clicks': 'INTEGER',
        'meta_trials': 'INTEGER',
        'meta_purchases': 'INTEGER'
    },
    'ad_performance_daily_country': {
        'ad_id': 'TEXT',
        'date': 'DATE',
        'country': 'TEXT',
        'adset_id': 'TEXT',
        'campaign_id': 'TEXT',
        'ad_name': 'TEXT',
        'adset_name': 'TEXT',
        'campaign_name': 'TEXT',
        'spend': 'DECIMAL',
        'impressions': 'INTEGER',
        'clicks': 'INTEGER',
        'meta_trials': 'INTEGER',
        'meta_purchases': 'INTEGER'
    },
    'ad_performance_daily_region': {
        'ad_id': 'TEXT',
        'date': 'DATE',
        'region': 'TEXT',
        'adset_id': 'TEXT',
        'campaign_id': 'TEXT',
        'ad_name': 'TEXT',
        'adset_name': 'TEXT',
        'campaign_name': 'TEXT',
        'spend': 'DECIMAL',
        'impressions': 'INTEGER',
        'clicks': 'INTEGER',
        'meta_trials': 'INTEGER',
        'meta_purchases': 'INTEGER'
    },
    'ad_performance_daily_device': {
        'ad_id': 'TEXT',
        'date': 'DATE',
        'device': 'TEXT',
        'adset_id': 'TEXT',
        'campaign_id': 'TEXT',
        'ad_name': 'TEXT',
        'adset_name': 'TEXT',
        'campaign_name': 'TEXT',
        'spend': 'DECIMAL',
        'impressions': 'INTEGER',
        'clicks': 'INTEGER',
        'meta_trials': 'INTEGER',
        'meta_purchases': 'INTEGER'
    },
    'currency_fx': {
        'date_day': 'DATE',
        'currency_code': 'CHAR',
        'usd_rate': 'DECIMAL'
    },
    'etl_job_control': {
        'job_name': 'TEXT',
        'last_run_timestamp': 'DATETIME',
        'last_success_timestamp': 'DATETIME',
        'status': 'TEXT',
        'error_message': 'TEXT',
        'run_duration_seconds': 'INTEGER'
    },
    'processed_event_days': {
        'date_day': 'DATE',
        'events_processed': 'INTEGER',
        'processing_timestamp': 'DATETIME',
        'status': 'TEXT'
    },
    'discovered_properties': {
        'property_id': 'INTEGER',
        'property_name': 'TEXT',
        'property_type': 'TEXT',
        'first_seen_date': 'DATE',
        'last_seen_date': 'DATE',
        'sample_value': 'TEXT'
    },
    'discovered_property_values': {
        'value_id': 'INTEGER',
        'property_id': 'INTEGER',
        'property_value': 'TEXT',
        'first_seen_date': 'DATE',
        'last_seen_date': 'DATE',
        'occurrence_count': 'INTEGER'
    },
    'refresh_pipeline_history': {
        'execution_id': 'INTEGER',
        'pipeline_name': 'TEXT',
        'start_time': 'DATETIME',
        'end_time': 'DATETIME',
        'status': 'TEXT',
        'records_processed': 'INTEGER',
        'error_details': 'TEXT',
        'execution_parameters': 'TEXT'
    },
    'interrupted_pipelines': {
        'pipeline_id': 'INTEGER',
        'pipeline_name': 'TEXT',
        'interruption_time': 'DATETIME',
        'last_processed_record': 'TEXT',
        'recovery_checkpoint': 'TEXT',
        'status': 'TEXT'
    },
    'dashboard_refresh_cache': {
        'cache_key': 'TEXT',
        'cache_value': 'TEXT',
        'created_timestamp': 'DATETIME',
        'expires_timestamp': 'DATETIME',
        'refresh_count': 'INTEGER'
    },
    'continent_country': {
        'country_code': 'CHAR',
        'country_name': 'TEXT',
        'continent_code': 'CHAR',
        'continent_name': 'TEXT',
        'region': 'TEXT',
        'sub_region': 'TEXT'
    },
    'saved_views': {
        'view_id': 'INTEGER',
        'view_name': 'TEXT',
        'view_description': 'TEXT',
        'view_sql': 'TEXT',
        'created_by': 'TEXT',
        'created_timestamp': 'DATETIME',
        'last_modified': 'DATETIME',
        'view_parameters': 'TEXT',
        'is_public': 'BOOLEAN'
    }
}

def main():
    try:
        logger.info("=== Module 2: Database Setup & Migration ===")
        logger.info("Ensuring database is properly configured...")
        
        # Ensure database directory exists
        os.makedirs(DATABASE_PATH.parent, exist_ok=True)
        
        # Validate schema file exists
        if not SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")
        
        # Setup database connection
        conn = create_database_connection()
        
        # Always create a fresh, clean database for the pipeline
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        existing_tables = cursor.fetchall()
        
        if existing_tables:
            logger.info("Database has existing tables - refreshing Mixpanel data while preserving Meta data")
            drop_mixpanel_tables(conn)
        
        logger.info("Creating fresh Mixpanel tables from authoritative schema")
        initialize_database_from_schema(conn)
        
        # Validate final schema
        validation_results = validate_database_schema(conn)
        if not validation_results['valid']:
            raise RuntimeError(f"Schema validation failed: {validation_results['errors']}")
        
        # Optimize database
        optimize_database(conn)
        
        conn.close()
        
        logger.info("Database setup completed successfully")
        logger.info("Database is ready for data ingestion")
        return 0
        
    except Exception as e:
        logger.error(f"Module 2 failed: {e}")
        print(f"Module 2 failed: {e}", file=sys.stderr)
        return 1

def create_database_connection() -> sqlite3.Connection:
    """Create optimized database connection with proper settings"""
    logger.info(f"Connecting to database: {DATABASE_PATH}")
    
    conn = sqlite3.connect(str(DATABASE_PATH), timeout=30.0)
    cursor = conn.cursor()
    
    # Enable essential SQLite optimizations
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")  # Better concurrency
    cursor.execute("PRAGMA synchronous = NORMAL")  # Balance safety/performance
    cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
    cursor.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp operations
    cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O
    
    conn.commit()
    
    logger.info("Database connection established with optimizations")
    return conn

def drop_mixpanel_tables(conn: sqlite3.Connection):
    """Drop only Mixpanel-related tables while preserving Meta advertising data"""
    logger.info("Dropping Mixpanel tables for fresh data...")
    
    cursor = conn.cursor()
    
    # Tables to drop (Mixpanel data only) - Order matters for foreign key constraints
    mixpanel_tables = [
        'user_product_metrics',  # Drop dependent tables first
        'mixpanel_event',        # Drop dependent tables first  
        'mixpanel_user',         # Drop parent table last
        'processed_event_days'   # This tracks which event dates have been processed
    ]
    
    try:
        cursor.execute("BEGIN TRANSACTION")
        
        # Disable foreign key constraints temporarily for clean deletion
        cursor.execute("PRAGMA foreign_keys = OFF")
        
        for table in mixpanel_tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS [{table}]")
                logger.info(f"Dropped table: {table}")
            except Exception as e:
                logger.warning(f"Could not drop table {table}: {e}")
        
        # Re-enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("COMMIT")
        
        logger.info("Successfully dropped Mixpanel tables while preserving Meta data")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        cursor.execute("PRAGMA foreign_keys = ON")  # Re-enable even on error
        logger.error(f"Failed to drop Mixpanel tables: {e}")
        raise

def initialize_database_from_schema(conn: sqlite3.Connection):
    """Initialize database by executing the authoritative schema (CREATE IF NOT EXISTS for existing tables)"""
    logger.info("Initializing database from authoritative schema...")
    
    cursor = conn.cursor()
    
    try:
        # Read and execute schema
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Modify CREATE TABLE statements to use CREATE TABLE IF NOT EXISTS
        # This allows Meta tables to be preserved while Mixpanel tables are recreated
        schema_sql = schema_sql.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ')
        
        # Clean SQL and split into individual statements
        statements = clean_and_split_sql(schema_sql)
        
        logger.info(f"Found {len(statements)} SQL statements to execute")
        
        # Execute schema in transaction with better error handling
        cursor.execute("BEGIN TRANSACTION")
        
        for i, statement in enumerate(statements):
            if statement.strip():
                try:
                    cursor.execute(statement)
                    logger.debug(f"Executed statement {i+1}/{len(statements)}")
                except Exception as stmt_error:
                    # Log but don't fail on index creation errors (indexes might already exist)
                    if "CREATE INDEX" in statement and "already exists" in str(stmt_error):
                        logger.debug(f"Index already exists (statement {i+1}): {str(stmt_error)}")
                    else:
                        logger.error(f"Failed to execute statement {i+1}: {statement[:100]}...")
                        logger.error(f"Error: {stmt_error}")
                        raise
        
        cursor.execute("COMMIT")
        
        logger.info("Database schema initialized successfully")
        
    except Exception as e:
        try:
            cursor.execute("ROLLBACK")
        except:
            pass  # Connection might be in a bad state
        logger.error(f"Failed to initialize database schema: {e}")
        raise



def get_current_database_structure(cursor: sqlite3.Cursor) -> Dict[str, Dict[str, str]]:
    """Get current database structure for comparison"""
    structure = {}
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        # Get column information - validate table name for safety
        if not table.replace('_', '').replace('-', '').isalnum():
            logger.warning(f"Skipping table with suspicious name: {table}")
            continue
        cursor.execute(f"PRAGMA table_info([{table}])")  # Use brackets to escape table name
        columns = {}
        for col_info in cursor.fetchall():
            col_name = col_info[1]
            col_type = col_info[2]
            columns[col_name] = col_type
        
        structure[table] = columns
    
    return structure

def validate_database_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Comprehensive schema validation"""
    logger.info("Validating database schema...")
    
    cursor = conn.cursor()
    errors = []
    warnings = []
    
    try:
        # Get current structure
        current_structure = get_current_database_structure(cursor)
        
        # Validate all expected tables exist
        for table_name in EXPECTED_TABLES.keys():
            if table_name not in current_structure:
                errors.append(f"Missing required table: {table_name}")
                continue
            
            # Validate columns
            expected_columns = EXPECTED_TABLES[table_name]
            current_columns = current_structure[table_name]
            
            for col_name, expected_type in expected_columns.items():
                if col_name not in current_columns:
                    errors.append(f"Missing column {col_name} in table {table_name}")
                else:
                    current_type = current_columns[col_name]
                    if not types_compatible(expected_type, current_type):
                        warnings.append(f"Type mismatch in {table_name}.{col_name}: expected {expected_type}, got {current_type}")
        
        # Validate indexes exist
        validate_indexes(cursor, errors, warnings)
        
        # Validate foreign key constraints
        validate_foreign_keys(cursor, errors, warnings)
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("✅ Schema validation passed")
        else:
            logger.error(f"❌ Schema validation failed with {len(errors)} errors")
        
        if warnings:
            logger.warning(f"⚠️  Found {len(warnings)} warnings")
            for warning in warnings:
                logger.warning(f"  - {warning}")
        
        return {
            'valid': is_valid,
            'errors': errors,
            'warnings': warnings,
            'tables_found': len(current_structure),
            'tables_expected': len(EXPECTED_TABLES)
        }
        
    except Exception as e:
        errors.append(f"Schema validation error: {e}")
        return {'valid': False, 'errors': errors, 'warnings': warnings}

def types_compatible(expected: str, actual: str) -> bool:
    """Check if database types are compatible"""
    expected = expected.upper().strip()
    actual = actual.upper().strip()
    
    # Direct match
    if expected == actual:
        return True
    
    # Handle common variations and SQLite type affinity
    type_equivalents = {
        'TEXT': ['TEXT', 'VARCHAR', 'CHAR'],
        'INTEGER': ['INTEGER', 'INT', 'BIGINT'],
        'REAL': ['REAL', 'FLOAT', 'DOUBLE'],
        'NUMERIC': ['NUMERIC', 'DECIMAL'],  # SQLite affinity mapping
        'BOOLEAN': ['BOOLEAN', 'BOOL'],
        'DATE': ['DATE', 'TEXT'],  # SQLite stores dates as TEXT
        'DATETIME': ['DATETIME', 'TIMESTAMP', 'TEXT'],  # SQLite stores datetime as TEXT
        'DECIMAL': ['DECIMAL', 'NUMERIC', 'REAL'],  # DECIMAL can map to NUMERIC or REAL in SQLite
    }
    
    # Handle type specifiers like DECIMAL(10,2), CHAR(3), etc.
    expected_base = expected.split('(')[0]
    actual_base = actual.split('(')[0]
    
    # Check base types
    for base_type, equivalents in type_equivalents.items():
        if base_type == expected_base and actual_base in equivalents:
            return True
        if base_type == actual_base and expected_base in equivalents:
            return True
    
    # Special case: CHAR(n) should be compatible with CHAR
    if (expected_base == 'CHAR' and actual_base == 'CHAR') or \
       (expected_base.startswith('CHAR') and actual_base == 'CHAR') or \
       (expected_base == 'CHAR' and actual_base.startswith('CHAR')):
        return True
    
    # Special handling for SQLite's type affinity
    # SQLite DECIMAL/NUMERIC affinity often returns as NUMERIC
    if expected_base == 'DECIMAL' and actual_base in ['NUMERIC', 'REAL']:
        return True
    if expected_base in ['DATE', 'DATETIME'] and actual_base == 'TEXT':
        return True
    
    return False

def validate_indexes(cursor: sqlite3.Cursor, errors: List[str], warnings: List[str]):
    """Validate critical indexes exist"""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
    existing_indexes = {row[0] for row in cursor.fetchall()}
    
    critical_indexes = [
        # User table indexes
        'idx_mixpanel_user_country',
        'idx_mixpanel_user_has_abi',
        'idx_mixpanel_user_first_seen',
        'idx_mixpanel_user_valid_user',
        'idx_mixpanel_user_economic_tier',
        'idx_mixpanel_user_abi_ad_id',
        
        # Event table indexes
        'idx_mixpanel_event_distinct_id',
        'idx_mixpanel_event_name',
        'idx_mixpanel_event_time',
        'idx_mixpanel_event_country',
        'idx_mixpanel_event_revenue',
        'idx_mixpanel_event_abi_ad_id',
        'idx_mixpanel_event_abi_campaign_id',
        'idx_mixpanel_event_abi_ad_set_id',
        
        # User Product Metrics indexes
        'idx_upm_distinct_id',
        'idx_upm_product_id',
        'idx_upm_credited_date',
        'idx_upm_country',
        'idx_upm_region',
        'idx_upm_device',
        'idx_upm_abi_ad_id',
        'idx_upm_abi_campaign_id',
        'idx_upm_abi_ad_set_id',
        'idx_upm_valid_lifecycle',
        'idx_upm_store',
        'idx_upm_price_bucket',
        'idx_upm_assignment_type',
        
        # Advertising performance table indexes
        'idx_ad_perf_date',
        'idx_ad_perf_campaign',
        'idx_ad_perf_adset',
        'idx_ad_perf_ad_id',
        'idx_ad_perf_country_date',
        'idx_ad_perf_country_campaign',
        'idx_ad_perf_country_ad_id',
        'idx_ad_perf_region_date',
        'idx_ad_perf_region_campaign',
        'idx_ad_perf_region_ad_id',
        'idx_ad_perf_device_date',
        'idx_ad_perf_device_campaign',
        'idx_ad_perf_device_ad_id',
        
        # Supporting table indexes
        'idx_currency_fx_date',
        'idx_etl_job_status',
        'idx_pipeline_history_name_time',
        'idx_dashboard_cache_expires'
    ]
    
    for index in critical_indexes:
        if index not in existing_indexes:
            warnings.append(f"Missing critical index: {index}")

def validate_foreign_keys(cursor: sqlite3.Cursor, errors: List[str], warnings: List[str]):
    """Validate foreign key constraints are enabled"""
    cursor.execute("PRAGMA foreign_keys")
    fk_enabled = cursor.fetchone()[0]
    
    if not fk_enabled:
        errors.append("Foreign key constraints are not enabled")

def optimize_database(conn: sqlite3.Connection):
    """Perform database optimization operations"""
    logger.info("Optimizing database...")
    
    cursor = conn.cursor()
    
    try:
        # Update table statistics
        cursor.execute("ANALYZE")
        
        # Rebuild indexes if needed
        cursor.execute("REINDEX")
        
        # Vacuum to optimize file size (only if not in WAL mode for this operation)
        cursor.execute("VACUUM")
        
        logger.info("Database optimization completed")
        
    except Exception as e:
        logger.warning(f"Database optimization warning: {e}")

def clean_and_split_sql(sql: str) -> List[str]:
    """Clean SQL and split into individual statements for safe execution"""
    # Remove SQL comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Split on semicolons but be careful about semicolons in strings
    statements = []
    current_statement = ""
    in_string = False
    string_char = None
    
    i = 0
    while i < len(sql):
        char = sql[i]
        
        if not in_string:
            if char in ('"', "'"):
                in_string = True
                string_char = char
            elif char == ';':
                # End of statement
                if current_statement.strip():
                    statements.append(current_statement.strip())
                current_statement = ""
                i += 1
                continue
        else:
            if char == string_char:
                # In SQL, quotes are escaped by doubling them, not with backslashes
                # Check if the next character is the same quote (escaped quote)
                if i + 1 < len(sql) and sql[i + 1] == string_char:
                    # This is an escaped quote, add both characters and continue in string
                    current_statement += char + sql[i + 1]
                    i += 2
                    continue
                else:
                    # This is the end of the string
                    in_string = False
                    string_char = None
        
        current_statement += char
        i += 1
    
    # Add final statement if exists
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    return statements

if __name__ == "__main__":
    sys.exit(main()) 