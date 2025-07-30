#!/usr/bin/env python3
"""
Module 3: Data Ingestion - Production Ready

This module ingests downloaded Mixpanel data into the database with:
- Robust error handling and retry mechanisms
- Memory-efficient streaming processing
- Comprehensive data validation and filtering
- Production-grade optimizations and monitoring
- Now reads from database tables instead of filesystem
- Supports both SQLite (local) and PostgreSQL (production)
"""

import os
import sys
import sqlite3
import json
import logging
import time
import datetime
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass
from datetime import timedelta

from urllib.parse import urlparse

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Try to import psycopg2, but don't fail if not available (for local SQLite testing)
try:
    import psycopg2
    import psycopg2.extensions
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    psycopg2 = None

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - Use centralized database path discovery
DATABASE_PATH = Path(get_database_path('mixpanel_data'))

# Database configuration - use Heroku Postgres if available, otherwise SQLite
DATABASE_URL = os.environ.get('DATABASE_URL')
USE_POSTGRES = DATABASE_URL is not None and HAS_POSTGRES

# Event names to process (important events only)
IMPORTANT_EVENTS = {
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal"
}

# Performance and safety configuration
BATCH_SIZE = 10000  # Optimized for 32GB RAM
MAX_MEMORY_USAGE = 100_000
CONNECTION_TIMEOUT = 60.0
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0

def get_raw_data_connection():
    """Get connection to database for raw data (PostgreSQL if available, otherwise SQLite)"""
    if USE_POSTGRES:
        logger.info("Using PostgreSQL for raw data")
        # Parse DATABASE_URL (Heroku format: postgres://user:pass@host:port/dbname)
        url = urlparse(DATABASE_URL)
        
        conn = psycopg2.connect(
            host=url.hostname,
            port=url.port,
            database=url.path[1:],  # Remove leading slash
            user=url.username,
            password=url.password,
            sslmode='require'  # Heroku requires SSL
        )
        return conn, 'postgres'
    else:
        logger.info("Using SQLite for raw data (local mode)")
        # Use centralized database path discovery for raw_data.db
        db_path = Path(get_database_path('raw_data'))
        
        if not db_path.exists():
            raise FileNotFoundError(f"Raw data database not found at {db_path}. Run Module 1 first.")
        
        conn = sqlite3.connect(str(db_path))
        return conn, 'sqlite'

@dataclass
class IngestionMetrics:
    """Track ingestion metrics for monitoring and reporting"""
    users_processed: int = 0
    users_filtered_atly: int = 0
    users_filtered_test: int = 0
    users_filtered_steps: int = 0
    events_processed: int = 0
    events_skipped_unimportant: int = 0
    events_skipped_invalid: int = 0
    events_skipped_missing_users: int = 0
    files_processed: int = 0
    dates_processed: int = 0
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    
    def elapsed_time(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

def main():
    """Main ingestion orchestrator with comprehensive error handling"""
    metrics = IngestionMetrics()
    metrics.start_time = now_in_timezone()
    
    try:
        logger.info("Starting data ingestion pipeline...")
        
        # Validate prerequisites
        validate_prerequisites()
        
        # Create database connections
        raw_data_conn, raw_db_type = get_raw_data_connection()
        sqlite_conn = create_sqlite_connection()
        
        try:
            # Show raw data summary
            show_raw_data_summary(raw_data_conn, raw_db_type)
            
            # Validate database schema before proceeding
            validate_database_schema(sqlite_conn)
            
            # Process all data with comprehensive error handling
            process_all_data(raw_data_conn, raw_db_type, sqlite_conn, metrics)
            
            # Verify data integrity
            verify_ingestion(sqlite_conn, metrics)
            
            # Show processed data summary
            show_processed_data_summary(sqlite_conn)
            
        finally:
            raw_data_conn.close()
            sqlite_conn.close()
        
        metrics.end_time = now_in_timezone()
        log_final_metrics(metrics)
        show_ingestion_metrics(metrics)
        
        logger.info("Data ingestion completed successfully")
        return 0
        
    except Exception as e:
        metrics.end_time = now_in_timezone()
        logger.error(f"Module 3 failed: {e}")
        logger.error(f"Elapsed time: {metrics.elapsed_time():.2f} seconds")
        print(f"Module 3 failed: {e}", file=sys.stderr)
        return 1

def validate_prerequisites():
    """Validate all prerequisites before starting ingestion"""
    logger.info("Validating prerequisites...")
    
    # Check SQLite database exists
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DATABASE_PATH}. Run Module 2 first.")
    
    # Check raw data source (either PostgreSQL or SQLite)
    if USE_POSTGRES:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set but PostgreSQL mode detected")
    else:
        raw_db_path = Path(get_database_path('raw_data'))
        if not raw_db_path.exists():
            raise FileNotFoundError(f"Raw data database not found at {raw_db_path}. Run Module 1 first.")
    
    logger.info("Prerequisites validated successfully")

def create_sqlite_connection():
    """Create optimized SQLite database connection with proper resource management"""
    logger.info(f"Connecting to SQLite database: {DATABASE_PATH}")
    
    conn = sqlite3.connect(
        str(DATABASE_PATH), 
        timeout=CONNECTION_TIMEOUT,
        check_same_thread=False
    )
    
    # Apply production optimizations
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O
    cursor.execute("PRAGMA optimize")
    
    logger.info("SQLite database connection established with optimizations")
    return conn

def show_raw_data_summary(raw_data_conn, raw_db_type: str):
    """Show summary of raw data available for processing"""
    cursor = raw_data_conn.cursor()
    
    try:
        # Count users
        cursor.execute("SELECT COUNT(*) FROM raw_user_data")
        user_count = cursor.fetchone()[0]
        
        # Count events by date (limit to recent dates for readability)
        cursor.execute("""
            SELECT date_day, COUNT(*) as event_count
            FROM raw_event_data 
            GROUP BY date_day 
            ORDER BY date_day DESC 
            LIMIT 15
        """)
        recent_dates = cursor.fetchall()
        
        # Total events
        cursor.execute("SELECT COUNT(*) FROM raw_event_data")
        total_events = cursor.fetchone()[0]
        
        print(f"\n📊 === RAW DATA SUMMARY ({raw_db_type.upper()}) ===")
        print(f"👥 Raw users available: {user_count:,}")
        print(f"📅 Raw event data by date:")
        for date_day, event_count in recent_dates:
            print(f"  - {date_day}: {event_count} events")
        print(f"📊 Total events available: {total_events:,}")
        print(f"🗃️  Raw events in database: {total_events:,}")
        
    except Exception as e:
        logger.error(f"Error generating raw data summary: {e}")

def show_processed_data_summary(sqlite_conn: sqlite3.Connection):
    """Show summary of processed data in SQLite"""
    cursor = sqlite_conn.cursor()
    
    try:
        # Count processed users
        cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
        user_count = cursor.fetchone()[0]
        
        # Count valid users (non-filtered)
        cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE is_valid = 1")
        valid_user_count = cursor.fetchone()[0]
        
        # Count processed events
        cursor.execute("SELECT COUNT(*) FROM mixpanel_event")
        event_count = cursor.fetchone()[0]
        
        # Count processed dates
        cursor.execute("SELECT COUNT(*) FROM processed_event_days")
        dates_count = cursor.fetchone()[0]
        
        # Recent processed dates
        cursor.execute("""
            SELECT date_day, events_processed
            FROM processed_event_days 
            ORDER BY date_day DESC 
            LIMIT 15
        """)
        recent_dates = cursor.fetchall()
        
        print(f"\n📊 === PROCESSED DATA SUMMARY (SQLITE) ===")
        print(f"👥 Processed users: {user_count:,}")
        print(f"✅ Valid users: {valid_user_count:,}")
        print(f"📊 Processed events: {event_count:,}")
        print(f"📅 Processed dates: {dates_count}")
        for date_day, event_count in recent_dates:
            print(f"  - {date_day}: {event_count} events")
        
    except Exception as e:
        logger.error(f"Error generating processed data summary: {e}")

def show_ingestion_metrics(metrics: IngestionMetrics):
    """Show ingestion metrics in a test-style format"""
    print(f"\n🧪 === INGESTION METRICS ===")
    print(f"⏱️  Total Processing Time: {metrics.elapsed_time():.2f} seconds")
    print(f"📅 Dates Processed: {metrics.dates_processed}")
    print(f"👥 Users Processed: {metrics.users_processed}")
    print(f"🚫 Users Filtered (@atly.com): {metrics.users_filtered_atly}")
    print(f"🚫 Users Filtered (test): {metrics.users_filtered_test}")
    print(f"🚫 Users Filtered (@steps.me): {metrics.users_filtered_steps}")
    print(f"📊 Events Processed: {metrics.events_processed}")
    print(f"⏭️  Events Skipped (unimportant): {metrics.events_skipped_unimportant}")
    print(f"❌ Events Skipped (invalid): {metrics.events_skipped_invalid}")
    print(f"🔗 Events Skipped (missing users): {metrics.events_skipped_missing_users}")
    print(f"🎉 Data ingestion completed successfully")

def validate_database_schema(conn: sqlite3.Connection):
    """Validate database has the required schema from Module 2"""
    logger.info("Validating database schema...")
    
    cursor = conn.cursor()
    
    # Check critical tables exist
    required_tables = [
        'mixpanel_user',
        'mixpanel_event', 
        'user_product_metrics',
        'ad_performance_daily',
        'processed_event_days'
    ]
    
    for table in required_tables:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
            (table,)
        )
        if not cursor.fetchone():
            raise RuntimeError(f"Required table '{table}' not found. Run Module 2 first.")
    
    # Check critical columns exist
    cursor.execute("PRAGMA table_info(mixpanel_user)")
    user_columns = {col[1] for col in cursor.fetchall()}
    required_user_columns = {'distinct_id', 'valid_user', 'economic_tier', 'abi_ad_id', 'abi_campaign_id', 'abi_ad_set_id'}
    
    missing_columns = required_user_columns - user_columns
    if missing_columns:
        raise RuntimeError(f"Missing columns in mixpanel_user: {missing_columns}")
    
    # Check processed_event_days structure matches schema
    cursor.execute("PRAGMA table_info(processed_event_days)")
    event_columns = {col[1] for col in cursor.fetchall()}
    if 'date_day' not in event_columns:
        raise RuntimeError("processed_event_days table not migrated to new schema. Run Module 2 first.")
    
    logger.info("Database schema validation passed")

def process_all_data(raw_data_conn, raw_db_type: str, sqlite_conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process all user and event data with comprehensive error handling"""
    
    # Step 1: Refresh all users
    logger.info("=== Step 1: Refreshing User Data ===")
    refresh_all_users(raw_data_conn, raw_db_type, sqlite_conn, metrics)
    
    # Step 2: Pre-load user mappings for performance (32GB RAM optimization)
    logger.info("=== Step 2: Pre-loading User Mappings for Performance ===")
    global_user_mappings = load_user_mappings_to_memory(sqlite_conn)
    logger.info(f"Loaded {len(global_user_mappings['distinct_ids'])} direct mappings and {len(global_user_mappings['user_id_to_distinct_id'])} identity merges into memory")
    
    # Step 3: Process events incrementally with pre-loaded mappings
    logger.info("=== Step 3: Processing Events Incrementally ===")
    process_events_incrementally(raw_data_conn, raw_db_type, sqlite_conn, metrics, global_user_mappings)

def load_user_mappings_to_memory(sqlite_conn: sqlite3.Connection) -> dict:
    """
    Pre-load all user identity mappings into memory for fast event processing
    
    Returns:
        dict: {
            'distinct_ids': set of all user distinct_ids,
            'user_id_to_distinct_id': dict mapping $user_id -> distinct_id  
        }
    """
    logger.info("Loading user mappings into memory for performance optimization...")
    
    cursor = sqlite_conn.cursor()
    
    # Load all user distinct_ids and their profile JSON
    cursor.execute("""
        SELECT distinct_id, profile_json FROM mixpanel_user 
        WHERE profile_json IS NOT NULL
    """)
    
    distinct_ids = set()
    user_id_to_distinct_id = {}
    
    for row in cursor.fetchall():
        distinct_id, profile_json = row
        distinct_ids.add(distinct_id)
        
        # Extract $user_id from profile_json for identity merging
        try:
            profile = json.loads(profile_json)
            properties = profile.get('properties', {})
            user_id = properties.get('$user_id')
            if user_id:
                user_id_to_distinct_id[user_id] = distinct_id
        except (json.JSONDecodeError, KeyError):
            continue
    
    return {
        'distinct_ids': distinct_ids,
        'user_id_to_distinct_id': user_id_to_distinct_id
    }

def refresh_all_users(raw_data_conn, raw_db_type: str, sqlite_conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process users from raw_user_data table"""
    sqlite_cursor = sqlite_conn.cursor()
    
    try:
        logger.info(f"Processing user data from {raw_db_type} database...")
        sqlite_cursor.execute("BEGIN IMMEDIATE")
        
        # Get user count from raw data database
        raw_cursor = raw_data_conn.cursor()
        raw_cursor.execute("SELECT COUNT(*) FROM raw_user_data")
        user_count = raw_cursor.fetchone()[0]
        
        logger.info(f"Found {user_count} users to process from {raw_db_type}")
        
        if user_count == 0:
            logger.warning(f"No user data found in {raw_db_type} raw_user_data table")
            sqlite_cursor.execute("COMMIT")
            return
        
        # Process users in batches
        batch_size = 10000  # Optimized for 32GB RAM
        offset = 0
        
        while offset < user_count:
            logger.info(f"Processing users {offset+1} to {min(offset+batch_size, user_count)}")
            
            if raw_db_type == 'postgres':
                raw_cursor.execute("""
                    SELECT distinct_id, user_data 
                    FROM raw_user_data 
                    ORDER BY distinct_id 
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
            else:
                raw_cursor.execute("""
                    SELECT distinct_id, user_data 
                    FROM raw_user_data 
                    ORDER BY distinct_id 
                    LIMIT ? OFFSET ?
                """, (batch_size, offset))
            
            users_batch = raw_cursor.fetchall()
            
            if not users_batch:
                break
                
            batch_metrics = process_user_batch_from_raw_data(sqlite_conn, users_batch)
            metrics.users_processed += batch_metrics['users_processed']
            metrics.users_filtered_atly += batch_metrics['users_filtered_atly']
            metrics.users_filtered_test += batch_metrics['users_filtered_test']
            metrics.users_filtered_steps += batch_metrics['users_filtered_steps']
            
            offset += batch_size
        
        sqlite_cursor.execute("COMMIT")
        logger.info(f"User processing completed: {metrics.users_processed} users processed")
        
    except Exception as e:
        sqlite_cursor.execute("ROLLBACK")
        logger.error(f"User processing failed: {e}")
        raise

def process_user_batch_from_raw_data(sqlite_conn: sqlite3.Connection, users_batch: List[Tuple]) -> Dict[str, int]:
    """Process users from raw data batch with comprehensive validation and error handling"""
    sqlite_cursor = sqlite_conn.cursor()
    
    batch_metrics = {
        'users_processed': 0,
        'users_filtered_atly': 0,
        'users_filtered_test': 0,
        'users_filtered_steps': 0
    }
    
    # Batch processing collections
    user_batch = []
    
    # Examples for logging
    atly_examples = []
    test_examples = []
    steps_examples = []
    
    # Track invalid records for summary logging
    invalid_distinct_id_count = 0
    
    try:
        for distinct_id, user_data_json in users_batch:
            try:
                # Handle both JSON string and dict data types
                if isinstance(user_data_json, str):
                    user_data = json.loads(user_data_json)
                elif isinstance(user_data_json, dict):
                    user_data = user_data_json
                else:
                    logger.warning(f"Unexpected user_data type for {distinct_id}: {type(user_data_json)}")
                    continue
                
                # Validate distinct_id
                if not distinct_id or not isinstance(distinct_id, str):
                    invalid_distinct_id_count += 1
                    continue
                
                # Apply user filtering logic - extract email from properties
                properties = user_data.get('properties', {})
                email = properties.get('$email', '')
                filter_result = should_filter_user(distinct_id, email)
                
                if filter_result['filter']:
                    if filter_result['reason'] == 'atly':
                        batch_metrics['users_filtered_atly'] += 1
                        if len(atly_examples) < 3:
                            atly_examples.append((distinct_id, email))
                    elif filter_result['reason'] == 'test':
                        batch_metrics['users_filtered_test'] += 1
                        if len(test_examples) < 3:
                            test_examples.append((distinct_id, email))
                    elif filter_result['reason'] == 'steps':
                        batch_metrics['users_filtered_steps'] += 1
                        if len(steps_examples) < 3:
                            steps_examples.append((distinct_id, email))
                    continue
                
                # Prepare user record
                user_record = prepare_user_record(user_data, distinct_id)
                user_batch.append(user_record)
                
                batch_metrics['users_processed'] += 1
                
                # Process in batches for memory management
                if len(user_batch) >= BATCH_SIZE:
                    process_user_batch(sqlite_cursor, user_batch)
                    user_batch.clear()
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON for user {distinct_id}")
            except Exception as e:
                logger.error(f"Error processing user {distinct_id}: {e}")
        
        # Process remaining batch
        if user_batch:
            process_user_batch(sqlite_cursor, user_batch)
        
        # Log filtering examples and invalid record summary
        if batch_metrics['users_filtered_atly'] > 0 or batch_metrics['users_filtered_test'] > 0 or batch_metrics['users_filtered_steps'] > 0:
            log_user_filtering_examples("Raw data batch", atly_examples, test_examples, steps_examples, batch_metrics)
        
        return batch_metrics
        
    except Exception as e:
        logger.error(f"Failed to process user batch from raw data: {e}")
        raise

def process_events_incrementally(raw_data_conn, raw_db_type: str, sqlite_conn: sqlite3.Connection, metrics: IngestionMetrics, global_user_mappings: dict):
    """Process events incrementally by date from raw_event_data table"""
    
    # Get already processed dates from SQLite
    processed_dates = get_processed_dates(sqlite_conn)
    logger.info(f"Found {len(processed_dates)} already processed dates in SQLite")
    
    # REFRESH LOGIC: Identify last 3 days for refresh requirement
    today = now_in_timezone().date()
    refresh_start = today - timedelta(days=2)  # Last 3 days including today
    refresh_dates = []
    current_date = refresh_start
    
    while current_date <= today:
        refresh_dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    refresh_dates_set = set(refresh_dates)
    logger.info(f"🔄 REFRESH REQUIREMENT: Will re-process last 3 days: {refresh_dates}")
    
    # Find unprocessed event dates in raw data database
    raw_cursor = raw_data_conn.cursor()
    raw_cursor.execute("""
        SELECT DISTINCT date_day 
        FROM raw_event_data 
        ORDER BY date_day
    """)
    
    available_dates = [row[0] for row in raw_cursor.fetchall()]
    
    # Handle date formatting differences between PostgreSQL and SQLite
    if raw_db_type == 'postgres':
        # For refresh dates, include them even if already processed
        unprocessed_dates = []
        refresh_dates_to_process = []
        
        for date in available_dates:
            date_str = date.strftime('%Y-%m-%d')
            
            if date_str in refresh_dates_set:
                # This is a refresh date - always process it
                refresh_dates_to_process.append(date)
                logger.info(f"🔄 Will re-process refresh date: {date_str}")
            elif date_str not in processed_dates:
                # This is a new date that hasn't been processed
                unprocessed_dates.append(date)
                logger.info(f"🆕 Will process new date: {date_str}")
        
        # Combine new dates + refresh dates
        dates_to_process = unprocessed_dates + refresh_dates_to_process
        
    else:
        # SQLite stores dates as strings, so we need to parse them
        unprocessed_dates = []
        refresh_dates_to_process = []
        
        for date_str in available_dates:
            try:
                if isinstance(date_str, str):
                    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                else:
                    date_obj = date_str
                
                normalized_date_str = date_obj.strftime('%Y-%m-%d')
                
                if normalized_date_str in refresh_dates_set:
                    # This is a refresh date - always process it
                    refresh_dates_to_process.append(date_obj)
                    logger.info(f"🔄 Will re-process refresh date: {normalized_date_str}")
                elif normalized_date_str not in processed_dates:
                    # This is a new date that hasn't been processed
                    unprocessed_dates.append(date_obj)
                    logger.info(f"🆕 Will process new date: {normalized_date_str}")
                    
            except ValueError:
                logger.warning(f"Invalid date format in raw data: {date_str}")
        
        # Combine new dates + refresh dates
        dates_to_process = unprocessed_dates + refresh_dates_to_process
    
    logger.info(f"Found {len(unprocessed_dates)} new dates and {len(refresh_dates_to_process)} refresh dates to process from {raw_db_type}")
    if not dates_to_process:
        logger.info("No new or refresh event dates to process")
        return
    
    # Process each date
    for date_obj in dates_to_process:
        date_str = date_obj.strftime('%Y-%m-%d')
        is_refresh_date = date_str in refresh_dates_set
        
        if is_refresh_date:
            logger.info(f"🔄 RE-PROCESSING refresh date: {date_str}")
            # Clear existing processed data for refresh dates
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute("DELETE FROM mixpanel_event WHERE DATE(event_time) = ?", (date_str,))
            sqlite_cursor.execute("DELETE FROM processed_event_days WHERE date_day = ?", (date_str,))
            sqlite_conn.commit()
            logger.info(f"🗑️  Cleared existing processed data for refresh date: {date_str}")
        else:
            logger.info(f"🆕 PROCESSING new date: {date_str}")
        
        try:
            # Process all events for this date in transaction
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute("BEGIN IMMEDIATE")
            
            # Get events for this date from raw data database
            if raw_db_type == 'postgres':
                raw_cursor.execute("""
                    SELECT event_data 
                    FROM raw_event_data 
                    WHERE date_day = %s
                    ORDER BY file_sequence
                """, (date_obj,))
            else:
                raw_cursor.execute("""
                    SELECT event_data 
                    FROM raw_event_data 
                    WHERE date_day = ?
                    ORDER BY file_sequence
                """, (date_str,))
            
            date_events = 0
            event_batch = []
            
            for (event_data_json,) in raw_cursor:
                try:
                    # Handle both JSON string and dict data types
                    if isinstance(event_data_json, str):
                        event_data = json.loads(event_data_json)
                    elif isinstance(event_data_json, dict):
                        event_data = event_data_json
                    else:
                        logger.warning(f"Unexpected event_data type: {type(event_data_json)}")
                        metrics.events_skipped_invalid += 1
                        continue
                    
                    # Handle both old and new event data formats
                    event_name = event_data.get('event') or event_data.get('event_name')
                    
                    # Skip unimportant events
                    if event_name not in IMPORTANT_EVENTS:
                        metrics.events_skipped_unimportant += 1
                        continue
                    
                    # Extract and validate event data
                    event_record = prepare_event_record(event_data, f"{raw_db_type}:{date_str}", 0)
                    if not event_record:
                        metrics.events_skipped_invalid += 1
                        continue
                    
                    event_batch.append(event_record)
                    
                    # Process in batches
                    if len(event_batch) >= BATCH_SIZE:
                        events_inserted = insert_event_batch(sqlite_cursor, event_batch, metrics, is_refresh_date, global_user_mappings)
                        date_events += events_inserted
                        event_batch.clear()
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in event data for {date_str}")
                    metrics.events_skipped_invalid += 1
                except Exception as e:
                    logger.error(f"Error processing event for {date_str}: {e}")
                    metrics.events_skipped_invalid += 1
            
            # Process remaining batch
            if event_batch:
                events_inserted = insert_event_batch(sqlite_cursor, event_batch, metrics, is_refresh_date, global_user_mappings)
                date_events += events_inserted
            
            # Mark date as processed
            mark_date_as_processed(sqlite_cursor, date_str, 1, date_events)
            
            sqlite_cursor.execute("COMMIT")
            
            metrics.dates_processed += 1
            metrics.events_processed += date_events
            
            action_type = "Re-processed" if is_refresh_date else "Processed"
            logger.info(f"{action_type} {date_events} events for date {date_str}")
            
        except Exception as e:
            sqlite_cursor.execute("ROLLBACK")
            logger.error(f"Failed to process date {date_str}: {e}")
            raise
    
    # Log summary of event processing
    if metrics.dates_processed > 0:
        logger.info(f"Event processing completed: {metrics.dates_processed} dates, {metrics.events_processed} events ingested")
        if len(refresh_dates_to_process) > 0:
            logger.info(f"🔄 Successfully re-processed {len(refresh_dates_to_process)} refresh dates for data freshness")

def should_filter_user(distinct_id: str, email: str) -> Dict[str, Any]:
    """Determine if user should be filtered and why"""
    if not email:
        return {'filter': False}
    
    email_lower = email.lower()
    
    # Filter users with '@atly.com' in email
    if '@atly.com' in email_lower:
        return {'filter': True, 'reason': 'atly'}
    
    # Filter users with 'test' in email
    if 'test' in email_lower:
        return {'filter': True, 'reason': 'test'}
    
    # Filter users with '@steps.me' in email
    if '@steps.me' in email_lower:
        return {'filter': True, 'reason': 'steps'}
    
    return {'filter': False}

def prepare_user_record(user_data: Dict[str, Any], distinct_id: str) -> Dict[str, Any]:
    """Prepare user record with proper data types and validation"""
    
    # Extract from properties (correct structure)
    properties = user_data.get('properties', {})
    
    # Extract attribution data (use direct values, not hash)
    abi_ad_id = properties.get('abi_~ad_id')
    abi_campaign_id = properties.get('abi_~campaign_id')
    abi_ad_set_id = properties.get('abi_~ad_set_id')
    
    # Validate attribution data extraction
    if abi_ad_id and not abi_campaign_id:
        logger.warning(f"User {distinct_id} has abi_ad_id but missing abi_campaign_id")
    if abi_ad_id and not abi_ad_set_id:
        logger.warning(f"User {distinct_id} has abi_ad_id but missing abi_ad_set_id")
    
    # Extract location data from properties with correct field names
    country = properties.get('$country_code')
    region = properties.get('$region')
    city = properties.get('$city')
    
    # Determine attribution status
    has_abi_attribution = bool(abi_ad_id)
    
    # Extract timestamps from properties with correct field names
    first_seen = properties.get('first_install_date') or properties.get('$ae_first_app_open_date')
    if first_seen:
        try:
            # Parse ISO format timestamp
            first_seen_dt = datetime.datetime.fromisoformat(first_seen.replace('Z', '+00:00'))
        except:
            first_seen_dt = now_in_timezone()
    else:
        first_seen_dt = now_in_timezone()
    
    last_updated = properties.get('$last_seen')
    if last_updated:
        try:
            last_updated_dt = datetime.datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
        except:
            last_updated_dt = now_in_timezone()
    else:
        last_updated_dt = now_in_timezone()
    
    return {
        'distinct_id': distinct_id,
        'abi_ad_id': abi_ad_id,
        'abi_campaign_id': abi_campaign_id,
        'abi_ad_set_id': abi_ad_set_id,
        'country': country,
        'region': region, 
        'city': city,
        'has_abi_attribution': has_abi_attribution,
        'profile_json': json.dumps(user_data) if isinstance(user_data, dict) else str(user_data),
        'first_seen': first_seen_dt.isoformat(),
        'last_updated': last_updated_dt.isoformat(),
        'valid_user': True,  # Set to TRUE during ingestion
        'economic_tier': None  # Will be calculated later by analytics
    }

def process_user_batch(cursor: sqlite3.Cursor, user_batch: List[Dict]):
    """Process user batch with proper column mapping"""
    
    # Process user records
    user_records = []
    
    for user in user_batch:
        user_records.append((
            user['distinct_id'],
            user['abi_ad_id'],
            user['abi_campaign_id'],
            user['abi_ad_set_id'],
            user['country'],
            user['region'],
            user['city'],
            user['has_abi_attribution'],
            user['profile_json'],
            user['first_seen'],
            user['last_updated'],
            user['valid_user'],
            user['economic_tier']
        ))
    
    # Batch insert users
    cursor.executemany(
        """
        INSERT OR REPLACE INTO mixpanel_user 
        (distinct_id, abi_ad_id, abi_campaign_id, abi_ad_set_id, country, region, city, has_abi_attribution, 
         profile_json, first_seen, last_updated, valid_user, economic_tier)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        user_records
    )

def log_user_filtering_examples(file_name: str, atly_examples: List[Tuple], 
                               test_examples: List[Tuple], steps_examples: List[Tuple], file_metrics: Dict[str, int]):
    """Log filtering examples and counts"""
    if file_metrics['users_filtered_atly'] > 0:
        logger.info(f"Filtered {file_metrics['users_filtered_atly']} users with '@atly.com' emails from {file_name}")
        if atly_examples:
            logger.info("Examples of filtered '@atly.com' users:")
            for distinct_id, email in atly_examples:
                logger.info(f"  - distinct_id: {distinct_id}, email: {email}")
    
    if file_metrics['users_filtered_test'] > 0:
        logger.info(f"Filtered {file_metrics['users_filtered_test']} users with 'test' emails from {file_name}")
        if test_examples:
            logger.info("Examples of filtered 'test' users:")
            for distinct_id, email in test_examples:
                logger.info(f"  - distinct_id: {distinct_id}, email: {email}")
    
    if file_metrics['users_filtered_steps'] > 0:
        logger.info(f"Filtered {file_metrics['users_filtered_steps']} users with '@steps.me' emails from {file_name}")
        if steps_examples:
            logger.info("Examples of filtered '@steps.me' users:")
            for distinct_id, email in steps_examples:
                logger.info(f"  - distinct_id: {distinct_id}, email: {email}")

def prepare_event_record(event_data: Dict[str, Any], file_path: str, line_num: int) -> Optional[Tuple]:
    """Prepare event record with validation and ALL required fields"""
    try:
        properties = event_data.get('properties', {})
        
        # Extract required fields - handle BOTH old and new data formats
        # OLD FORMAT: fields in properties, NEW FORMAT: fields at top level
        
        # Event UUID: Try properties first, then top level
        event_uuid = properties.get('$insert_id') or event_data.get('insert_id')
        
        # Distinct ID: Try properties first, then top level
        distinct_id = properties.get('distinct_id') or event_data.get('distinct_id')
        
        # Event name: Try top level first (old: 'event', new: 'event_name'), then properties
        event_name = event_data.get('event') or event_data.get('event_name') or properties.get('event')
        
        # Validate required fields
        if not distinct_id:
            logger.warning(f"Missing distinct_id in {file_path}:{line_num}")
            return None
        
        # Generate fallback UUID if needed
        if not event_uuid:
            # Time: Try properties first, then top level
            timestamp = properties.get('time') or event_data.get('time', 0)
            event_uuid = f"{distinct_id}_{event_name}_{timestamp}_{hash(str(properties))}"
        
        # Parse timestamp with proper UTC handling - try both locations
        try:
            timestamp = properties.get('time') or event_data.get('time', 0)
            if isinstance(timestamp, str):
                timestamp = float(timestamp)
            event_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        except (ValueError, TypeError, OSError):
            logger.warning(f"Invalid timestamp in {file_path}:{line_num}")
            event_time = datetime.datetime.now(tz=datetime.timezone.utc)
        
        # Extract attribution data - events don't have attribution fields
        # Attribution is only available in user records
        abi_ad_id = None
        abi_campaign_id = None
        abi_ad_set_id = None
        
        # Extract geographic data - events don't have location fields
        # Geographic data is only available in user records
        country = None
        region = None
        
        # Parse revenue safely
        try:
            revenue = float(properties.get('revenue', 0)) if properties.get('revenue') else 0
        except (ValueError, TypeError):
            revenue = 0
        
        currency = properties.get('currency', 'USD') or 'USD'
        
        # Extract additional fields
        refund_flag = False  # Could be derived from event type
        is_late_event = False  # Could be calculated based on processing time
        trial_expiration_at_calc = None
        
        # Check for trial expiration
        expiration_at = properties.get('expiration_at')
        if expiration_at:
            try:
                trial_expiration_at_calc = datetime.datetime.fromisoformat(expiration_at.replace('Z', '+00:00'))
            except:
                pass
        
        return (
            event_uuid,
            event_name,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            distinct_id,
            event_time.isoformat(),
            country,
            region,
            revenue,
            revenue,  # raw_amount same as revenue for now
            currency,
            refund_flag,
            is_late_event,
            trial_expiration_at_calc.isoformat() if trial_expiration_at_calc else None,
            json.dumps(event_data) if isinstance(event_data, dict) else str(event_data)
        )
        
    except Exception as e:
        logger.error(f"Error preparing event record {file_path}:{line_num}: {e}")
        return None

def insert_event_batch(cursor: sqlite3.Cursor, event_batch: List[Tuple], metrics: IngestionMetrics, is_refresh_date: bool, global_user_mappings: dict) -> int:
    """Insert event batch with pre-loaded user mappings for optimal performance"""
    
    # Filter events to only include those with existing users
    valid_events = []
    skipped_events = 0
    
    # Use pre-loaded mappings (32GB RAM optimization - no database queries needed!)
    existing_distinct_ids = global_user_mappings['distinct_ids']
    user_id_to_distinct_id = global_user_mappings['user_id_to_distinct_id']
    
    # Filter events to only include those with existing users
    for event in event_batch:
        original_distinct_id = event[5]  # distinct_id is at index 5
        
        # Check if event distinct_id matches user distinct_id OR user $user_id
        if original_distinct_id in existing_distinct_ids:
            # Direct match - use event as-is
            valid_events.append(event)
        elif original_distinct_id in user_id_to_distinct_id:
            # Cross-reference match - update event's distinct_id to match user table
            mapped_distinct_id = user_id_to_distinct_id[original_distinct_id]
            # Create new event tuple with corrected distinct_id
            corrected_event = list(event)
            corrected_event[5] = mapped_distinct_id  # Update distinct_id field
            valid_events.append(tuple(corrected_event))
        else:
            skipped_events += 1
    
    # Track skipped events in metrics (no longer log per batch)
    if skipped_events > 0:
        metrics.events_skipped_missing_users += skipped_events
    
    # Insert only valid events
    if valid_events:
        # CRITICAL: Use INSERT OR REPLACE for refresh dates to ensure updates are applied
        # Use INSERT OR IGNORE for new dates to avoid duplicates
        if is_refresh_date:
            insert_sql = """
                INSERT OR REPLACE INTO mixpanel_event 
                (event_uuid, event_name, abi_ad_id, abi_campaign_id, abi_ad_set_id, 
                 distinct_id, event_time, country, region, revenue_usd, 
                 raw_amount, currency, refund_flag, is_late_event, 
                 trial_expiration_at_calc, event_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            logger.debug(f"Using INSERT OR REPLACE for {len(valid_events)} refresh events")
        else:
            insert_sql = """
                INSERT OR IGNORE INTO mixpanel_event 
                (event_uuid, event_name, abi_ad_id, abi_campaign_id, abi_ad_set_id, 
                 distinct_id, event_time, country, region, revenue_usd, 
                 raw_amount, currency, refund_flag, is_late_event, 
                 trial_expiration_at_calc, event_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            logger.debug(f"Using INSERT OR IGNORE for {len(valid_events)} new events")
        
        cursor.executemany(insert_sql, valid_events)
    
    return len(valid_events)

def get_processed_dates(conn: sqlite3.Connection) -> Set[str]:
    """Get set of already processed dates"""
    cursor = conn.execute("SELECT date_day FROM processed_event_days")
    return {row[0] for row in cursor.fetchall()}

def mark_date_as_processed(cursor: sqlite3.Cursor, date_str: str, files_processed: int, events_ingested: int):
    """Mark date as processed with proper schema"""
    cursor.execute(
        """
        INSERT OR REPLACE INTO processed_event_days 
        (date_day, events_processed, processing_timestamp, status)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
        """,
        (date_str, events_ingested, 'complete')
    )

def verify_ingestion(conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Comprehensive verification of data ingestion"""
    logger.info("Verifying data ingestion...")
    
    cursor = conn.cursor()
    
    # Get counts
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
    user_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE valid_user = TRUE")
    valid_user_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_event")
    event_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
    user_products_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM processed_event_days")
    processed_dates_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(events_processed) FROM processed_event_days")
    total_events_processed = cursor.fetchone()[0] or 0
    
    # Verification checks
    checks = []
    
    if user_count == 0:
        checks.append("❌ No users found in database")
    else:
        checks.append(f"✅ {user_count} total users")
    
    if valid_user_count != user_count:
        checks.append(f"⚠️  Only {valid_user_count}/{user_count} users marked as valid")
    else:
        checks.append(f"✅ All {valid_user_count} users marked as valid")
    
    if event_count == 0:
        checks.append("❌ No events found in database")
    else:
        checks.append(f"✅ {event_count} total events")
    
    if total_events_processed != metrics.events_processed:
        checks.append(f"⚠️  Event count mismatch: processed {metrics.events_processed}, recorded {total_events_processed}")
    
    logger.info("Verification Results:")
    for check in checks:
        logger.info(f"  {check}")
    
    logger.info(f"Additional Details:")
    logger.info(f"  - User-product records: {user_products_count}")
    logger.info(f"  - Processed date entries: {processed_dates_count}")

def log_final_metrics(metrics: IngestionMetrics):
    """Log comprehensive final metrics"""
    logger.info("=== INGESTION METRICS ===")
    logger.info(f"Total Processing Time: {metrics.elapsed_time():.2f} seconds")
    logger.info(f"Files Processed: {metrics.files_processed}")
    logger.info(f"Dates Processed: {metrics.dates_processed}")
    logger.info(f"Users Processed: {metrics.users_processed}")
    logger.info(f"Users Filtered (@atly.com): {metrics.users_filtered_atly}")
    logger.info(f"Users Filtered (test): {metrics.users_filtered_test}")
    logger.info(f"Users Filtered (@steps.me): {metrics.users_filtered_steps}")
    logger.info(f"Events Processed: {metrics.events_processed}")
    logger.info(f"Events Skipped (unimportant): {metrics.events_skipped_unimportant}")
    logger.info(f"Events Skipped (invalid): {metrics.events_skipped_invalid}")
    logger.info(f"Events Skipped (missing users): {metrics.events_skipped_missing_users}")
    
    # Summary of data integrity
    if metrics.events_skipped_missing_users > 0:
        total_events_attempted = metrics.events_processed + metrics.events_skipped_missing_users + metrics.events_skipped_unimportant + metrics.events_skipped_invalid
        integrity_rate = (metrics.events_processed / total_events_attempted) * 100 if total_events_attempted > 0 else 0
        logger.info(f"Data Integrity: {integrity_rate:.1f}% of events successfully linked to existing users")
    
    if metrics.events_processed > 0:
        events_per_second = metrics.events_processed / metrics.elapsed_time()
        logger.info(f"Processing Rate: {events_per_second:.2f} events/second")

if __name__ == "__main__":
    sys.exit(main()) 