#!/usr/bin/env python3
"""
TEST VERSION: Module 3: Data Ingestion
Simplified version for testing the database migration from raw data to processed data.
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
    """Main ingestion orchestrator - TEST VERSION"""
    metrics = IngestionMetrics()
    metrics.start_time = now_in_timezone()
    
    try:
        print("🧪 === TEST VERSION: Module 3: Data Ingestion ===")
        logger.info("🧪 Starting test data ingestion pipeline...")
        
        # Validate prerequisites
        validate_prerequisites()
        
        # Create database connections
        raw_data_conn, raw_db_type = get_raw_data_connection()
        sqlite_conn = create_sqlite_connection()
        
        try:
            # Show what we have in raw data
            show_raw_data_summary(raw_data_conn, raw_db_type)
            
            # Validate database schema before proceeding
            validate_database_schema(sqlite_conn)
            
            # Process all data with comprehensive error handling
            process_all_data(raw_data_conn, raw_db_type, sqlite_conn, metrics)
            
            # Show final results
            show_processed_data_summary(sqlite_conn)
            
        finally:
            raw_data_conn.close()
            sqlite_conn.close()
        
        metrics.end_time = now_in_timezone()
        log_final_metrics(metrics)
        
        print("🎉 Test data ingestion completed successfully")
        logger.info("🧪 Test data ingestion completed successfully")
        return 0
        
    except Exception as e:
        metrics.end_time = now_in_timezone()
        logger.error(f"🧪 Test Module 3 failed: {e}")
        logger.error(f"Elapsed time: {metrics.elapsed_time():.2f} seconds")
        print(f"❌ Test Module 3 failed: {e}", file=sys.stderr)
        return 1

def show_raw_data_summary(conn, db_type):
    """Show summary of raw data available for processing"""
    cursor = conn.cursor()
    
    print(f"\n📊 === RAW DATA SUMMARY ({db_type.upper()}) ===")
    
    # Count users
    cursor.execute("SELECT COUNT(*) FROM raw_user_data")
    user_count = cursor.fetchone()[0]
    print(f"👥 Raw users available: {user_count}")
    
    # Count events by date
    cursor.execute("SELECT date_day, events_downloaded FROM downloaded_dates ORDER BY date_day")
    date_results = cursor.fetchall()
    
    if date_results:
        print(f"📅 Raw event data by date:")
        total_events = 0
        for date_day, events in date_results:
            print(f"  - {date_day}: {events} events")
            total_events += events
        print(f"📊 Total events available: {total_events}")
    else:
        print(f"📅 No raw event dates found")
    
    # Count raw events
    cursor.execute("SELECT COUNT(*) FROM raw_event_data")
    raw_event_count = cursor.fetchone()[0]
    print(f"🗃️  Raw events in database: {raw_event_count}")

def show_processed_data_summary(conn):
    """Show summary of processed data in SQLite"""
    cursor = conn.cursor()
    
    print(f"\n📊 === PROCESSED DATA SUMMARY (SQLITE) ===")
    
    # Count users
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
    user_count = cursor.fetchone()[0]
    print(f"👥 Processed users: {user_count}")
    
    # Count valid users
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE valid_user = TRUE")
    valid_user_count = cursor.fetchone()[0]
    print(f"✅ Valid users: {valid_user_count}")
    
    # Count events
    cursor.execute("SELECT COUNT(*) FROM mixpanel_event")
    event_count = cursor.fetchone()[0]
    print(f"📊 Processed events: {event_count}")
    
    # Count processed dates
    cursor.execute("SELECT COUNT(*) FROM processed_event_days")
    processed_dates_count = cursor.fetchone()[0]
    print(f"📅 Processed dates: {processed_dates_count}")
    
    if processed_dates_count > 0:
        cursor.execute("SELECT date_day, events_processed FROM processed_event_days ORDER BY date_day")
        for date_day, events in cursor.fetchall():
            print(f"  - {date_day}: {events} events")

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
    
    conn = sqlite3.connect(str(DATABASE_PATH), timeout=60.0, check_same_thread=False)
    
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
    
    logger.info("Database schema validation passed")

def process_all_data(raw_data_conn, raw_db_type: str, sqlite_conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process all user and event data with comprehensive error handling"""
    
    # Step 1: Refresh all users
    logger.info("=== Step 1: Refreshing User Data ===")
    refresh_all_users(raw_data_conn, raw_db_type, sqlite_conn, metrics)
    
    # Step 2: Process events incrementally  
    logger.info("=== Step 2: Processing Events Incrementally ===")
    process_events_incrementally(raw_data_conn, raw_db_type, sqlite_conn, metrics)

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
        batch_size = 1000
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
                user_data = json.loads(user_data_json)
                
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
                if len(user_batch) >= 1000:
                    process_user_batch(sqlite_cursor, user_batch)
                    user_batch.clear()
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON for user {distinct_id}")
            except Exception as e:
                logger.error(f"Error processing user {distinct_id}: {e}")
        
        # Process remaining batch
        if user_batch:
            process_user_batch(sqlite_cursor, user_batch)
        
        return batch_metrics
        
    except Exception as e:
        logger.error(f"Failed to process user batch from raw data: {e}")
        raise

def process_events_incrementally(raw_data_conn, raw_db_type: str, sqlite_conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process events incrementally by date from raw_event_data table"""
    
    # Get already processed dates from SQLite
    processed_dates = get_processed_dates(sqlite_conn)
    logger.info(f"Found {len(processed_dates)} already processed dates in SQLite")
    
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
        unprocessed_dates = [date for date in available_dates if date.strftime('%Y-%m-%d') not in processed_dates]
    else:
        # SQLite stores dates as strings, so we need to parse them
        unprocessed_dates = []
        for date_str in available_dates:
            try:
                if isinstance(date_str, str):
                    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                else:
                    date_obj = date_str
                
                if date_obj.strftime('%Y-%m-%d') not in processed_dates:
                    unprocessed_dates.append(date_obj)
            except ValueError:
                logger.warning(f"Invalid date format in raw data: {date_str}")
    
    logger.info(f"Found {len(unprocessed_dates)} dates to process from {raw_db_type}")
    if not unprocessed_dates:
        logger.info("No new event dates to process")
        return
    
    # Process each date
    for date_obj in unprocessed_dates:
        date_str = date_obj.strftime('%Y-%m-%d')
        logger.info(f"Processing events for date {date_str}")
        
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
                    event_data = json.loads(event_data_json)
                    event_name = event_data.get('event')
                    
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
                    if len(event_batch) >= 1000:
                        events_inserted = insert_event_batch(sqlite_cursor, event_batch, metrics)
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
                events_inserted = insert_event_batch(sqlite_cursor, event_batch, metrics)
                date_events += events_inserted
            
            # Mark date as processed
            mark_date_as_processed(sqlite_cursor, date_str, 1, date_events)
            
            sqlite_cursor.execute("COMMIT")
            
            metrics.dates_processed += 1
            metrics.events_processed += date_events
            
            logger.info(f"Processed {date_events} events for date {date_str}")
            
        except Exception as e:
            sqlite_cursor.execute("ROLLBACK")
            logger.error(f"Failed to process date {date_str}: {e}")
            raise

# Helper functions (keeping the most important ones)
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
        'profile_json': json.dumps(user_data),
        'first_seen': first_seen_dt.isoformat(),
        'last_updated': last_updated_dt.isoformat(),
        'valid_user': True,
        'economic_tier': None
    }

def process_user_batch(cursor: sqlite3.Cursor, user_batch: List[Dict]):
    """Process user batch with proper column mapping"""
    
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

def prepare_event_record(event_data: Dict[str, Any], file_path: str, line_num: int) -> Optional[Tuple]:
    """Prepare event record with validation and ALL required fields"""
    try:
        properties = event_data.get('properties', {})
        
        # Extract required fields - based on actual data structure
        # Events only have $insert_id in properties (not at top level)
        event_uuid = properties.get('$insert_id')
        # Events only have distinct_id in properties (not at top level)
        distinct_id = properties.get('distinct_id')
        # Events have event name at top level
        event_name = event_data.get('event')
        
        # Validate required fields
        if not distinct_id:
            return None
        
        # Generate fallback UUID if needed
        if not event_uuid:
            # Time is only in properties
            timestamp = properties.get('time', 0)
            event_uuid = f"{distinct_id}_{event_name}_{timestamp}_{hash(str(properties))}"
        
        # Parse timestamp with proper UTC handling - time is only in properties
        try:
            timestamp = properties.get('time', 0)
            if isinstance(timestamp, str):
                timestamp = float(timestamp)
            event_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        except (ValueError, TypeError, OSError):
            event_time = datetime.datetime.now(tz=datetime.timezone.utc)
        
        # Extract attribution data
        abi_ad_id = None
        abi_campaign_id = None
        abi_ad_set_id = None
        
        # Check subscriber_attributes for attribution
        subscriber_attrs = properties.get('subscriber_attributes', {})
        if subscriber_attrs:
            for key, value in subscriber_attrs.items():
                if 'ad_id' in key.lower():
                    abi_ad_id = value
                elif 'campaign_id' in key.lower():
                    abi_campaign_id = value
                elif 'adset_id' in key.lower() or 'ad_set_id' in key.lower():
                    abi_ad_set_id = value
        
        # Check direct properties for attribution
        for key, value in properties.items():
            if key == 'abi_ad_id':
                abi_ad_id = value
            elif key == 'abi_campaign_id':
                abi_campaign_id = value
            elif key == 'abi_ad_set_id':
                abi_ad_set_id = value
        
        # Parse revenue safely
        try:
            revenue = float(properties.get('revenue', 0)) if properties.get('revenue') else 0
        except (ValueError, TypeError):
            revenue = 0
        
        currency = properties.get('currency', 'USD') or 'USD'
        
        # Extract additional fields
        refund_flag = False
        is_late_event = False
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
            None,  # country
            None,  # region
            revenue,
            revenue,  # raw_amount same as revenue for now
            currency,
            refund_flag,
            is_late_event,
            trial_expiration_at_calc.isoformat() if trial_expiration_at_calc else None,
            json.dumps(event_data)
        )
        
    except Exception as e:
        logger.error(f"Error preparing event record {file_path}:{line_num}: {e}")
        return None

def insert_event_batch(cursor: sqlite3.Cursor, event_batch: List[Tuple], metrics: IngestionMetrics) -> int:
    """Insert event batch with user existence validation and error handling"""
    
    # Filter events to only include those with existing users
    valid_events = []
    skipped_events = 0
    
    # Get all distinct_ids from the batch
    batch_distinct_ids = {event[5] for event in event_batch}  # distinct_id is at index 5
    
    # Check which distinct_ids exist in the user table
    placeholders = ','.join('?' * len(batch_distinct_ids))
    cursor.execute(
        f"SELECT distinct_id FROM mixpanel_user WHERE distinct_id IN ({placeholders})",
        list(batch_distinct_ids)
    )
    existing_distinct_ids = {row[0] for row in cursor.fetchall()}
    
    # Filter events to only include those with existing users
    for event in event_batch:
        distinct_id = event[5]  # distinct_id is at index 5
        if distinct_id in existing_distinct_ids:
            valid_events.append(event)
        else:
            skipped_events += 1
    
    # Track skipped events in metrics
    if skipped_events > 0:
        metrics.events_skipped_missing_users += skipped_events
    
    # Insert only valid events
    if valid_events:
        cursor.executemany(
            """
            INSERT OR IGNORE INTO mixpanel_event 
            (event_uuid, event_name, abi_ad_id, abi_campaign_id, abi_ad_set_id, 
             distinct_id, event_time, country, region, revenue_usd, 
             raw_amount, currency, refund_flag, is_late_event, 
             trial_expiration_at_calc, event_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            valid_events
        )
    
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

def log_final_metrics(metrics: IngestionMetrics):
    """Log comprehensive final metrics"""
    print(f"\n🧪 === TEST INGESTION METRICS ===")
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
    
    if metrics.events_processed > 0:
        events_per_second = metrics.events_processed / metrics.elapsed_time()
        print(f"⚡ Processing Rate: {events_per_second:.2f} events/second")

if __name__ == "__main__":
    sys.exit(main()) 