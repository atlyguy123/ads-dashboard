#!/usr/bin/env python3
"""
Module 3: Data Ingestion - Production Ready

This module ingests downloaded Mixpanel data into the database with:
- Robust error handling and retry mechanisms
- Memory-efficient streaming processing
- Comprehensive data validation and filtering
- Production-grade optimizations and monitoring
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
from contextlib import contextmanager

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - Use centralized database path discovery
DATABASE_PATH = Path(get_database_path('mixpanel_data'))
# Find project root for data paths  
script_dir = Path(__file__).parent
project_root = DATABASE_PATH.parent.parent  # database is in project root, so go up one more level
EVENTS_BASE_PATH = project_root / "data" / "events"
USER_FILES_PATH = project_root / "data" / "users"

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
BATCH_SIZE = 1000
MAX_MEMORY_USAGE = 100_000
CONNECTION_TIMEOUT = 60.0
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0

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
    metrics.start_time = datetime.datetime.now()
    
    try:
        logger.info("=== Module 3: Data Ingestion ===")
        logger.info("Starting production data ingestion pipeline...")
        
        # Validate prerequisites
        validate_prerequisites()
        
        # Create database connection with optimization
        with create_database_connection() as conn:
            # Validate database schema before proceeding
            validate_database_schema(conn)
            
            # Process all data with comprehensive error handling
            process_all_data(conn, metrics)
            
            # Verify data integrity
            verify_ingestion(conn, metrics)
        
        metrics.end_time = datetime.datetime.now()
        log_final_metrics(metrics)
        
        logger.info("Data ingestion completed successfully")
        return 0
        
    except Exception as e:
        metrics.end_time = datetime.datetime.now()
        logger.error(f"Module 3 failed: {e}")
        logger.error(f"Elapsed time: {metrics.elapsed_time():.2f} seconds")
        print(f"Module 3 failed: {e}", file=sys.stderr)
        return 1

def validate_prerequisites():
    """Validate all prerequisites before starting ingestion"""
    logger.info("Validating prerequisites...")
    
    # Check database exists
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DATABASE_PATH}. Run Module 1 first.")
    
    # Check data directories exist
    if not USER_FILES_PATH.exists():
        raise FileNotFoundError(f"User data directory not found: {USER_FILES_PATH}")
    
    if not EVENTS_BASE_PATH.exists():
        logger.warning(f"Events directory not found: {EVENTS_BASE_PATH}")
    
    logger.info("Prerequisites validated successfully")

@contextmanager
def create_database_connection():
    """Create optimized database connection with proper resource management"""
    logger.info(f"Connecting to database: {DATABASE_PATH}")
    
    conn = None
    try:
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
        
        logger.info("Database connection established with optimizations")
        yield conn
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

def validate_database_schema(conn: sqlite3.Connection):
    """Validate database has the required schema from Module 1"""
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
            raise RuntimeError(f"Required table '{table}' not found. Run Module 1 first.")
    
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
        raise RuntimeError("processed_event_days table not migrated to new schema. Run Module 1 first.")
    
    logger.info("Database schema validation passed")

def process_all_data(conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process all user and event data with comprehensive error handling"""
    
    # Step 1: Refresh all users
    logger.info("=== Step 1: Refreshing User Data ===")
    refresh_all_users(conn, metrics)
    
    # Step 2: Process events incrementally  
    logger.info("=== Step 2: Processing Events Incrementally ===")
    process_events_incrementally(conn, metrics)

def refresh_all_users(conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process users - table should already be empty after database setup"""
    cursor = conn.cursor()
    
    try:
        logger.info("Processing user data...")
        cursor.execute("BEGIN IMMEDIATE")
        
        # Process all user files
        user_files = list(USER_FILES_PATH.glob("*.json"))
        logger.info(f"Found {len(user_files)} user files to process")
        
        if not user_files:
            logger.warning("No user files found for processing")
            cursor.execute("COMMIT")
            return
        
        for file_path in user_files:
            logger.info(f"Processing user file: {file_path.name}")
            try:
                file_metrics = process_user_file(conn, file_path)
                metrics.users_processed += file_metrics['users_processed']
                metrics.users_filtered_atly += file_metrics['users_filtered_atly']
                metrics.users_filtered_test += file_metrics['users_filtered_test']
                metrics.users_filtered_steps += file_metrics['users_filtered_steps']
                metrics.files_processed += 1
                
                logger.info(f"Processed {file_metrics['users_processed']} users from {file_path.name}")
                
            except Exception as e:
                logger.error(f"Failed to process user file {file_path.name}: {e}")
                raise
        
        cursor.execute("COMMIT")
        logger.info(f"User processing completed: {metrics.users_processed} users processed")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"User processing failed: {e}")
        raise



def process_user_file(conn: sqlite3.Connection, file_path: Path) -> Dict[str, int]:
    """Process users from a file with comprehensive validation and error handling"""
    cursor = conn.cursor()
    
    file_metrics = {
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
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    line = line.strip()
                    if not line:
                        continue
                        
                    user_data = json.loads(line)
                    
                    # Extract and validate distinct_id
                    distinct_id = user_data.get('mp_distinct_id') or user_data.get('abi_distinct_id')
                    if not distinct_id or not isinstance(distinct_id, str):
                        invalid_distinct_id_count += 1
                        continue
                    
                    # Apply user filtering logic
                    email = user_data.get('email2') or user_data.get('mp_email', '')
                    filter_result = should_filter_user(distinct_id, email)
                    
                    if filter_result['filter']:
                        if filter_result['reason'] == 'atly':
                            file_metrics['users_filtered_atly'] += 1
                            if len(atly_examples) < 3:
                                atly_examples.append((distinct_id, email))
                        elif filter_result['reason'] == 'test':
                            file_metrics['users_filtered_test'] += 1
                            if len(test_examples) < 3:
                                test_examples.append((distinct_id, email))
                        elif filter_result['reason'] == 'steps':
                            file_metrics['users_filtered_steps'] += 1
                            if len(steps_examples) < 3:
                                steps_examples.append((distinct_id, email))
                        continue
                    
                    # Prepare user record
                    user_record = prepare_user_record(user_data, distinct_id)
                    user_batch.append(user_record)
                    
                    file_metrics['users_processed'] += 1
                    
                    # Process in batches for memory management
                    if len(user_batch) >= BATCH_SIZE:
                        process_user_batch(cursor, user_batch)
                        user_batch.clear()
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {file_path.name}:{line_num}")
                except Exception as e:
                    logger.error(f"Error processing line {line_num} in {file_path.name}: {e}")
        
        # Process remaining batch
        if user_batch:
            process_user_batch(cursor, user_batch)
        
        # Log filtering examples and invalid record summary
        log_user_filtering_examples(file_path.name, atly_examples, test_examples, steps_examples, file_metrics)
        
        # Log summary of invalid distinct_id records
        if invalid_distinct_id_count > 0:
            logger.info(f"Skipped {invalid_distinct_id_count} records with invalid distinct_id in {file_path.name}")
            total_records = file_metrics['users_processed'] + file_metrics['users_filtered_atly'] + file_metrics['users_filtered_test'] + file_metrics['users_filtered_steps'] + invalid_distinct_id_count
            invalid_percentage = (invalid_distinct_id_count / total_records * 100) if total_records > 0 else 0
            logger.info(f"Invalid distinct_id rate: {invalid_percentage:.1f}% of total records")
        
        return file_metrics
        
    except Exception as e:
        logger.error(f"Failed to process user file {file_path}: {e}")
        raise

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
    
    # Extract attribution data (use direct values, not hash)
    abi_ad_id = user_data.get('abi_ad_id')
    abi_campaign_id = user_data.get('abi_campaign_id')
    abi_ad_set_id = user_data.get('abi_ad_set_id')
    
    # Validate attribution data extraction
    if abi_ad_id and not abi_campaign_id:
        logger.warning(f"User {distinct_id} has abi_ad_id but missing abi_campaign_id")
    if abi_ad_id and not abi_ad_set_id:
        logger.warning(f"User {distinct_id} has abi_ad_id but missing abi_ad_set_id")
    
    # Extract location data
    country = user_data.get('mp_country_code')
    region = user_data.get('mp_region')
    city = user_data.get('mp_city')
    
    # Determine attribution status
    has_abi_attribution = bool(abi_ad_id)
    
    # Extract timestamps
    first_seen = user_data.get('first_install_date') or user_data.get('mp_ae_first_app_open_date')
    if first_seen:
        try:
            # Parse ISO format timestamp
            first_seen_dt = datetime.datetime.fromisoformat(first_seen.replace('Z', '+00:00'))
        except:
            first_seen_dt = datetime.datetime.now()
    else:
        first_seen_dt = datetime.datetime.now()
    
    last_updated = user_data.get('mp_last_seen')
    if last_updated:
        try:
            last_updated_dt = datetime.datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
        except:
            last_updated_dt = datetime.datetime.now()
    else:
        last_updated_dt = datetime.datetime.now()
    
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

def process_events_incrementally(conn: sqlite3.Connection, metrics: IngestionMetrics):
    """Process events incrementally by date with comprehensive error handling"""
    
    # Get already processed dates
    processed_dates = get_processed_dates(conn)
    logger.info(f"Found {len(processed_dates)} already processed dates")
    
    # Find unprocessed event files
    date_files = find_event_files(EVENTS_BASE_PATH, processed_dates)
    unprocessed_dates = sorted(date_files.keys())
    
    logger.info(f"Found {len(unprocessed_dates)} dates to process")
    if not unprocessed_dates:
        logger.info("No new event dates to process")
        return
    
    # Process each date
    for date_str in unprocessed_dates:
        files = date_files[date_str]
        logger.debug(f"Processing {len(files)} files for date {date_str}")
        
        date_events = 0
        try:
            # Process all files for this date in transaction
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            
            for file_path in files:
                logger.debug(f"Processing event file: {file_path}")
                file_events = process_event_file(cursor, file_path, metrics)
                date_events += file_events
                
            # Mark date as processed
            mark_date_as_processed(cursor, date_str, len(files), date_events)
            
            cursor.execute("COMMIT")
            
            metrics.dates_processed += 1
            metrics.files_processed += len(files)
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Failed to process date {date_str}: {e}")
            raise
    
    # Log summary of event processing
    if metrics.dates_processed > 0:
        logger.info(f"Event processing completed: {metrics.dates_processed} dates, {metrics.events_processed} events ingested")

def process_event_file(cursor: sqlite3.Cursor, file_path: str, metrics: IngestionMetrics) -> int:
    """Process events from a file with validation and error handling"""
    events_processed = 0
    event_batch = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    line = line.strip()
                    if not line:
                        continue
                        
                    event_data = json.loads(line)
                    event_name = event_data.get('event')
                    
                    # Skip unimportant events
                    if event_name not in IMPORTANT_EVENTS:
                        metrics.events_skipped_unimportant += 1
                        continue
                    
                    # Extract and validate event data
                    event_record = prepare_event_record(event_data, file_path, line_num)
                    if not event_record:
                        metrics.events_skipped_invalid += 1
                        continue
                    
                    event_batch.append(event_record)
                    
                    # Process in batches
                    if len(event_batch) >= BATCH_SIZE:
                        insert_event_batch(cursor, event_batch, metrics)
                        events_processed += len(event_batch)
                        event_batch.clear()
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {file_path}:{line_num}")
                    metrics.events_skipped_invalid += 1
                except Exception as e:
                    logger.error(f"Error processing event {file_path}:{line_num}: {e}")
                    metrics.events_skipped_invalid += 1
        
        # Process remaining batch
        if event_batch:
            insert_event_batch(cursor, event_batch, metrics)
            events_processed += len(event_batch)
        
        metrics.events_processed += events_processed
        return events_processed
        
    except Exception as e:
        logger.error(f"Failed to process event file {file_path}: {e}")
        raise

def prepare_event_record(event_data: Dict[str, Any], file_path: str, line_num: int) -> Optional[Tuple]:
    """Prepare event record with validation and ALL required fields"""
    try:
        properties = event_data.get('properties', {})
        
        # Extract required fields
        event_uuid = properties.get('$insert_id')
        distinct_id = properties.get('distinct_id')
        event_name = event_data.get('event')
        
        # Validate required fields
        if not distinct_id:
            logger.warning(f"Missing distinct_id in {file_path}:{line_num}")
            return None
        
        # Generate fallback UUID
        if not event_uuid:
            timestamp = properties.get('time', 0)
            event_uuid = f"{distinct_id}_{event_name}_{timestamp}_{hash(str(properties))}"
        
        # Parse timestamp with proper UTC handling
        try:
            timestamp = properties.get('time', 0)
            if isinstance(timestamp, str):
                timestamp = float(timestamp)
            event_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        except (ValueError, TypeError, OSError):
            logger.warning(f"Invalid timestamp in {file_path}:{line_num}")
            event_time = datetime.datetime.now(tz=datetime.timezone.utc)
        
        # Extract attribution data - check multiple possible locations
        abi_ad_id = None
        abi_campaign_id = None
        abi_ad_set_id = None
        
        # Check subscriber_attributes for attribution
        subscriber_attrs = properties.get('subscriber_attributes', {})
        if subscriber_attrs:
            # Look for attribution fields in subscriber_attributes
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
        
        # Extract geographic data from IP
        country = None
        region = None
        ip_address = subscriber_attrs.get('$ip') or properties.get('ip')
        if ip_address:
            # For now, we'll leave country/region as None
            # In production, you'd use a geolocation service here
            # country, region = lookup_location_from_ip(ip_address)
            pass
        
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
            json.dumps(event_data)
        )
        
    except Exception as e:
        logger.error(f"Error preparing event record {file_path}:{line_num}: {e}")
        return None

def insert_event_batch(cursor: sqlite3.Cursor, event_batch: List[Tuple], metrics: IngestionMetrics):
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
    
    # Track skipped events in metrics (no longer log per batch)
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

def find_event_files(base_path: Path, processed_dates: Set[str]) -> Dict[str, List[str]]:
    """Find event files grouped by date, excluding processed dates"""
    date_files = {}
    
    if not base_path.exists():
        logger.warning(f"Events path {base_path} not found")
        return date_files
    
    for file_path in base_path.rglob("*.json"):
        date_str = extract_date_from_path(str(file_path))
        
        if date_str and date_str not in processed_dates:
            if date_str not in date_files:
                date_files[date_str] = []
            date_files[date_str].append(str(file_path))
    
    return date_files

def extract_date_from_path(file_path: str) -> Optional[str]:
    """Extract date from file path in YYYY-MM-DD format"""
    pattern = r'data/events/(\d{4})/(\d{2})/(\d{2})/'
    match = re.search(pattern, file_path)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None

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
    sys.exit(main()) 