#!/usr/bin/env python3
"""
Module 1: Download/Update Data
Checks what data exists and downloads any missing data.
Ensures data is present and up to date through yesterday.
Now stores data directly in database instead of filesystem.
Supports both SQLite (local) and PostgreSQL (production).
"""
import os
import sys
import boto3
import gzip
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Try to import psycopg2, but don't fail if not available (for local SQLite testing)
try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    psycopg2 = None

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# FIXED: Load environment variables from project root (same fix as meta_service.py)
project_root = Path(__file__).resolve().parent.parent.parent
env_file = project_root / '.env'
load_dotenv(env_file)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Credentials and S3 Configuration - loaded from environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
S3_BUCKET_EVENTS = os.environ.get('S3_BUCKET_EVENTS')
S3_BUCKET_USERS = os.environ.get('S3_BUCKET_USERS')
PROJECT_ID = os.environ.get('PROJECT_ID')

# Database configuration - use Heroku Postgres if available, otherwise SQLite
DATABASE_URL = os.environ.get('DATABASE_URL')
USE_POSTGRES = DATABASE_URL is not None and HAS_POSTGRES

# List of event names to keep
EVENTS_TO_KEEP = [
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal",
    "RC Expiration"
]

# DEBUG MODE: Set to True to save JSON files locally for verification
# PERFORMANCE: Disabled by default for maximum speed
DEBUG_SAVE_JSON_FILES = os.environ.get('DEBUG_SAVE_JSON_FILES', 'False').lower() == 'true'
DEBUG_JSON_OUTPUT_DIR = Path("data/events")

def get_database_connection():
    """Get connection to database (PostgreSQL if available, otherwise SQLite)"""
    if USE_POSTGRES:
        logger.info("Using PostgreSQL database")
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
        logger.info("Using SQLite database (local mode)")
        # Use centralized database path discovery for raw_data.db
        db_path = Path(get_database_path('raw_data'))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(str(db_path))
        return conn, 'sqlite'

def ensure_raw_data_tables(conn, db_type):
    """Create tables to store raw downloaded data"""
    cursor = conn.cursor()
    
    if db_type == 'postgres':
        # PostgreSQL version with JSONB
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_event_data (
                id SERIAL PRIMARY KEY,
                date_day DATE NOT NULL,
                file_sequence INTEGER NOT NULL,
                event_data JSONB NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_user_data (
                distinct_id TEXT PRIMARY KEY,
                user_data JSONB NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_dates (
                date_day DATE PRIMARY KEY,
                files_downloaded INTEGER DEFAULT 0,
                events_downloaded INTEGER DEFAULT 0,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_event_data_date ON raw_event_data(date_day)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_event_data_event ON raw_event_data((event_data->>'event'))")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_user_data_downloaded ON raw_user_data(downloaded_at)")
        
    else:
        # SQLite version with TEXT JSON (no JSON expressions in PRIMARY KEY)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_event_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_day DATE NOT NULL,
                file_sequence INTEGER NOT NULL,
                event_data TEXT NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_user_data (
                distinct_id TEXT PRIMARY KEY,
                user_data TEXT NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_dates (
                date_day DATE PRIMARY KEY,
                files_downloaded INTEGER DEFAULT 0,
                events_downloaded INTEGER DEFAULT 0,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_event_data_date ON raw_event_data(date_day)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_event_data_event ON raw_event_data(json_extract(event_data, '$.event'))")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_user_data_downloaded ON raw_user_data(downloaded_at)")
    
    conn.commit()
    logger.info(f"Raw data tables ensured in {db_type} database")

def show_database_summary(conn, db_type):
    """Show summary of data in the database"""
    cursor = conn.cursor()
    
    try:
        # Count users
        cursor.execute("SELECT COUNT(*) FROM raw_user_data")
        user_count = cursor.fetchone()[0]
        
        # Count events by date (limit to recent dates for readability)
        cursor.execute("""
            SELECT date_day, events_downloaded 
            FROM downloaded_dates 
            WHERE events_downloaded > 0 
            ORDER BY date_day DESC 
            LIMIT 15
        """)
        recent_dates = cursor.fetchall()
        
        # Total events
        cursor.execute("SELECT SUM(events_downloaded) FROM downloaded_dates WHERE events_downloaded > 0")
        total_events = cursor.fetchone()[0] or 0
        
        # Count raw events in database
        cursor.execute("SELECT COUNT(*) FROM raw_event_data")
        raw_events = cursor.fetchone()[0]
        
        print(f"\n📊 === DATABASE SUMMARY ({db_type.upper()}) ===")
        print(f"👥 Users: {user_count:,}")
        print(f"📅 Event data by date:")
        for date_day, event_count in recent_dates:
            print(f"  - {date_day}: {event_count} events")
        print(f"📊 Total events: {total_events:,}")
        print(f"🗃️  Raw events stored: {raw_events:,}")
        
    except Exception as e:
        logger.error(f"Error generating database summary: {e}")

def main():
    try:
        print("=== Module 1: Download/Update Data ===")
        print(f"Starting data download and update process...")
        logger.info("=== MODULE 1 STARTED: Download/Update Data ===")
        logger.info("Initializing download and update process...")
        
        # Validate required environment variables
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_EVENTS', 'S3_BUCKET_USERS', 'PROJECT_ID']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Connect to database
        logger.info("Connecting to database...")
        conn, db_type = get_database_connection()
        
        # Ensure raw data tables exist
        ensure_raw_data_tables(conn, db_type)
        
        # Check existing data coverage
        logger.info("Scanning existing data to find latest date...")
        latest_date = find_latest_data_date(conn)
        if latest_date:
            print(f"Latest data found: {latest_date}")
            logger.info(f"✓ Found existing data up to: {latest_date}")
        else:
            print(f"Latest data found: None (no existing data)")
            logger.info("ℹ No existing data found - will download from scratch")
        
        # Determine what data needs to be downloaded
        logger.info("Calculating missing dates...")
        missing_dates, refresh_dates_set = identify_missing_data(conn, latest_date)
        print(f"Missing data for {len(missing_dates)} days")
        
        if missing_dates:
            print("⬇️  Downloading missing data...")
            logger.info("Starting download process for missing data...")
            success = download_missing_data(conn, db_type, missing_dates, refresh_dates_set)
            if not success:
                print("Some downloads failed, but continuing...")
                logger.warning("Some downloads failed, but process completed")
            else:
                logger.info("✅ All downloads completed successfully")
        else:
            print("✅ All required data is already present")
            logger.info("✅ No downloads needed - all data is current")
            
            # Still need to download user data even when no event data is missing
            print("Downloading user data...")
            logger.info("Starting user data download (always refresh user data)...")
            s3_client = get_s3_client()
            user_success = download_user_data(conn, db_type, s3_client)
            if user_success:
                logger.info("✓ User data download completed successfully")
            else:
                logger.warning("User data download failed, but continuing...")
        
        # Show database summary
        show_database_summary(conn, db_type)
        
        conn.close()
        print("🎉 Data download and update completed successfully")
        logger.info("=== MODULE 1 COMPLETED SUCCESSFULLY ===")
        return 0
        
    except Exception as e:
        print(f"Module 1 failed: {e}", file=sys.stderr)
        logger.error(f"=== MODULE 1 FAILED ===")
        logger.error(f"Error: {e}")
        logger.exception("Full error details:")
        return 1

def find_latest_data_date(conn):
    """Find the latest date for which we have event data in database"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date_day) FROM downloaded_dates")
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def identify_missing_data(conn, latest_date):
    """
    Identify which dates need data downloads using two requirements:
    1. Check last 90 days for missing data (gap filling)
    2. Always re-download last 3 days (refresh requirement)
    """
    today = now_in_timezone().date()
    
    # REQUIREMENT 1: Check last 90 days for missing data (gap filling)
    start_date = today - timedelta(days=89)  # 90 days total including today
    logger.info(f"📅 GAP FILLING: Checking last 90 days ({start_date} to {today}) for missing data")
    
    missing_dates = []
    gap_fill_dates = []
    current_date = start_date
    dates_checked = 0
    dates_found = 0
    
    cursor = conn.cursor()
    
    while current_date <= today:
        dates_checked += 1
        # Check if this date has data in database
        if USE_POSTGRES:
            cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = %s", (current_date,))
        else:
            cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = ?", (current_date,))
        result = cursor.fetchone()
        
        if not result or result[0] == 0:
            gap_fill_dates.append(current_date)
            logger.info(f"🔍 Missing data: {current_date}")
        else:
            dates_found += 1
            logger.info(f"✅ Found data: {current_date} ({result[0]} events)")
        
        current_date += timedelta(days=1)
    
    logger.info(f"📊 Checked {dates_checked} dates: {dates_found} have data, {len(gap_fill_dates)} are missing")
    
    # REQUIREMENT 2: Always re-download last 3 days (refresh requirement)
    refresh_start = today - timedelta(days=2)  # Last 3 days including today
    refresh_dates = []
    current_date = refresh_start
    
    logger.info(f"🔄 REFRESH REQUIREMENT: Always re-downloading last 3 days ({refresh_start} to {today})")
    
    while current_date <= today:
        refresh_dates.append(current_date)
        logger.info(f"🔄 Refresh required: {current_date}")
        current_date += timedelta(days=1)
    
    # COMBINE: Gap filling + refresh requirement (remove duplicates)
    missing_dates = list(set(gap_fill_dates + refresh_dates))
    missing_dates.sort()
    
    # CRITICAL: Store refresh dates separately for download logic
    # This allows download_events_for_date to know when to force re-download
    refresh_dates_set = set(refresh_dates)
    
    logger.info(f"📊 SUMMARY:")
    logger.info(f"  - Gap filling: {len(gap_fill_dates)} missing dates")
    logger.info(f"  - Refresh requirement: {len(refresh_dates)} dates (last 3 days)")
    logger.info(f"  - Total downloads needed: {len(missing_dates)} dates")
    
    print(f"📊 Missing data for {len(gap_fill_dates)} days (out of {dates_checked} checked)")
    print(f"🔄 Plus {len(refresh_dates)} days for refresh requirement")
    print(f"📥 Total downloads needed: {len(missing_dates)} dates")
    
    if len(missing_dates) > 0:
        logger.info(f"🔍 Will download {len(missing_dates)} dates:")
        for date in missing_dates:
            is_refresh = date in refresh_dates_set
            logger.info(f"  - {date} {'(REFRESH)' if is_refresh else '(GAP FILL)'}")
    
    return missing_dates, refresh_dates_set

def get_s3_client():
    """Initializes and returns an S3 client."""
    logger.info("Initializing S3 client...")
    try:
        client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME
        )
        logger.info("S3 client initialized successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

def list_s3_objects(s3_client, bucket_name, prefix=''):
    """Lists objects in an S3 bucket with a given prefix, handling pagination."""
    logger.debug(f"Listing objects in bucket: {bucket_name}, prefix: {prefix}")
    paginator = s3_client.get_paginator('list_objects_v2')
    object_keys = []
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj.get('Size', 0) > 0 and not obj['Key'].endswith('/'):
                        object_keys.append(obj['Key'])
        logger.debug(f"Found {len(object_keys)} objects for prefix {prefix} in bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error listing S3 objects for bucket {bucket_name}, prefix {prefix}: {e}")
    return object_keys

def download_and_store_event_file(conn, db_type, s3_client, bucket_name, object_key, target_date, file_sequence):
    """
    Downloads an event .json.gz file from S3, decompresses it,
    filters events by name, and stores in database with optimized bulk processing.
    
    In DEBUG mode, also saves filtered events to JSON files for verification.
    """
    cursor = conn.cursor()
    BATCH_SIZE = 25000  # Optimized batch size for events
    
    # Prepare debug JSON file if in debug mode
    debug_json_file = None
    if DEBUG_SAVE_JSON_FILES:
        # Create directory structure: data/events/YYYY-MM-DD/
        date_str = str(target_date)  # Convert datetime.date to string
        date_dir = DEBUG_JSON_OUTPUT_DIR / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename based on S3 object key
        s3_filename = Path(object_key).stem.replace('.json', '')  # Remove .json from .json.gz
        debug_json_filename = f"{s3_filename}.json"
        debug_json_file = date_dir / debug_json_filename
        logger.info(f"🔍 DEBUG MODE: Will save filtered events to {debug_json_file}")

    try:
        logger.info(f"Downloading and processing s3://{bucket_name}/{object_key}")
        
        # Download file to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        filtered_count = 0
        total_count = 0
        debug_events = []  # Store filtered events for debug file
        batch_data = []  # Collect data for bulk insert
        
        # Process gzipped content with optimized bulk processing
        with gzip.GzipFile(fileobj=response['Body']) as f:
            for line in f:
                total_count += 1
                try:
                    # SPEED OPTIMIZATION: Parse JSON once, store raw line if needed
                    line_str = line.decode('utf-8').strip()
                    event_data = json.loads(line_str)
                    # Handle both old and new event data formats
                    event_name = event_data.get("event") or event_data.get("event_name")
                    
                    # Only store events that match our filter list
                    if event_name in EVENTS_TO_KEEP:
                        # Store raw JSON string (avoid re-encoding)
                        batch_data.append((target_date, file_sequence, line_str))

                        # Save for debug JSON file
                        if DEBUG_SAVE_JSON_FILES:
                            debug_events.append(event_data)
                        
                        filtered_count += 1
                        
                        # SPEED OPTIMIZATION: Use bulk insert every BATCH_SIZE records
                        if len(batch_data) >= BATCH_SIZE:
                            if db_type == 'postgres':
                                cursor.executemany("""
                                    INSERT INTO raw_event_data (date_day, file_sequence, event_data)
                                    VALUES (%s, %s, %s)
                                """, batch_data)
                            else:
                                cursor.executemany("""
                                    INSERT OR IGNORE INTO raw_event_data (date_day, file_sequence, event_data)
                                    VALUES (?, ?, ?)
                                """, batch_data)
                            
                            conn.commit()
                            batch_data.clear()  # Clear batch after commit
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                except Exception as e:
                    logger.error(f"Error processing line: {e}")
        
        # SPEED OPTIMIZATION: Final bulk insert for remaining records
        if batch_data:
            if db_type == 'postgres':
                cursor.executemany("""
                    INSERT INTO raw_event_data (date_day, file_sequence, event_data)
                    VALUES (%s, %s, %s)
                """, batch_data)
            else:
                cursor.executemany("""
                    INSERT OR IGNORE INTO raw_event_data (date_day, file_sequence, event_data)
                    VALUES (?, ?, ?)
                """, batch_data)
            
            conn.commit()
        
        # Write debug JSON file if in debug mode
        if DEBUG_SAVE_JSON_FILES and debug_json_file and debug_events:
            with open(debug_json_file, 'w', encoding='utf-8') as f:
                for event in debug_events:
                    f.write(json.dumps(event) + '\n')
            logger.info(f"🔍 DEBUG: Saved {len(debug_events)} filtered events to {debug_json_file}")
        
        logger.info(f"Stored {filtered_count} out of {total_count} events from {object_key}")
        return filtered_count
        
    except Exception as e:
        logger.error(f"Error processing s3://{bucket_name}/{object_key}: {e}")
        conn.rollback()
        return 0

def download_and_store_user_file(conn, db_type, s3_client, bucket_name, object_key):
    """
    Downloads a user profile .json.gz file from S3, decompresses it,
    and stores all users in database with optimized bulk processing.
    """
    cursor = conn.cursor()
    BATCH_SIZE = 50000  # Increased from 10k for better performance

    try:
        logger.info(f"Downloading and processing user file s3://{bucket_name}/{object_key}")
        
        # Download file to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        total_count = 0
        stored_count = 0
        batch_data = []  # Collect data for bulk insert
        
        # Process gzipped content with optimized bulk processing
        with gzip.GzipFile(fileobj=response['Body']) as f:
            for line in f:
                total_count += 1
                
                # Progress logging every 100,000 lines (less frequent)
                if total_count % 100000 == 0:
                    logger.info(f"Progress: {total_count:,} lines processed, {stored_count:,} users stored")
                
                try:
                    # SPEED OPTIMIZATION: Parse JSON only to extract distinct_id, 
                    # then store raw line to avoid double JSON processing
                    line_str = line.decode('utf-8').strip()
                    user_data = json.loads(line_str)
                    distinct_id = user_data.get('distinct_id')
                    
                    if distinct_id:
                        # Store raw JSON string (avoid re-encoding)
                        batch_data.append((distinct_id, line_str))
                        stored_count += 1
                        
                        # SPEED OPTIMIZATION: Use bulk insert every BATCH_SIZE records
                        if len(batch_data) >= BATCH_SIZE:
                            if db_type == 'postgres':
                                # Bulk insert with ON CONFLICT for PostgreSQL
                                cursor.executemany("""
                                    INSERT INTO raw_user_data (distinct_id, user_data)
                                    VALUES (%s, %s)
                                    ON CONFLICT (distinct_id) DO UPDATE SET
                                        user_data = EXCLUDED.user_data,
                                        downloaded_at = CURRENT_TIMESTAMP
                                """, batch_data)
                            else:
                                # Bulk insert for SQLite
                                cursor.executemany("""
                                    INSERT OR REPLACE INTO raw_user_data (distinct_id, user_data)
                                    VALUES (?, ?)
                                """, batch_data)
                            
                            conn.commit()
                            logger.info(f"Committed batch: {stored_count:,} users processed so far...")
                            batch_data.clear()  # Clear batch after commit
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                except Exception as e:
                    logger.error(f"Error processing user line: {e}")
        
        # SPEED OPTIMIZATION: Final bulk insert for remaining records
        if batch_data:
            if db_type == 'postgres':
                cursor.executemany("""
                    INSERT INTO raw_user_data (distinct_id, user_data)
                    VALUES (%s, %s)
                    ON CONFLICT (distinct_id) DO UPDATE SET
                        user_data = EXCLUDED.user_data,
                        downloaded_at = CURRENT_TIMESTAMP
                """, batch_data)
            else:
                cursor.executemany("""
                    INSERT OR REPLACE INTO raw_user_data (distinct_id, user_data)
                    VALUES (?, ?)
                """, batch_data)
            
            conn.commit()
            logger.info(f"Final commit: {stored_count:,} total users processed")
        
        logger.info(f"Stored {stored_count} out of {total_count} users from {object_key}")
        return stored_count
        
    except Exception as e:
        logger.error(f"Error processing user file s3://{bucket_name}/{object_key}: {e}")
        conn.rollback()
        return 0

def download_missing_data(conn, db_type, missing_dates, refresh_dates_set):
    """Download data for missing dates - one at a time, always download user data"""
    logger.info(f"=== STARTING DOWNLOAD PROCESS ===")
    logger.info(f"Will download data for {len(missing_dates)} missing dates...")
    
    # Initialize S3 client
    logger.info("Initializing AWS S3 connection...")
    try:
        s3_client = get_s3_client()
        logger.info("✓ S3 client initialized successfully")
    except Exception as e:
        logger.critical("✗ Could not initialize S3 client")
        logger.error(f"S3 Error: {e}")
        return False
    
    # ALWAYS download user data (100% of the time)
    logger.info("=== DOWNLOADING USER DATA ===")
    logger.info("Downloading user data (done every time)...")
    user_download_success = download_user_data(conn, db_type, s3_client)
    if user_download_success:
        logger.info("✓ User data download completed successfully")
    else:
        logger.warning("✗ User data download failed, but continuing with event data...")
    
    # Download event data for each missing date - ONE AT A TIME
    if len(missing_dates) > 0:
        logger.info("=== DOWNLOADING EVENT DATA ===")
        logger.info(f"Processing {len(missing_dates)} missing dates sequentially...")
        
        success_count = 0
        for i, date in enumerate(missing_dates, 1):
            logger.info(f">>> Processing date {i}/{len(missing_dates)}: {date.strftime('%Y-%m-%d')}")
            try:
                # Pass refresh_dates_set to download_events_for_date
                is_refresh_date = date in refresh_dates_set
                if download_events_for_date(conn, db_type, s3_client, date, is_refresh_date):
                    success_count += 1
                    logger.info(f"✓ Successfully downloaded data for {date.strftime('%Y-%m-%d')} ({i}/{len(missing_dates)})")
                else:
                    logger.warning(f"✗ Failed to download data for {date.strftime('%Y-%m-%d')} ({i}/{len(missing_dates)})")
            except Exception as e:
                logger.error(f"✗ Error downloading data for {date.strftime('%Y-%m-%d')}: {e}")
        
        logger.info(f"=== DOWNLOAD SUMMARY ===")
        logger.info(f"Event download completed: {success_count}/{len(missing_dates)} dates successful")
        logger.info(f"Success rate: {(success_count/len(missing_dates)*100):.1f}%")
        return success_count > 0
    else:
        logger.info("=== NO EVENT DATA TO DOWNLOAD ===")
        logger.info("All event data is already up to date")
        return True

def download_events_for_date(conn, db_type, s3_client, target_date, is_refresh_date=False):
    """Download event data for a specific date and store in database"""
    try:
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        # NEW BUCKET STRUCTURE: Use hourly pipeline structure
        event_s3_prefix = f"{PROJECT_ID}/mp_master_event/{year}/{month}/{day}/"
        
        logger.info(f"Downloading event files for {target_date.strftime('%Y-%m-%d')} from s3://{S3_BUCKET_EVENTS}/{event_s3_prefix}")
        
        # Get event files from new bucket structure
        event_object_keys = list_s3_objects(s3_client, S3_BUCKET_EVENTS, prefix=event_s3_prefix)
        event_export_keys = [k for k in event_object_keys if k.endswith('.json.gz')]
        
        if not event_export_keys:
            logger.warning(f"No event export files found for {target_date.strftime('%Y-%m-%d')}")
            return False
        else:
            logger.info(f"Found {len(event_export_keys)} event export files for {target_date.strftime('%Y-%m-%d')}.")

        # Check if already downloaded
        cursor = conn.cursor()
        if db_type == 'postgres':
            cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = %s", (target_date,))
        else:
            cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = ?", (target_date,))
        result = cursor.fetchone()

        # If it's a refresh date and data exists, force re-download
        if result and result[0] > 0 and is_refresh_date:
            logger.info(f"Event data for {target_date.strftime('%Y-%m-%d')} already exists in database. Forcing re-download for refresh.")
            # Clear existing data for this date to force re-download
            if db_type == 'postgres':
                cursor.execute("DELETE FROM raw_event_data WHERE date_day = %s", (target_date,))
                cursor.execute("DELETE FROM downloaded_dates WHERE date_day = %s", (target_date,))
            else:
                cursor.execute("DELETE FROM raw_event_data WHERE date_day = ?", (target_date,))
                cursor.execute("DELETE FROM downloaded_dates WHERE date_day = ?", (target_date,))
            conn.commit()
            logger.info(f"🗑️  Cleared existing raw data for {target_date.strftime('%Y-%m-%d')} to enable refresh")
        elif result and result[0] > 0 and not is_refresh_date:
            logger.info(f"Event data for {target_date.strftime('%Y-%m-%d')} already exists in database. Skipping download.")
            return True

        # Download and store all files for this date
        total_events = 0
        for i, s3_key in enumerate(event_export_keys):
            logger.info(f"Processing file {i+1}/{len(event_export_keys)}: {os.path.basename(s3_key)}")
            events_stored = download_and_store_event_file(conn, db_type, s3_client, S3_BUCKET_EVENTS, s3_key, target_date, i)
            total_events += events_stored

        # Record that this date has been processed
        if db_type == 'postgres':
            cursor.execute("""
                INSERT INTO downloaded_dates (date_day, files_downloaded, events_downloaded)
                VALUES (%s, %s, %s)
                ON CONFLICT (date_day) DO UPDATE SET
                    files_downloaded = EXCLUDED.files_downloaded,
                    events_downloaded = EXCLUDED.events_downloaded,
                    downloaded_at = CURRENT_TIMESTAMP
            """, (target_date, len(event_export_keys), total_events))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO downloaded_dates (date_day, files_downloaded, events_downloaded)
                VALUES (?, ?, ?)
            """, (target_date, len(event_export_keys), total_events))
            
        conn.commit()
        logger.info(f"Stored {total_events} events from {len(event_export_keys)} files for {target_date.strftime('%Y-%m-%d')}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading events for {target_date}: {e}")
        conn.rollback()
        return False

def download_user_data(conn, db_type, s3_client):
    """Download user profile data and store in database"""
    try:
        logger.info("Downloading user profile data...")
        
        # Clear existing user data to ensure only the newest files remain
        cursor = conn.cursor()
        cursor.execute("DELETE FROM raw_user_data")
        conn.commit()
        logger.info("Cleared existing user data from database")
        
        # User Profiles
        user_profile_base_prefix = f"{PROJECT_ID}/mp_people_data/"
        logger.info(f"Listing user profile files from s3://{S3_BUCKET_USERS}/{user_profile_base_prefix}")
        user_profile_keys = list_s3_objects(s3_client, S3_BUCKET_USERS, prefix=user_profile_base_prefix)
        user_profile_gz_keys = [k for k in user_profile_keys if k.endswith('.json.gz')]

        if not user_profile_gz_keys:
            logger.warning(f"No user profile (.json.gz) files found under {user_profile_base_prefix}")
            return False
        else:
            logger.info(f"Found {len(user_profile_gz_keys)} user profile files to download.")

        success_count = 0
        total_users = 0
        for s3_key in user_profile_gz_keys:
            filename_gz = os.path.basename(s3_key)
            logger.info(f"Processing user file: {filename_gz}")
            users_stored = download_and_store_user_file(conn, db_type, s3_client, S3_BUCKET_USERS, s3_key)
            if users_stored > 0:
                success_count += 1
                total_users += users_stored

        logger.info(f"User download completed: {success_count}/{len(user_profile_gz_keys)} files successful, {total_users} total users stored")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error downloading user data: {e}")
        return False

if __name__ == "__main__":
    sys.exit(main()) 