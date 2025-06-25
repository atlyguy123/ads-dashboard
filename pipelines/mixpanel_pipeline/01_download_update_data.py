#!/usr/bin/env python3
"""
Module 1: Download/Update Data
Checks what data exists and downloads any missing data.
Ensures data is present and up to date through yesterday.
Now stores data directly in Postgres database instead of filesystem.
"""
import os
import sys
import boto3
import gzip
import json
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse

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

# Database configuration - use Heroku Postgres
DATABASE_URL = os.environ.get('DATABASE_URL')

# List of event names to keep
EVENTS_TO_KEEP = [
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal"
]

def get_database_connection():
    """Get connection to Postgres database"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set")
    
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
    return conn

def ensure_raw_data_tables(conn):
    """Create tables to store raw downloaded data"""
    cursor = conn.cursor()
    
    # Table for raw event data (by date)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_event_data (
            date_day DATE NOT NULL,
            file_sequence INTEGER NOT NULL,
            event_data JSONB NOT NULL,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_day, file_sequence, (event_data->>'event'), (event_data->'properties'->>'distinct_id'))
        )
    """)
    
    # Table for raw user data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_user_data (
            distinct_id TEXT PRIMARY KEY,
            user_data JSONB NOT NULL,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Table to track processed dates (replaces filesystem scanning)
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
    
    conn.commit()
    logger.info("Raw data tables ensured in database")

def main():
    try:
        print("=== Module 1: Download/Update Data ===")
        print(f"Starting data download and update process...")
        logger.info("=== MODULE 1 STARTED: Download/Update Data ===")
        logger.info("Initializing download and update process...")
        
        # Validate required environment variables
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_EVENTS', 'S3_BUCKET_USERS', 'PROJECT_ID', 'DATABASE_URL']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Connect to database
        logger.info("Connecting to PostgreSQL database...")
        conn = get_database_connection()
        
        # Ensure raw data tables exist
        ensure_raw_data_tables(conn)
        
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
        missing_dates = identify_missing_data(conn, latest_date)
        print(f"Missing data for {len(missing_dates)} days")
        
        if len(missing_dates) > 0:
            logger.info(f"Found {len(missing_dates)} missing dates:")
            for i, date in enumerate(missing_dates[:10]):  # Show first 10
                logger.info(f"  - {date}")
            if len(missing_dates) > 10:
                logger.info(f"  - ... and {len(missing_dates) - 10} more dates")
        else:
            logger.info("✓ No missing dates found - all data is up to date")
        
        if missing_dates:
            print("Downloading missing data...")
            logger.info("Starting download process for missing data...")
            success = download_missing_data(conn, missing_dates)
            if not success:
                print("Some downloads failed, but continuing...")
                logger.warning("Some downloads failed, but process completed")
            else:
                logger.info("✓ All downloads completed successfully")
        else:
            print("All required data is already present")
            logger.info("✓ No downloads needed - all data is current")
            
            # Still need to download user data even when no event data is missing
            print("Downloading user data...")
            logger.info("Starting user data download (always refresh user data)...")
            s3_client = get_s3_client()
            user_success = download_user_data(conn, s3_client)
            if user_success:
                logger.info("✓ User data download completed successfully")
            else:
                logger.warning("User data download failed, but continuing...")
        
        conn.close()
        print("Data download and update completed successfully")
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
    """Identify which dates need data downloads by checking database records for the last 90 days"""
    yesterday = datetime.now().date() - timedelta(days=1)
    
    # Always check the last 90 days (including yesterday)
    start_date = yesterday - timedelta(days=89)  # 90 days total including yesterday
    logger.info(f"Checking last 90 days for missing data: {start_date} to {yesterday}")
    
    missing_dates = []
    current_date = start_date
    dates_checked = 0
    dates_found = 0
    
    cursor = conn.cursor()
    
    while current_date <= yesterday:
        dates_checked += 1
        # Check if this date has data in database
        cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = %s", (current_date,))
        result = cursor.fetchone()
        
        if not result or result[0] == 0:
            missing_dates.append(current_date)
            logger.debug(f"Missing data: {current_date}")
        else:
            dates_found += 1
            logger.debug(f"Found data: {current_date} ({result[0]} events)")
        
        current_date += timedelta(days=1)
    
    logger.info(f"Checked {dates_checked} dates: {dates_found} have data, {len(missing_dates)} are missing")
    
    if len(missing_dates) > 0:
        logger.info(f"Missing dates range from {missing_dates[0]} to {missing_dates[-1]}")
    else:
        logger.info("✓ All data for the last 90 days is present")
    
    return missing_dates

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

def download_and_store_event_file(conn, s3_client, bucket_name, object_key, target_date, file_sequence):
    """
    Downloads an event .json.gz file from S3, decompresses it,
    filters events by name, and stores in database.
    """
    cursor = conn.cursor()
    
    try:
        logger.info(f"Downloading and processing s3://{bucket_name}/{object_key}")
        
        # Download file to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        filtered_count = 0
        total_count = 0
        
        # Process gzipped content
        with gzip.GzipFile(fileobj=response['Body']) as f:
            for line in f:
                total_count += 1
                try:
                    event_data = json.loads(line.decode('utf-8').strip())
                    event_name = event_data.get("event")
                    
                    # Only store events that match our filter list
                    if event_name in EVENTS_TO_KEEP:
                        # Store in database
                        cursor.execute("""
                            INSERT INTO raw_event_data (date_day, file_sequence, event_data)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (target_date, file_sequence, json.dumps(event_data)))
                        filtered_count += 1
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                except Exception as e:
                    logger.error(f"Error processing line: {e}")
        
        conn.commit()
        logger.info(f"Stored {filtered_count} out of {total_count} events from {object_key}")
        return filtered_count
        
    except Exception as e:
        logger.error(f"Error processing s3://{bucket_name}/{object_key}: {e}")
        conn.rollback()
        return 0

def download_and_store_user_file(conn, s3_client, bucket_name, object_key):
    """
    Downloads a user profile .json.gz file from S3, decompresses it,
    and stores all users in database.
    """
    cursor = conn.cursor()
    
    try:
        logger.info(f"Downloading and processing user file s3://{bucket_name}/{object_key}")
        
        # Download file to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        total_count = 0
        stored_count = 0
        
        # Process gzipped content
        with gzip.GzipFile(fileobj=response['Body']) as f:
            for line in f:
                total_count += 1
                try:
                    user_data = json.loads(line.decode('utf-8').strip())
                    distinct_id = user_data.get('mp_distinct_id') or user_data.get('abi_distinct_id')
                    
                    if distinct_id:
                        # Store in database (replace existing)
                        cursor.execute("""
                            INSERT INTO raw_user_data (distinct_id, user_data)
                            VALUES (%s, %s)
                            ON CONFLICT (distinct_id) DO UPDATE SET
                                user_data = EXCLUDED.user_data,
                                downloaded_at = CURRENT_TIMESTAMP
                        """, (distinct_id, json.dumps(user_data)))
                        stored_count += 1
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                except Exception as e:
                    logger.error(f"Error processing user line: {e}")
        
        conn.commit()
        logger.info(f"Stored {stored_count} out of {total_count} users from {object_key}")
        return stored_count
        
    except Exception as e:
        logger.error(f"Error processing user file s3://{bucket_name}/{object_key}: {e}")
        conn.rollback()
        return 0

def download_missing_data(conn, missing_dates):
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
    user_download_success = download_user_data(conn, s3_client)
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
                if download_events_for_date(conn, s3_client, date):
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

def download_events_for_date(conn, s3_client, target_date):
    """Download event data for a specific date and store in database"""
    try:
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        event_s3_prefix = f"{PROJECT_ID}/{year}/{month}/{day}/full_day/"
        logger.info(f"Downloading event files for {target_date.strftime('%Y-%m-%d')} from s3://{S3_BUCKET_EVENTS}/{event_s3_prefix}")
        
        event_object_keys = list_s3_objects(s3_client, S3_BUCKET_EVENTS, prefix=event_s3_prefix)
        # Filter for actual event export files
        event_export_keys = [k for k in event_object_keys if k.endswith('export.json.gz')]

        if not event_export_keys:
            logger.warning(f"No event export files found for {target_date.strftime('%Y-%m-%d')}")
            return False
        else:
            logger.info(f"Found {len(event_export_keys)} event export files for {target_date.strftime('%Y-%m-%d')}.")

        # Check if already downloaded
        cursor = conn.cursor()
        cursor.execute("SELECT events_downloaded FROM downloaded_dates WHERE date_day = %s", (target_date,))
        result = cursor.fetchone()
        
        if result and result[0] > 0:
            logger.info(f"Event data for {target_date.strftime('%Y-%m-%d')} already exists in database. Skipping download.")
            return True

        # Download and store all files for this date
        total_events = 0
        for i, s3_key in enumerate(event_export_keys):
            logger.info(f"Processing file {i+1}/{len(event_export_keys)}: {os.path.basename(s3_key)}")
            events_stored = download_and_store_event_file(conn, s3_client, S3_BUCKET_EVENTS, s3_key, target_date, i)
            total_events += events_stored

        # Record that this date has been processed
        cursor.execute("""
            INSERT INTO downloaded_dates (date_day, files_downloaded, events_downloaded)
            VALUES (%s, %s, %s)
            ON CONFLICT (date_day) DO UPDATE SET
                files_downloaded = EXCLUDED.files_downloaded,
                events_downloaded = EXCLUDED.events_downloaded,
                downloaded_at = CURRENT_TIMESTAMP
        """, (target_date, len(event_export_keys), total_events))
        
        conn.commit()
        logger.info(f"Stored {total_events} events from {len(event_export_keys)} files for {target_date.strftime('%Y-%m-%d')}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading events for {target_date}: {e}")
        conn.rollback()
        return False

def download_user_data(conn, s3_client):
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
            users_stored = download_and_store_user_file(conn, s3_client, S3_BUCKET_USERS, s3_key)
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