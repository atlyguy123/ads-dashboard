#!/usr/bin/env python3
"""
Module 1: Download/Update Data
Checks what data exists and downloads any missing data.
Ensures data is present and up to date through yesterday.
"""
import os
import sys
import boto3
import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = "../../data"
EVENTS_DIR = os.path.join(DATA_DIR, "events")
USERS_DIR = os.path.join(DATA_DIR, "users")

# AWS Credentials and S3 Configuration - loaded from environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
S3_BUCKET_EVENTS = os.environ.get('S3_BUCKET_EVENTS')
S3_BUCKET_USERS = os.environ.get('S3_BUCKET_USERS')
PROJECT_ID = os.environ.get('PROJECT_ID')

# Optional configuration - removed unused MAX_WORKERS

# List of event names to keep
EVENTS_TO_KEEP = [
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal"
]

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
        
        # Ensure directories exist
        logger.info("Creating data directories if they don't exist...")
        os.makedirs(EVENTS_DIR, exist_ok=True)
        os.makedirs(USERS_DIR, exist_ok=True)
        logger.info(f"✓ Events directory: {EVENTS_DIR}")
        logger.info(f"✓ Users directory: {USERS_DIR}")
        
        # Check existing data coverage
        logger.info("Scanning existing data to find latest date...")
        latest_date = find_latest_data_date()
        if latest_date:
            print(f"Latest data found: {latest_date}")
            logger.info(f"✓ Found existing data up to: {latest_date}")
        else:
            print(f"Latest data found: None (no existing data)")
            logger.info("ℹ No existing data found - will download from scratch")
        
        # Determine what data needs to be downloaded
        logger.info("Calculating missing dates...")
        missing_dates = identify_missing_data(latest_date)
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
            success = download_missing_data(missing_dates)
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
            user_success = download_user_data(s3_client)
            if user_success:
                logger.info("✓ User data download completed successfully")
            else:
                logger.warning("User data download failed, but continuing...")
        
        print("Data download and update completed successfully")
        logger.info("=== MODULE 1 COMPLETED SUCCESSFULLY ===")
        return 0
        
    except Exception as e:
        print(f"Module 1 failed: {e}", file=sys.stderr)
        logger.error(f"=== MODULE 1 FAILED ===")
        logger.error(f"Error: {e}")
        logger.exception("Full error details:")
        return 1

def find_latest_data_date():
    """Find the latest date for which we have export.json data"""
    if not os.path.exists(EVENTS_DIR):
        logger.info(f"Events directory {EVENTS_DIR} does not exist")
        return None
    
    logger.info(f"Scanning directory structure in {EVENTS_DIR}...")
    latest_date = None
    total_dates_checked = 0
    
    # Walk through year directories
    for year_name in sorted(os.listdir(EVENTS_DIR)):
        year_path = os.path.join(EVENTS_DIR, year_name)
        if not os.path.isdir(year_path) or not year_name.isdigit():
            continue
            
        # Walk through month directories
        for month_name in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month_name)
            if not os.path.isdir(month_path) or not month_name.isdigit():
                continue
                
            # Walk through day directories
            for day_name in sorted(os.listdir(month_path)):
                day_path = os.path.join(month_path, day_name)
                if not os.path.isdir(day_path) or not day_name.isdigit():
                    continue
                
                total_dates_checked += 1
                
                # Check if this day has the export.json file
                export_json_path = os.path.join(day_path, 'export.json')
                if os.path.exists(export_json_path):
                    try:
                        date_str = f"{year_name}-{month_name.zfill(2)}-{day_name.zfill(2)}"
                        candidate_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        if latest_date is None or candidate_date > latest_date:
                            latest_date = candidate_date
                            logger.debug(f"Found data for: {date_str}")
                    except ValueError:
                        logger.warning(f"Invalid date format: {year_name}-{month_name}-{day_name}")
                        continue
    
    logger.info(f"Scanned {total_dates_checked} date directories")
    if latest_date:
        logger.info(f"Latest data found: {latest_date}")
    else:
        logger.info("No export.json files found in any date directories")
    return latest_date

def identify_missing_data(latest_date):
    """Identify which dates need data downloads by checking for export.json files in the last 90 days"""
    yesterday = datetime.now().date() - timedelta(days=1)
    
    # Always check the last 90 days (including yesterday)
    start_date = yesterday - timedelta(days=89)  # 90 days total including yesterday
    logger.info(f"Checking last 90 days for missing data: {start_date} to {yesterday}")
    
    missing_dates = []
    current_date = start_date
    dates_checked = 0
    dates_found = 0
    
    while current_date <= yesterday:
        dates_checked += 1
        # Check if this date has export.json
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        
        export_json_path = os.path.join(EVENTS_DIR, year, month, day, 'export.json')
        if not os.path.exists(export_json_path):
            missing_dates.append(current_date)
            logger.debug(f"Missing data: {current_date}")
        else:
            dates_found += 1
            logger.debug(f"Found data: {current_date}")
        
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

def download_and_process_event_file(s3_client, bucket_name, object_key, local_json_path):
    """
    Downloads an event .json.gz file from S3, decompresses it,
    filters events by name, and saves only the filtered events.
    """
    # Ensure the target directory exists
    os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
    
    # Define a temporary path for the downloaded .gz file
    temp_gz_path = local_json_path + "_" + datetime.now().strftime("%Y%m%d%H%M%S%f") + ".gz.temp"

    try:
        logger.info(f"Downloading s3://{bucket_name}/{object_key} to {temp_gz_path}")
        s3_client.download_file(bucket_name, object_key, temp_gz_path)
        
        logger.info(f"Processing and filtering events from {temp_gz_path} to {local_json_path}")
        
        filtered_count = 0
        total_count = 0
        
        with gzip.open(temp_gz_path, 'rt', encoding='utf-8') as f_in:
            with open(local_json_path, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    total_count += 1
                    try:
                        event_data = json.loads(line.strip())
                        event_name = event_data.get("event")
                        
                        # Only write events that match our filter list
                        if event_name in EVENTS_TO_KEEP:
                            f_out.write(line)
                            filtered_count += 1
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
        
        logger.info(f"Filtered {filtered_count} out of {total_count} events for {local_json_path}")
        
    except Exception as e:
        logger.error(f"Error processing s3://{bucket_name}/{object_key} -> {local_json_path}: {e}")
        return False
    finally:
        # Clean up the temporary .gz file
        if os.path.exists(temp_gz_path):
            try:
                os.remove(temp_gz_path)
                logger.debug(f"Removed temporary file: {temp_gz_path}")
            except Exception as e:
                logger.error(f"Error removing temporary file {temp_gz_path}: {e}")
    
    return True

def download_and_process_user_file(s3_client, bucket_name, object_key, local_json_path):
    """
    Downloads a user profile .json.gz file from S3, decompresses it,
    and saves all users to the final output file.
    """
    # Ensure the target directory exists
    os.makedirs(os.path.dirname(local_json_path), exist_ok=True)
    
    # Define a temporary path for the downloaded .gz file
    temp_gz_path = local_json_path + "_" + datetime.now().strftime("%Y%m%d%H%M%S%f") + ".gz.temp"

    try:
        logger.info(f"Downloading s3://{bucket_name}/{object_key} to {temp_gz_path}")
        s3_client.download_file(bucket_name, object_key, temp_gz_path)
        
        logger.info(f"Processing user file from {temp_gz_path} to {local_json_path}")
        
        total_count = 0
        filtered_count = 0
        
        with gzip.open(temp_gz_path, 'rt', encoding='utf-8') as f_in:
            with open(local_json_path, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    total_count += 1
                    try:
                        user_data = json.loads(line.strip())
                        
                        # Keep all users - no filtering
                        f_out.write(line)
                        filtered_count += 1
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
        
        logger.info(f"Processed {filtered_count} out of {total_count} users for {local_json_path}")
        if total_count - filtered_count > 0:
            logger.info(f"Skipped {total_count - filtered_count} users due to JSON parsing errors")
        
    except Exception as e:
        logger.error(f"Error processing s3://{bucket_name}/{object_key} -> {local_json_path}: {e}")
        return False
    finally:
        # Clean up the temporary .gz file
        if os.path.exists(temp_gz_path):
            try:
                os.remove(temp_gz_path)
                logger.debug(f"Removed temporary file: {temp_gz_path}")
            except Exception as e:
                logger.error(f"Error removing temporary file {temp_gz_path}: {e}")
    
    return True

def download_missing_data(missing_dates):
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
    user_download_success = download_user_data(s3_client)
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
                if download_events_for_date(s3_client, date):
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

def download_events_for_date(s3_client, target_date):
    """Download event data for a specific date"""
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

        # For each date, we'll combine all export files into a single export.json file
        local_dir = os.path.join(EVENTS_DIR, year, month, day)
        local_json_path = os.path.join(local_dir, 'export.json')

        if os.path.exists(local_json_path):
            logger.info(f"Event file {local_json_path} already exists. Skipping download.")
            return True

        # If there's only one file, download it directly as export.json  
        if len(event_export_keys) == 1:
            s3_key = event_export_keys[0]
            if download_and_process_event_file(s3_client, S3_BUCKET_EVENTS, s3_key, local_json_path):
                logger.info(f"Successfully downloaded {s3_key} to {local_json_path}")
                return True
            else:
                return False
        else:
            # If there are multiple files, download and combine them
            logger.info(f"Combining {len(event_export_keys)} event files into {local_json_path}")
            os.makedirs(local_dir, exist_ok=True)
            
            combined_count = 0
            with open(local_json_path, 'w', encoding='utf-8') as combined_file:
                for i, s3_key in enumerate(event_export_keys):
                    logger.info(f"Processing file {i+1}/{len(event_export_keys)}: {os.path.basename(s3_key)}")
                    
                    # Use a temporary file for this individual download
                    temp_json_path = local_json_path + f".temp_{i}"
                    
                    if download_and_process_event_file(s3_client, S3_BUCKET_EVENTS, s3_key, temp_json_path):
                        # Read the temporary file and append to combined file
                        with open(temp_json_path, 'r', encoding='utf-8') as temp_file:
                            for line in temp_file:
                                combined_file.write(line)
                                combined_count += 1
                        
                        # Remove the temporary file
                        os.remove(temp_json_path)
                    else:
                        logger.warning(f"Failed to download {s3_key}")
            
            logger.info(f"Combined {len(event_export_keys)} files into {local_json_path} with {combined_count} total events")
            return True  # Return True if download process completed, regardless of filtered event count
        
    except Exception as e:
        logger.error(f"Error downloading events for {target_date}: {e}")
        return False

def download_user_data(s3_client):
    """Download user profile data (done once, not per date)"""
    try:
        logger.info("Downloading user profile data...")
        
        # FIRST: Clear the entire users directory to ensure only the newest files remain
        if os.path.exists(USERS_DIR):
            logger.info(f"Clearing existing user files from {USERS_DIR}")
            for filename in os.listdir(USERS_DIR):
                file_path = os.path.join(USERS_DIR, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logger.info(f"Removed old user file: {filename}")
        else:
            logger.info(f"Creating users directory: {USERS_DIR}")
            os.makedirs(USERS_DIR, exist_ok=True)
        
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
        for s3_key in user_profile_gz_keys:
            filename_gz = os.path.basename(s3_key)
            # Remove .gz extension to get final filename
            filename_json = filename_gz[:-3] if filename_gz.endswith(".json.gz") else filename_gz.replace(".gz", "")
                
            local_json_path = os.path.join(USERS_DIR, filename_json)
            
            # Download and process user files (directory already cleared above)
            logger.info(f"Processing user file: {filename_json}")
            if download_and_process_user_file(s3_client, S3_BUCKET_USERS, s3_key, local_json_path):
                success_count += 1

        logger.info(f"User download completed: {success_count}/{len(user_profile_gz_keys)} files successful")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error downloading user data: {e}")
        return False

if __name__ == "__main__":
    sys.exit(main()) 