#!/usr/bin/env python3
"""
Debug Script: Download S3 Data to Local Files
Downloads raw data from S3 to local filesystem for manual inspection and debugging.
Based on the existing download script but saves files locally instead of database.
"""
import os
import sys
import boto3
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
from dotenv import load_dotenv

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Load environment variables from project root
project_root = Path(__file__).resolve().parent
env_file = project_root / '.env'
load_dotenv(env_file)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Credentials and S3 Configuration
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
S3_BUCKET_EVENTS = os.environ.get('S3_BUCKET_EVENTS')
S3_BUCKET_USERS = os.environ.get('S3_BUCKET_USERS')
PROJECT_ID = os.environ.get('PROJECT_ID')

# Local storage configuration
LOCAL_DATA_DIR = Path("/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final/data")
EVENTS_DIR = LOCAL_DATA_DIR / "events"
USERS_DIR = LOCAL_DATA_DIR / "users"

def setup_local_directories():
    """Create local directory structure for downloaded files"""
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    USERS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"✓ Local directories ready:")
    logger.info(f"  Events: {EVENTS_DIR}")
    logger.info(f"  Users: {USERS_DIR}")

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

def download_file_from_s3(s3_client, bucket_name, s3_key, local_path):
    """Download a single file from S3 to local filesystem"""
    try:
        # Ensure local directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download file
        logger.info(f"📥 Downloading: s3://{bucket_name}/{s3_key}")
        logger.info(f"📁 Saving to: {local_path}")
        
        s3_client.download_file(bucket_name, s3_key, str(local_path))
        
        # Get file size for logging
        file_size = local_path.stat().st_size
        logger.info(f"✓ Downloaded {file_size:,} bytes")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error downloading s3://{bucket_name}/{s3_key}: {e}")
        return False

def get_dates_to_download(days_back=7):
    """Get list of recent dates to download for debugging"""
    today = now_in_timezone().date()
    dates = []
    
    for i in range(days_back):
        date = today - timedelta(days=i)
        dates.append(date)
    
    logger.info(f"📅 Will download data for last {days_back} days:")
    for date in dates:
        logger.info(f"  - {date.strftime('%Y-%m-%d')}")
    
    return dates

def download_events_for_date(s3_client, target_date):
    """Download event data for a specific date to local files"""
    try:
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        
        # S3 prefix for this date
        event_s3_prefix = f"{PROJECT_ID}/mp_master_event/{year}/{month}/{day}/"
        
        logger.info(f"🔍 Searching for event files: {target_date.strftime('%Y-%m-%d')}")
        logger.info(f"📂 S3 prefix: s3://{S3_BUCKET_EVENTS}/{event_s3_prefix}")
        
        # Get event files from S3
        event_object_keys = list_s3_objects(s3_client, S3_BUCKET_EVENTS, prefix=event_s3_prefix)
        event_export_keys = [k for k in event_object_keys if k.endswith('.json.gz')]
        
        if not event_export_keys:
            logger.warning(f"❌ No event export files found for {target_date.strftime('%Y-%m-%d')}")
            return False
        
        logger.info(f"📊 Found {len(event_export_keys)} event files for {target_date.strftime('%Y-%m-%d')}")
        
        # Create local directory for this date
        date_dir = EVENTS_DIR / target_date.strftime('%Y-%m-%d')
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Download each file
        success_count = 0
        for i, s3_key in enumerate(event_export_keys, 1):
            filename = Path(s3_key).name
            local_path = date_dir / filename
            
            logger.info(f"📥 [{i}/{len(event_export_keys)}] {filename}")
            
            if download_file_from_s3(s3_client, S3_BUCKET_EVENTS, s3_key, local_path):
                success_count += 1
        
        logger.info(f"📊 Event download for {target_date.strftime('%Y-%m-%d')}: {success_count}/{len(event_export_keys)} successful")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ Error downloading events for {target_date}: {e}")
        return False

def download_user_data(s3_client):
    """Download user profile data to local files"""
    try:
        logger.info("👥 Downloading user profile data...")
        
        # User Profiles
        user_profile_base_prefix = f"{PROJECT_ID}/mp_people_data/"
        logger.info(f"📂 S3 prefix: s3://{S3_BUCKET_USERS}/{user_profile_base_prefix}")
        
        user_profile_keys = list_s3_objects(s3_client, S3_BUCKET_USERS, prefix=user_profile_base_prefix)
        user_profile_gz_keys = [k for k in user_profile_keys if k.endswith('.json.gz')]

        if not user_profile_gz_keys:
            logger.warning(f"❌ No user profile (.json.gz) files found")
            return False
        
        logger.info(f"📊 Found {len(user_profile_gz_keys)} user profile files")

        # Download each user file
        success_count = 0
        for i, s3_key in enumerate(user_profile_gz_keys, 1):
            filename = Path(s3_key).name
            local_path = USERS_DIR / filename
            
            logger.info(f"📥 [{i}/{len(user_profile_gz_keys)}] {filename}")
            
            if download_file_from_s3(s3_client, S3_BUCKET_USERS, s3_key, local_path):
                success_count += 1

        logger.info(f"📊 User download: {success_count}/{len(user_profile_gz_keys)} successful")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ Error downloading user data: {e}")
        return False

def show_download_summary():
    """Show summary of downloaded files"""
    print(f"\n📊 === DOWNLOAD SUMMARY ===")
    
    # Count event files
    event_files = list(EVENTS_DIR.rglob("*.json.gz"))
    event_dates = set()
    total_event_size = 0
    
    for file in event_files:
        # Extract date from path
        date_parts = file.parent.name.split('-')
        if len(date_parts) == 3:
            event_dates.add(file.parent.name)
        total_event_size += file.stat().st_size
    
    # Count user files
    user_files = list(USERS_DIR.glob("*.json.gz"))
    total_user_size = 0
    for file in user_files:
        total_user_size += file.stat().st_size
    
    print(f"📅 Event data:")
    print(f"  - Dates downloaded: {len(event_dates)}")
    print(f"  - Total files: {len(event_files)}")
    print(f"  - Total size: {total_event_size / (1024*1024):.1f} MB")
    
    print(f"👥 User data:")
    print(f"  - Files: {len(user_files)}")
    print(f"  - Total size: {total_user_size / (1024*1024):.1f} MB")
    
    print(f"📁 All files saved to: {LOCAL_DATA_DIR}")
    print(f"")
    print(f"🔍 === HOW TO INSPECT THE DATA ===")
    print(f"1. Event files are in: {EVENTS_DIR}")
    print(f"   - Organized by date folders (YYYY-MM-DD)")
    print(f"   - Files are gzipped JSON (.json.gz)")
    print(f"")
    print(f"2. User files are in: {USERS_DIR}")
    print(f"   - User profile data (.json.gz)")
    print(f"")
    print(f"3. To examine files:")
    print(f"   - gunzip filename.json.gz  (to decompress)")
    print(f"   - zcat filename.json.gz | head -10  (to peek at first 10 lines)")
    print(f"   - zcat filename.json.gz | jq '.' | head  (to pretty-print JSON)")

def main():
    try:
        print("🔍 === DEBUG: Download S3 Data to Local Files ===")
        print(f"This script downloads raw data files from S3 for manual inspection")
        logger.info("=== DEBUG DOWNLOAD STARTED ===")
        
        # Validate required environment variables
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_EVENTS', 'S3_BUCKET_USERS', 'PROJECT_ID']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Setup local directories
        setup_local_directories()
        
        # Initialize S3 client
        s3_client = get_s3_client()
        
        # Get dates to download (July 16-29, 2025 for investigation)
        dates_to_download = get_dates_to_download(days_back=14)
        
        print(f"\n📥 Starting downloads...")
        
        # Download event data for recent dates
        event_success_count = 0
        for date in dates_to_download:
            logger.info(f"🗓️  Processing date: {date.strftime('%Y-%m-%d')}")
            if download_events_for_date(s3_client, date):
                event_success_count += 1
        
        # Download user data
        logger.info(f"👥 Processing user data...")
        user_success = download_user_data(s3_client)
        
        # Show summary
        show_download_summary()
        
        print(f"\n🎉 Download completed!")
        print(f"📊 Event data: {event_success_count}/{len(dates_to_download)} dates successful")
        print(f"👥 User data: {'✓' if user_success else '✗'}")
        
        logger.info("=== DEBUG DOWNLOAD COMPLETED ===")
        return 0
        
    except Exception as e:
        print(f"❌ Script failed: {e}", file=sys.stderr)
        logger.error(f"=== DEBUG DOWNLOAD FAILED ===")
        logger.error(f"Error: {e}")
        logger.exception("Full error details:")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 