#!/usr/bin/env python3
"""
Meta Data Update Module

Automatically updates Meta (Facebook) advertising data by checking the most recent
data in the database and filling any gaps from that date up to today.

The module will:
1. Check today's date
2. Find the most recent date in the meta analytics tables
3. Re-fill the last existing date (overwrite with fresh data)
4. Fill all missing dates from that point to today
5. Use async-first processing with sync fallback for reliability

Author: Analytics Pipeline Team
Created: 2025
"""

import os
import sys
import sqlite3
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add parent directories to path for imports
current_dir = Path(__file__).resolve().parent  # Use resolve() for absolute path
sys.path.append(str(current_dir.parent.parent))  # Add project root
sys.path.append(str(current_dir.parent.parent / "utils"))  # Add utils
sys.path.append(str(current_dir.parent.parent / "orchestrator"))  # Add orchestrator

try:
    # Import specific Meta API functions to avoid initialization issues
    sys.path.append(str(current_dir.parent.parent / "orchestrator" / "meta" / "services"))
    from meta_service import fetch_meta_data, check_async_job_status, get_async_job_results
    from utils.database_utils import get_database_path
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    logger.error("Ensure meta_service and utils modules are available")
    sys.exit(1)


class MetaActionProcessor:
    """Process Meta API actions to extract trial and purchase counts"""
    
    # Action type mappings - CORRECTED based on actual business logic
    # "add_payment_info" = trial_started
    # "complete_registration" = initial_purchase
    TRIAL_ACTIONS = [
        'add_payment_info'
    ]
    
    PURCHASE_ACTIONS = [
        'complete_registration'
    ]
    
    @classmethod
    def process_actions(cls, actions_data) -> tuple[int, int]:
        """
        Process Meta API actions array to extract trial and purchase counts
        
        Args:
            actions_data: Actions array from Meta API response
            
        Returns:
            tuple: (trial_count, purchase_count)
        """
        trial_count = 0
        purchase_count = 0
        
        if not actions_data or not isinstance(actions_data, list):
            return trial_count, purchase_count
        
        for action in actions_data:
            if not isinstance(action, dict):
                continue
                
            action_type = action.get('action_type', '')
            value = action.get('value', 0)
            
            try:
                value = int(float(value))  # Convert to int, handling float strings
            except (ValueError, TypeError):
                continue
            
            if action_type in cls.TRIAL_ACTIONS:
                trial_count += value
            elif action_type in cls.PURCHASE_ACTIONS:
                purchase_count += value
        
        return trial_count, purchase_count


class MetaDataUpdater:
    """
    Meta data updater with comprehensive logging and error handling
    """
    
    def __init__(self):
        """Initialize the Meta data updater"""
        try:
            self.db_path = get_database_path('meta_analytics')
            logger.info(f"📊 Meta Data Updater initialized")
            logger.info(f"📁 Database path: {self.db_path}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Meta Data Updater: {e}")
            raise
    
    def get_most_recent_date_in_db(self) -> Optional[str]:
        """
        Find the most recent date across all meta analytics tables
        
        Returns:
            Most recent date as string (YYYY-MM-DD) or None if no data exists
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check all meta analytics tables
            tables = [
                'ad_performance_daily',
                # BREAKDOWN TABLES (commented out for now)
                # 'ad_performance_daily_country',
                # 'ad_performance_daily_region', 
                # 'ad_performance_daily_device'
            ]
            
            most_recent_date = None
            
            for table in tables:
                try:
                    cursor.execute(f'SELECT MAX(date) FROM {table}')
                    result = cursor.fetchone()
                    table_max_date = result[0] if result and result[0] else None
                    
                    if table_max_date:
                        logger.debug(f"📅 {table}: Most recent date = {table_max_date}")
                        if not most_recent_date or table_max_date > most_recent_date:
                            most_recent_date = table_max_date
                    else:
                        logger.debug(f"📅 {table}: No data found")
                        
                except sqlite3.OperationalError as e:
                    logger.warning(f"⚠️  Table {table} not found or accessible: {e}")
                    continue
            
            conn.close()
            
            if most_recent_date:
                logger.info(f"📅 Most recent date across all tables: {most_recent_date}")
            else:
                logger.info(f"📅 No existing data found in any meta analytics tables")
                
            return most_recent_date
            
        except Exception as e:
            logger.error(f"❌ Error checking most recent date: {e}")
            return None
    
    def calculate_dates_to_update(self, most_recent_date: Optional[str] = None) -> List[str]:
        """
        Calculate which dates need to be updated
        
        Args:
            most_recent_date: Most recent date in DB, or None if DB is empty
            
        Returns:
            List of dates to update (including re-filling the most recent date)
        """
        today = datetime.now().strftime('%Y-%m-%d')
        
        if not most_recent_date:
            # If no data exists, start from 30 days ago
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            logger.info(f"📅 No existing data - will fill from {start_date} to {today}")
        else:
            # Re-fill the most recent date + fill any gaps to today
            start_date = most_recent_date
            logger.info(f"📅 Will re-fill from {start_date} (overwrite) to {today}")
        
        # Generate date list
        dates_to_update = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(today, '%Y-%m-%d')
        
        while current_date <= end_date:
            dates_to_update.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        logger.info(f"📊 Dates to update: {len(dates_to_update)} days")
        logger.debug(f"📋 Date range: {dates_to_update[0]} to {dates_to_update[-1]}")
        
        return dates_to_update
    
    def fetch_meta_data(self, from_date: str, to_date: str, breakdown_type: Optional[str] = None) -> List[Dict]:
        """
        Fetch Meta data using the actual Meta API service
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            breakdown_type: Breakdown type (None, 'country', 'region', 'device')
            
        Returns:
            List of raw Meta API records
        """
        breakdown_desc = breakdown_type or "no breakdown"
        logger.info(f"📡 Fetching Meta data: {from_date} to {to_date} ({breakdown_desc})")
        
        # Define fields for Meta API request
        fields = 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,impressions,clicks,actions'
        
        # Map breakdown types to proper field names
        breakdowns = None
        if breakdown_type == "country":
            breakdowns = 'country'
        elif breakdown_type == "region":
            breakdowns = 'region'
        elif breakdown_type == "device":
            breakdowns = 'impression_device'
        
        try:
            # Use the actual Meta API service
            api_response, error = fetch_meta_data(
                start_date=from_date,
                end_date=to_date,
                time_increment=1,
                fields=fields,
                breakdowns=breakdowns,
                use_async=True  # Use async by default
            )
            
            if error:
                logger.error(f"   ❌ Meta API error: {error}")
                return []
            
            # Handle async job response
            if api_response.get('async_job'):
                report_run_id = api_response.get('report_run_id')
                logger.info(f"   ✅ Async job started: {report_run_id}")
                
                # Wait for completion
                records = self._wait_for_async_completion(report_run_id)
                
                if records:
                    logger.info(f"   ✅ Async job completed: {len(records)} records")
                    return records
                else:
                    logger.warning(f"   ⚠️  Async job failed or returned no data")
                    return []
            
            # Handle synchronous response
            if isinstance(api_response, dict) and 'data' in api_response:
                records = api_response['data'].get('data', [])
                logger.info(f"   ✅ Sync response: {len(records)} records")
                return records
            
            logger.error(f"   ❌ Unknown response format: {type(api_response)}")
            return []
            
        except Exception as e:
            logger.error(f"   ❌ Error fetching Meta data: {e}")
            return []
    
    def _wait_for_async_completion(self, report_run_id: str, max_wait_time: int = 300) -> List[Dict]:
        """
        Wait for async job completion and return results
        
        Args:
            report_run_id: Meta API report run ID
            max_wait_time: Maximum wait time in seconds
            
        Returns:
            List of records from completed job
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            # Check job status
            status_info, error = check_async_job_status(report_run_id)
            
            if error:
                logger.error(f"   ❌ Error checking job status: {error}")
                return []
            
            async_status = status_info.get('async_status', '')
            completion = status_info.get('async_percent_completion', 0)
            
            logger.info(f"   ⏳ Async job {async_status}: {completion}% complete")
            
            if async_status == 'Job Completed':
                # Get results
                results, error = get_async_job_results(report_run_id)
                
                if error:
                    logger.error(f"   ❌ Error getting results: {error}")
                    return []
                
                if results and 'data' in results and 'data' in results['data']:
                    return results['data']['data']
                else:
                    logger.warning(f"   ⚠️  No data in results")
                    return []
            
            elif async_status == 'Job Failed':
                logger.error(f"   ❌ Async job failed")
                return []
            
            # Wait before checking again
            time.sleep(10)
        
        logger.error(f"   ❌ Async job timed out after {max_wait_time} seconds")
        return []
    
    def process_meta_records(self, raw_records: List[Dict], breakdown_type: Optional[str] = None) -> List[Dict]:
        """
        Process raw Meta API records into database-ready format
        
        Args:
            raw_records: Raw records from Meta API
            breakdown_type: Breakdown type for additional fields
            
        Returns:
            List of processed records ready for database insertion
        """
        processed_records = []
        
        for record in raw_records:
            try:
                # Extract basic fields
                processed_record = {
                    'ad_id': record.get('ad_id', ''),
                    'date': record.get('date_start', ''),  # Meta API uses date_start
                    'adset_id': record.get('adset_id', ''),
                    'campaign_id': record.get('campaign_id', ''),
                    'ad_name': record.get('ad_name', ''),
                    'adset_name': record.get('adset_name', ''),
                    'campaign_name': record.get('campaign_name', ''),
                    'spend': float(record.get('spend', 0) or 0),
                    'impressions': int(record.get('impressions', 0) or 0),
                    'clicks': int(record.get('clicks', 0) or 0)
                }
                
                # Process actions to extract trials and purchases
                actions = record.get('actions', [])
                meta_trials, meta_purchases = MetaActionProcessor.process_actions(actions)
                
                processed_record['meta_trials'] = meta_trials
                processed_record['meta_purchases'] = meta_purchases
                
                # Add breakdown-specific fields
                if breakdown_type == "country":
                    processed_record['country'] = record.get('country', '')
                elif breakdown_type == "region":
                    processed_record['region'] = record.get('region', '')
                elif breakdown_type == "device":
                    processed_record['device'] = record.get('impression_device', '')
                
                processed_records.append(processed_record)
                
            except Exception as e:
                logger.error(f"   ❌ Error processing record {record.get('ad_id', 'unknown')}: {e}")
                continue
        
        return processed_records
    
    def load_data_to_table(self, processed_records: List[Dict], table_name: str) -> int:
        """
        Load processed data to specified table using REPLACE to handle overwrites
        
        Args:
            processed_records: List of processed records
            table_name: Target table name
            
        Returns:
            Number of records successfully loaded
        """
        if not processed_records:
            logger.warning(f"   ⚠️  No records to load to {table_name}")
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Build INSERT OR REPLACE statement based on table structure
            if table_name == 'ad_performance_daily':
                sql = """
                INSERT OR REPLACE INTO ad_performance_daily 
                (ad_id, date, adset_id, campaign_id, ad_name, adset_name, campaign_name, 
                 spend, impressions, clicks, meta_trials, meta_purchases)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                values = [
                    (r['ad_id'], r['date'], r['adset_id'], r['campaign_id'], 
                     r['ad_name'], r['adset_name'], r['campaign_name'],
                     r['spend'], r['impressions'], r['clicks'], r['meta_trials'], r['meta_purchases'])
                    for r in processed_records
                ]
            
            # BREAKDOWN TABLES (now active)
            elif table_name == 'ad_performance_daily_country':
                sql = """
                INSERT OR REPLACE INTO ad_performance_daily_country 
                (ad_id, date, country, adset_id, campaign_id, ad_name, adset_name, campaign_name, 
                 spend, impressions, clicks, meta_trials, meta_purchases)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                values = [
                    (r['ad_id'], r['date'], r['country'], r['adset_id'], r['campaign_id'], 
                     r['ad_name'], r['adset_name'], r['campaign_name'],
                     r['spend'], r['impressions'], r['clicks'], r['meta_trials'], r['meta_purchases'])
                    for r in processed_records
                ]
            elif table_name == 'ad_performance_daily_region':
                sql = """
                INSERT OR REPLACE INTO ad_performance_daily_region 
                (ad_id, date, region, adset_id, campaign_id, ad_name, adset_name, campaign_name, 
                 spend, impressions, clicks, meta_trials, meta_purchases)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                values = [
                    (r['ad_id'], r['date'], r['region'], r['adset_id'], r['campaign_id'], 
                     r['ad_name'], r['adset_name'], r['campaign_name'],
                     r['spend'], r['impressions'], r['clicks'], r['meta_trials'], r['meta_purchases'])
                    for r in processed_records
                ]
            elif table_name == 'ad_performance_daily_device':
                sql = """
                INSERT OR REPLACE INTO ad_performance_daily_device 
                (ad_id, date, device, adset_id, campaign_id, ad_name, adset_name, campaign_name, 
                 spend, impressions, clicks, meta_trials, meta_purchases)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                values = [
                    (r['ad_id'], r['date'], r['device'], r['adset_id'], r['campaign_id'], 
                     r['ad_name'], r['adset_name'], r['campaign_name'],
                     r['spend'], r['impressions'], r['clicks'], r['meta_trials'], r['meta_purchases'])
                    for r in processed_records
                ]
            
            else:
                logger.error(f"   ❌ Unknown table: {table_name}")
                conn.close()
                return 0
            
            # Execute batch insert
            cursor.executemany(sql, values)
            conn.commit()
            
            loaded_count = cursor.rowcount
            conn.close()
            
            logger.info(f"   ✅ Loaded {loaded_count} records to {table_name}")
            return loaded_count
            
        except Exception as e:
            logger.error(f"   ❌ Error loading to {table_name}: {e}")
            return 0
    
    def check_existing_dates_in_table(self, table_name: str, start_date: str, end_date: str) -> List[str]:
        """
        Check which dates already exist in a specific table
        
        Args:
            table_name: Name of the table to check
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of dates that already exist in the table
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT DISTINCT date 
                FROM {table_name} 
                WHERE date BETWEEN ? AND ?
                ORDER BY date
            ''', (start_date, end_date))
            
            existing_dates = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            logger.info(f"📅 {table_name}: Found {len(existing_dates)} existing dates between {start_date} and {end_date}")
            return existing_dates
            
        except Exception as e:
            logger.error(f"❌ Error checking existing dates in {table_name}: {e}")
            return []
    
    def calculate_missing_dates(self, start_date: str, end_date: str, existing_dates: List[str]) -> List[str]:
        """
        Calculate which dates are missing from the date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            existing_dates: List of dates that already exist
            
        Returns:
            List of missing dates that need to be filled
        """
        # Generate all dates in range
        all_dates = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            all_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # Find missing dates
        existing_dates_set = set(existing_dates)
        missing_dates = [date for date in all_dates if date not in existing_dates_set]
        
        logger.info(f"📊 Date analysis:")
        logger.info(f"   Total dates in range: {len(all_dates)}")
        logger.info(f"   Existing dates: {len(existing_dates)}")
        logger.info(f"   Missing dates: {len(missing_dates)}")
        
        return missing_dates
    
    def update_specific_breakdown_table(self, 
                                      table_name: str, 
                                      breakdown_type: str, 
                                      start_date: str, 
                                      end_date: str, 
                                      skip_existing: bool = True) -> bool:
        """
        Update a specific breakdown table for a custom date range
        
        Args:
            table_name: Target table name (e.g., 'ad_performance_daily_country')
            breakdown_type: Breakdown type ('country', 'region', 'device', or None)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            skip_existing: If True, skip dates that already have data; if False, overwrite
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 UPDATING SPECIFIC TABLE: {table_name}")
        logger.info(f"🔍 Breakdown: {breakdown_type or 'no breakdown'}")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"⏭️  Skip existing: {skip_existing}")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # Step 1: Determine which dates to process
            if skip_existing:
                existing_dates = self.check_existing_dates_in_table(table_name, start_date, end_date)
                dates_to_update = self.calculate_missing_dates(start_date, end_date, existing_dates)
                
                if not dates_to_update:
                    logger.info("✅ No missing dates found - table is already complete for this range")
                    return True
            else:
                # Generate all dates in range (overwrite mode)
                dates_to_update = []
                current_date = datetime.strptime(start_date, '%Y-%m-%d')
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                
                while current_date <= end_date_obj:
                    dates_to_update.append(current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
                
                logger.info(f"📊 Will process {len(dates_to_update)} dates (overwrite mode)")
            
            # Step 2: Process dates in chunks
            chunk_size = 3
            total_success = 0
            total_requests = 0
            
            for i in range(0, len(dates_to_update), chunk_size):
                chunk_dates = dates_to_update[i:i+chunk_size]
                from_date = chunk_dates[0]
                to_date = chunk_dates[-1]
                
                total_requests += 1
                
                logger.info(f"📡 REQUEST {total_requests}: {from_date} to {to_date}")
                
                # Fetch raw data
                raw_records = self.fetch_meta_data(
                    from_date=from_date,
                    to_date=to_date,
                    breakdown_type=breakdown_type
                )
                
                if not raw_records:
                    logger.warning(f"   ⚠️  No data fetched")
                    continue
                
                # Process records
                processed_records = self.process_meta_records(raw_records, breakdown_type)
                
                if not processed_records:
                    logger.warning(f"   ⚠️  No records processed")
                    continue
                
                # Log summary
                total_trials = sum(r.get('meta_trials', 0) for r in processed_records)
                total_purchases = sum(r.get('meta_purchases', 0) for r in processed_records)
                total_spend = sum(r.get('spend', 0) for r in processed_records)
                
                logger.info(f"   📊 Data summary:")
                logger.info(f"      Records: {len(processed_records)}")
                logger.info(f"      Total spend: ${total_spend:.2f}")
                logger.info(f"      Total trials: {total_trials}")
                logger.info(f"      Total purchases: {total_purchases}")
                
                # Load to database
                loaded_count = self.load_data_to_table(processed_records, table_name)
                
                if loaded_count > 0:
                    total_success += 1
                    logger.info(f"   🎯 SUCCESS: {loaded_count} records loaded")
                else:
                    logger.warning(f"   ⚠️  FAILED: Could not load data")
                
                # Rate limiting
                if i + chunk_size < len(dates_to_update):
                    logger.info("   ⏳ Waiting 5 seconds (rate limiting)...")
                    time.sleep(5)
            
            # Final summary
            elapsed_time = time.time() - start_time
            success_rate = (total_success / total_requests * 100) if total_requests > 0 else 0
            
            logger.info(f"🎯 SPECIFIC TABLE UPDATE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"📊 Summary:")
            logger.info(f"   Table: {table_name}")
            logger.info(f"   Total API requests: {total_requests}")
            logger.info(f"   Successful requests: {total_success}")
            logger.info(f"   Success rate: {success_rate:.1f}%")
            logger.info(f"   Elapsed time: {elapsed_time:.1f} seconds")
            logger.info(f"   Date range processed: {dates_to_update[0]} to {dates_to_update[-1] if dates_to_update else 'None'}")
            
            return total_success > 0
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"❌ Specific table update failed after {elapsed_time:.1f} seconds: {e}")
            return False
    
    def update_meta_data(self) -> bool:
        """
        Main function to update Meta data from most recent date to today
        
        Returns:
            True if update was successful, False otherwise
        """
        logger.info("🚀 STARTING META DATA UPDATE")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # Step 1: Find most recent data in database
            most_recent_date = self.get_most_recent_date_in_db()
            
            # Step 2: Calculate dates to update
            dates_to_update = self.calculate_dates_to_update(most_recent_date)
            
            if not dates_to_update:
                logger.info("✅ No dates to update - database is already current")
                return True
            
            # Step 3: Define table configurations
            table_configs = [
                {
                    'table': 'ad_performance_daily',
                    'breakdown': None,
                    'description': 'no breakdown'
                },
                # BREAKDOWN TABLES (commented out for now, uncomment when ready)
                # {
                #     'table': 'ad_performance_daily_country',
                #     'breakdown': 'country',
                #     'description': 'country breakdown'
                # },
                # {
                #     'table': 'ad_performance_daily_region',
                #     'breakdown': 'region',
                #     'description': 'region breakdown'
                # },
                # {
                #     'table': 'ad_performance_daily_device',
                #     'breakdown': 'device',
                #     'description': 'device breakdown'
                # }
            ]
            
            # Step 4: Process each table
            total_success = 0
            total_requests = 0
            
            for table_config in table_configs:
                table_name = table_config['table']
                breakdown_type = table_config['breakdown']
                description = table_config['description']
                
                logger.info(f"📋 PROCESSING TABLE: {table_name}")
                logger.info(f"🔍 Breakdown: {description}")
                logger.info("-" * 50)
                
                # Process dates in chunks of 3 days to manage API rate limits
                chunk_size = 3
                
                for i in range(0, len(dates_to_update), chunk_size):
                    chunk_dates = dates_to_update[i:i+chunk_size]
                    from_date = chunk_dates[0]
                    to_date = chunk_dates[-1]
                    
                    total_requests += 1
                    
                    logger.info(f"📡 REQUEST {total_requests}: {from_date} to {to_date}")
                    
                    # Step 1: Fetch raw data
                    raw_records = self.fetch_meta_data(
                        from_date=from_date,
                        to_date=to_date,
                        breakdown_type=breakdown_type
                    )
                    
                    if not raw_records:
                        logger.warning(f"   ⚠️  No data fetched")
                        continue
                    
                    # Step 2: Process records
                    processed_records = self.process_meta_records(raw_records, breakdown_type)
                    
                    if not processed_records:
                        logger.warning(f"   ⚠️  No records processed")
                        continue
                    
                    # Step 3: Log summary
                    total_trials = sum(r.get('meta_trials', 0) for r in processed_records)
                    total_purchases = sum(r.get('meta_purchases', 0) for r in processed_records)
                    total_spend = sum(r.get('spend', 0) for r in processed_records)
                    
                    logger.info(f"   📊 Data summary:")
                    logger.info(f"      Records: {len(processed_records)}")
                    logger.info(f"      Total spend: ${total_spend:.2f}")
                    logger.info(f"      Total trials: {total_trials}")
                    logger.info(f"      Total purchases: {total_purchases}")
                    
                    # Step 4: Load to database
                    loaded_count = self.load_data_to_table(processed_records, table_name)
                    
                    if loaded_count > 0:
                        total_success += 1
                        logger.info(f"   🎯 SUCCESS: {loaded_count} records loaded")
                    else:
                        logger.warning(f"   ⚠️  FAILED: Could not load data")
                    
                    # Rate limiting: 5-second delay between requests
                    if i + chunk_size < len(dates_to_update):
                        logger.info("   ⏳ Waiting 5 seconds (rate limiting)...")
                        time.sleep(5)
                
                logger.info(f"✅ Completed {table_name}")
                logger.info("")
            
            # Step 5: Final summary
            elapsed_time = time.time() - start_time
            success_rate = (total_success / total_requests * 100) if total_requests > 0 else 0
            
            logger.info("🎯 META DATA UPDATE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"📊 Summary:")
            logger.info(f"   Total API requests: {total_requests}")
            logger.info(f"   Successful requests: {total_success}")
            logger.info(f"   Success rate: {success_rate:.1f}%")
            logger.info(f"   Elapsed time: {elapsed_time:.1f} seconds")
            logger.info(f"   Date range updated: {dates_to_update[0]} to {dates_to_update[-1]}")
            
            return total_success > 0
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"❌ Meta data update failed after {elapsed_time:.1f} seconds: {e}")
            return False


def main():
    """
    Main function for the Meta data update module.
    
    This function is called by the pipeline orchestrator.
    """
    logger.info("🔮 Starting Meta Data Update Module")
    
    try:
        updater = MetaDataUpdater()
        success = updater.update_meta_data()
        
        if success:
            logger.info("✅ Meta data update completed successfully")
            return True
        else:
            logger.error("❌ Meta data update completed with errors")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error in Meta data update module: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 