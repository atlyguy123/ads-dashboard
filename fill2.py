#!/usr/bin/env python3

import sys
import os
import sqlite3
import json
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Import ONLY production analytics pipeline code
from analytics_pipeline.meta_processor import MetaDataProcessor

class RateLimitMonitor:
    """Monitor and log Meta API rate limiting in real-time"""
    
    def __init__(self):
        self.monitoring = False
        self.log_data = []
        self.current_request_info = {}
        self.log_file = f"meta_rate_limit_logs_{now_in_timezone().strftime('%Y%m%d_%H%M%S')}.json"
        
    def start_monitoring(self):
        """Start real-time monitoring thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print("🔍 Started real-time rate limit monitoring (every 2 seconds)")
        print(f"📄 Incremental logging to: {self.log_file}")
        
    def stop_monitoring(self):
        """Stop monitoring and save final logs"""
        self.monitoring = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=5)
        self._save_final_logs()
        
    def log_request_start(self, table_name, breakdown_type, date_range):
        """Log when a request starts"""
        self.current_request_info = {
            'table': table_name,
            'breakdown': breakdown_type,
            'date_range': date_range,
            'start_time': time.time(),
            'status': 'in_progress'
        }
        
    def log_request_complete(self, record_count, duration, rate_limit_headers=None, processing_mode=None, api_sample=None):
        """Log when a request completes"""
        self.current_request_info.update({
            'end_time': time.time(),
            'duration': duration,
            'record_count': record_count,
            'status': 'completed',
            'rate_limit_headers': rate_limit_headers or {},
            'processing_mode': processing_mode,
            'api_response_sample': api_sample
        })
        
        # Add to permanent log and save incrementally
        self.log_data.append(self.current_request_info.copy())
        self._save_incremental()
        
    def log_request_error(self, error_message, error_details=None):
        """Log when a request fails"""
        self.current_request_info.update({
            'end_time': time.time(),
            'status': 'failed',
            'error': error_message,
            'error_details': error_details or {}
        })
        self.log_data.append(self.current_request_info.copy())
        self._save_incremental()
        
    def log_async_fallback(self, reason):
        """Log when we fall back from async to sync"""
        self.current_request_info.update({
            'fallback_reason': reason,
            'processing_mode': 'async_fallback_to_sync'
        })
        
    def _monitor_loop(self):
        """Real-time monitoring loop"""
        while self.monitoring:
            timestamp = now_in_timezone().isoformat()
            
            if self.current_request_info:
                status = self.current_request_info.get('status', 'unknown')
                table = self.current_request_info.get('table', 'unknown')
                breakdown = self.current_request_info.get('breakdown', 'none')
                
                if status == 'in_progress':
                    elapsed = time.time() - self.current_request_info.get('start_time', 0)
                    print(f"⏱️  {timestamp}: Processing {table} ({breakdown}) - {elapsed:.1f}s elapsed")
                
            time.sleep(2)  # Real-time updates every 2 seconds
            
    def _save_incremental(self):
        """Save logs incrementally after each request"""
        log_summary = {
            'session_info': {
                'start_time': now_in_timezone().isoformat(),
                'total_requests': len(self.log_data),
                'session_duration': sum(req.get('duration', 0) for req in self.log_data if req.get('duration')),
                'last_updated': now_in_timezone().isoformat()
            },
            'requests': self.log_data
        }
        
        try:
            with open(self.log_file, 'w') as f:
            json.dump(log_summary, f, indent=2)
        except Exception as e:
            print(f"⚠️ Warning: Could not save incremental log: {e}")
            
    def _save_final_logs(self):
        """Save final logs"""
        print(f"📊 Final rate limit logs saved to: {self.log_file}")
        print(f"📈 Total requests logged: {len(self.log_data)}")

class DiagnosticMetaDataProcessor(MetaDataProcessor):
    """Extended MetaDataProcessor with comprehensive diagnostic logging"""
    
    def fetch_meta_data_with_diagnostic_processing(self, breakdown_type: str = None, 
                                                 from_date: str = None, to_date: str = None,
                                                 monitor: RateLimitMonitor = None):
        """
        Diagnostic Meta API processing with detailed logging of API responses and processing
        """
        from meta.services.meta_service import fetch_meta_data
        
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
        
        print(f"   🔄 Trying ASYNC processing first...")
        print(f"   📊 Fields: {fields}")
        print(f"   📊 Breakdowns: {breakdowns}")
        
        try:
            # First try: Async processing (default)
            api_response, error = fetch_meta_data(
                start_date=from_date,
                end_date=to_date,
                time_increment=1,
                fields=fields,
                breakdowns=breakdowns,
                use_async=True
            )
            
            if error:
                print(f"   ⚠️ Async failed: {error}")
                if monitor:
                    monitor.log_async_fallback(f"Async API error: {error}")
                return self._try_sync_fallback_with_diagnostics(from_date, to_date, fields, breakdowns, monitor)
            
            # Handle async job response
            if api_response.get('async_job'):
                report_run_id = api_response.get('report_run_id')
                print(f"   ✅ Async job started: {report_run_id}")
                
                # Wait for completion
                records = self._wait_for_async_job_completion(report_run_id)
                
                if not records:
                    print(f"   ⚠️ Async job failed or returned no data")
                    if monitor:
                        monitor.log_async_fallback("Async job failed or empty results")
                    return self._try_sync_fallback_with_diagnostics(from_date, to_date, fields, breakdowns, monitor)
                
                print(f"   ✅ Async processing successful: {len(records)} records")
                
                # DIAGNOSTIC: Log sample API response
                self._log_api_response_sample(records, breakdown_type, monitor)
                
                if monitor:
                    monitor.current_request_info['processing_mode'] = 'async_success'
                return records
            
            # Handle synchronous response (shouldn't happen with use_async=True, but just in case)
            if isinstance(api_response, dict) and 'data' in api_response:
                records = api_response['data'].get('data', [])
                print(f"   ✅ Sync response (unexpected): {len(records)} records")
                
                # DIAGNOSTIC: Log sample API response
                self._log_api_response_sample(records, breakdown_type, monitor)
                
                if monitor:
                    monitor.current_request_info['processing_mode'] = 'sync_unexpected'
                return records
            
            # Unknown response format
            print(f"   ⚠️ Unknown response format: {type(api_response)}")
            if monitor:
                monitor.log_async_fallback(f"Unknown response format: {type(api_response)}")
            return self._try_sync_fallback_with_diagnostics(from_date, to_date, fields, breakdowns, monitor)
            
        except Exception as e:
            print(f"   ❌ Async processing exception: {e}")
            if monitor:
                monitor.log_async_fallback(f"Async exception: {str(e)}")
            return self._try_sync_fallback_with_diagnostics(from_date, to_date, fields, breakdowns, monitor)
    
    def _try_sync_fallback_with_diagnostics(self, from_date, to_date, fields, breakdowns, monitor):
        """Try synchronous processing as fallback with diagnostic logging"""
        from meta.services.meta_service import fetch_meta_data
        
        print(f"   🔄 Falling back to SYNC processing...")
        
        try:
            api_response, error = fetch_meta_data(
                start_date=from_date,
                end_date=to_date,
                time_increment=1,
                fields=fields,
                breakdowns=breakdowns,
                use_async=False  # Force synchronous
            )
            
            if error:
                print(f"   ❌ Sync also failed: {error}")
                if monitor:
                    monitor.log_request_error(f"Both async and sync failed", {
                        'sync_error': error,
                        'fields': fields,
                        'breakdowns': breakdowns,
                        'date_range': f"{from_date} to {to_date}"
                    })
                return []
            
            # Handle synchronous response
            if isinstance(api_response, dict) and 'data' in api_response:
                records = api_response['data'].get('data', [])
                print(f"   ✅ Sync fallback successful: {len(records)} records")
                
                # DIAGNOSTIC: Log sample API response
                self._log_api_response_sample(records, breakdowns, monitor)
                
                if monitor:
                    monitor.current_request_info['processing_mode'] = 'sync_fallback_success'
                return records
            
            print(f"   ❌ Sync returned unexpected format: {type(api_response)}")
            if monitor:
                monitor.log_request_error("Sync returned unexpected format", {
                    'response_type': str(type(api_response)),
                    'response_sample': str(api_response)[:500] if api_response else None
                })
            return []
            
        except Exception as e:
            print(f"   ❌ Sync processing exception: {e}")
            if monitor:
                monitor.log_request_error(f"Sync exception: {str(e)}", {
                    'exception_type': type(e).__name__,
                    'fields': fields,
                    'breakdowns': breakdowns
                })
            return []
    
    def _log_api_response_sample(self, records, breakdown_type, monitor):
        """Log detailed sample of API response for diagnostic purposes"""
        if not records:
            print(f"   🔍 No records to sample")
            return
        
        # Take first 3 records as sample
        sample_records = records[:3]
        
        print(f"   🔍 API RESPONSE DIAGNOSTIC SAMPLE ({breakdown_type or 'no breakdown'}):")
        for i, record in enumerate(sample_records):
            print(f"   📝 Record {i+1}:")
            print(f"      ad_id: {record.get('ad_id', 'N/A')}")
            print(f"      spend: {record.get('spend', 'N/A')}")
            
            # Log breakdown fields
            if breakdown_type == "country":
                print(f"      country: {record.get('country', 'N/A')}")
            elif breakdown_type == "region":
                print(f"      region: {record.get('region', 'N/A')}")
            elif breakdown_type == "device":
                print(f"      impression_device: {record.get('impression_device', 'N/A')}")
            
            # CRITICAL: Log actions and conversions structure
            actions = record.get('actions', [])
            conversions = record.get('conversions', [])
            
            print(f"      actions: {len(actions) if isinstance(actions, list) else 'Not a list'}")
            if isinstance(actions, list) and actions:
                for action in actions[:3]:  # First 3 actions
                    print(f"        - {action.get('action_type', 'N/A')}: {action.get('value', 'N/A')}")
            
            print(f"      conversions: {len(conversions) if isinstance(conversions, list) else 'Not a list'}")
            if isinstance(conversions, list) and conversions:
                for conversion in conversions[:3]:  # First 3 conversions
                    print(f"        - {conversion.get('action_type', 'N/A')}: {conversion.get('value', 'N/A')}")
            
            if not actions and not conversions:
                print(f"      ⚠️  NO ACTIONS OR CONVERSIONS FOUND!")
        
        # Store sample in monitor for JSON logging
        if monitor:
            monitor.current_request_info['api_response_sample'] = {
                'total_records': len(records),
                'sample_count': len(sample_records),
                'breakdown_type': breakdown_type,
                'sample_records': sample_records
            }

def get_missing_dates_for_table(table_name):
    """Check database to find missing May dates for a specific table"""
    conn = sqlite3.connect('meta_analytics.db')
    cursor = conn.cursor()
    
    # Get existing dates
    cursor.execute(f'SELECT DISTINCT date FROM {table_name} WHERE date BETWEEN ? AND ? ORDER BY date', 
                  ('2025-05-01', '2025-05-31'))
    existing_dates = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    # Generate all May dates
    all_may_dates = []
    for day in range(1, 32):  # May has 31 days
        all_may_dates.append(f"2025-05-{day:02d}")
    
    # Find missing dates
    missing_dates = [date for date in all_may_dates if date not in existing_dates]
    
    return missing_dates, len(existing_dates)

def fill_meta_analytics_tables_diagnostic(test_mode=False):
    """Fill missing Meta analytics data with comprehensive diagnostic logging"""
    
    print("🔬 META ANALYTICS DIAGNOSTIC DATA COLLECTION")
    print("=" * 80)
    
    if test_mode:
        print("🧪 TEST MODE: Processing only first 2-4 days for diagnosis")
        print("📅 Target: May 1-4, 2025 (Test Range)")
    else:
        print("📅 Target: May 2025 - Missing Data Only")
    
    print("🔄 Method: Async First → Sync Fallback")
    print("⏱️  Delays: 5 seconds between requests")
    print("📊 Monitoring: Real-time + Comprehensive JSON logging")
    print("🔬 Diagnostics: API response structure analysis")
    print("📦 Processing: 2-day increments")
    print()
    
    # Initialize components
    processor = DiagnosticMetaDataProcessor()
    monitor = RateLimitMonitor()
    
    # Start monitoring
    monitor.start_monitoring()
    
    # Define tables and check missing data
    table_configs = [
        {
            'table': 'ad_performance_daily',
            'breakdown': None,
            'description': 'no breakdown'
        },
        {
            'table': 'ad_performance_daily_country',
            'breakdown': 'country',
            'description': 'country breakdown'
        },
        {
            'table': 'ad_performance_daily_region',
            'breakdown': 'region',
            'description': 'region breakdown'
        },
        {
            'table': 'ad_performance_daily_device',
            'breakdown': 'device',
            'description': 'device breakdown (impression_device)'
        }
    ]
    
    # Check what's missing for each table
    print("📊 CHECKING EXISTING DATA")
    print("=" * 60)
    
    total_api_calls = 0
    for table_config in table_configs:
        table_name = table_config['table']
        missing_dates, existing_count = get_missing_dates_for_table(table_name)
        
        if test_mode:
            # In test mode, only process May 1-4 regardless of existing data
            test_dates = ['2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04']
            table_config['missing_dates'] = test_dates
        else:
            table_config['missing_dates'] = missing_dates
        
        print(f"📋 {table_name}:")
        print(f"   ✅ Existing: {existing_count}/31 dates")
        if test_mode:
            print(f"   🧪 Test processing: {len(table_config['missing_dates'])} dates (May 1-4)")
        else:
            print(f"   ❌ Missing: {len(missing_dates)} dates")
        
        if table_config['missing_dates']:
            chunks = (len(table_config['missing_dates']) + 1) // 2  # 2-day chunks
            total_api_calls += chunks
            print(f"   📡 API calls needed: {chunks}")
        print()
    
    if total_api_calls == 0 and not test_mode:
        print("🎉 All data already exists! Nothing to process.")
        monitor.stop_monitoring()
        return
    
    print(f"📡 Total API calls needed: {total_api_calls}")
    print()
    
    current_request = 0
    
    # Process each table sequentially
    for table_config in table_configs:
        table_name = table_config['table']
        breakdown_type = table_config['breakdown']
        description = table_config['description']
        missing_dates = table_config['missing_dates']
        
        if not missing_dates:
            print(f"✅ {table_name}: All data exists - skipping")
            print()
            continue
        
        print(f"📋 PROCESSING TABLE: {table_name}")
        print(f"🔍 Breakdown: {description}") 
        print(f"📅 Processing dates: {len(missing_dates)} dates")
        print("-" * 60)
        
        # Process dates in 2-day chunks
        for i in range(0, len(missing_dates), 2):
            chunk_dates = missing_dates[i:i+2]
            from_date = chunk_dates[0]
            to_date = chunk_dates[-1]  # Last date in chunk
            
            current_request += 1
            date_range = f"{from_date} to {to_date}"
            
            print(f"📡 REQUEST {current_request}/{total_api_calls}: {table_name}")
            print(f"   Breakdown: {breakdown_type or 'none'}")
            print(f"   Date range: {date_range}")
            print(f"   Dates: {', '.join(chunk_dates)}")
            
            # Log request start
            monitor.log_request_start(table_name, breakdown_type, date_range)
            
            try:
                # Diagnostic processing: async first, sync fallback
                start_time = time.time()
                
                print(f"   🔄 Fetching data...")
                raw_records = processor.fetch_meta_data_with_diagnostic_processing(
                    breakdown_type=breakdown_type,
                    from_date=from_date,
                    to_date=to_date,
                    monitor=monitor
                )
                
                fetch_duration = time.time() - start_time
                print(f"   ✅ Fetched {len(raw_records)} records in {fetch_duration:.2f}s")
                
                # DIAGNOSTIC: Process records with detailed logging
                print(f"   🔄 Processing records with ActionMapper...")
                processed_records = processor._process_meta_records(raw_records, breakdown_type)
                print(f"   ✅ Processed {len(processed_records)} records")
                
                # DIAGNOSTIC: Check for conversion data in processed records
                if processed_records:
                    trials_found = sum(1 for r in processed_records if r.get('meta_trials', 0) > 0)
                    purchases_found = sum(1 for r in processed_records if r.get('meta_purchases', 0) > 0)
                    total_trials = sum(r.get('meta_trials', 0) for r in processed_records)
                    total_purchases = sum(r.get('meta_purchases', 0) for r in processed_records)
                    
                    print(f"   🔍 CONVERSION ANALYSIS:")
                    print(f"      Records with trials: {trials_found}/{len(processed_records)}")
                    print(f"      Records with purchases: {purchases_found}/{len(processed_records)}")
                    print(f"      Total trials: {total_trials}")
                    print(f"      Total purchases: {total_purchases}")
                
                if test_mode:
                    print(f"   🧪 TEST MODE: Skipping database load")
                    loaded_count = len(processed_records)
                else:
                # Step 3: Load to database using production method
                print(f"   🔄 Loading to database...")
                loaded_count = processor._load_to_table(processed_records, table_name)
                print(f"   ✅ Loaded {loaded_count} records to {table_name}")
                
                total_duration = time.time() - start_time
                
                # Log successful completion
                processing_mode = monitor.current_request_info.get('processing_mode', 'unknown')
                api_sample = monitor.current_request_info.get('api_response_sample')
                monitor.log_request_complete(
                    record_count=loaded_count,
                    duration=total_duration,
                    processing_mode=processing_mode,
                    api_sample=api_sample
                )
                
                print(f"   ⏱️  Total duration: {total_duration:.2f}s")
                print(f"   🎯 Status: SUCCESS ({processing_mode})")
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ Error: {error_msg}")
                
                # Log error with details
                monitor.log_request_error(error_msg, {
                    'table': table_name,
                    'breakdown': breakdown_type,
                    'date_range': date_range,
                    'exception_type': type(e).__name__
                })
                
                # Continue with next request instead of failing completely
                print(f"   ⚠️  Continuing with next request...")
            
            print()
            
            # 5-second delay between requests (except for last request)
            if current_request < total_api_calls:
                print("⏳ Waiting 5 seconds before next request...")
                time.sleep(5)
                print()
    
    # Stop monitoring and save logs
    monitor.stop_monitoring()
    
    if not test_mode:
    # Final verification
    print("📊 FINAL VERIFICATION")
    print("=" * 60)
    
    conn = sqlite3.connect('meta_analytics.db')
    cursor = conn.cursor()
    
    for table_config in table_configs:
        table_name = table_config['table']
        
            # Check May 1-31 data
        cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE date BETWEEN ? AND ?', 
                          ('2025-05-01', '2025-05-31'))
        total_count = cursor.fetchone()[0]
        
        # Check distinct dates
        cursor.execute(f'SELECT COUNT(DISTINCT date) FROM {table_name} WHERE date BETWEEN ? AND ?', 
                          ('2025-05-01', '2025-05-31'))
        date_count = cursor.fetchone()[0]
        
            print(f"✅ {table_name}: {total_count} records across {date_count}/31 dates")
    
    conn.close()
    
    print("\n🔬 DIAGNOSTIC DATA COLLECTION COMPLETE")
    print("📊 Check the JSON file for detailed API response analysis")
    print("🔧 Used async-first with sync fallback + comprehensive diagnostics")

if __name__ == "__main__":
    # Run in test mode to diagnose conversion tracking issues
    fill_meta_analytics_tables_diagnostic(test_mode=True) 