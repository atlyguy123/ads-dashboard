#!/usr/bin/env python3

import sys
import os
import sqlite3
import json
import time
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Import production analytics pipeline code
from analytics_pipeline.meta_processor import MetaDataProcessor

class AdPerformanceDailyFiller:
    """Focused class for filling missing data in ad_performance_daily table"""
    
    def __init__(self):
        self.processor = MetaDataProcessor()
        self.db_path = 'meta_analytics.db'
        
    def get_missing_dates(self, start_date='2025-01-01', end_date=None):
        """Get list of missing dates from the database"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get existing dates in range
            cursor.execute(
                'SELECT DISTINCT date FROM ad_performance_daily WHERE date BETWEEN ? AND ? ORDER BY date', 
                (start_date, end_date)
            )
            existing_dates = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            # Generate all dates in range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            all_dates = []
            current = start_dt
            while current <= end_dt:
                all_dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            
            # Find missing dates
            missing_dates = [date for date in all_dates if date not in existing_dates]
            
            print(f"📅 Date Analysis ({start_date} to {end_date}):")
            print(f"   Total days in range: {len(all_dates)}")
            print(f"   Days with data: {len(existing_dates)}")
            print(f"   Missing days: {len(missing_dates)}")
            
            return missing_dates
            
        except Exception as e:
            print(f"❌ Error checking missing dates: {e}")
            return []
    
    def fetch_data_for_single_date(self, date):
        """Fetch Meta data for a single date - for testing"""
        print(f"📡 Fetching data for {date}...")
        
        try:
            # Use the production processor to fetch data
            raw_records = self.processor.fetch_meta_data_with_rate_limiting(
                breakdown_type=None,  # No breakdown for ad_performance_daily
                from_date=date,
                to_date=date
            )
            
            print(f"   ✅ Fetched {len(raw_records)} raw records")
            
            if not raw_records:
                print(f"   ⚠️  No data returned from API for {date}")
                return []
            
            # Process the records
            processed_records = self.processor._process_meta_records(raw_records, None)
            print(f"   ✅ Processed {len(processed_records)} records")
            
            return processed_records
            
        except Exception as e:
            print(f"   ❌ Error fetching data for {date}: {e}")
            return []
    
    def load_data_to_db(self, processed_records, test_mode=False):
        """Load processed data to database"""
        if not processed_records:
            print("   ⚠️  No records to load")
            return 0
        
        if test_mode:
            print("   🧪 TEST MODE: Would load the following records:")
            for i, record in enumerate(processed_records[:3], 1):  # Show first 3
                print(f"      Record {i}: ad_id={record.get('ad_id')}, date={record.get('date')}, spend={record.get('spend')}")
            if len(processed_records) > 3:
                print(f"      ... and {len(processed_records) - 3} more records")
            return len(processed_records)
        
        # Load to database using production method
        try:
            loaded_count = self.processor._load_to_table(processed_records, "ad_performance_daily")
            print(f"   ✅ Loaded {loaded_count} records to ad_performance_daily")
            return loaded_count
        except Exception as e:
            print(f"   ❌ Error loading to database: {e}")
            return 0
    
    def verify_date_data(self, date):
        """Verify that data was loaded correctly for a specific date"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get records for the date
            cursor.execute(
                'SELECT COUNT(*), SUM(spend), SUM(impressions), SUM(clicks), SUM(meta_trials), SUM(meta_purchases) FROM ad_performance_daily WHERE date = ?',
                (date,)
            )
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                count, total_spend, total_impressions, total_clicks, total_trials, total_purchases = result
                print(f"   ✅ Verification for {date}:")
                print(f"      Records: {count}")
                print(f"      Total spend: ${total_spend:.2f}")
                print(f"      Total impressions: {total_impressions:,}")
                print(f"      Total clicks: {total_clicks:,}")
                print(f"      Total trials: {total_trials}")
                print(f"      Total purchases: {total_purchases}")
                
                # Get sample records
                cursor.execute('SELECT ad_id, ad_name, spend, impressions FROM ad_performance_daily WHERE date = ? LIMIT 3', (date,))
                samples = cursor.fetchall()
                print(f"      Sample records:")
                for i, (ad_id, ad_name, spend, impressions) in enumerate(samples, 1):
                    print(f"        {i}. Ad {ad_id} ({ad_name}): ${spend:.2f}, {impressions:,} impressions")
                
                conn.close()
                return True
            else:
                print(f"   ❌ No data found for {date}")
                conn.close()
                return False
                
        except Exception as e:
            print(f"   ❌ Error verifying data: {e}")
            return False
    
    def test_single_date(self, date):
        """Test the complete process for a single date"""
        print(f"\n🧪 MICRO-TEST: Single Date ({date})")
        print("=" * 50)
        
        # Step 1: Fetch data
        processed_records = self.fetch_data_for_single_date(date)
        if not processed_records:
            print(f"❌ Failed to fetch data for {date}")
            return False
        
        # Step 2: Load to database
        loaded_count = self.load_data_to_db(processed_records, test_mode=False)
        if loaded_count == 0:
            print(f"❌ Failed to load data for {date}")
            return False
        
        # Step 3: Verify
        time.sleep(1)  # Brief pause
        success = self.verify_date_data(date)
        
        if success:
            print(f"✅ Single date test PASSED for {date}")
            return True
        else:
            print(f"❌ Single date test FAILED for {date}")
            return False
    
    def test_multiple_dates(self, dates):
        """Test the complete process for multiple dates"""
        print(f"\n🧪 MULTI-DATE TEST: {len(dates)} dates")
        print("=" * 50)
        
        successful_dates = []
        failed_dates = []
        
        for i, date in enumerate(dates, 1):
            print(f"\n📅 Processing date {i}/{len(dates)}: {date}")
            
            # Fetch data
            processed_records = self.fetch_data_for_single_date(date)
            if not processed_records:
                print(f"   ❌ No data for {date}")
                failed_dates.append(date)
                continue
            
            # Load to database
            loaded_count = self.load_data_to_db(processed_records, test_mode=False)
            if loaded_count == 0:
                print(f"   ❌ Failed to load {date}")
                failed_dates.append(date)
                continue
            
            # Quick verification
            time.sleep(0.5)
            if self.verify_date_data(date):
                successful_dates.append(date)
                print(f"   ✅ Success: {date}")
            else:
                failed_dates.append(date)
                print(f"   ❌ Failed verification: {date}")
            
            # 3-second delay between requests
            if i < len(dates):
                print("   ⏳ Waiting 3 seconds before next request...")
                time.sleep(3)
        
        print(f"\n📊 Multi-date test results:")
        print(f"   ✅ Successful: {len(successful_dates)}")
        print(f"   ❌ Failed: {len(failed_dates)}")
        
        if failed_dates:
            print(f"   Failed dates: {failed_dates}")
        
        return len(successful_dates) == len(dates)
    
    def fill_missing_data(self, batch_size=5, delay_seconds=3):
        """Fill all missing data with batching"""
        print(f"\n🚀 FULL DATA FILL")
        print("=" * 50)
        
        # Get missing dates
        missing_dates = self.get_missing_dates()
        
        if not missing_dates:
            print("✅ No missing dates! Database is complete.")
            return
        
        print(f"📋 Will process {len(missing_dates)} missing dates")
        print(f"⚙️  Batch size: {batch_size} dates")
        print(f"⏱️  Delay between batches: {delay_seconds} seconds")
        print()
        
        # Process in batches
        successful_dates = []
        failed_dates = []
        
        for i in range(0, len(missing_dates), batch_size):
            batch = missing_dates[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(missing_dates) + batch_size - 1) // batch_size
            
            print(f"📦 BATCH {batch_num}/{total_batches}: {len(batch)} dates")
            print(f"   Dates: {batch[0]} to {batch[-1]}")
            
            # Process batch
            batch_successful = []
            batch_failed = []
            
            for j, date in enumerate(batch, 1):
                print(f"   📅 Processing {j}/{len(batch)}: {date}")
                
                # Fetch and load
                processed_records = self.fetch_data_for_single_date(date)
                if processed_records:
                    loaded_count = self.load_data_to_db(processed_records)
                    if loaded_count > 0:
                        batch_successful.append(date)
                        print(f"      ✅ Success: {loaded_count} records")
                    else:
                        batch_failed.append(date)
                        print(f"      ❌ Load failed")
                else:
                    batch_failed.append(date)
                    print(f"      ❌ Fetch failed")
                
                # Short delay between dates in batch
                if j < len(batch):
                    time.sleep(1)
            
            successful_dates.extend(batch_successful)
            failed_dates.extend(batch_failed)
            
            print(f"   📊 Batch results: {len(batch_successful)} success, {len(batch_failed)} failed")
            
            # Delay between batches
            if i + batch_size < len(missing_dates):
                print(f"   ⏳ Waiting {delay_seconds} seconds before next batch...")
                time.sleep(delay_seconds)
            
            print()
        
        # Final results
        print(f"🎯 FINAL RESULTS:")
        print(f"   ✅ Successfully filled: {len(successful_dates)}/{len(missing_dates)} dates")
        print(f"   ❌ Failed: {len(failed_dates)}")
        
        if failed_dates:
            print(f"   Failed dates: {failed_dates[:10]}")
            if len(failed_dates) > 10:
                print(f"   ... and {len(failed_dates) - 10} more")
        
        # Final verification
        remaining_missing = self.get_missing_dates()
        print(f"\n✅ Remaining missing dates: {len(remaining_missing)}")

def main():
    """Main execution function"""
    filler = AdPerformanceDailyFiller()
    
    print("🎯 AD PERFORMANCE DAILY DATA FILLER")
    print("=" * 60)
    print("Focus: Fill missing data in ad_performance_daily table (no breakdown)")
    print("Range: January 1, 2025 to today")
    print("Method: Micro-testing approach")
    print()
    
    # Check current state
    missing_dates = filler.get_missing_dates()
    
    if not missing_dates:
        print("✅ No missing dates found! Database is complete.")
        return
    
    print(f"❌ Found {len(missing_dates)} missing dates")
    print()
    
    # Ask user what they want to do
    print("🤔 What would you like to do?")
    print("1. Test with 1 date (micro-test)")
    print("2. Test with 3-5 dates (small batch test)")
    print("3. Fill all missing data (full process)")
    print("4. Exit")
    
    try:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            # Micro-test with 1 date
            test_date = missing_dates[0]  # First missing date
            print(f"\n🧪 Running micro-test with date: {test_date}")
            success = filler.test_single_date(test_date)
            
            if success:
                print(f"\n🎉 Micro-test PASSED! Ready for larger batches.")
            else:
                print(f"\n❌ Micro-test FAILED. Check the issues above.")
        
        elif choice == "2":
            # Test with 3-5 dates
            test_dates = missing_dates[:5]  # First 5 missing dates
            print(f"\n🧪 Running small batch test with {len(test_dates)} dates")
            success = filler.test_multiple_dates(test_dates)
            
            if success:
                print(f"\n🎉 Small batch test PASSED! Ready for full process.")
            else:
                print(f"\n❌ Small batch test had issues. Check the results above.")
        
        elif choice == "3":
            # Full process
            print(f"\n🚀 Running full data fill process...")
            filler.fill_missing_data()
        
        elif choice == "4":
            print("👋 Exiting...")
            
        else:
            print("❌ Invalid choice. Exiting...")
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main() 