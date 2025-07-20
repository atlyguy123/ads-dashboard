#!/usr/bin/env python3
"""
S3 Data Progress Monitor
Monitors Mixpanel data availability in S3 and estimates completion time.
"""

import boto3
import os
import time
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Import timezone utilities for consistent timezone handling
import sys
sys.path.append(str(Path(__file__).resolve().parent))
from orchestrator.utils.timezone_utils import now_in_timezone

class S3ProgressMonitor:
    def __init__(self):
        # Load environment variables
        env_file = Path(__file__).parent / '.env'
        load_dotenv(env_file)
        
        # AWS Configuration
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_region_name = os.environ.get('AWS_REGION_NAME', 'us-east-1')
        self.s3_bucket_events = os.environ.get('S3_BUCKET_EVENTS')
        self.s3_bucket_users = os.environ.get('S3_BUCKET_USERS')
        self.project_id = os.environ.get('PROJECT_ID')
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name
        )
        
        # Monitoring configuration
        self.start_date = datetime(2025, 4, 14)
        self.end_date = now_in_timezone()
        self.total_days = (self.end_date - self.start_date).days + 1
        
        # Progress tracking - NEW APPROACH
        self.day_appearances = {}  # {date_str: timestamp when first seen}
        self.previously_seen_dates = set()  # Track what we've seen before
        self.start_time = now_in_timezone()
        self.first_check_done = False  # Track if we've done the initial baseline check
        
        # Keep history for display purposes
        self.history = []  # List of (timestamp, available_count) tuples
    
    def check_data_availability(self):
        """Check current data availability in S3 and track new appearances"""
        available_dates = []
        new_dates = []
        
        current_date = self.start_date
        while current_date <= self.end_date:
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            day = current_date.strftime('%d')
            date_str = current_date.strftime('%Y-%m-%d')
            
            event_prefix = f'{self.project_id}/mp_master_event/{year}/{month}/{day}/'
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket_events, 
                    Prefix=event_prefix, 
                    MaxKeys=1
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    available_dates.append(date_str)
                    
                    # Only track as "new appearance" if we've done the baseline check
                    if self.first_check_done and date_str not in self.previously_seen_dates:
                        self.day_appearances[date_str] = now_in_timezone()
                        new_dates.append(date_str)
                    
                    # Always update our tracking of what we've seen
                    self.previously_seen_dates.add(date_str)
                        
            except Exception:
                pass  # Ignore errors, treat as missing
            
            current_date += timedelta(days=1)
        
        # Mark that we've done the first check
        if not self.first_check_done:
            self.first_check_done = True
            print(f"📊 Baseline established: Found {len(available_dates)} existing days")
        
        return available_dates, new_dates
    
    def calculate_progress_rate(self):
        """Calculate the rate based on actual day appearance times"""
        if len(self.day_appearances) < 2:
            return None
        
        # Get all appearance times sorted by when they appeared
        appearance_times = sorted(self.day_appearances.values())
        
        # Calculate rate based on time from first appearance to last appearance
        first_appearance = appearance_times[0]
        last_appearance = appearance_times[-1]
        
        time_span_hours = (last_appearance - first_appearance).total_seconds() / 3600
        days_appeared_in_span = len(appearance_times) - 1  # Don't count the initial day
        
        if time_span_hours > 0 and days_appeared_in_span > 0:
            return days_appeared_in_span / time_span_hours  # days per hour
        
        return None
    
    def estimate_completion_time(self, current_count):
        """Estimate when all data will be available based on actual appearance rate"""
        missing_days = self.total_days - current_count
        if missing_days <= 0:
            return "Complete!"
        
        rate = self.calculate_progress_rate()
        if rate is None or rate <= 0:
            return "Calculating..."
        
        hours_remaining = missing_days / rate
        completion_time = now_in_timezone() + timedelta(hours=hours_remaining)
        
        if hours_remaining < 1:
            return f"~{int(hours_remaining * 60)} minutes"
        elif hours_remaining < 24:
            return f"~{hours_remaining:.1f} hours"
        else:
            return f"~{hours_remaining/24:.1f} days"
    
    def display_progress(self, available_dates, new_dates):
        """Display current progress in terminal"""
        current_count = len(available_dates)
        percentage = (current_count / self.total_days) * 100
        
        # Clear screen and move cursor to top
        print("\033[2J\033[H")
        
        print("=" * 60)
        print("🔍 S3 MIXPANEL DATA PROGRESS MONITOR")
        print("=" * 60)
        print()
        
        print(f"📅 Monitoring Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"🕐 Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🕐 Current: {now_in_timezone().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Progress bar
        bar_width = 40
        filled_width = int(bar_width * percentage / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        print(f"📊 Progress: [{bar}] {percentage:.1f}%")
        print(f"📈 Available: {current_count}/{self.total_days} days")
        print()
        
        # Show new dates detected this check
        if new_dates:
            print(f"🆕 New dates detected this check: {', '.join(new_dates)}")
            print()
        
        # Rate and estimate based on actual appearances
        rate = self.calculate_progress_rate()
        if rate is not None:
            print(f"⚡ Rate: {rate:.2f} days/hour (based on {len(self.day_appearances)} witnessed appearances)")
            
            # Show time span being used for rate calculation
            if len(self.day_appearances) >= 2:
                appearance_times = sorted(self.day_appearances.values())
                first_time = appearance_times[0]
                last_time = appearance_times[-1]
                time_span = (last_time - first_time).total_seconds() / 3600
                print(f"📊 Rate calculation: {len(self.day_appearances)-1} days over {time_span:.1f} hours")
        else:
            if not self.first_check_done:
                print("⚡ Rate: Establishing baseline...")
            else:
                print("⚡ Rate: Calculating... (need to witness at least 2 day appearances)")
        
        estimate = self.estimate_completion_time(current_count)
        print(f"⏰ Estimated completion: {estimate}")
        print()
        
        # Show recent day appearances (last 10)
        if self.day_appearances:
            print("📋 Recent day appearances (NEW days witnessed):")
            recent_appearances = sorted(self.day_appearances.items(), key=lambda x: x[1])[-10:]
            for date, timestamp in recent_appearances:
                time_str = timestamp.strftime('%H:%M:%S')
                print(f"   ✅ {date} (first seen at {time_str})")
            if len(self.day_appearances) > 10:
                print(f"   ... and {len(self.day_appearances) - 10} more")
        else:
            if self.first_check_done:
                print("📋 No new day appearances witnessed yet (monitoring for new days...)")
            else:
                print("📋 Establishing baseline...")
        
        print()
        print("Press Ctrl+C to stop monitoring...")
        print("-" * 60)
    
    def run(self):
        """Main monitoring loop"""
        print("🚀 Starting S3 Progress Monitor...")
        print("📡 Checking data availability every 30 seconds...")
        print("🧠 Using smart rate calculation based on actual day appearances...")
        print()
        
        try:
            while True:
                # Check current availability and detect new dates
                available_dates, new_dates = self.check_data_availability()
                current_count = len(available_dates)
                
                # Record this data point for history
                self.history.append((now_in_timezone(), current_count))
                
                # Keep only last 20 data points for history
                if len(self.history) > 20:
                    self.history = self.history[-20:]
                
                # Display progress
                self.display_progress(available_dates, new_dates)
                
                # Check if complete
                if current_count >= self.total_days:
                    print("\n🎉 ALL DATA IS NOW AVAILABLE! 🎉")
                    break
                
                # Wait 30 seconds
                time.sleep(30)
                
        except KeyboardInterrupt:
            print("\n\n👋 Monitoring stopped by user")
        except Exception as e:
            print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    monitor = S3ProgressMonitor()
    monitor.run() 