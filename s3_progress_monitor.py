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
        self.end_date = datetime.now()
        self.total_days = (self.end_date - self.start_date).days + 1
        
        # Progress tracking
        self.history = []  # List of (timestamp, available_count) tuples
        self.start_time = datetime.now()
    
    def check_data_availability(self):
        """Check current data availability in S3"""
        available_dates = []
        
        current_date = self.start_date
        while current_date <= self.end_date:
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            day = current_date.strftime('%d')
            
            event_prefix = f'{self.project_id}/mp_master_event/{year}/{month}/{day}/'
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket_events, 
                    Prefix=event_prefix, 
                    MaxKeys=1
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    available_dates.append(current_date.strftime('%Y-%m-%d'))
            except Exception:
                pass  # Ignore errors, treat as missing
            
            current_date += timedelta(days=1)
        
        return available_dates
    
    def calculate_progress_rate(self):
        """Calculate the rate of progress (days per hour)"""
        if len(self.history) < 2:
            return None
        
        # Use the last two data points to calculate rate
        latest_time, latest_count = self.history[-1]
        earlier_time, earlier_count = self.history[-2]
        
        time_diff_hours = (latest_time - earlier_time).total_seconds() / 3600
        count_diff = latest_count - earlier_count
        
        if time_diff_hours > 0 and count_diff > 0:
            return count_diff / time_diff_hours  # days per hour
        
        return None
    
    def estimate_completion_time(self, current_count):
        """Estimate when all data will be available"""
        missing_days = self.total_days - current_count
        if missing_days <= 0:
            return "Complete!"
        
        rate = self.calculate_progress_rate()
        if rate is None or rate <= 0:
            return "Calculating..."
        
        hours_remaining = missing_days / rate
        completion_time = datetime.now() + timedelta(hours=hours_remaining)
        
        if hours_remaining < 1:
            return f"~{int(hours_remaining * 60)} minutes"
        elif hours_remaining < 24:
            return f"~{hours_remaining:.1f} hours"
        else:
            return f"~{hours_remaining/24:.1f} days"
    
    def display_progress(self, available_dates):
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
        print(f"🕐 Current: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Progress bar
        bar_width = 40
        filled_width = int(bar_width * percentage / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        print(f"📊 Progress: [{bar}] {percentage:.1f}%")
        print(f"📈 Available: {current_count}/{self.total_days} days")
        print()
        
        # Rate and estimate
        rate = self.calculate_progress_rate()
        if rate is not None:
            print(f"⚡ Rate: {rate:.2f} days/hour")
        else:
            print("⚡ Rate: Calculating...")
        
        estimate = self.estimate_completion_time(current_count)
        print(f"⏰ Estimated completion: {estimate}")
        print()
        
        # Recent dates (last 10)
        if available_dates:
            print("📋 Recent available dates:")
            recent_dates = sorted(available_dates)[-10:]
            for date in recent_dates:
                print(f"   ✅ {date}")
            if len(available_dates) > 10:
                print(f"   ... and {len(available_dates) - 10} more")
        else:
            print("📋 No dates available yet")
        
        print()
        print("Press Ctrl+C to stop monitoring...")
        print("-" * 60)
        
        # Show history if we have it
        if len(self.history) > 1:
            print("📈 Progress History:")
            for i, (timestamp, count) in enumerate(self.history[-5:]):  # Last 5 entries
                time_str = timestamp.strftime('%H:%M:%S')
                print(f"   {time_str}: {count} days available")
    
    def run(self):
        """Main monitoring loop"""
        print("🚀 Starting S3 Progress Monitor...")
        print("📡 Checking data availability every 30 seconds...")
        print()
        
        try:
            while True:
                # Check current availability
                available_dates = self.check_data_availability()
                current_count = len(available_dates)
                
                # Record this data point
                self.history.append((datetime.now(), current_count))
                
                # Keep only last 20 data points
                if len(self.history) > 20:
                    self.history = self.history[-20:]
                
                # Display progress
                self.display_progress(available_dates)
                
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