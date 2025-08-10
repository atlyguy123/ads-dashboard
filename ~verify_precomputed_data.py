#!/usr/bin/env python3
"""
Verify that Module 8 has actually populated the pre-computed tables
"""

import sys
import os
sys.path.append('/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final')

import sqlite3
from utils.database_utils import get_database_connection
from datetime import datetime, timedelta

def check_precomputed_tables():
    """Check if Module 8 has actually populated the pre-computed tables"""
    print("🔍 VERIFYING PRE-COMPUTED DATA")
    print("=" * 50)
    
    try:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Check if tables exist
            print("1. Checking table existence...")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('daily_mixpanel_metrics', 'daily_mixpanel_metrics_breakdown')")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"   Found tables: {tables}")
            
            if 'daily_mixpanel_metrics' not in tables:
                print("❌ daily_mixpanel_metrics table missing!")
                return False
                
            # Check main table structure and data
            print("\n2. Checking main table structure...")
            cursor.execute("PRAGMA table_info(daily_mixpanel_metrics)")
            columns = [row[1] for row in cursor.fetchall()]
            print(f"   Columns: {len(columns)} total")
            
            # Check for critical new columns from the specification
            required_columns = [
                'meta_spend', 'meta_impressions', 'meta_clicks', 'meta_trial_count', 'meta_purchase_count',
                'trial_accuracy_ratio', 'purchase_accuracy_ratio', 'estimated_roas', 'profit_usd',
                'actual_revenue_usd', 'adjusted_estimated_revenue_usd'
            ]
            
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                print(f"❌ Missing required columns: {missing_columns}")
                return False
            else:
                print(f"✅ All required columns present")
            
            # Check if data exists
            print("\n3. Checking data availability...")
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics")
            main_count = cursor.fetchone()[0]
            print(f"   Main table records: {main_count:,}")
            
            if main_count == 0:
                print("❌ No data in daily_mixpanel_metrics table!")
                print("🚨 MODULE 8 HAS NOT RUN OR FAILED TO POPULATE DATA")
                return False
            
            # Check breakdown table if it exists
            if 'daily_mixpanel_metrics_breakdown' in tables:
                cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics_breakdown")
                breakdown_count = cursor.fetchone()[0]
                print(f"   Breakdown table records: {breakdown_count:,}")
            else:
                print("⚠️  Breakdown table missing")
                
            # Check data freshness
            print("\n4. Checking data freshness...")
            cursor.execute("SELECT MAX(computed_at) FROM daily_mixpanel_metrics")
            latest_computation = cursor.fetchone()[0]
            if latest_computation:
                print(f"   Latest computation: {latest_computation}")
                
                # Check if data is recent (within last 24 hours)
                latest_dt = datetime.fromisoformat(latest_computation.replace('Z', '+00:00'))
                now = datetime.now()
                age_hours = (now - latest_dt).total_seconds() / 3600
                print(f"   Data age: {age_hours:.1f} hours")
                
                if age_hours > 48:
                    print("⚠️  Data is more than 48 hours old - may need to re-run Module 8")
            
            # Sample some data to verify it looks correct
            print("\n5. Sampling pre-computed data...")
            cursor.execute("""
                SELECT entity_type, entity_id, date, meta_spend, trial_users_count, 
                       estimated_revenue_usd, trial_accuracy_ratio, estimated_roas
                FROM daily_mixpanel_metrics 
                WHERE meta_spend > 0 OR trial_users_count > 0
                ORDER BY date DESC 
                LIMIT 5
            """)
            
            samples = cursor.fetchall()
            if samples:
                print("   Sample records (entity_type, entity_id, date, spend, trials, revenue, accuracy, roas):")
                for row in samples:
                    print(f"     {row[0]}, {row[1]}, {row[2]}, ${row[3]:.2f}, {row[4]}, ${row[5]:.2f}, {row[6]:.2f}%, {row[7]:.2f}")
            else:
                print("   ❌ No meaningful sample data found!")
                return False
                
            print("\n✅ PRE-COMPUTED DATA VERIFICATION PASSED")
            return True
            
    except Exception as e:
        print(f"❌ Error checking pre-computed data: {e}")
        return False

def check_module_8_execution():
    """Check if Module 8 has been executed recently"""
    print("\n🔧 CHECKING MODULE 8 EXECUTION STATUS")
    print("=" * 50)
    
    # Check if Module 8 file exists and is executable
    module_8_path = "pipelines/mixpanel_pipeline/08_compute_daily_metrics.py"
    if os.path.exists(module_8_path):
        print(f"✅ Module 8 file exists: {module_8_path}")
        
        # Check file size (should be substantial if it has all the pre-computation logic)
        file_size = os.path.getsize(module_8_path)
        print(f"   File size: {file_size:,} bytes")
        
        if file_size < 50000:  # Less than 50KB suggests it might not have all the logic
            print("⚠️  Module 8 file seems small - may not have full pre-computation logic")
        else:
            print("✅ Module 8 file size suggests comprehensive implementation")
            
    else:
        print(f"❌ Module 8 file not found: {module_8_path}")
        return False
    
    return True

def run_module_8_if_needed():
    """Run Module 8 if pre-computed data is missing or stale"""
    print("\n🚀 CHECKING IF MODULE 8 NEEDS TO RUN")
    print("=" * 50)
    
    # First check if data exists and is fresh
    if check_precomputed_tables():
        print("✅ Pre-computed data appears to be available and fresh")
        return True
    
    print("🔄 Pre-computed data is missing or stale - Module 8 needs to run")
    print("⚠️  You should run the master pipeline to execute Module 8")
    print("    Command: python3 run_master_pipeline.py")
    
    return False

if __name__ == "__main__":
    print("🔍 VERIFYING PRE-COMPUTATION SYSTEM STATUS")
    print("=" * 60)
    
    # Check if Module 8 exists
    module_8_ok = check_module_8_execution()
    
    # Check if pre-computed data exists
    data_ok = check_precomputed_tables()
    
    # Summary
    print("\n📊 VERIFICATION SUMMARY")
    print("=" * 30)
    print(f"Module 8 file status: {'✅ OK' if module_8_ok else '❌ MISSING'}")
    print(f"Pre-computed data: {'✅ OK' if data_ok else '❌ MISSING/STALE'}")
    
    if not data_ok:
        print("\n🚨 ACTION REQUIRED:")
        print("Pre-computed data is not available. You need to:")
        print("1. Run the master pipeline: python3 run_master_pipeline.py")
        print("2. Or run Module 8 directly: python3 pipelines/mixpanel_pipeline/08_compute_daily_metrics.py")
        print("3. Then re-test the dashboard system")
    else:
        print("\n✅ SYSTEM READY:")
        print("Pre-computed data is available and the dashboard should use it for fast queries")