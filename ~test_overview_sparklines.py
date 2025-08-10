#!/usr/bin/env python3
"""
Test script to verify 28-day overview sparkline functionality with pre-computed data
"""

import sys
import os
sys.path.append('/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService
from datetime import datetime, timedelta
import json

def test_overview_sparklines():
    """Test the overview sparklines with pre-computed data"""
    
    # Initialize the service
    service = AnalyticsQueryService()
    
    # Calculate 28-day period ending today 
    end_date = datetime.now()
    start_date = end_date - timedelta(days=27)  # 28 days total
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"🧪 Testing overview sparklines for {start_date_str} to {end_date_str}")
    
    try:
        # Test overview ROAS chart data (the function we just fixed)
        result = service.get_overview_roas_chart_data(start_date_str, end_date_str, 'all')
        
        print(f"✅ Overview sparklines test completed!")
        print(f"📊 Success: {result.get('success', False)}")
        print(f"📈 Chart data days: {len(result.get('chart_data', []))}")
        
        if result.get('success'):
            metadata = result.get('metadata', {})
            print(f"💰 Total spend: ${metadata.get('total_spend', 0):,.2f}")
            print(f"💵 Total revenue: ${metadata.get('total_revenue', 0):,.2f}")
            print(f"🎯 Total conversions: {metadata.get('total_conversions', 0)}")
            print(f"🔄 Total trials: {metadata.get('total_trials', 0)}")
            print(f"📊 Avg daily ROAS: {metadata.get('avg_daily_roas', 0):.2f}")
            print(f"🎯 Overall trial accuracy: {metadata.get('overall_trial_accuracy', 0):.4f}")
            
            # Check data structure of first few days
            chart_data = result.get('chart_data', [])
            if chart_data:
                print(f"\n📋 Sample daily data (first day):")
                first_day = chart_data[0]
                print(f"  Date: {first_day.get('date')}")
                print(f"  Daily spend: ${first_day.get('daily_spend', 0):,.2f}")
                print(f"  Daily revenue: ${first_day.get('rolling_1d_revenue', 0):,.2f}")
                print(f"  Daily ROAS: {first_day.get('rolling_1d_roas', 0):.2f}")
                print(f"  Daily trials: {first_day.get('daily_mixpanel_trials', 0)}")
                print(f"  Daily purchases: {first_day.get('daily_mixpanel_purchases', 0)}")
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Exception during test: {e}")
        import traceback
        traceback.print_exc()

def check_pre_computed_data():
    """Quick check that pre-computed data exists"""
    import sqlite3
    from utils.database_utils import get_database_connection
    
    try:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Check daily_mixpanel_metrics table
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics")
            main_count = cursor.fetchone()[0]
            print(f"📊 Main metrics table: {main_count:,} records")
            
            # Check breakdown table  
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics_breakdown")
            breakdown_count = cursor.fetchone()[0]
            print(f"🌍 Breakdown table: {breakdown_count:,} records")
            
            # Check date range in main table
            cursor.execute("SELECT MIN(date), MAX(date) FROM daily_mixpanel_metrics")
            date_range = cursor.fetchone()
            print(f"📅 Data range: {date_range[0]} to {date_range[1]}")
            
            # Check recent data (last 7 days)
            from datetime import datetime, timedelta
            recent_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics WHERE date >= ?", [recent_date])
            recent_count = cursor.fetchone()[0]
            print(f"🔄 Recent data (last 7 days): {recent_count:,} records")
            
    except Exception as e:
        print(f"❌ Error checking pre-computed data: {e}")

if __name__ == "__main__":
    print("🚀 OVERVIEW SPARKLINES TEST")
    print("=" * 50)
    
    print("\n1. Checking pre-computed data...")
    check_pre_computed_data()
    
    print("\n2. Testing overview sparklines functionality...")
    test_overview_sparklines()
    
    print("\n✅ Test completed!")