#!/usr/bin/env python3
"""
Debug script to test sparkline chart data retrieval
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent))

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig
from utils.database_utils import get_database_path

def test_sparkline_chart_data():
    """Test the chart data retrieval for sparklines"""
    print("=== SPARKLINE DEBUG TEST ===")
    
    try:
        # Test database path discovery
        print("\n1. Testing database path discovery...")
        meta_db_path = get_database_path('meta_analytics')
        mixpanel_db_path = get_database_path('mixpanel_data')
        
        print(f"Meta DB path: {meta_db_path}")
        print(f"Mixpanel DB path: {mixpanel_db_path}")
        
        # Check if files exist
        meta_exists = Path(meta_db_path).exists()
        mixpanel_exists = Path(mixpanel_db_path).exists()
        
        print(f"Meta DB exists: {meta_exists}")
        print(f"Mixpanel DB exists: {mixpanel_exists}")
        
        if not meta_exists:
            print("❌ Meta database not found - this will cause sparkline failures")
            return False
            
        # Initialize analytics service
        print("\n2. Initializing analytics service...")
        analytics_service = AnalyticsQueryService()
        
        # Test with a real campaign ID from the database
        print("\n3. Testing chart data retrieval...")
        import sqlite3
        
        # Get a sample campaign ID
        conn = sqlite3.connect(meta_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT campaign_id FROM ad_performance_daily LIMIT 1")
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            print("❌ No campaign data found in database")
            return False
            
        campaign_id = result[0]
        print(f"Testing with campaign ID: {campaign_id}")
        
        # Create query config (14 days ending today)
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=13)).strftime('%Y-%m-%d')
        
        config = QueryConfig(
            breakdown='all',
            start_date=start_date,
            end_date=end_date,
            include_mixpanel=True
        )
        
        print(f"Query config: {start_date} to {end_date}")
        
        # Test chart data retrieval
        result = analytics_service.get_chart_data(config, 'campaign', campaign_id)
        
        print(f"\n4. Chart data result:")
        print(f"Success: {result.get('success', False)}")
        
        if result.get('success'):
            chart_data = result.get('chart_data', [])
            print(f"Chart data length: {len(chart_data)} days")
            
            if chart_data:
                first_day = chart_data[0]
                print(f"Sample data structure: {list(first_day.keys())}")
                print(f"Has rolling_1d_roas: {'rolling_1d_roas' in first_day}")
                print(f"Sample ROAS values: {[d.get('rolling_1d_roas', 0) for d in chart_data[:3]]}")
            else:
                print("❌ Empty chart data returned")
        else:
            error = result.get('error', 'Unknown error')
            print(f"❌ Chart data failed: {error}")
            return False
            
        print("\n✅ Sparkline test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Sparkline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_sparkline_chart_data()
    sys.exit(0 if success else 1) 