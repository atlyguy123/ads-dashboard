#!/usr/bin/env python3
"""
Test script to demonstrate parametric rolling window functionality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'orchestrator'))

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig
from datetime import datetime, timedelta

def test_rolling_windows():
    """Test different rolling window configurations"""
    
    # Initialize service
    service = AnalyticsQueryService()
    
    # Test configuration
    config = QueryConfig(
        breakdown='all',
        start_date='2024-12-01',
        end_date='2024-12-14'
    )
    
    # Test different rolling windows
    rolling_windows = [1, 3, 7]
    
    print("🧪 TESTING PARAMETRIC ROLLING WINDOWS")
    print("=" * 50)
    
    for window_days in rolling_windows:
        print(f"\n📊 Testing {window_days}-day rolling window:")
        print("-" * 30)
        
        try:
            # Test with a sample campaign (you'd need actual data)
            result = service.get_chart_data(
                config=config,
                entity_type='campaign',
                entity_id='sample_campaign_123',  # This would need to exist in your data
                rolling_window_days=window_days
            )
            
            if result.get('success'):
                chart_data = result.get('chart_data', [])
                print(f"✅ Successfully generated {len(chart_data)} days of data")
                print(f"📈 Rolling calculation: {result.get('rolling_calculation_info')}")
                
                # Show field names for first day
                if chart_data:
                    first_day = chart_data[0]
                    rolling_fields = [k for k in first_day.keys() if k.startswith(f'rolling_{window_days}d_')]
                    print(f"🔢 Dynamic fields: {rolling_fields}")
                    print(f"📅 Date range: {chart_data[0]['date']} to {chart_data[-1]['date']}")
                    
                    # Show sample ROAS values
                    roas_field = f'rolling_{window_days}d_roas'
                    sample_roas = [d.get(roas_field, 0) for d in chart_data[:3]]  # First 3 days
                    print(f"📊 Sample ROAS values: {sample_roas}")
                    
            else:
                print(f"❌ Error: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    print("\n" + "=" * 50)
    print("✅ PARAMETRIC ROLLING WINDOW TEST COMPLETE")
    print("\nKey Benefits:")
    print("• 🔧 Single codebase handles any rolling window (1d, 3d, 7d, etc.)")
    print("• 📊 Dynamic field names: rolling_Nd_roas, rolling_Nd_spend, etc.")
    print("• 🎯 Frontend automatically adapts to different window sizes")
    print("• 💾 No need to rewrite code when changing rolling periods")
    print("• ⚡ Easy to switch between 1-day and 3-day by changing one parameter")

if __name__ == "__main__":
    test_rolling_windows() 