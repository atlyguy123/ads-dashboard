#!/usr/bin/env python3
import sys
import os
sys.path.append('/Users/joshuakaufman/untitled folder 3/orchestrator')

from services.analytics_query_service import AnalyticsQueryService, QueryConfig
from datetime import datetime, timedelta

def debug_sparkline_mismatch():
    """Debug the mismatch between dashboard totals and sparkline rolling values"""
    
    # Initialize service
    service = AnalyticsQueryService()
    
    # Use the exact same config as the dashboard (7 days from yesterday)
    config = QueryConfig(
        breakdown='country',
        start_date='2025-06-17',  # 7 days ago
        end_date='2025-06-24',    # today
        group_by='campaign',
        enable_breakdown_mapping=True
    )
    
    entity_id = '120217904661980178'
    
    print("=== MAIN DASHBOARD QUERY ===")
    main_result = service.execute_analytics_query(config)
    print(f"Success: {main_result.get('success')}")
    
    if main_result.get('success') and main_result.get('data'):
        for campaign in main_result['data']:
            if campaign.get('id') == entity_id:
                print(f"Campaign ID: {campaign['id']}")
                print(f"Meta trials: {campaign.get('meta_trials_started', 0)}")
                print(f"Mixpanel trials: {campaign.get('mixpanel_trials_started', 0)}")
                print(f"Trial accuracy: {campaign.get('trial_accuracy_ratio', 0) * 100:.1f}%")
                print(f"Estimated revenue: ${campaign.get('estimated_revenue_usd', 0):.2f}")
                print(f"Spend: ${campaign.get('spend', 0):.2f}")
                
                # Check US breakdown data
                if campaign.get('breakdowns'):
                    for breakdown in campaign['breakdowns']:
                        if breakdown.get('type') == 'country':
                            for value in breakdown.get('values', []):
                                if value.get('name') == 'US':
                                    print(f"US breakdown - Meta trials: {value.get('meta_trials_started', 0)}")
                                    print(f"US breakdown - Mixpanel trials: {value.get('mixpanel_trials_started', 0)}")
                                    print(f"US breakdown - Trial accuracy: {value.get('trial_accuracy_ratio', 0) * 100:.1f}%")
                                    print(f"US breakdown - Estimated revenue: ${value.get('estimated_revenue_usd', 0):.2f}")
                                    print(f"US breakdown - Spend: ${value.get('spend', 0):.2f}")
                break
    
    print("\n=== SPARKLINE CHART QUERY ===")
    chart_result = service.get_chart_data(config, 'campaign', entity_id)
    print(f"Success: {chart_result.get('success')}")
    
    if chart_result.get('success') and chart_result.get('chart_data'):
        chart_data = chart_result['chart_data']
        if chart_data:
            last_day = chart_data[-1]  # Last day should match dashboard totals
            print(f"Last day date: {last_day['date']}")
            print(f"Rolling 3d Meta trials: {last_day.get('rolling_3d_meta_trials', 0)}")
            print(f"Rolling 3d Mixpanel trials: {last_day.get('rolling_3d_trials', 0)}")
            print(f"Rolling 3d spend: ${last_day.get('rolling_3d_spend', 0):.2f}")
            print(f"Rolling 3d revenue: ${last_day.get('rolling_3d_revenue', 0):.2f}")
            print(f"Period accuracy ratio: {last_day.get('period_accuracy_ratio', 0) * 100:.1f}%")
            
            print(f"\nChart data date range: {chart_data[0]['date']} to {chart_data[-1]['date']}")
            print(f"Chart data days: {len(chart_data)}")
            
            # Show daily breakdown for the last 3 days
            print("\nLast 3 days breakdown:")
            for day in chart_data[-3:]:
                print(f"  {day['date']}: spend=${day.get('daily_spend', 0):.2f}, meta_trials={day.get('daily_meta_trials', 0)}, mp_trials={day.get('daily_mixpanel_trials', 0)}")

if __name__ == "__main__":
    debug_sparkline_mismatch() 