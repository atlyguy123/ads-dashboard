#!/usr/bin/env python3
import sys
import os
sys.path.append('/Users/joshuakaufman/untitled folder 3/orchestrator')

from dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig
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
    
    # CRITICAL FIX: Use the correct entity ID format
    dashboard_entity_id = 'campaign_120217904661980178'  # Dashboard uses this format
    sparkline_entity_id = '120217904661980178'           # Sparkline uses this format
    
    print("=== MAIN DASHBOARD QUERY ===")
    main_result = service.execute_analytics_query(config)
    print(f"Success: {main_result.get('success')}")
    
    if main_result.get('success') and main_result.get('data'):
        print(f"Total campaigns returned: {len(main_result['data'])}")
        
        # Show all campaign IDs
        campaign_ids = [c.get('id') for c in main_result['data']]
        print(f"Campaign IDs: {campaign_ids}")
        
        # Look for our specific campaign using dashboard format
        target_campaign = None
        for campaign in main_result['data']:
            if campaign.get('id') == dashboard_entity_id:
                target_campaign = campaign
                break
        
        if target_campaign:
            print(f"✅ FOUND Campaign ID: {target_campaign['id']}")
            print(f"Meta trials: {target_campaign.get('meta_trials_started', 0)}")
            print(f"Mixpanel trials: {target_campaign.get('mixpanel_trials_started', 0)}")
            print(f"Trial accuracy: {target_campaign.get('trial_accuracy_ratio', 0) * 100:.1f}%")
            print(f"Estimated revenue: ${target_campaign.get('estimated_revenue_usd', 0):.2f}")
            print(f"Spend: ${target_campaign.get('spend', 0):.2f}")
            
            # Check US breakdown data
            if target_campaign.get('breakdowns'):
                for breakdown in target_campaign['breakdowns']:
                    if breakdown.get('type') == 'country':
                        print(f"Found {len(breakdown.get('values', []))} country breakdown values")
                        for value in breakdown.get('values', []):
                            if value.get('name') == 'US':
                                print(f"US breakdown - Meta trials: {value.get('meta_trials_started', 0)}")
                                print(f"US breakdown - Mixpanel trials: {value.get('mixpanel_trials_started', 0)}")
                                print(f"US breakdown - Trial accuracy: {value.get('trial_accuracy_ratio', 0) * 100:.1f}%")
                                print(f"US breakdown - Estimated revenue: ${value.get('estimated_revenue_usd', 0):.2f}")
                                print(f"US breakdown - Spend: ${value.get('spend', 0):.2f}")
        else:
            print(f"❌ Campaign {dashboard_entity_id} NOT FOUND in main dashboard results")
            print("Available campaigns:")
            for i, campaign in enumerate(main_result['data'][:3]):  # Show first 3
                print(f"  {i+1}. ID: {campaign.get('id')}, Name: {campaign.get('name', 'N/A')}")
    else:
        print(f"❌ Main dashboard query failed or returned no data")
        if main_result.get('error'):
            print(f"Error: {main_result['error']}")
    
    print("\n=== SPARKLINE CHART QUERY ===")
    chart_result = service.get_chart_data(config, 'campaign', sparkline_entity_id)
    print(f"Success: {chart_result.get('success')}")
    
    if chart_result.get('success') and chart_result.get('chart_data'):
        chart_data = chart_result['chart_data']
        if chart_data:
            last_day = chart_data[-1]  # Last day should match dashboard totals
            print(f"✅ FOUND Chart Data")
            print(f"Last day date: {last_day['date']}")
            print(f"Rolling 7d Meta trials: {last_day.get('rolling_7d_meta_trials', 0)}")
            print(f"Rolling 7d Mixpanel trials: {last_day.get('rolling_7d_trials', 0)}")
            print(f"Rolling 7d spend: ${last_day.get('rolling_7d_spend', 0):.2f}")
            print(f"Rolling 7d revenue: ${last_day.get('rolling_7d_revenue', 0):.2f}")
            print(f"Period accuracy ratio: {last_day.get('period_accuracy_ratio', 0) * 100:.1f}%")
            
            print(f"\nChart data date range: {chart_data[0]['date']} to {chart_data[-1]['date']}")
            print(f"Chart data days: {len(chart_data)}")
            
            # Show all daily data to understand the 7-day rolling calculation
            print(f"\nAll daily data (last 7 days should sum to rolling totals):")
            rolling_spend = 0
            rolling_meta_trials = 0 
            rolling_mp_trials = 0
            rolling_revenue = 0
            
            for i, day in enumerate(chart_data[-7:]):  # Last 7 days
                rolling_spend += day.get('daily_spend', 0)
                rolling_meta_trials += day.get('daily_meta_trials', 0)
                rolling_mp_trials += day.get('daily_mixpanel_trials', 0)
                rolling_revenue += day.get('daily_estimated_revenue', 0)
                print(f"  {day['date']}: spend=${day.get('daily_spend', 0):.2f}, meta_trials={day.get('daily_meta_trials', 0)}, mp_trials={day.get('daily_mixpanel_trials', 0)}, revenue=${day.get('daily_estimated_revenue', 0):.2f}")
            
            print(f"\nManual 7-day sum calculation:")
            print(f"  Spend sum: ${rolling_spend:.2f}")
            print(f"  Meta trials sum: {rolling_meta_trials}")
            print(f"  MP trials sum: {rolling_mp_trials}")
            print(f"  Revenue sum: ${rolling_revenue:.2f}")
            print(f"  Manual accuracy: {(rolling_mp_trials/rolling_meta_trials*100) if rolling_meta_trials > 0 else 0:.1f}%")
    else:
        print(f"❌ Chart query failed or returned no data")
        if chart_result.get('error'):
            print(f"Chart Error: {chart_result['error']}")

if __name__ == "__main__":
    debug_sparkline_mismatch() 