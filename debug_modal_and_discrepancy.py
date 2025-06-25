#!/usr/bin/env python3

import sys
import os
sys.path.append('orchestrator')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService
import sqlite3

def debug_modal_and_discrepancy():
    """
    Debug the modal data issues and user count discrepancy
    """
    
    print("🐛 DEBUGGING MODAL DATA AND USER DISCREPANCY")
    print("=" * 80)
    
    campaign_id = '120215772671800178'
    start_date = '2025-06-18'
    end_date = '2025-06-24'
    
    # Test the tooltip API
    analytics_service = AnalyticsQueryService()
    result = analytics_service.get_user_details_for_tooltip(
        entity_type='campaign',
        entity_id=f'campaign_{campaign_id}',
        start_date=start_date,
        end_date=end_date,
        breakdown='all'
    )
    
    print(f"1. TOOLTIP API RESPONSE ANALYSIS:")
    print("-" * 60)
    
    if result.get('success'):
        summary = result.get('summary', {})
        users = result.get('users', [])
        
        print(f"API Success: {result.get('success')}")
        print(f"Summary object: {summary}")
        print(f"Users count: {len(users)}")
        
        # Check specific summary fields
        print(f"\nSummary Fields:")
        print(f"  total_users: {summary.get('total_users', 'MISSING')}")
        print(f"  avg_trial_conversion_rate: {summary.get('avg_trial_conversion_rate', 'MISSING')}")
        print(f"  avg_trial_refund_rate: {summary.get('avg_trial_refund_rate', 'MISSING')}")
        print(f"  avg_purchase_refund_rate: {summary.get('avg_purchase_refund_rate', 'MISSING')}")
        
        if users:
            # Manual calculation to verify
            trial_rates = [u.get('trial_conversion_rate', 0) for u in users]
            manual_avg = sum(trial_rates) / len(trial_rates) if trial_rates else 0
            print(f"\nManual Calculation:")
            print(f"  Users with trial rates: {len([r for r in trial_rates if r > 0])}")
            print(f"  Manual average: {manual_avg:.2f}%")
            
    else:
        print(f"API Failed: {result.get('error', 'Unknown error')}")
    
    print(f"\n2. DASHBOARD DISCREPANCY ANALYSIS:")
    print("-" * 60)
    
    try:
        conn = sqlite3.connect('database/mixpanel_data.db')
        cursor = conn.cursor()
        
        # Count Mixpanel trials (what dashboard shows as 82)
        cursor.execute("""
            SELECT COUNT(*) as trial_events
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id  
            WHERE u.abi_campaign_id = ?
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
        """, (campaign_id, start_date, end_date))
        
        trial_events = cursor.fetchone()[0]
        print(f"Mixpanel trial events (dashboard): {trial_events}")
        
        # Count Mixpanel purchases (what dashboard shows as 2)
        cursor.execute("""
            SELECT COUNT(*) as purchase_events
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id  
            WHERE u.abi_campaign_id = ?
              AND e.event_name IN ('RC Purchase', 'RC Renewal')
              AND DATE(e.event_time) BETWEEN ? AND ?
        """, (campaign_id, start_date, end_date))
        
        purchase_events = cursor.fetchone()[0]
        print(f"Mixpanel purchase events (dashboard): {purchase_events}")
        print(f"Dashboard total events: {trial_events + purchase_events}")
        
        # Count users in tooltip query (what shows as 87)
        cursor.execute("""
            SELECT COUNT(DISTINCT upm.distinct_id) as tooltip_users
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
              AND upm.credited_date BETWEEN ? AND ?
              AND upm.trial_conversion_rate IS NOT NULL
              AND upm.trial_converted_to_refund_rate IS NOT NULL  
              AND upm.initial_purchase_to_refund_rate IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM mixpanel_event e 
                  WHERE e.distinct_id = upm.distinct_id 
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
              )
        """, (campaign_id, start_date, end_date, start_date, end_date))
        
        tooltip_users = cursor.fetchone()[0]
        print(f"Tooltip users: {tooltip_users}")
        
        # Find users with trials but no trial events in date range
        cursor.execute("""
            SELECT 
                upm.distinct_id,
                upm.credited_date,
                COUNT(e.distinct_id) as trial_events_in_range,
                MIN(DATE(e.event_time)) as first_trial,
                MAX(DATE(e.event_time)) as last_trial
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            LEFT JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id 
                AND e.event_name = 'RC Trial started'
                AND DATE(e.event_time) BETWEEN ? AND ?
            WHERE u.abi_campaign_id = ?
              AND upm.credited_date BETWEEN ? AND ?
              AND upm.trial_conversion_rate IS NOT NULL
            GROUP BY upm.distinct_id
            HAVING COUNT(e.distinct_id) = 0
            LIMIT 10
        """, (start_date, end_date, campaign_id, start_date, end_date))
        
        users_no_trials = cursor.fetchall()
        print(f"\nUsers with rates but NO trial events in date range: {len(users_no_trials)}")
        
        if users_no_trials:
            for user in users_no_trials[:3]:
                distinct_id, credited_date, trial_events, first_trial, last_trial = user
                print(f"  {distinct_id[:20]}...: credited {credited_date}, {trial_events} trials in range")
        
        # Check if users have trials outside the date range
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT upm.distinct_id) as users_with_external_trials
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND upm.credited_date BETWEEN ? AND ?
              AND upm.trial_conversion_rate IS NOT NULL
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) NOT BETWEEN ? AND ?
        """, (campaign_id, start_date, end_date, start_date, end_date))
        
        external_trials = cursor.fetchone()[0]
        print(f"Users with trials OUTSIDE date range: {external_trials}")
        
        conn.close()
        
    except Exception as e:
        print(f"Database error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_modal_and_discrepancy() 