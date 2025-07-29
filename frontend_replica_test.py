#!/usr/bin/env python3
"""
Frontend API Call Replica Test

This replicates EXACTLY what the frontend tooltip does to find the real discrepancy.
"""

import sqlite3
import sys
import os
sys.path.append('orchestrator')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService

def test_frontend_replica():
    """Replicate the exact frontend API call"""
    
    print("🕵️ FRONTEND API CALL REPLICA TEST")
    print("=" * 50)
    
    # EXACT parameters the user sees
    campaign_id = "120223331225260178"  # Campaign ID from the image
    
    # Try different date combinations to find what matches "9 trials"
    test_dates = [
        ("2025-07-22", "2025-07-28"),  # User specified range
        ("2025-07-01", "2025-07-31"),  # Full July 
        ("2025-01-01", "2025-12-31"),  # Default frontend range
        ("2025-06-01", "2025-07-31"),  # Extended range
    ]
    
    print(f"🎯 Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign")
    print(f"🆔 Campaign ID: {campaign_id}")
    print()
    
    # Test dashboard trials count for each date range
    with sqlite3.connect("database/mixpanel_data.db") as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        print("📊 TESTING DIFFERENT DATE RANGES:")
        print("-" * 40)
        
        matching_range = None
        
        for start_date, end_date in test_dates:
            # Dashboard count
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """, [start_date, end_date, campaign_id])
            
            dashboard_result = cursor.fetchone()
            mixpanel_trials = dashboard_result['mixpanel_trials']
            
            status = "🎯 MATCH!" if mixpanel_trials == 9 else "  "
            print(f"   {start_date} to {end_date}: {mixpanel_trials} trials {status}")
            
            if mixpanel_trials == 9:
                matching_range = (start_date, end_date)
    
    if not matching_range:
        print("\n❌ COULD NOT FIND DATE RANGE THAT GIVES 9 TRIALS")
        print("🔍 Let's check what the frontend dashboard is actually showing...")
        
        # Show trials by day to help identify the range
        print("\n📅 TRIALS BY DAY (last 60 days):")
        cursor.execute("""
            SELECT 
                DATE(e.event_time) as trial_date,
                COUNT(DISTINCT u.distinct_id) as daily_trials
            FROM mixpanel_user u
            JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) >= DATE('now', '-60 days')
            GROUP BY DATE(e.event_time)
            ORDER BY trial_date DESC
            LIMIT 20
        """, [campaign_id])
        
        daily_trials = cursor.fetchall()
        for row in daily_trials:
            print(f"   {row['trial_date']}: {row['daily_trials']} trials")
        
        return
    
    # Found the matching range - now test the API call
    start_date, end_date = matching_range
    print(f"\n✅ FOUND MATCHING RANGE: {start_date} to {end_date}")
    print("🔧 Testing tooltip API call...")
    
    # Replicate EXACT API call
    try:
        analytics_service = AnalyticsQueryService()
        
        # This is the EXACT call the frontend makes
        result = analytics_service.get_user_details_for_tooltip(
            entity_type='campaign',
            entity_id=f'campaign_{campaign_id}',
            start_date=start_date,
            end_date=end_date,
            breakdown='all',
            breakdown_value=None
        )
        
        if result.get('success'):
            summary = result.get('summary', {})
            users = result.get('users', [])
            
            total_users = summary.get('total_users', 0)
            unique_users = len(users)
            
            print(f"\n📊 API RESPONSE:")
            print(f"   Dashboard trials: 9")
            print(f"   API total_users: {total_users}")
            print(f"   API unique users: {unique_users}")
            print(f"   Users array length: {len(users)}")
            
            if total_users == 9:
                print("   ✅ PERFECT: API matches dashboard!")
            else:
                print(f"   ❌ DISCREPANCY: {total_users} ≠ 9")
                
                # Debug the specific users
                print(f"\n🔍 DEBUGGING THE {total_users} USERS:")
                user_products = {}
                for user in users[:10]:  # Show first 10
                    distinct_id = user['distinct_id']
                    product_id = user.get('product_id', 'unknown')
                    
                    if distinct_id not in user_products:
                        user_products[distinct_id] = []
                    user_products[distinct_id].append(product_id)
                
                duplicate_count = 0
                for distinct_id, products in user_products.items():
                    if len(products) > 1:
                        duplicate_count += len(products) - 1
                        print(f"   👤 {distinct_id[:15]}... | {len(products)} products: {', '.join(products[:2])}")
                    else:
                        print(f"   👤 {distinct_id[:15]}... | 1 product: {products[0]}")
                
                print(f"\n   📊 Analysis:")
                print(f"   Unique users: {len(user_products)}")
                print(f"   Extra records from duplicates: {duplicate_count}")
                print(f"   Expected after deduplication: {total_users - duplicate_count}")
                
        else:
            print(f"❌ API ERROR: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_frontend_replica() 