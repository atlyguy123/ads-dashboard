#!/usr/bin/env python3

import sys
import os
sys.path.append('orchestrator')

import sqlite3
from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService

def debug_campaign_filtering():
    """
    Debug why campaign 120215772671800178 isn't showing in dashboard
    """
    
    print("🔍 DEBUGGING CAMPAIGN FILTERING")
    print("=" * 80)
    
    campaign_id = '120215772671800178'
    start_date = '2025-06-18'
    end_date = '2025-06-24'
    
    try:
        conn = sqlite3.connect('database/mixpanel_data.db')
        cursor = conn.cursor()
        
        # Check if campaign exists in meta_campaigns
        cursor.execute("""
            SELECT campaign_id, campaign_name, campaign_status
            FROM meta_campaigns 
            WHERE campaign_id = ?
        """, (campaign_id,))
        
        meta_campaign = cursor.fetchone()
        print(f"1. META CAMPAIGN CHECK:")
        print("-" * 60)
        if meta_campaign:
            print(f"  ✅ Found in meta_campaigns: {meta_campaign}")
        else:
            print(f"  ❌ NOT found in meta_campaigns")
            
            # Check what campaigns do exist
            cursor.execute("""
                SELECT campaign_id, campaign_name, campaign_status
                FROM meta_campaigns 
                ORDER BY campaign_id
                LIMIT 10
            """)
            existing_campaigns = cursor.fetchall()
            print(f"  Available campaigns:")
            for camp in existing_campaigns:
                print(f"    {camp[0]}: {camp[1]} ({camp[2]})")
        
        # Check if campaign has data in mixpanel_user
        cursor.execute("""
            SELECT COUNT(*) as user_count
            FROM mixpanel_user 
            WHERE abi_campaign_id = ?
        """, (campaign_id,))
        
        user_count = cursor.fetchone()[0]
        print(f"\n2. MIXPANEL USER DATA:")
        print("-" * 60)
        print(f"  Users with this campaign ID: {user_count}")
        
        # Check if campaign has events in date range
        cursor.execute("""
            SELECT COUNT(*) as event_count
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
              AND DATE(e.event_time) BETWEEN ? AND ?
        """, (campaign_id, start_date, end_date))
        
        event_count = cursor.fetchone()[0]
        print(f"  Events in date range: {event_count}")
        
        # Check what the Step 1 query would return
        print(f"\n3. STEP 1 QUERY TEST:")
        print("-" * 60)
        
        step1_query = """
        SELECT 
            'campaign_' || mc.campaign_id as entity_id,
            mc.campaign_name,
            mc.campaign_id,
            COUNT(DISTINCT CASE WHEN me.event_name = 'RC Trial started' THEN me.distinct_id END) as trials_mixpanel,
            COUNT(DISTINCT CASE WHEN me.event_name IN ('RC Purchase', 'RC Renewal') THEN me.distinct_id END) as purchases_mixpanel
        FROM meta_campaigns mc
        LEFT JOIN mixpanel_user mu ON mc.campaign_id = mu.abi_campaign_id
        LEFT JOIN mixpanel_event me ON mu.distinct_id = me.distinct_id 
            AND DATE(me.event_time) BETWEEN ? AND ?
        WHERE mc.campaign_id = ?
        GROUP BY mc.campaign_id, mc.campaign_name
        """
        
        cursor.execute(step1_query, (start_date, end_date, campaign_id))
        step1_result = cursor.fetchone()
        
        if step1_result:
            print(f"  ✅ Step 1 would return: {step1_result}")
        else:
            print(f"  ❌ Step 1 returns nothing")
            
            # Try without the date filter
            cursor.execute("""
                SELECT 
                    'campaign_' || mc.campaign_id as entity_id,
                    mc.campaign_name,
                    mc.campaign_id,
                    COUNT(DISTINCT me.distinct_id) as total_events
                FROM meta_campaigns mc
                LEFT JOIN mixpanel_user mu ON mc.campaign_id = mu.abi_campaign_id
                LEFT JOIN mixpanel_event me ON mu.distinct_id = me.distinct_id
                WHERE mc.campaign_id = ?
                GROUP BY mc.campaign_id, mc.campaign_name
            """, (campaign_id,))
            
            no_date_result = cursor.fetchone()
            if no_date_result:
                print(f"  Without date filter: {no_date_result}")
            else:
                print(f"  Even without date filter: No results")
        
        # Check the actual analytics service step 1
        print(f"\n4. ANALYTICS SERVICE STEP 1:")
        print("-" * 60)
        
        analytics_service = AnalyticsQueryService()
        
        # Test Step 1 directly
        step1_records = analytics_service._get_step1_base_records(
            start_date=start_date,
            end_date=end_date,
            group_by='campaign',
            include_mixpanel=True
        )
        
        print(f"  Step 1 returned {len(step1_records)} records")
        
        # Look for our campaign
        our_campaign = None
        for record in step1_records:
            if record.get('campaign_id') == campaign_id:
                our_campaign = record
                break
        
        if our_campaign:
            print(f"  ✅ Found our campaign: {our_campaign}")
        else:
            print(f"  ❌ Our campaign not in Step 1 results")
            print(f"  Available campaigns in Step 1:")
            for record in step1_records[:5]:
                print(f"    {record.get('campaign_id', 'N/A')}: {record.get('campaign_name', 'N/A')}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_campaign_filtering() 