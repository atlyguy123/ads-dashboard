#!/usr/bin/env python3
"""
Test Simple Mixpanel Logic

Testing the straightforward approach:
- Users with RC Trial started events between July 16-29, 2025
- Who have the ABI campaign ID set

Expected results:
- Campaign 120223331225260178: 39-40 users (original case)
- Campaign 120213263905490178: 11 users 
- Campaign 1837710760734754: 18 users
"""

import sqlite3
import json
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Test simple Mixpanel logic across all campaigns"""
    
    # Test campaigns
    campaigns = [
        {
            'name': 'ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign',
            'id': '120223331225260178',
            'expected': 39  # or 40 based on image
        },
        {
            'name': 'ppc_atly_fb_partners_nov24',
            'id': '120213263905490178', 
            'expected': 11
        },
        {
            'name': 'ppc_atly_tiktok_top_videos_start_trial_july_25',
            'id': '1837710760734754',
            'expected': 18
        }
    ]
    
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    print("🧪 TESTING SIMPLE MIXPANEL LOGIC")
    print("=" * 60)
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Logic: RC Trial started events + ABI campaign ID")
    print()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            for campaign in campaigns:
                test_campaign_simple_logic(cursor, campaign, start_date, end_date)
                
            # Test potential variations if simple doesn't work
            print("\n🔍 TESTING VARIATIONS IF SIMPLE LOGIC DOESN'T MATCH")
            print("-" * 50)
            
            for campaign in campaigns:
                test_campaign_variations(cursor, campaign, start_date, end_date)
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def test_campaign_simple_logic(cursor, campaign, start_date, end_date):
    """Test the simple logic: RC Trial started + campaign ID"""
    
    print(f"📊 CAMPAIGN: {campaign['name']}")
    print(f"🆔 ID: {campaign['id']}")
    print(f"🎯 Expected: {campaign['expected']} users")
    print("-" * 40)
    
    # Test 1: Exact simple logic (what user describes)
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as user_count
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
    """, [campaign['id'], start_date, end_date])
    
    result = cursor.fetchone()
    simple_count = result['user_count']
    
    print(f"   🧪 Simple Logic: {simple_count} users")
    
    if simple_count == campaign['expected']:
        print(f"   ✅ PERFECT MATCH! Logic is correct for this campaign")
    else:
        diff = abs(simple_count - campaign['expected'])
        print(f"   ❌ Mismatch: Off by {diff} users")
    
    # Test 2: Check what we get without has_abi_attribution filter
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as user_count
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
    """, [campaign['id'], start_date, end_date])
    
    result = cursor.fetchone()
    no_abi_filter_count = result['user_count']
    
    print(f"   🔍 Without ABI filter: {no_abi_filter_count} users")
    
    if no_abi_filter_count == campaign['expected']:
        print(f"   💡 MATCH WITHOUT ABI FILTER! Mixpanel might not use this filter")
    
    print()

def test_campaign_variations(cursor, campaign, start_date, end_date):
    """Test variations if simple logic doesn't work"""
    
    print(f"🔬 VARIATIONS FOR: {campaign['name']} (Expected: {campaign['expected']})")
    print("-" * 50)
    
    variations = {
        'events_only': {
            'description': 'Events table only (no user join required)',
            'query': """
                SELECT COUNT(DISTINCT e.distinct_id) as user_count
                FROM mixpanel_event e
                WHERE e.abi_campaign_id = ?
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            """,
            'params': [campaign['id'], start_date, end_date]
        },
        
        'total_events': {
            'description': 'Total events (not unique users)',
            'query': """
                SELECT COUNT(*) as event_count
                FROM mixpanel_event e
                JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            """,
            'params': [campaign['id'], start_date, end_date]
        },
        
        'expanded_date_range': {
            'description': 'Expanded date range (July 15-30)',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            """,
            'params': [campaign['id'], '2025-07-15', '2025-07-30']
        },
        
        'profile_campaign_check': {
            'description': 'Campaign from profile JSON',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE (
                    u.abi_campaign_id = ?
                    OR JSON_EXTRACT(u.profile_json, '$.initial_utm_id') = ?
                    OR JSON_EXTRACT(u.profile_json, '$.utm_campaign') LIKE '%' || ? || '%'
                )
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            """,
            'params': [campaign['id'], campaign['id'], campaign['id'], start_date, end_date]
        }
    }
    
    for var_name, var_config in variations.items():
        try:
            cursor.execute(var_config['query'], var_config['params'])
            result = cursor.fetchone()
            
            if 'user_count' in result.keys():
                count = result['user_count']
            elif 'event_count' in result.keys():
                count = result['event_count']
            else:
                count = result[0]
            
            print(f"   🧪 {var_name}:")
            print(f"      📝 {var_config['description']}")
            print(f"      📊 Result: {count}")
            
            if count == campaign['expected']:
                print(f"      🎯 MATCH! This variation works for this campaign")
            elif abs(count - campaign['expected']) <= 2:
                print(f"      🔸 Close: Off by {abs(count - campaign['expected'])}")
            
            print()
            
        except Exception as e:
            print(f"      ❌ Error: {e}")
            print()

if __name__ == "__main__":
    exit(main()) 