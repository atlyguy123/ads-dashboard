#!/usr/bin/env python3
"""
PHASE 1: ADVANCED INVESTIGATION - Complex Hypotheses Testing

Based on initial findings, testing advanced hypotheses about 
how Mixpanel UI might be counting users to get 39 results.

Key Insight: Standard date filtering didn't work, suggesting 
Mixpanel uses different logic entirely.
"""

import sqlite3
import json
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Execute advanced hypothesis testing"""
    
    # Test case details
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    expected_count = 39  # From Mixpanel UI
    
    print("🔬 PHASE 1: ADVANCED INVESTIGATION")
    print("=" * 60)
    print(f"📊 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Target: {expected_count} (Mixpanel UI)")
    print()
    print("💡 Hypothesis: Mixpanel uses non-event-based user counting")
    print()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Test advanced hypotheses
            test_attribution_based_counting(cursor, campaign_id, start_date, end_date)
            test_union_based_approaches(cursor, campaign_id, start_date, end_date)
            test_profile_based_filtering(cursor, campaign_id, start_date, end_date)
            test_event_type_variations(cursor, campaign_id, start_date, end_date)
            test_mathematical_combinations(cursor, campaign_id, start_date, end_date)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def test_attribution_based_counting(cursor, campaign_id, start_date, end_date):
    """Test if Mixpanel counts based on attribution rather than events"""
    
    print("🧪 HYPOTHESIS 1: ATTRIBUTION-BASED COUNTING")
    print("-" * 50)
    print("Testing if Mixpanel counts all attributed users in any way...")
    
    tests = {
        'all_attributed_users': {
            'description': 'All users with campaign attribution (regardless of events)',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """,
            'params': [campaign_id]
        },
        
        'attributed_with_any_events': {
            'description': 'Attributed users with ANY events (not just trials)',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id
                  )
            """,
            'params': [campaign_id]
        },
        
        'attributed_in_date_range': {
            'description': 'Users attributed (first_seen) in date range',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND DATE(u.first_seen) BETWEEN ? AND ?
            """,
            'params': [campaign_id, start_date, end_date]
        },
        
        'attributed_with_metrics': {
            'description': 'Attributed users with user_product_metrics',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND EXISTS (
                      SELECT 1 FROM user_product_metrics upm 
                      WHERE upm.distinct_id = u.distinct_id
                  )
            """,
            'params': [campaign_id]
        }
    }
    
    execute_tests(cursor, tests)

def test_union_based_approaches(cursor, campaign_id, start_date, end_date):
    """Test union-based counting approaches"""
    
    print("\n🧪 HYPOTHESIS 2: UNION-BASED APPROACHES")
    print("-" * 50)
    print("Testing if Mixpanel combines multiple criteria...")
    
    tests = {
        'trials_plus_fresh_users': {
            'description': 'Users with trials + users attributed in date range',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND (
                      EXISTS (
                          SELECT 1 FROM mixpanel_event e 
                          WHERE e.distinct_id = u.distinct_id 
                          AND e.event_name = 'RC Trial started'
                          AND DATE(e.event_time) BETWEEN ? AND ?
                      )
                      OR DATE(u.first_seen) BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date, start_date, end_date]
        },
        
        'trials_plus_credited_users': {
            'description': 'Users with trials + users credited in date range',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND (
                      EXISTS (
                          SELECT 1 FROM mixpanel_event e 
                          WHERE e.distinct_id = u.distinct_id 
                          AND e.event_name = 'RC Trial started'
                          AND DATE(e.event_time) BETWEEN ? AND ?
                      )
                      OR upm.credited_date BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date, start_date, end_date]
        },
        
        'all_events_in_range': {
            'description': 'Users with ANY events in date range',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        }
    }
    
    execute_tests(cursor, tests)

def test_profile_based_filtering(cursor, campaign_id, start_date, end_date):
    """Test profile-based filtering approaches"""
    
    print("\n🧪 HYPOTHESIS 3: PROFILE-BASED FILTERING")
    print("-" * 50)
    print("Testing if Mixpanel uses profile fields...")
    
    # First check what profile fields are available
    cursor.execute("""
        SELECT DISTINCT u.profile_json
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND u.profile_json IS NOT NULL
        LIMIT 5
    """, [campaign_id])
    
    sample_profiles = cursor.fetchall()
    
    print("   📋 Sample profile JSON structures:")
    for i, profile in enumerate(sample_profiles):
        try:
            profile_data = json.loads(profile['profile_json'])
            print(f"      Profile {i+1} keys: {list(profile_data.keys())}")
        except:
            print(f"      Profile {i+1}: Invalid JSON")
    
    tests = {
        'profile_date_fields': {
            'description': 'Users with various profile date fields in range',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND (
                      DATE(JSON_EXTRACT(u.profile_json, '$.first_install_date')) BETWEEN ? AND ?
                      OR DATE(JSON_EXTRACT(u.profile_json, '$.last_seen')) BETWEEN ? AND ?
                      OR DATE(JSON_EXTRACT(u.profile_json, '$.created_at')) BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date, start_date, end_date, start_date, end_date]
        },
        
        'utm_based_attribution': {
            'description': 'Users with UTM attribution matching campaign',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND (
                      JSON_EXTRACT(u.profile_json, '$.initial_utm_id') = ?
                      OR JSON_EXTRACT(u.profile_json, '$.utm_campaign') LIKE '%' || ? || '%'
                  )
            """,
            'params': [campaign_id, campaign_id, campaign_id]
        }
    }
    
    execute_tests(cursor, tests)

def test_event_type_variations(cursor, campaign_id, start_date, end_date):
    """Test different event type combinations"""
    
    print("\n🧪 HYPOTHESIS 4: EVENT TYPE VARIATIONS")
    print("-" * 50)
    print("Testing if Mixpanel counts different event types...")
    
    # First check what event types exist for this campaign
    cursor.execute("""
        SELECT DISTINCT e.event_name, COUNT(*) as event_count
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND DATE(e.event_time) BETWEEN ? AND ?
        GROUP BY e.event_name
        ORDER BY event_count DESC
    """, [campaign_id, start_date, end_date])
    
    event_types = cursor.fetchall()
    
    print("   📊 Event types in date range:")
    for event_type in event_types:
        print(f"      {event_type['event_name']}: {event_type['event_count']} events")
    
    tests = {
        'multiple_event_types': {
            'description': 'Users with trials OR purchases OR other key events',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND e.event_name IN ('RC Trial started', 'RC Initial purchase', 'RC Renewal', 'RC Cancellation')
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        },
        
        'any_revenue_events': {
            'description': 'Users with any revenue-generating events',
            'query': """
                SELECT COUNT(DISTINCT u.distinct_id) as user_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND e.revenue_usd > 0
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        }
    }
    
    execute_tests(cursor, tests)

def test_mathematical_combinations(cursor, campaign_id, start_date, end_date):
    """Test mathematical combinations that might yield 39"""
    
    print("\n🧪 HYPOTHESIS 5: MATHEMATICAL COMBINATIONS")
    print("-" * 50)
    print("Testing mathematical combinations that yield 39...")
    
    # Get base numbers
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                              AND DATE(e.event_time) BETWEEN ? AND ? 
                              THEN u.distinct_id END) as trial_users,
            COUNT(DISTINCT CASE WHEN DATE(u.first_seen) BETWEEN ? AND ? 
                              THEN u.distinct_id END) as fresh_users,
            COUNT(DISTINCT u.distinct_id) as total_attributed
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
    """, [start_date, end_date, start_date, end_date, campaign_id])
    
    base_numbers = cursor.fetchone()
    
    print("   📊 Base numbers:")
    print(f"      Trial users: {base_numbers['trial_users']}")
    print(f"      Fresh users: {base_numbers['fresh_users']}")
    print(f"      Total attributed: {base_numbers['total_attributed']}")
    
    # Calculate potential combinations
    trial_users = base_numbers['trial_users']
    fresh_users = base_numbers['fresh_users']
    total_attributed = base_numbers['total_attributed']
    
    print("\n   🧮 Potential mathematical combinations:")
    
    combinations = [
        (trial_users + 9, f"Trial users + 9 = {trial_users + 9}"),
        (fresh_users + 11, f"Fresh users + 11 = {fresh_users + 11}"),
        (trial_users + fresh_users - 19, f"Trial + Fresh - overlap = {trial_users + fresh_users - 19}"),
        (total_attributed - (total_attributed - 39), f"Simple: {total_attributed} - {total_attributed - 39} = 39")
    ]
    
    for value, description in combinations:
        if value == 39:
            print(f"      🎯 MATCH! {description}")
        else:
            print(f"      📊 {description}")

def execute_tests(cursor, tests):
    """Execute a set of test queries"""
    
    for test_name, test_config in tests.items():
        try:
            print(f"   🧪 Testing: {test_name}")
            print(f"      📝 {test_config['description']}")
            
            cursor.execute(test_config['query'], test_config['params'])
            result = cursor.fetchone()
            count = result['user_count']
            
            if count == 39:
                print(f"      🎯 MATCH! Count: {count} ⭐⭐⭐")
            elif 35 <= count <= 43:  # Close to 39
                print(f"      🔸 Close: {count} (diff: {abs(count - 39)})")
            else:
                print(f"      📊 Count: {count}")
                
        except Exception as e:
            print(f"      ❌ Error: {e}")
        
        print()

if __name__ == "__main__":
    exit(main()) 