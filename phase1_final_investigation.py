#!/usr/bin/env python3
"""
PHASE 1: FINAL INVESTIGATION - The Missing 9 Users

Mathematical pattern discovered: Trial users + 9 = 39
Goal: Identify exactly what those 9 missing users represent
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
    """Find the exact 9 missing users"""
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    print("🎯 PHASE 1: FINAL INVESTIGATION - THE MISSING 9 USERS")
    print("=" * 70)
    print(f"📊 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🧮 Pattern: 30 trial users + 9 missing users = 39")
    print()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 1. Identify exactly who the 30 current users are
            current_trial_users = get_current_trial_users(cursor, campaign_id, start_date, end_date)
            
            # 2. Find potential candidates for the missing 9
            potential_candidates = find_missing_user_candidates(cursor, campaign_id, start_date, end_date, current_trial_users)
            
            # 3. Test specific combinations to reach 39
            test_specific_combinations(cursor, campaign_id, start_date, end_date, current_trial_users, potential_candidates)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def get_current_trial_users(cursor, campaign_id, start_date, end_date):
    """Get the exact 30 users currently counted"""
    
    print("1️⃣ CURRENT TRIAL USERS (30)")
    print("-" * 40)
    
    cursor.execute("""
        SELECT DISTINCT u.distinct_id,
               u.first_seen,
               e.event_time as trial_time,
               upm.credited_date
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
    """, [campaign_id, start_date, end_date])
    
    current_users = cursor.fetchall()
    
    print(f"   📊 Found {len(current_users)} current trial users")
    print("   📋 Sample current users:")
    for i, user in enumerate(current_users[:5]):
        print(f"      {i+1}. {user['distinct_id'][:12]}... | Trial: {user['trial_time']}")
    
    return [user['distinct_id'] for user in current_users]

def find_missing_user_candidates(cursor, campaign_id, start_date, end_date, current_trial_users):
    """Find potential candidates for the missing 9 users"""
    
    print("\n2️⃣ MISSING USER CANDIDATES")
    print("-" * 40)
    
    # Convert current users to SQL IN clause
    current_users_placeholders = ','.join(['?' for _ in current_trial_users])
    base_params = [campaign_id] + current_trial_users
    
    candidate_queries = {
        'users_with_other_events': {
            'description': 'Users with other events in date range (not trial)',
            'query': f"""
                SELECT DISTINCT u.distinct_id,
                       u.first_seen,
                       e.event_name,
                       e.event_time,
                       upm.credited_date
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND u.distinct_id NOT IN ({current_users_placeholders})
                  AND DATE(e.event_time) BETWEEN ? AND ?
                ORDER BY e.event_time
            """,
            'params': base_params + [start_date, end_date]
        },
        
        'users_attributed_in_range': {
            'description': 'Users first seen in date range (no trial events yet)',
            'query': f"""
                SELECT DISTINCT u.distinct_id,
                       u.first_seen,
                       upm.credited_date,
                       'no_trial_yet' as event_name
                FROM mixpanel_user u
                LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND u.distinct_id NOT IN ({current_users_placeholders})
                  AND DATE(u.first_seen) BETWEEN ? AND ?
                ORDER BY u.first_seen
            """,
            'params': base_params + [start_date, end_date]
        },
        
        'users_with_trials_outside_range': {
            'description': 'Users with trial events outside date range',
            'query': f"""
                SELECT DISTINCT u.distinct_id,
                       u.first_seen,
                       e.event_time as trial_time,
                       upm.credited_date,
                       'trial_outside_range' as event_name
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND u.distinct_id NOT IN ({current_users_placeholders})
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) NOT BETWEEN ? AND ?
                ORDER BY e.event_time
            """,
            'params': base_params + [start_date, end_date]
        },
        
        'users_credited_in_range': {
            'description': 'Users credited in date range (regardless of trial timing)',
            'query': f"""
                SELECT DISTINCT u.distinct_id,
                       u.first_seen,
                       upm.credited_date,
                       'credited_in_range' as event_name
                FROM mixpanel_user u
                JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND u.distinct_id NOT IN ({current_users_placeholders})
                  AND upm.credited_date BETWEEN ? AND ?
                ORDER BY upm.credited_date
            """,
            'params': base_params + [start_date, end_date]
        }
    }
    
    all_candidates = {}
    
    for candidate_type, config in candidate_queries.items():
        try:
            cursor.execute(config['query'], config['params'])
            candidates = cursor.fetchall()
            
            print(f"   🔍 {candidate_type}:")
            print(f"      📝 {config['description']}")
            print(f"      📊 Found: {len(candidates)} candidates")
            
            if candidates:
                print("      📋 Sample candidates:")
                for i, candidate in enumerate(candidates[:3]):
                    print(f"         {i+1}. {candidate['distinct_id'][:12]}... | {candidate['event_name']}")
                
                all_candidates[candidate_type] = [c['distinct_id'] for c in candidates]
                
                # Check if adding these gets us to 39
                if len(candidates) == 9:
                    print(f"      🎯 POTENTIAL MATCH! Exactly 9 candidates!")
                elif 6 <= len(candidates) <= 12:
                    print(f"      🔸 Close to 9: {len(candidates)} candidates")
            
            print()
            
        except Exception as e:
            print(f"      ❌ Error: {e}")
            print()
    
    return all_candidates

def test_specific_combinations(cursor, campaign_id, start_date, end_date, current_trial_users, candidates):
    """Test specific combinations to reach exactly 39"""
    
    print("3️⃣ TESTING SPECIFIC COMBINATIONS TO REACH 39")
    print("-" * 50)
    
    # Test adding different candidate groups to current 30
    for candidate_type, candidate_ids in candidates.items():
        combined_users = set(current_trial_users + candidate_ids)
        total_count = len(combined_users)
        
        print(f"   🧪 Current (30) + {candidate_type}:")
        print(f"      📊 Total unique users: {total_count}")
        
        if total_count == 39:
            print(f"      🎯 EXACT MATCH! This combination yields 39!")
            
            # Verify this combination matches Mixpanel logic
            verify_combination(cursor, campaign_id, start_date, end_date, list(combined_users), candidate_type)
        elif 35 <= total_count <= 43:
            print(f"      🔸 Close: {total_count} (diff: {abs(total_count - 39)})")
        else:
            print(f"      📊 Count: {total_count}")
        
        print()
    
    # Test intersections and unions
    if len(candidates) >= 2:
        candidate_keys = list(candidates.keys())
        
        print("   🔬 Testing candidate intersections:")
        for i in range(len(candidate_keys)):
            for j in range(i+1, len(candidate_keys)):
                type1, type2 = candidate_keys[i], candidate_keys[j]
                
                # Union
                union_candidates = list(set(candidates[type1] + candidates[type2]))
                combined_union = set(current_trial_users + union_candidates)
                
                # Intersection
                intersection_candidates = list(set(candidates[type1]) & set(candidates[type2]))
                combined_intersection = set(current_trial_users + intersection_candidates)
                
                print(f"      Union {type1} + {type2}: {len(combined_union)} total")
                print(f"      Intersection {type1} ∩ {type2}: {len(combined_intersection)} total")
                
                if len(combined_union) == 39:
                    print(f"      🎯 UNION MATCH! {type1} + {type2} = 39")
                if len(combined_intersection) == 39:
                    print(f"      🎯 INTERSECTION MATCH! {type1} ∩ {type2} = 39")

def verify_combination(cursor, campaign_id, start_date, end_date, user_list, combination_type):
    """Verify a combination that yields 39 users"""
    
    print(f"      🔍 VERIFYING COMBINATION: {combination_type}")
    print(f"      📊 User count: {len(user_list)}")
    
    # Convert to SQL IN clause for verification
    user_placeholders = ','.join(['?' for _ in user_list])
    
    cursor.execute(f"""
        SELECT 
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN EXISTS (
                SELECT 1 FROM mixpanel_event e 
                WHERE e.distinct_id = u.distinct_id 
                AND e.event_name = 'RC Trial started'
                AND DATE(e.event_time) BETWEEN ? AND ?
            ) THEN u.distinct_id END) as users_with_trials_in_range,
            COUNT(DISTINCT CASE WHEN DATE(u.first_seen) BETWEEN ? AND ? 
                              THEN u.distinct_id END) as users_first_seen_in_range
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND u.distinct_id IN ({user_placeholders})
    """, [start_date, end_date, start_date, end_date, campaign_id] + user_list)
    
    verification = cursor.fetchone()
    
    print(f"         Total users in combination: {verification['total_users']}")
    print(f"         Users with trials in range: {verification['users_with_trials_in_range']}")
    print(f"         Users first seen in range: {verification['users_first_seen_in_range']}")
    
    if verification['total_users'] == 39:
        print(f"         ✅ VERIFIED: Combination has exactly 39 users!")
        print(f"         💡 MIXPANEL LOGIC DISCOVERED: {combination_type}")
    else:
        print(f"         ❌ Verification failed: Expected 39, got {verification['total_users']}")

if __name__ == "__main__":
    exit(main()) 