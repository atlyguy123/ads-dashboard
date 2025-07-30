#!/usr/bin/env python3
"""
Investigate the 4 specific users that are in database but not in Mixpanel CSV
to understand why Mixpanel excluded them from the export.
"""

import sqlite3
import json
from typing import List, Dict, Any

# The 4 users that are in DB but not in CSV
EXCLUDED_USERS = [
    "$device:61833B32-C2E6-4F11-A5B7-F5C42665AA45",
    "$device:98E17F2E-2836-4C78-9D3F-75D128E16D9E", 
    "$device:D684F83A-B2AB-41DC-B0EC-A19C7980C45E",
    "197a0f6786d8-06993978608dde-497c7f60-59b90-197a0f6786ec48"
]

CAMPAIGN_ID = "120223331225260178"

def get_database_path():
    return "database/mixpanel_data.db"

def investigate_user_profiles(user_ids: List[str]) -> List[Dict[str, Any]]:
    """Get detailed user profile information"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in user_ids])
        query = f"""
        SELECT 
            distinct_id,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            country,
            region,
            city,
            has_abi_attribution,
            profile_json,
            first_seen,
            last_updated,
            valid_user,
            economic_tier
        FROM mixpanel_user
        WHERE distinct_id IN ({placeholders})
        """
        
        cursor.execute(query, user_ids)
        return [dict(row) for row in cursor.fetchall()]

def investigate_user_events(user_ids: List[str]) -> List[Dict[str, Any]]:
    """Get all events for these users"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in user_ids])
        query = f"""
        SELECT 
            event_uuid,
            event_name,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            distinct_id,
            event_time,
            country,
            region,
            revenue_usd,
            raw_amount,
            currency,
            refund_flag,
            is_late_event,
            trial_expiration_at_calc,
            event_json
        FROM mixpanel_event
        WHERE distinct_id IN ({placeholders})
        ORDER BY distinct_id, event_time
        """
        
        cursor.execute(query, user_ids)
        return [dict(row) for row in cursor.fetchall()]

def compare_with_csv_users() -> Dict[str, Any]:
    """Compare these users with a few users that ARE in the CSV"""
    import csv
    
    # Get first few users from CSV for comparison
    csv_users = []
    with open("mixpanel_user.csv", 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < 3:  # Get first 3 users from CSV
                csv_users.append(row.get('Distinct ID', '').strip())
            else:
                break
    
    # Get their profiles from database
    csv_user_profiles = investigate_user_profiles(csv_users)
    
    return {
        'csv_user_ids': csv_users,
        'csv_user_profiles': csv_user_profiles
    }

def main():
    print("🕵️ INVESTIGATING EXCLUDED USERS")
    print("=" * 50)
    print("Users in DB but NOT in Mixpanel CSV:")
    for user in EXCLUDED_USERS:
        print(f"  - {user}")
    print()
    
    # Step 1: Get detailed user profiles
    print("📋 Step 1: User Profile Analysis")
    print("-" * 30)
    user_profiles = investigate_user_profiles(EXCLUDED_USERS)
    
    for profile in user_profiles:
        print(f"\n👤 User: {profile['distinct_id']}")
        print(f"   Campaign ID: {profile['abi_campaign_id']}")
        print(f"   Ad ID: {profile['abi_ad_id']}")
        print(f"   Ad Set ID: {profile['abi_ad_set_id']}")
        print(f"   Has Attribution: {profile['has_abi_attribution']}")
        print(f"   Country: {profile['country']}")
        print(f"   Region: {profile['region']}")
        print(f"   City: {profile['city']}")
        print(f"   First Seen: {profile['first_seen']}")
        print(f"   Valid User: {profile['valid_user']}")
        print(f"   Economic Tier: {profile['economic_tier']}")
        
        # Parse profile JSON if available
        if profile['profile_json']:
            try:
                profile_data = json.loads(profile['profile_json'])
                print(f"   Profile Keys: {list(profile_data.keys())}")
                # Check for any special flags
                special_fields = ['$lib_version', '$insert_id', '$time', '$distinct_id']
                for field in special_fields:
                    if field in profile_data:
                        print(f"   {field}: {profile_data[field]}")
            except json.JSONDecodeError:
                print("   Profile JSON: Invalid JSON")
    
    # Step 2: Get all events for these users
    print(f"\n📅 Step 2: Event Analysis")
    print("-" * 25)
    user_events = investigate_user_events(EXCLUDED_USERS)
    
    events_by_user = {}
    for event in user_events:
        user_id = event['distinct_id']
        if user_id not in events_by_user:
            events_by_user[user_id] = []
        events_by_user[user_id].append(event)
    
    for user_id, events in events_by_user.items():
        print(f"\n👤 {user_id}:")
        print(f"   Total Events: {len(events)}")
        
        # Group by event type
        event_types = {}
        for event in events:
            event_name = event['event_name']
            if event_name not in event_types:
                event_types[event_name] = 0
            event_types[event_name] += 1
        
        for event_type, count in event_types.items():
            print(f"   {event_type}: {count} events")
        
        # Show trial events specifically
        trial_events = [e for e in events if e['event_name'] == 'RC Trial started']
        if trial_events:
            print(f"   Trial Events Details:")
            for event in trial_events:
                print(f"     Time: {event['event_time']}")
                print(f"     UUID: {event['event_uuid']}")
                print(f"     Campaign: {event['abi_campaign_id']}")
    
    # Step 3: Compare with CSV users
    print(f"\n🔄 Step 3: Comparison with CSV Users")
    print("-" * 35)
    comparison = compare_with_csv_users()
    
    print("Sample users that ARE in CSV:")
    for profile in comparison['csv_user_profiles']:
        print(f"  {profile['distinct_id']}")
        print(f"    Valid User: {profile['valid_user']}")
        print(f"    Economic Tier: {profile['economic_tier']}")
        print(f"    Has Attribution: {profile['has_abi_attribution']}")
    
    # Step 4: Look for patterns
    print(f"\n🔍 Step 4: Pattern Analysis")
    print("-" * 25)
    
    excluded_valid_users = [p['valid_user'] for p in user_profiles]
    excluded_economic_tiers = [p['economic_tier'] for p in user_profiles]
    excluded_countries = [p['country'] for p in user_profiles]
    
    csv_valid_users = [p['valid_user'] for p in comparison['csv_user_profiles']]
    csv_economic_tiers = [p['economic_tier'] for p in comparison['csv_user_profiles']]
    csv_countries = [p['country'] for p in comparison['csv_user_profiles']]
    
    print(f"Excluded users - Valid User flags: {excluded_valid_users}")
    print(f"CSV users - Valid User flags: {csv_valid_users}")
    print(f"Excluded users - Economic tiers: {excluded_economic_tiers}")
    print(f"CSV users - Economic tiers: {csv_economic_tiers}")
    print(f"Excluded users - Countries: {excluded_countries}")
    print(f"CSV users - Countries: {csv_countries}")

if __name__ == "__main__":
    main() 