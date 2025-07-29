#!/usr/bin/env python3
"""
Analyze Mixpanel Users - Individual User Analysis

Compare the 40 users from Mixpanel CSV with our database to find:
1. Which users are missing from our database
2. Which users we have but aren't counting  
3. Why we're getting 30 instead of 40

Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign
Campaign ID: 120223331225260178
Date Range: July 16-29, 2025
Mixpanel Count: 40 users
Our Count: 30 users
Missing: 10 users
"""

import sqlite3
import csv
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Analyze each Mixpanel user individually"""
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    print("🔍 INDIVIDUAL USER ANALYSIS - MIXPANEL vs DATABASE")
    print("=" * 70)
    print(f"📊 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Expected: 40 users (from Mixpanel)")
    print(f"📊 Current: 30 users (from database)")
    print(f"❓ Missing: 10 users")
    print()
    
    # Read Mixpanel users from CSV
    mixpanel_users = read_mixpanel_users()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Analyze each user individually
            analyze_each_user(cursor, mixpanel_users, campaign_id, start_date, end_date)
            
            # Summary analysis
            summary_analysis(cursor, mixpanel_users, campaign_id, start_date, end_date)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def read_mixpanel_users():
    """Read users from the Mixpanel CSV file"""
    
    mixpanel_users = []
    
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                mixpanel_users.append({
                    'user_id': row['User ID'],
                    'campaign_name': row['abi_~campaign'],
                    'campaign_id': row['abi_~campaign_id'],
                    'trial_count': int(row['Uniques of RC Trial started'])
                })
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []
    
    print(f"📋 Loaded {len(mixpanel_users)} users from Mixpanel CSV")
    print()
    
    return mixpanel_users

def analyze_each_user(cursor, mixpanel_users, campaign_id, start_date, end_date):
    """Analyze each user individually"""
    
    print("👤 INDIVIDUAL USER ANALYSIS")
    print("-" * 40)
    
    found_in_db = 0
    missing_from_db = 0
    has_trials_in_range = 0
    has_campaign_id = 0
    has_abi_attribution = 0
    
    missing_users = []
    found_but_not_counted = []
    
    for i, user in enumerate(mixpanel_users, 1):
        user_id = user['user_id']
        
        print(f"   {i:2d}. {user_id}")
        
        # Check if user exists in our database
        cursor.execute("""
            SELECT 
                u.distinct_id,
                u.abi_campaign_id,
                u.has_abi_attribution,
                COUNT(CASE WHEN e.event_name = 'RC Trial started' 
                           AND DATE(e.event_time) BETWEEN ? AND ? 
                           THEN 1 END) as trials_in_range,
                COUNT(CASE WHEN e.event_name = 'RC Trial started' 
                           THEN 1 END) as total_trials
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.distinct_id = ?
            GROUP BY u.distinct_id, u.abi_campaign_id, u.has_abi_attribution
        """, [start_date, end_date, user_id])
        
        db_user = cursor.fetchone()
        
        if db_user:
            found_in_db += 1
            
            # Check campaign ID match
            if db_user['abi_campaign_id'] == campaign_id:
                has_campaign_id += 1
                campaign_match = "✅"
            else:
                campaign_match = f"❌ ({db_user['abi_campaign_id']})"
            
            # Check ABI attribution
            if db_user['has_abi_attribution']:
                has_abi_attribution += 1
                abi_status = "✅"
            else:
                abi_status = "❌"
            
            # Check trials in range
            if db_user['trials_in_range'] > 0:
                has_trials_in_range += 1
                trials_status = f"✅ ({db_user['trials_in_range']})"
            else:
                trials_status = f"❌ (0 in range, {db_user['total_trials']} total)"
                found_but_not_counted.append({
                    'user_id': user_id,
                    'reason': f"No trials in range (has {db_user['total_trials']} total trials)",
                    'campaign_match': db_user['abi_campaign_id'] == campaign_id,
                    'abi_attribution': db_user['has_abi_attribution']
                })
            
            print(f"       🏢 In DB: ✅ | Campaign: {campaign_match} | ABI: {abi_status} | Trials: {trials_status}")
            
        else:
            missing_from_db += 1
            missing_users.append(user_id)
            print(f"       🏢 In DB: ❌ NOT FOUND")
    
    print()
    print(f"📊 SUMMARY:")
    print(f"   👥 Total Mixpanel users: {len(mixpanel_users)}")
    print(f"   ✅ Found in database: {found_in_db}")
    print(f"   ❌ Missing from database: {missing_from_db}")
    print(f"   🎯 Have correct campaign ID: {has_campaign_id}")
    print(f"   🏷️  Have ABI attribution: {has_abi_attribution}")
    print(f"   📅 Have trials in date range: {has_trials_in_range}")
    print()
    
    if missing_users:
        print("❌ USERS MISSING FROM DATABASE:")
        for user_id in missing_users:
            print(f"     {user_id}")
        print()
    
    if found_but_not_counted:
        print("🔍 USERS FOUND BUT NOT COUNTED:")
        for user in found_but_not_counted:
            print(f"     {user['user_id']}: {user['reason']}")
        print()

def summary_analysis(cursor, mixpanel_users, campaign_id, start_date, end_date):
    """Perform summary analysis"""
    
    print("📊 DETAILED ANALYSIS")
    print("-" * 30)
    
    # Get our current query results
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT u.distinct_id) as our_count,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                              AND DATE(e.event_time) BETWEEN ? AND ? 
                              THEN u.distinct_id END) as our_trial_count
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
    """, [start_date, end_date, campaign_id])
    
    our_results = cursor.fetchone()
    
    print(f"🎯 Our current logic results:")
    print(f"   Total users with campaign ID + ABI: {our_results['our_count']}")
    print(f"   Users with trials in range: {our_results['our_trial_count']}")
    print()
    
    # Check how many Mixpanel users we can find with our query
    mixpanel_user_ids = [user['user_id'] for user in mixpanel_users]
    user_placeholders = ','.join(['?' for _ in mixpanel_user_ids])
    
    cursor.execute(f"""
        SELECT 
            COUNT(DISTINCT u.distinct_id) as found_mixpanel_users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                              AND DATE(e.event_time) BETWEEN ? AND ? 
                              THEN u.distinct_id END) as found_with_trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.distinct_id IN ({user_placeholders})
          AND u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
    """, [start_date, end_date] + mixpanel_user_ids + [campaign_id])
    
    found_results = cursor.fetchone()
    
    print(f"📋 Mixpanel users in our database:")
    print(f"   Found in our DB: {found_results['found_mixpanel_users']}/40")
    print(f"   With trials in range: {found_results['found_with_trials']}/40")
    print()
    
    # Calculate the gap
    total_gap = 40 - our_results['our_trial_count']
    missing_from_db = 40 - found_results['found_mixpanel_users']
    found_but_not_counted = found_results['found_mixpanel_users'] - found_results['found_with_trials']
    
    print(f"🔍 GAP ANALYSIS:")
    print(f"   Total gap: {total_gap} users (40 - {our_results['our_trial_count']})")
    print(f"   Missing from DB: {missing_from_db} users")
    print(f"   Found but not counted: {found_but_not_counted} users")
    print()
    
    if total_gap == missing_from_db + found_but_not_counted:
        print("✅ GAP FULLY EXPLAINED!")
    else:
        print("❓ Gap not fully explained - additional investigation needed")

if __name__ == "__main__":
    exit(main()) 