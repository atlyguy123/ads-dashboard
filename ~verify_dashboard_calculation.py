#!/usr/bin/env python3
"""
Script to verify the exact dashboard calculation for trial counts.
This runs the EXACT same query logic that the dashboard uses.
"""

import sqlite3
from pathlib import Path

# Configuration
CAMPAIGN_ID = "120223331225260178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    """Get the mixpanel database path"""
    return "database/mixpanel_data.db"

def verify_dashboard_trial_calculation():
    """
    Run the EXACT same query that the dashboard uses for trial counts.
    From analytics_query_service.py lines 1189-1200
    """
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # This is the EXACT query from the dashboard (analytics_query_service.py)
        # Line 1189-1200: COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started
        query = """
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
        """
        
        # Note: The dashboard query has the campaign_id in the WHERE clause, then the date range in the CASE WHEN
        params = [START_DATE, END_DATE, CAMPAIGN_ID]
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        return result['mixpanel_trials_started']

def verify_csv_count():
    """Count distinct IDs from CSV"""
    csv_users = set()
    with open("mixpanel_user.csv", 'r') as f:
        import csv
        reader = csv.DictReader(f)
        for row in reader:
            distinct_id = row.get('Distinct ID', '').strip()
            if distinct_id:
                csv_users.add(distinct_id)
    return len(csv_users)

def verify_database_direct_trial_count():
    """Count unique users with trial events directly from database"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Direct query to count unique trial users
        query = """
        SELECT COUNT(DISTINCT u.distinct_id) as trial_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        """
        
        params = [CAMPAIGN_ID, START_DATE, END_DATE]
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        return result['trial_users']

def main():
    print("🔍 VERIFYING ALL CALCULATIONS")
    print("=" * 50)
    
    # 1. Dashboard calculation (exact replica)
    print("\n1️⃣ DASHBOARD CALCULATION (exact replica):")
    dashboard_count = verify_dashboard_trial_calculation()
    print(f"   Result: {dashboard_count} trials")
    
    # 2. CSV count
    print("\n2️⃣ CSV COUNT:")
    csv_count = verify_csv_count()
    print(f"   Result: {csv_count} users")
    
    # 3. Direct database trial count
    print("\n3️⃣ DATABASE DIRECT TRIAL COUNT:")
    db_count = verify_database_direct_trial_count()
    print(f"   Result: {db_count} users")
    
    print("\n" + "=" * 50)
    print("📊 SUMMARY:")
    print(f"Dashboard (exact replica): {dashboard_count}")
    print(f"CSV count: {csv_count}")
    print(f"Database direct count: {db_count}")
    
    if dashboard_count == 42:
        print("✅ Dashboard calculation confirmed at 42")
    else:
        print(f"⚠️ Dashboard calculation shows {dashboard_count}, not 42")
    
    if csv_count == 41:
        print("✅ CSV count confirmed at 41")
    else:
        print(f"⚠️ CSV count shows {csv_count}, not 41")

if __name__ == "__main__":
    main() 