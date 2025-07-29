#!/usr/bin/env python3
"""
Find the 9 Trials - Detective Mode

Find EXACTLY when this campaign has 9 trials and test the tooltip query.
"""

import sqlite3

def find_9_trials():
    """Find the date range that gives exactly 9 trials"""
    
    campaign_id = "120223331225260178"
    
    print("🕵️ DETECTIVE MODE: FINDING THE 9 TRIALS")
    print("=" * 55)
    print(f"🎯 Campaign: {campaign_id}")
    print()
    
    with sqlite3.connect("database/mixpanel_data.db") as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Show ALL trial events for this campaign by date
        print("📅 ALL TRIAL EVENTS FOR THIS CAMPAIGN:")
        print("-" * 45)
        
        cursor.execute("""
            SELECT 
                DATE(e.event_time) as trial_date,
                COUNT(DISTINCT u.distinct_id) as daily_trials,
                u.distinct_id
            FROM mixpanel_user u
            JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND e.event_name = 'RC Trial started'
            GROUP BY DATE(e.event_time)
            ORDER BY trial_date DESC
        """, [campaign_id])
        
        daily_data = cursor.fetchall()
        
        total_trials = 0
        for row in daily_data:
            trial_date = row['trial_date']
            daily_trials = row['daily_trials']
            total_trials += daily_trials
            print(f"   {trial_date}: {daily_trials} trials (cumulative: {total_trials})")
        
        print(f"\n📊 Total trials across all time: {total_trials}")
        
        # Test different date ranges to find which gives 9
        print(f"\n🔍 TESTING DATE RANGES FOR 9 TRIALS:")
        print("-" * 45)
        
        test_ranges = []
        
        # Build ranges from recent dates backwards
        if daily_data:
            recent_dates = [row['trial_date'] for row in daily_data[:10]]
            
            # Test various combinations
            for i, start_date in enumerate(recent_dates):
                for j, end_date in enumerate(recent_dates[:i+1]):
                    test_ranges.append((end_date, start_date))  # end_date is more recent
        
        # Add some standard ranges
        test_ranges.extend([
            ("2025-07-22", "2025-07-28"),
            ("2025-07-20", "2025-07-28"),
            ("2025-07-01", "2025-07-31"),
            ("2025-06-01", "2025-07-31"),
            ("2025-01-01", "2025-12-31"),
        ])
        
        matching_ranges = []
        
        for start_date, end_date in test_ranges:
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """, [start_date, end_date, campaign_id])
            
            result = cursor.fetchone()
            trials = result['mixpanel_trials']
            
            if trials == 9:
                matching_ranges.append((start_date, end_date))
                print(f"   🎯 {start_date} to {end_date}: {trials} trials - MATCH!")
            elif trials > 0:
                print(f"      {start_date} to {end_date}: {trials} trials")
        
        if not matching_ranges:
            print("\n❌ NO DATE RANGE GIVES EXACTLY 9 TRIALS!")
            print("🤔 This suggests the frontend might be using different logic or caching")
            
            # Show closest matches
            print("\n🔍 CLOSEST MATCHES:")
            for start_date, end_date in test_ranges[:10]:
                cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials
                    FROM mixpanel_user u
                    LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                    WHERE u.abi_campaign_id = ?
                      AND u.has_abi_attribution = TRUE
                """, [start_date, end_date, campaign_id])
                
                result = cursor.fetchone()
                trials = result['mixpanel_trials']
                
                if trials in [8, 9, 10, 11]:  # Close to 9
                    diff = abs(trials - 9)
                    print(f"      {start_date} to {end_date}: {trials} trials (off by {diff})")
            
            return
        
        # Test tooltip query for each matching range
        print(f"\n🔧 TESTING TOOLTIP QUERIES:")
        print("-" * 35)
        
        for start_date, end_date in matching_ranges[:3]:  # Test top 3 matches
            print(f"\n📅 Range: {start_date} to {end_date}")
            
            # Test the EXACT tooltip query (after our fixes)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT upm.distinct_id) as unique_users
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND (upm.valid_lifecycle = 1 OR upm.valid_lifecycle IS NULL)
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND upm.trial_converted_to_refund_rate IS NOT NULL  
                  AND upm.initial_purchase_to_refund_rate IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = upm.distinct_id 
                      AND e.event_name = 'RC Trial started'
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
            """, [campaign_id, start_date, end_date])
            
            tooltip_result = cursor.fetchone()
            total_records = tooltip_result['total_records']
            unique_users = tooltip_result['unique_users']
            
            print(f"   Dashboard: 9 trials")
            print(f"   Tooltip: {total_records} records, {unique_users} unique users")
            
            if total_records == 9:
                print("   ✅ PERFECT MATCH!")
            elif unique_users == 9:
                print(f"   ⚠️  Unique users match, but {total_records - unique_users} duplicate records")
            else:
                print(f"   ❌ MISMATCH: Expected 9, got {total_records}")
                
                # Show the specific users causing issues
                cursor.execute("""
                    SELECT 
                        upm.distinct_id,
                        COUNT(*) as record_count,
                        GROUP_CONCAT(upm.product_id) as products
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE u.abi_campaign_id = ?
                      AND (upm.valid_lifecycle = 1 OR upm.valid_lifecycle IS NULL)
                      AND upm.trial_conversion_rate IS NOT NULL
                      AND EXISTS (
                          SELECT 1 FROM mixpanel_event e 
                          WHERE e.distinct_id = upm.distinct_id 
                          AND e.event_name = 'RC Trial started'
                          AND DATE(e.event_time) BETWEEN ? AND ?
                      )
                    GROUP BY upm.distinct_id
                    HAVING COUNT(*) > 1
                """, [campaign_id, start_date, end_date])
                
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"   🔍 {len(duplicates)} users with multiple records:")
                    for dup in duplicates[:5]:
                        distinct_id = dup['distinct_id']
                        record_count = dup['record_count']
                        products = dup['products']
                        print(f"      👤 {distinct_id[:15]}... | {record_count} records | {products}")

if __name__ == "__main__":
    find_9_trials() 