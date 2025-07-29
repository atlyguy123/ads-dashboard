#!/usr/bin/env python3
"""
Find the REAL 9 Trials

Check if the 9 trials are at ad/adset level within this campaign.
"""

import sqlite3

def find_real_9_trials():
    """Check if 9 trials are at ad/adset level"""
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-22"  
    end_date = "2025-07-28"
    
    print("🔍 FINDING THE REAL 9 TRIALS")
    print("=" * 40)
    print(f"🎯 Campaign: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print()
    
    with sqlite3.connect("database/mixpanel_data.db") as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check campaign level first
        print("1️⃣ CAMPAIGN LEVEL:")
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as trials
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
        """, [start_date, end_date, campaign_id])
        
        campaign_trials = cursor.fetchone()['trials']
        print(f"   Campaign {campaign_id}: {campaign_trials} trials")
        print()
        
        # Check adset level
        print("2️⃣ ADSET LEVEL:")
        cursor.execute("""
            SELECT 
                u.abi_ad_set_id,
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as trials
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND u.abi_ad_set_id IS NOT NULL
            GROUP BY u.abi_ad_set_id
            ORDER BY trials DESC
        """, [start_date, end_date, campaign_id])
        
        adsets = cursor.fetchall()
        print(f"   Found {len(adsets)} adsets:")
        
        adset_with_9 = None
        for adset in adsets:
            adset_id = adset['abi_ad_set_id']
            trials = adset['trials']
            status = "🎯 MATCH!" if trials == 9 else ""
            print(f"      Adset {adset_id}: {trials} trials {status}")
            
            if trials == 9:
                adset_with_9 = adset_id
        
        print()
        
        # Check ad level
        print("3️⃣ AD LEVEL:")
        cursor.execute("""
            SELECT 
                u.abi_ad_id,
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as trials
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND u.abi_ad_id IS NOT NULL
            GROUP BY u.abi_ad_id
            ORDER BY trials DESC
        """, [start_date, end_date, campaign_id])
        
        ads = cursor.fetchall()
        print(f"   Found {len(ads)} ads:")
        
        ad_with_9 = None
        for ad in ads[:10]:  # Show top 10
            ad_id = ad['abi_ad_id']
            trials = ad['trials']
            status = "🎯 MATCH!" if trials == 9 else ""
            print(f"      Ad {ad_id}: {trials} trials {status}")
            
            if trials == 9:
                ad_with_9 = ad_id
        
        print()
        
        # Test tooltip for any entity that has 9 trials
        if adset_with_9:
            print(f"4️⃣ TESTING TOOLTIP FOR ADSET {adset_with_9}:")
            test_tooltip_for_entity('adset', adset_with_9, start_date, end_date, cursor)
        elif ad_with_9:
            print(f"4️⃣ TESTING TOOLTIP FOR AD {ad_with_9}:")
            test_tooltip_for_entity('ad', ad_with_9, start_date, end_date, cursor)
        else:
            print("4️⃣ NO ENTITY FOUND WITH EXACTLY 9 TRIALS")
            print("🤔 The 9 might be from:")
            print("   - A different date range")
            print("   - Frontend caching/stale data") 
            print("   - A different campaign entirely")
            print("   - Some other filtering logic")
            
            # Show the closest matches
            print(f"\n🔍 CLOSEST MATCHES TO 9:")
            all_entities = []
            
            # Add adsets close to 9
            for adset in adsets:
                if abs(adset['trials'] - 9) <= 2:
                    all_entities.append(('adset', adset['abi_ad_set_id'], adset['trials']))
            
            # Add ads close to 9
            for ad in ads:
                if abs(ad['trials'] - 9) <= 2:
                    all_entities.append(('ad', ad['abi_ad_id'], ad['trials']))
            
            all_entities.sort(key=lambda x: abs(x[2] - 9))
            
            for entity_type, entity_id, trials in all_entities[:5]:
                diff = abs(trials - 9)
                print(f"      {entity_type.title()} {entity_id}: {trials} trials (off by {diff})")

def test_tooltip_for_entity(entity_type, entity_id, start_date, end_date, cursor):
    """Test tooltip query for a specific entity"""
    
    # Map entity type to field name
    entity_field_map = {
        'campaign': 'u.abi_campaign_id',
        'adset': 'u.abi_ad_set_id', 
        'ad': 'u.abi_ad_id'
    }
    
    entity_field = entity_field_map[entity_type]
    
    cursor.execute(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT upm.distinct_id) as unique_users
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE {entity_field} = ?
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
    """, [entity_id, start_date, end_date])
    
    result = cursor.fetchone()
    if result:
        total_records = result['total_records']
        unique_users = result['unique_users']
        
        print(f"   Dashboard: 9 trials")
        print(f"   Tooltip: {total_records} records, {unique_users} unique users")
        
        if total_records == 14:
            print("   🎯 BINGO! This matches your 14 tooltip users!")
        elif total_records == 9:
            print("   ✅ Perfect match!")
        else:
            print(f"   📊 Different numbers...")

if __name__ == "__main__":
    find_real_9_trials() 