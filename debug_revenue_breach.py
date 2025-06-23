#!/usr/bin/env python3
"""
🔍 Revenue Breach Analysis Script

This script systematically analyzes users who have positive revenue events 
but zero estimated value to identify and fix remaining edge cases.

Edge Cases We're Solving:
1. Users with revenue but no refunds getting zero estimated value in pre-refund phase
2. Users in post-conversion phase with incorrect value calculations
3. Users missing proper conversion rate fallbacks
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def analyze_revenue_breach():
    """Analyze all users with revenue but zero estimated value"""
    
    print("🔍 COMPREHENSIVE REVENUE BREACH ANALYSIS")
    print("=" * 60)
    
    conn = sqlite3.connect('database/mixpanel_data.db')
    cursor = conn.cursor()
    
    # Find all users with positive revenue but zero estimated value
    query = """
    WITH breached_users AS (
        SELECT DISTINCT 
            me.distinct_id,
            JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id,
            me.event_name,
            me.revenue_usd,
            me.event_time,
            upm.current_value,
            upm.current_status,
            upm.value_status,
            upm.credited_date,
            upm.price_bucket,
            upm.trial_conversion_rate,
            upm.trial_converted_to_refund_rate,
            upm.initial_purchase_to_refund_rate,
            julianday('now') - julianday(me.event_time) as days_since_conversion
        FROM mixpanel_event me 
        JOIN user_product_metrics upm ON me.distinct_id = upm.distinct_id 
            AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
        WHERE me.event_name IN ('RC Initial purchase', 'RC Trial converted')
        AND me.revenue_usd > 0
        AND upm.current_value = 0
    )
    SELECT * FROM breached_users ORDER BY revenue_usd DESC;
    """
    
    cursor.execute(query)
    breached_users = cursor.fetchall()
    
    print(f"📊 Found {len(breached_users)} users with revenue breach")
    print()
    
    # Categorize the issues
    categories = {
        'missing_conversion_rates': [],
        'zero_price_bucket': [],
        'pre_refund_calculation_error': [],
        'status_calculation_error': [],
        'unknown_error': []
    }
    
    for user in breached_users:
        (distinct_id, product_id, event_name, revenue_usd, event_time, 
         current_value, current_status, value_status, credited_date, price_bucket, 
         trial_conversion_rate, trial_converted_to_refund_rate, 
         initial_purchase_to_refund_rate, days_since_conversion) = user
        
        # Check for missing conversion rates (NULL values)
        if trial_conversion_rate is None or trial_converted_to_refund_rate is None:
            categories['missing_conversion_rates'].append(user)
        elif price_bucket is None or price_bucket == 0:
            categories['zero_price_bucket'].append(user)
        elif value_status in ['post_conversion_pre_refund', 'post_purchase_pre_refund']:
            categories['pre_refund_calculation_error'].append(user)
        elif current_status in ['PLACEHOLDER_STATUS', 'unknown']:
            categories['status_calculation_error'].append(user)
        else:
            categories['unknown_error'].append(user)
    
    # Report findings
    print("📋 CATEGORIZED ISSUES:")
    for category, users in categories.items():
        if users:
            print(f"  {category.upper()}: {len(users)} users")
            print(f"    Revenue Impact: ${sum(float(u[3]) for u in users):,.2f}")
            
            # Show sample
            if users:
                sample = users[0]
                print(f"    Sample: {sample[0][:8]}... - ${sample[3]} revenue, status: {sample[6]}, value_status: {sample[7]}")
            print()
    
    # Check for refund events for these users
    print("🔍 CHECKING FOR ACTUAL REFUNDS...")
    users_without_refunds = check_for_refunds(cursor, breached_users)
    print(f"📊 Users without refund events: {len(users_without_refunds)}")
    print(f"💰 Revenue without refunds: ${sum(float(u[3]) for u in users_without_refunds):,.2f}")
    print()
    
    # Verify 31+ day rule
    print("📅 VERIFYING 31+ DAY RULE...")
    verify_final_value_rule(cursor)
    
    conn.close()
    return categories

def check_for_refunds(cursor, breached_users):
    """Check which breached users actually have refund events"""
    users_without_refunds = []
    
    for user in breached_users:
        distinct_id, product_id = user[0], user[1]
        
        # Check for negative revenue events (refunds)
        cursor.execute("""
            SELECT COUNT(*) FROM mixpanel_event 
            WHERE distinct_id = ? 
            AND JSON_EXTRACT(event_json, '$.properties.product_id') = ?
            AND revenue_usd < 0
        """, (distinct_id, product_id))
        
        refund_count = cursor.fetchone()[0]
        if refund_count == 0:
            users_without_refunds.append(user)
    
    return users_without_refunds

def verify_final_value_rule(cursor):
    """Verify that users 31+ days post-conversion have revenue = estimated value"""
    cursor.execute("""
        SELECT 
            COUNT(*) as total_final_users,
            COUNT(CASE WHEN ABS(upm.current_value - me.revenue_usd) < 0.01 THEN 1 END) as exact_matches,
            COUNT(CASE WHEN ABS(upm.current_value - me.revenue_usd) >= 0.01 THEN 1 END) as mismatches
        FROM user_product_metrics upm
        JOIN mixpanel_event me ON upm.distinct_id = me.distinct_id 
            AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
        WHERE me.event_name IN ('RC Initial purchase', 'RC Trial converted')
        AND me.revenue_usd > 0
        AND upm.current_value > 0
        AND julianday('now') - julianday(me.event_time) > 31
        AND upm.value_status = 'final_value'
    """)
    
    result = cursor.fetchone()
    total_final, exact_matches, mismatches = result
    
    print(f"  Total final value users (31+ days): {total_final}")
    print(f"  Exact revenue matches: {exact_matches}")
    print(f"  Mismatches: {mismatches}")
    
    if mismatches > 0:
        print("  ⚠️  Some users in final phase don't have exact revenue matching!")
    else:
        print("  ✅ All final phase users have exact revenue matching!")

def fix_conversion_rate_defaults():
    """Fix users missing conversion rate defaults"""
    print("\n🔧 FIXING MISSING CONVERSION RATE DEFAULTS...")
    
    conn = sqlite3.connect('database/mixpanel_data.db')
    cursor = conn.cursor()
    
    # Update NULL conversion rates with defaults
    cursor.execute("""
        UPDATE user_product_metrics 
        SET trial_conversion_rate = 0.25,
            trial_converted_to_refund_rate = 0.20,
            initial_purchase_to_refund_rate = 0.40
        WHERE trial_conversion_rate IS NULL 
           OR trial_converted_to_refund_rate IS NULL 
           OR initial_purchase_to_refund_rate IS NULL
    """)
    
    rows_updated = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"  ✅ Updated {rows_updated} records with default conversion rates")

if __name__ == "__main__":
    try:
        categories = analyze_revenue_breach()
        
        # Fix missing conversion rates if found
        if categories['missing_conversion_rates']:
            fix_conversion_rate_defaults()
            print("\n🔄 Re-run the value estimation script after fixing conversion rates")
        
        print("\n🎯 NEXT STEPS:")
        print("1. Fix missing conversion rate defaults (if any)")
        print("2. Re-run value estimation script")
        print("3. Verify pre-refund phase calculations")
        print("4. Document edge cases in code")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1) 