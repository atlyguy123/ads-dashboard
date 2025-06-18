#!/usr/bin/env python3
"""
Fixed script to verify price bucket assignments.
Properly categorizes users as converters vs trial-only users.
"""

import sqlite3
import pandas as pd

# Database path
DB_PATH = "/Users/joshuakaufman/Ads Dashboard V3 copy 12 - updated ingest copy/database/mixpanel_data.db"

def get_fixed_price_bucket_summary():
    """
    Extract price bucket data with proper user categorization.
    """
    print("🔍 Extracting corrected price bucket verification data...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get all users with their price buckets
    users_query = """
    SELECT 
        upm.distinct_id,
        upm.product_id,
        COALESCE(mu.country, 'Unknown') as country,
        upm.price_bucket
    FROM user_product_metrics upm
    LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
    WHERE upm.price_bucket IS NOT NULL
    """
    
    # Get all conversion events
    conversions_query = """
    SELECT DISTINCT 
        me.distinct_id,
        JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id,
        me.event_name
    FROM mixpanel_event me
    WHERE me.revenue_usd > 0 
    AND me.event_name IN ('RC Initial purchase', 'RC Trial converted')
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
    """
    
    users_df = pd.read_sql_query(users_query, conn)
    conversions_df = pd.read_sql_query(conversions_query, conn)
    conn.close()
    
    print(f"📊 Loaded {len(users_df)} users and {len(conversions_df)} conversions")
    
    # Create conversion lookup
    conversion_lookup = {}
    for _, row in conversions_df.iterrows():
        key = (row['distinct_id'], row['product_id'])
        if key not in conversion_lookup:
            conversion_lookup[key] = []
        conversion_lookup[key].append(row['event_name'])
    
    # Categorize users and create summary
    results = []
    
    for _, user in users_df.iterrows():
        user_key = (user['distinct_id'], user['product_id'])
        
        if user_key in conversion_lookup:
            # User has conversions
            for event_name in conversion_lookup[user_key]:
                results.append({
                    'product_id': user['product_id'],
                    'country': user['country'],
                    'event': event_name,
                    'price_amount': user['price_bucket'],
                    'user_type': 'converter'
                })
        else:
            # User has no conversions (trial only or inherited)
            results.append({
                'product_id': user['product_id'],
                'country': user['country'],
                'event': 'Trial Only' if user['price_bucket'] > 0 else 'No Events',
                'price_amount': user['price_bucket'],
                'user_type': 'trial_only' if user['price_bucket'] > 0 else 'no_events'
            })
    
    # Convert to DataFrame and aggregate
    results_df = pd.DataFrame(results)
    
    # Group by product_id, country, event, price_amount and count users
    summary_df = results_df.groupby(['product_id', 'country', 'event', 'price_amount']).size().reset_index(name='number_of_users')
    
    # Sort for better readability
    summary_df = summary_df.sort_values(['product_id', 'country', 'event', 'price_amount'])
    
    print(f"📈 Created summary with {len(summary_df)} unique combinations")
    
    # Save to CSV
    output_file = "price_bucket_verification_fixed.csv"
    summary_df.to_csv(output_file, index=False)
    
    print(f"✅ Saved corrected verification data to: {output_file}")
    
    # Show sample data
    print("\n📋 Sample of corrected price bucket data:")
    print(summary_df.head(15).to_string(index=False))
    
    # Show the problematic case
    print(f"\n🔍 Checking gluten.free.eats.2.yearly in AE:")
    ae_gluten = summary_df[
        (summary_df['product_id'] == 'gluten.free.eats.2.yearly') & 
        (summary_df['country'] == 'AE')
    ]
    print(ae_gluten.to_string(index=False))
    
    return summary_df

if __name__ == "__main__":
    get_fixed_price_bucket_summary() 