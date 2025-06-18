#!/usr/bin/env python3
"""
Script to verify price bucket assignments by generating a CSV report.
Shows the actual price buckets created and how many users are assigned to each.
"""

import sqlite3
import pandas as pd
import csv
from collections import defaultdict

# Database path
DB_PATH = "/Users/joshuakaufman/Ads Dashboard V3 copy 12 - updated ingest copy/database/mixpanel_data.db"

def get_price_bucket_summary():
    """
    Extract price bucket data and generate summary CSV.
    Format: product_id, country, event, price_amount, number_of_users
    """
    print("🔍 Extracting price bucket verification data...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Query to get all users with their assigned price buckets and conversion events
    query = """
    SELECT 
        upm.product_id,
        COALESCE(mu.country, 'Unknown') as country,
        CASE 
            WHEN me.event_name IS NOT NULL THEN me.event_name
            ELSE 'No Conversion'
        END as event,
        upm.price_bucket,
        COUNT(*) as number_of_users
    FROM user_product_metrics upm
    LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
    LEFT JOIN (
        SELECT DISTINCT 
            distinct_id,
            JSON_EXTRACT(event_json, '$.properties.product_id') as product_id,
            event_name
        FROM mixpanel_event 
        WHERE revenue_usd > 0 
        AND event_name IN ('RC Initial purchase', 'RC Trial converted')
    ) me ON upm.distinct_id = me.distinct_id AND upm.product_id = me.product_id
    WHERE upm.price_bucket IS NOT NULL
    GROUP BY upm.product_id, mu.country, me.event_name, upm.price_bucket
    ORDER BY upm.product_id, mu.country, me.event_name, upm.price_bucket
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Rename price_bucket to price_amount for the output
    df = df.rename(columns={'price_bucket': 'price_amount'})
    
    print(f"📊 Found {len(df)} unique price bucket assignments")
    print(f"📈 Total users with price buckets: {df['number_of_users'].sum():,}")
    
    # Save to CSV
    output_file = "price_bucket_verification.csv"
    df.to_csv(output_file, index=False)
    
    print(f"✅ Saved verification data to: {output_file}")
    
    # Show some sample data
    print("\n📋 Sample of price bucket data:")
    print(df.head(10).to_string(index=False))
    
    # Show summary statistics
    print(f"\n📊 Summary Statistics:")
    print(f"   • Unique product/country/event combinations: {len(df)}")
    print(f"   • Price range: ${df['price_amount'].min():.2f} - ${df['price_amount'].max():.2f}")
    print(f"   • Users with $0 buckets: {df[df['price_amount'] == 0]['number_of_users'].sum():,}")
    print(f"   • Users with non-zero buckets: {df[df['price_amount'] > 0]['number_of_users'].sum():,}")
    
    return df

if __name__ == "__main__":
    get_price_bucket_summary() 