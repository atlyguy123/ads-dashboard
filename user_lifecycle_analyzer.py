#!/usr/bin/env python3
"""
User Lifecycle Sequence Analyzer

This script analyzes the mixpanel database to identify and count all unique
user lifecycle sequences for user-product pairings.

It differentiates between:
- RC cancellation with zero revenue (regular cancellation)  
- RC cancellation with negative revenue (refund)

Output: Aggregated counts of each unique lifecycle scenario
"""

import sqlite3
import pandas as pd
from collections import defaultdict, Counter
import json
from datetime import datetime

# Import timezone utilities for consistent timezone handling
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))
from orchestrator.utils.timezone_utils import now_in_timezone

def connect_to_database(db_path):
    """Connect to the mixpanel database"""
    try:
        conn = sqlite3.connect(db_path)
        print(f"✅ Connected to database: {db_path}")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

def get_user_product_pairings(conn):
    """Get all user-product pairings from user_product_metrics table"""
    query = """
    SELECT DISTINCT distinct_id, product_id 
    FROM user_product_metrics
    ORDER BY distinct_id, product_id
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✅ Found {len(df)} user-product pairings")
        return df
    except Exception as e:
        print(f"❌ Error getting user-product pairings: {e}")
        return pd.DataFrame()

def get_events_for_user_product(conn, distinct_id, product_id):
    """Get all events for a specific user-product pairing, ordered chronologically"""
    query = """
    SELECT 
        event_name,
        event_time,
        revenue_usd,
        raw_amount,
        refund_flag
    FROM mixpanel_event 
    WHERE distinct_id = ? 
    ORDER BY event_time ASC
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=[distinct_id])
        return df
    except Exception as e:
        print(f"❌ Error getting events for user {distinct_id}: {e}")
        return pd.DataFrame()

def normalize_event_name(event_name, revenue_usd, refund_flag):
    """
    Normalize event names, especially for RC cancellation to differentiate
    between regular cancellation (zero revenue) and refund (negative revenue)
    """
    if event_name == "RC cancellation":
        if revenue_usd is None:
            revenue_usd = 0
        
        if revenue_usd < 0 or refund_flag:
            return "RC cancellation (refund)"
        else:
            return "RC cancellation (regular)"
    
    return event_name

def create_event_sequence(events_df):
    """Create a sequence of normalized event names from the events dataframe"""
    if events_df.empty:
        return []
    
    sequence = []
    for _, row in events_df.iterrows():
        normalized_event = normalize_event_name(
            row['event_name'], 
            row['revenue_usd'], 
            row['refund_flag']
        )
        sequence.append(normalized_event)
    
    return sequence

def analyze_all_lifecycles(conn):
    """Main function to analyze all user lifecycles"""
    print("🔍 Starting lifecycle analysis...")
    
    # Get all user-product pairings
    user_products = get_user_product_pairings(conn)
    
    if user_products.empty:
        print("❌ No user-product pairings found")
        return {}, {}
    
    lifecycle_sequences = []
    sequence_details = defaultdict(list)  # Store user-product details for each sequence
    processed_count = 0
    
    # Analyze each user-product pairing
    for _, row in user_products.iterrows():
        distinct_id = row['distinct_id']
        product_id = row['product_id']
        
        # Get events for this user-product pairing
        events_df = get_events_for_user_product(conn, distinct_id, product_id)
        
        # Create event sequence
        sequence = create_event_sequence(events_df)
        
        # Convert sequence to string for grouping
        sequence_str = " → ".join(sequence) if sequence else "No events"
        lifecycle_sequences.append(sequence_str)
        
        # Store user-product details for this sequence
        sequence_details[sequence_str].append({
            "distinct_id": distinct_id,
            "product_id": product_id
        })
        
        processed_count += 1
        if processed_count % 1000 == 0:
            print(f"   Processed {processed_count}/{len(user_products)} user-product pairings...")
    
    print(f"✅ Processed all {processed_count} user-product pairings")
    
    # Count occurrences of each unique sequence
    sequence_counts = Counter(lifecycle_sequences)
    
    return sequence_counts, sequence_details

def save_results(sequence_counts, sequence_details, output_file="lifecycle_sequences_detailed.json"):
    """Save detailed results with user lists to JSON file"""
    
    # Create detailed results with user lists
    detailed_sequences = {}
    for sequence, count in sequence_counts.items():
        detailed_sequences[sequence] = {
            "count": count,
            "users": sequence_details[sequence]
        }
    
    results = {
        "analysis_timestamp": now_in_timezone().isoformat(),
        "total_unique_sequences": len(sequence_counts),
        "total_user_product_pairings": sum(sequence_counts.values()),
        "sequences": detailed_sequences
    }
    
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"✅ Detailed results saved to {output_file}")
        print(f"   File contains {len(sequence_counts)} sequences with complete user lists")
    except Exception as e:
        print(f"❌ Error saving results: {e}")

def print_top_sequences(sequence_counts, sequence_details, top_n=20):
    """Print the most common sequences with user details"""
    print(f"\n📊 ALL {len(sequence_counts)} UNIQUE LIFECYCLE SEQUENCES (sorted by frequency):")
    print("=" * 100)
    
    # Sort by count (descending)
    sorted_sequences = sequence_counts.most_common(top_n)
    
    for i, (sequence, count) in enumerate(sorted_sequences, 1):
        print(f"{i:2d}. [{count:5d} users] {sequence}")
        
        # Show first few users as preview (to avoid overwhelming output)
        users_preview = sequence_details[sequence][:5]  # Show first 5 users
        for user in users_preview:
            print(f"     - {user['distinct_id']} (product: {user['product_id']})")
        
        if count > 5:
            print(f"     ... and {count - 5} more users")
        print()  # Empty line for readability
    
    print("=" * 100)

def print_summary_stats(sequence_counts):
    """Print summary statistics"""
    total_sequences = len(sequence_counts)
    total_user_products = sum(sequence_counts.values())
    
    print(f"\n📈 SUMMARY STATISTICS:")
    print(f"   Total unique lifecycle sequences: {total_sequences:,}")
    print(f"   Total user-product pairings analyzed: {total_user_products:,}")
    print(f"   Average users per sequence: {total_user_products/total_sequences:.1f}")
    
    # Show sequences with only 1 user (rare scenarios)
    singleton_sequences = sum(1 for count in sequence_counts.values() if count == 1)
    print(f"   Sequences with only 1 user: {singleton_sequences:,} ({singleton_sequences/total_sequences*100:.1f}%)")

def main():
    """Main execution function"""
    db_path = "/Users/joshuakaufman/untitled folder 3/database/mixpanel_data.db"
    
    print("🚀 User Lifecycle Sequence Analyzer")
    print("=" * 50)
    
    # Connect to database
    conn = connect_to_database(db_path)
    if not conn:
        return
    
    try:
        # Analyze all lifecycles
        sequence_counts, sequence_details = analyze_all_lifecycles(conn)
        
        if not sequence_counts:
            print("❌ No sequences found")
            return
        
        # Print results
        print_summary_stats(sequence_counts)
        print_top_sequences(sequence_counts, sequence_details, top_n=30)  # Show top 30 with user preview
        
        # Save detailed results
        save_results(sequence_counts, sequence_details)
        
        print(f"\n✅ Analysis complete! Check lifecycle_sequences_detailed.json for full results with user lists.")
        
    finally:
        conn.close()
        print("🔒 Database connection closed")

if __name__ == "__main__":
    main() 