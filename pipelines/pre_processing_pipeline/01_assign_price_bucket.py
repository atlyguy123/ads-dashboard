#!/usr/bin/env python3
"""
🎯 Price Bucket Assignment Module - Pre-processing Pipeline (v3 - Iterative Bucketing)

This module implements a sophisticated iterative price bucketing algorithm to assign price buckets
to user-product records based on conversion data patterns.

Key improvements in v3:
- Uses iterative merge algorithm for more natural price groupings
- Implements two-pass assignment system for comprehensive coverage
- Handles edge cases with proper fallback mechanisms
- Provides detailed statistics and verification
"""

import os
import sys
import sqlite3
import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from collections import defaultdict
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# --- Configuration ---
# Configuration - use centralized database path discovery
DB_PATH = get_database_path('mixpanel_data')
BUCKET_PERCENT_THRESHOLD = 0.175
BUCKET_DOLLAR_THRESHOLD = 5.0

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main() -> bool:
    logger.info("🎯 STARTING PRICE BUCKET ASSIGNMENT (v3 - Iterative Bucketing)")
    logger.info("=" * 60)
    if not os.path.exists(DB_PATH):
        logger.error(f"❌ Database not found at path: {DB_PATH}")
        return False
    try:
        conversions_df, users_df, trial_starts_df = get_all_batch_data()
        conversion_buckets = create_conversion_buckets_iterative(conversions_df)
        assign_price_buckets_to_users(conversion_buckets, conversions_df, users_df, trial_starts_df)
        return True
    except Exception as e:
        logger.error(f"❌ Error in price bucket assignment: {str(e)}", exc_info=True)
        return False

# Database loading function - unchanged.
def get_all_batch_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("📊 Loading all required data in batch queries...")
    conn = sqlite3.connect(DB_PATH)
    conversions_query = """
    SELECT me.distinct_id, me.event_time, COALESCE(mu.country, 'Unknown') as country,
           me.revenue_usd, JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id, me.event_name
    FROM mixpanel_event me LEFT JOIN mixpanel_user mu ON me.distinct_id = mu.distinct_id
    WHERE me.revenue_usd > 0 AND me.event_name IN ('RC Initial purchase', 'RC Trial converted')
      AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
    """
    users_query = """
    SELECT DISTINCT upm.distinct_id, upm.product_id, COALESCE(mu.country, 'Unknown') as country
    FROM user_product_metrics upm LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
    -- WHERE upm.valid_lifecycle = 1  -- Commented out to process ALL users in user_product_metrics table
    """
    trial_starts_query = """
    SELECT me.distinct_id, me.event_time, JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id
    FROM mixpanel_event me WHERE me.event_name = 'RC Trial started'
      AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
    """
    conversions_df = pd.read_sql_query(conversions_query, conn)
    users_df = pd.read_sql_query(users_query, conn)
    trial_starts_df = pd.read_sql_query(trial_starts_query, conn)
    conn.close()
    logger.info(f"   Loaded {len(conversions_df):,} conversions, {len(users_df):,} users, {len(trial_starts_df):,} trial starts")
    return conversions_df, users_df, trial_starts_df

# The new, superior iterative bucketing algorithm.
def create_price_buckets_iterative(prices: List[float]) -> List[Dict[str, Any]]:
    if not prices: return []
    unique_prices = sorted(list(set(p for p in prices if p > 0)))
    if not unique_prices: return []
    
    buckets = [{'prices': [p], 'min_price': p, 'max_price': p} for p in unique_prices]

    while True:
        min_distance = float('inf')
        merge_indices = None
        for i in range(len(buckets) - 1):
            dist = buckets[i+1]['min_price'] - buckets[i]['max_price']
            if dist < min_distance:
                min_distance = dist
                merge_indices = (i, i + 1)
        
        if merge_indices is None: break

        i, j = merge_indices
        bucket1, bucket2 = buckets[i], buckets[j]

        # The threshold is the most permissive one based on the potential new bucket's combined range.
        threshold = max(bucket2['max_price'] * BUCKET_PERCENT_THRESHOLD, BUCKET_DOLLAR_THRESHOLD)

        if bucket2['min_price'] <= bucket1['max_price'] + threshold:
            new_prices = bucket1['prices'] + bucket2['prices']
            new_bucket = {'prices': new_prices, 'min_price': min(new_prices), 'max_price': max(new_prices)}
            buckets = buckets[:i] + [new_bucket] + buckets[j+1:]
        else:
            break
            
    # Finalize by calculating the average price for each bucket.
    final_buckets = []
    for bucket in buckets:
        final_buckets.append({
            'min_price': bucket['min_price'],
            'max_price': bucket['max_price'],
            'avg_price': sum(bucket['prices']) / len(bucket['prices'])
        })
    return final_buckets

# Main wrapper for the iterative bucketing function.
def create_conversion_buckets_iterative(conversions_df: pd.DataFrame) -> Dict:
    logger.info("🛠️  Creating conversion price buckets (Iterative Method)...")
    buckets = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    grouped = conversions_df.groupby(['country', 'product_id', 'event_name'])
    for (country, product_id, event_name), group in grouped:
        if pd.isna(country) or pd.isna(product_id): continue
        price_buckets = create_price_buckets_iterative(group['revenue_usd'].tolist())
        if price_buckets:
            buckets[country][product_id][event_name] = price_buckets
    logger.info(f"✅ Created buckets using iterative method.")
    return dict(buckets)

# Helper function to find a bucket for a given price.
def find_bucket_for_price(price: float, buckets: List[Dict[str, Any]]) -> Optional[float]:
    if not buckets: return None
    for bucket in buckets:
        if bucket['min_price'] - 1e-9 <= price <= bucket['max_price'] + 1e-9:
            return bucket['avg_price']
    return None

# The main assignment logic, now strictly following the two-pass system.
def assign_price_buckets_to_users(conversion_buckets: Dict, conversions_df: pd.DataFrame, users_df: pd.DataFrame, trial_starts_df: pd.DataFrame) -> None:
    logger.info("🚀 Starting batch price bucket assignment (Two-Pass Method)...")

    conversion_lookup = conversions_df.groupby(['distinct_id', 'product_id']).apply(lambda x: x.to_dict('records')).to_dict()
    trial_lookup = trial_starts_df.sort_values('event_time').groupby(['distinct_id', 'product_id']).first().to_dict('index')

    assignments = []
    stats = defaultdict(int)
    
    # --- PASS 1: Direct Conversions & Strict Backward Inheritance ---
    logger.info("   PASS 1: Assigning direct conversions and prior trial inheritance...")
    for _, user_row in users_df.iterrows():
        distinct_id, product_id, country = user_row['distinct_id'], user_row['product_id'], user_row['country']
        user_key = (distinct_id, product_id)
        
        bucket_value = 0
        assignment_type = 'unassigned'
        inherited_event_type = None

        if user_key in conversion_lookup:
            conv = conversion_lookup[user_key][0]
            buckets = conversion_buckets.get(conv['country'], {}).get(product_id, {}).get(conv['event_name'], [])
            bucket_value = find_bucket_for_price(conv['revenue_usd'], buckets) or 0
            assignment_type = 'conversion' if bucket_value > 0 else 'conversion_no_bucket'
            inherited_event_type = conv['event_name'] if bucket_value > 0 else None  # Track the event type for conversions too
        elif user_key in trial_lookup:
            trial_time = trial_lookup[user_key]['event_time']
            bucket_result = find_previous_trial_bucket(country, product_id, trial_time, conversions_df, conversion_buckets)
            if bucket_result:
                bucket_value, inherited_event_type = bucket_result
                assignment_type = 'inherited_prior'
            else:
                bucket_value = 0
                inherited_event_type = None
                assignment_type = 'needs_pass_2'
        else:
            assignment_type = 'no_event'

        assignments.append({'distinct_id': distinct_id, 'product_id': product_id, 'price_bucket': bucket_value, 'assignment_type': assignment_type, 'country': country, 'inherited_from_event_type': inherited_event_type})
        stats[assignment_type] += 1
    
    # --- PASS 2: Closest-Time Inheritance for Remaining Users ---
    logger.info("   PASS 2: Assigning remaining users based on closest conversion...")
    
    # Filter for users that need the second pass
    pass_2_assignments = [a for a in assignments if a['assignment_type'] == 'needs_pass_2']
    if pass_2_assignments:
        logger.info(f"      Found {len(pass_2_assignments):,} users for Pass 2 processing.")
        for assignment in pass_2_assignments:
            user_key = (assignment['distinct_id'], assignment['product_id'])
            trial_time = trial_lookup[user_key]['event_time']
            
            bucket_result = find_closest_conversion_bucket(assignment['country'], assignment['product_id'], trial_time, conversions_df, conversion_buckets)
            
            if bucket_result:
                bucket_value, inherited_event_type = bucket_result
                assignment['price_bucket'] = bucket_value
                assignment['inherited_from_event_type'] = inherited_event_type
                assignment['assignment_type'] = 'inherited_closest'
                stats['inherited_closest'] += 1
                stats['needs_pass_2'] -= 1
            else:
                assignment['price_bucket'] = 0
                assignment['inherited_from_event_type'] = None
                assignment['assignment_type'] = 'no_conversions_ever'
                stats['no_conversions_ever'] += 1
                stats['needs_pass_2'] -= 1

    # --- FINAL DATABASE UPDATE ---
    logger.info("💾 Updating database with price bucket assignments...")
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("ALTER TABLE user_product_metrics ADD COLUMN price_bucket REAL")
        logger.info("   Added 'price_bucket' column.")
    except sqlite3.OperationalError: pass
    
    try:
        conn.execute("ALTER TABLE user_product_metrics ADD COLUMN assignment_type TEXT")
        logger.info("   Added 'assignment_type' column.")
    except sqlite3.OperationalError: pass
    
    try:
        conn.execute("ALTER TABLE user_product_metrics ADD COLUMN inherited_from_event_type TEXT")
        logger.info("   Added 'inherited_from_event_type' column.")
    except sqlite3.OperationalError: pass
        
    update_data = [(a['price_bucket'], a['assignment_type'], a['inherited_from_event_type'], a['distinct_id'], a['product_id']) for a in assignments]
    cursor = conn.cursor()
    cursor.executemany("UPDATE user_product_metrics SET price_bucket = ?, assignment_type = ?, inherited_from_event_type = ? WHERE distinct_id = ? AND product_id = ?", update_data)
    conn.commit()
    logger.info(f"   Successfully updated {cursor.rowcount:,} records.")
    conn.close()

    log_final_summary(assignments, stats)

# Inheritance function: Looks ONLY for prior 'RC Trial converted' events.
def find_previous_trial_bucket(country: str, product_id: str, trial_time: str, conversions_df: pd.DataFrame, conversion_buckets: Dict) -> Optional[Tuple[float, str]]:
    prev_conversions = conversions_df[
        (conversions_df['country'] == country) &
        (conversions_df['product_id'] == product_id) &
        (conversions_df['event_name'] == 'RC Trial converted') &
        (conversions_df['event_time'] < trial_time)
    ]
    if prev_conversions.empty: return None
    last_conversion = prev_conversions.sort_values('event_time', ascending=False).iloc[0]
    buckets = conversion_buckets.get(country, {}).get(product_id, {}).get('RC Trial converted', [])
    bucket_value = find_bucket_for_price(last_conversion['revenue_usd'], buckets)
    if bucket_value:
        return (bucket_value, 'RC Trial converted')
    return None

# Inheritance function: Finds closest conversion of ANY type.
def find_closest_conversion_bucket(country: str, product_id: str, trial_time: str, conversions_df: pd.DataFrame, conversion_buckets: Dict) -> Optional[Tuple[float, str]]:
    relevant_conversions = conversions_df[(conversions_df['country'] == country) & (conversions_df['product_id'] == product_id)].copy()
    if relevant_conversions.empty: return None
    
    relevant_conversions['time_diff'] = (pd.to_datetime(relevant_conversions['event_time']) - pd.to_datetime(trial_time)).abs()
    closest_conversion = relevant_conversions.loc[relevant_conversions['time_diff'].idxmin()]
    
    buckets = conversion_buckets.get(country, {}).get(product_id, {}).get(closest_conversion['event_name'], [])
    bucket_value = find_bucket_for_price(closest_conversion['revenue_usd'], buckets)
    if bucket_value:
        return (bucket_value, closest_conversion['event_name'])
    return None

# Logging function for the final summary.
def log_final_summary(assignments: List[Dict], stats: Dict):
    total_assigned = len(assignments)
    assignment_df = pd.DataFrame(assignments)
    positive_buckets = assignment_df[assignment_df['price_bucket'] > 0]
    
    logger.info("=" * 60)
    logger.info("🎯 PRICE BUCKET ASSIGNMENT COMPLETE (v3)")
    logger.info("=" * 60)
    logger.info(f"📈 Total User-Product Records Processed: {total_assigned:,}")
    logger.info("Assignment Method Breakdown:")
    logger.info(f"  💰 Direct Conversion: {stats['conversion']:,}")
    logger.info(f"  ⬅️  Inherited from Prior Trial (Pass 1): {stats['inherited_prior']:,}")
    logger.info(f"  ↔️  Inherited from Closest Conversion (Pass 2): {stats['inherited_closest']:,}")
    logger.info("-" * 20)
    logger.info("Zero-Value Breakdown:")
    logger.info(f"  ❓ No Conversion/Trial Event: {stats['no_event']:,}")
    logger.info(f"  🚫 No Relevant Conversions Found After Both Passes: {stats['no_conversions_ever']:,}")
    if stats['conversion_no_bucket'] > 0:
        logger.warning(f"  ⚠️ Conversion Event Found but No Bucket Matched: {stats['conversion_no_bucket']:,}")
    logger.info("=" * 60)

if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1)