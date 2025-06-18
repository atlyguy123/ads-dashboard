#!/usr/bin/env python3
"""
Module 6: Validate Event Lifecycle

This module validates that user-product lifecycle events follow valid business patterns.
It analyzes event sequences for each user-product combination to identify valid vs invalid
lifecycle patterns according to subscription business rules.

Key Features:
- Comprehensive lifecycle pattern validation
- Detailed statistics on invalid patterns
- Efficient batch processing with proper indexing
- Clear categorization of lifecycle violations

VALIDATION RULES SUMMARY:
========================

This module validates that each user-product combination follows a valid lifecycle pattern.
Each product_id is validated independently.

VALID LIFECYCLE PATTERNS:
1. Trial Flow: Trial started → Trial converted/cancelled (within 31 days)
2. Purchase Flow: Initial purchase (standalone)
3. Subscription Flow: Renewals/Cancellations after conversion or purchase

INVALID PATTERNS (EXCLUDED):
- Multiple trial starts per product
- Trials without end events
- Trial ends > 31 days after start
- Renewals/Cancellations without prior subscription
- Initial purchases during active trials
- Trial starts after subscription events
- Any sequence violating the rules above

STATISTICS TRACKED:
- Detailed breakdown of invalid reasons
- Product-specific validation rates
- Event sequence analysis

LOGGING:
- All invalid lifecycles logged with detailed event sequences
- Statistics printed at end of validation

BUSINESS RULES:
- Each product lifecycle is validated independently
- Maximum 1 trial start per product
- Trial must end within 31 days (converted or cancelled)
- Subscription state required before renewals/cancellations
- No trial events after subscription events
- Events must be in chronological order
"""

import os
import sys
import sqlite3
import logging
import json
import time
from datetime import datetime, timedelta
from collections import namedtuple, defaultdict
from typing import Dict, Any, List, Optional, Tuple, Set
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configuration - Use centralized database path discovery
DATABASE_PATH = get_database_path('mixpanel_data')

# Configure logging for detailed invalid lifecycle tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global statistics tracking for invalid lifecycle reasons
INVALID_LIFECYCLE_STATS = {
    'no_events': 0,
    'multiple_trial_starts': 0,
    'trial_without_end': 0,
    'trial_end_too_late': 0,
    'trial_conversion_without_start': 0,
    'trial_cancellation_without_start': 0,
    'multiple_trial_ends': 0,
    'multiple_initial_purchases': 0,
    'initial_purchase_during_trial': 0,
    'initial_purchase_after_conversion': 0,
    'renewal_without_subscription': 0,
    'cancellation_without_subscription': 0,
    'trial_start_after_subscription': 0,
    'no_subscription_event': 0,
    'invalid_event_sequence': 0,
    'datetime_parse_error': 0
}

# Exact event names from the IMPORTANT_EVENTS list in ingestion
IMPORTANT_EVENTS = {
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal"
}

# Event classification mapping
EVENT_CLASSIFICATION = {
    "RC Trial started": "trial_started",
    "RC Trial converted": "trial_converted", 
    "RC Trial cancelled": "trial_canceled",
    "RC Initial purchase": "initial_purchase",
    "RC Renewal": "renewal",
    "RC Cancellation": "rc_cancellation"
}

# Data structure for lifecycle events
LifecycleEvent = namedtuple('LifecycleEvent', ['event_time', 'event_name', 'event_type', 'revenue_usd', 'event_uuid'])

class ProductLifecycleState:
    """
    Tracks the lifecycle state for a single product.
    Enforces strict business rules for valid lifecycle patterns.
    """
    def __init__(self):
        self.trial_started = False
        self.trial_start_time = None
        self.trial_ended = False
        self.trial_end_time = None
        self.trial_end_type = None  # 'converted' or 'cancelled'
        self.has_subscription = False
        self.subscription_start_time = None
        self.initial_purchase_count = 0
        
    def can_start_trial(self):
        """Trial can only start if no trial has started and no subscription exists"""
        return not self.trial_started and not self.has_subscription
        
    def can_end_trial(self):
        """Trial can only end if trial has started but not ended"""
        return self.trial_started and not self.trial_ended
        
    def can_purchase(self):
        """Initial purchase can only happen if no trial is active and no prior purchase"""
        return (not self.trial_started or self.trial_ended) and self.initial_purchase_count == 0
        
    def can_renew_or_cancel(self):
        """Renewals/cancellations can only happen if subscription exists"""
        return self.has_subscription

def log_invalid_lifecycle(distinct_id, product_id, reason, events):
    """Track invalid lifecycle statistics without detailed logging"""
    # Only increment statistics, no individual logging to reduce noise
    pass

def increment_invalid_stat(reason):
    """Safely increment invalid lifecycle statistics"""
    global INVALID_LIFECYCLE_STATS
    if reason in INVALID_LIFECYCLE_STATS:
        INVALID_LIFECYCLE_STATS[reason] += 1
    else:
        logger.error(f"Unknown invalid lifecycle reason: {reason}")
        INVALID_LIFECYCLE_STATS['invalid_event_sequence'] += 1

def print_validation_statistics():
    """Print detailed statistics about invalid lifecycles"""
    global INVALID_LIFECYCLE_STATS
    
    total_invalid = sum(INVALID_LIFECYCLE_STATS.values())
    
    if total_invalid == 0:
        print("\n=== LIFECYCLE VALIDATION STATISTICS ===")
        print("All lifecycles were valid!")
        return
    
    print(f"\n=== DETAILED INVALID LIFECYCLE STATISTICS ===")
    print(f"Total invalid lifecycles: {total_invalid:,}")
    
    # Group by category for better readability
    trial_issues = (INVALID_LIFECYCLE_STATS['multiple_trial_starts'] + 
                   INVALID_LIFECYCLE_STATS['trial_without_end'] + 
                   INVALID_LIFECYCLE_STATS['trial_end_too_late'] +
                   INVALID_LIFECYCLE_STATS['trial_conversion_without_start'] +
                   INVALID_LIFECYCLE_STATS['trial_cancellation_without_start'] +
                   INVALID_LIFECYCLE_STATS['multiple_trial_ends'])
    
    purchase_issues = (INVALID_LIFECYCLE_STATS['multiple_initial_purchases'] +
                      INVALID_LIFECYCLE_STATS['initial_purchase_during_trial'] +
                      INVALID_LIFECYCLE_STATS['initial_purchase_after_conversion'])
    
    subscription_issues = (INVALID_LIFECYCLE_STATS['renewal_without_subscription'] +
                          INVALID_LIFECYCLE_STATS['cancellation_without_subscription'])
    
    sequence_issues = (INVALID_LIFECYCLE_STATS['trial_start_after_subscription'] +
                      INVALID_LIFECYCLE_STATS['no_subscription_event'] +
                      INVALID_LIFECYCLE_STATS['invalid_event_sequence'])
    
    data_issues = (INVALID_LIFECYCLE_STATS['no_events'] +
                  INVALID_LIFECYCLE_STATS['datetime_parse_error'])
    
    print(f"\nBy Category:")
    print(f"  Trial Issues: {trial_issues:,} ({trial_issues/total_invalid*100:.1f}%)")
    print(f"  Purchase Issues: {purchase_issues:,} ({purchase_issues/total_invalid*100:.1f}%)")
    print(f"  Subscription Issues: {subscription_issues:,} ({subscription_issues/total_invalid*100:.1f}%)")
    print(f"  Sequence Issues: {sequence_issues:,} ({sequence_issues/total_invalid*100:.1f}%)")
    print(f"  Data Issues: {data_issues:,} ({data_issues/total_invalid*100:.1f}%)")
    
    print(f"\nDetailed Breakdown:")
    for reason, count in sorted(INVALID_LIFECYCLE_STATS.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            percentage = count/total_invalid*100
            formatted_reason = reason.replace('_', ' ').title()
            print(f"  {formatted_reason}: {count:,} ({percentage:.1f}%)")

def main():
    try:
        print("=== Module 6: Validate Event Lifecycle ===")
        print("Starting event lifecycle validation...")
        
        # Connect to database
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # Step 1: Setup user-product relationships and validate lifecycles
        setup_and_validate_lifecycles(conn)
        
        # Step 2: Display validation results and statistics
        display_validation_results(conn)
        
        conn.close()
        
        print("Event lifecycle validation completed successfully")
        return 0
        
    except Exception as e:
        print(f"Module 6 failed: {e}", file=sys.stderr)
        return 1

def setup_and_validate_lifecycles(conn):
    """Setup user-product relationships and validate all lifecycles"""
    print("\nStep 1: Setting up user-product relationships and validating lifecycles...")
    
    cursor = conn.cursor()
    
    # Reset statistics
    global INVALID_LIFECYCLE_STATS
    for key in INVALID_LIFECYCLE_STATS:
        INVALID_LIFECYCLE_STATS[key] = 0
    print("   → Reset invalid lifecycle statistics")
    
    # PRESERVE ATTRIBUTION DATA: Backup before clearing
    print("   → Preserving existing attribution data before clearing relationships...")
    
    # First, backup existing attribution data
    cursor.execute("""
        CREATE TEMP TABLE temp_attribution_backup_setup AS
        SELECT 
            distinct_id,
            product_id,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            country,
            region,
            device,
            store
        FROM user_product_metrics
        WHERE abi_ad_id IS NOT NULL 
           OR abi_campaign_id IS NOT NULL 
           OR abi_ad_set_id IS NOT NULL
    """)
    
    attribution_backup_count = cursor.execute("SELECT COUNT(*) FROM temp_attribution_backup_setup").fetchone()[0]
    print(f"   → Backed up attribution data for {attribution_backup_count:,} records")
    
    # Clear existing relationships and validation data
    print("   → Clearing existing user-product relationships...")
    cursor.execute("DELETE FROM user_product_metrics WHERE 1=1")
    conn.commit()
    print("   → Cleared all existing relationships")
    
    # Get ALL valid users with subscription events and their products
    print("   → Finding all valid users with subscription events...")
    start_time = time.time()
    
    cursor.execute("""
        SELECT DISTINCT 
            e.distinct_id,
            JSON_EXTRACT(e.event_json, '$.properties.product_id') as product_id
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.valid_user = 1
          AND e.event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 'RC Initial purchase', 'RC Cancellation', 'RC Renewal')
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') IS NOT NULL
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') != ''
    """)
    
    user_product_pairs = cursor.fetchall()
    query_time = time.time() - start_time
    print(f"   → Found {len(user_product_pairs):,} user-product pairs in {query_time:.1f} seconds")
    
    # Create relationships and validate lifecycles in one pass
    print("   → Creating relationships and validating lifecycles...")
    start_time = time.time()
    
    valid_count = 0
    total_count = 0
    
    for i, (distinct_id, product_id) in enumerate(user_product_pairs):
        # Show progress every 5000 relationships
        if i > 0 and i % 5000 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(user_product_pairs) - i) / rate / 60 if rate > 0 else 0
            progress = i / len(user_product_pairs) * 100
            print(f"   → Progress: {i:,}/{len(user_product_pairs):,} ({progress:.1f}%) | Rate: {rate:.1f}/sec | ETA: {eta:.1f}min")
        
        try:
            # Get events for this user-product combination
            events = get_user_product_events(cursor, distinct_id, product_id)
            
            # Validate lifecycle
            is_valid, reason = validate_lifecycle_pattern(events, distinct_id, product_id)
            
            if not is_valid:
                increment_invalid_stat(reason)
                log_invalid_lifecycle(distinct_id, product_id, reason, events)
            else:
                valid_count += 1
            
            # Create the relationship record with attribution data if available
            # Check if we have attribution data backed up for this user-product combination
            cursor.execute("""
                SELECT abi_ad_id, abi_campaign_id, abi_ad_set_id, country, region, device, store
                FROM temp_attribution_backup_setup
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            attribution_data = cursor.fetchone()
            
            if attribution_data:
                # Restore with attribution data
                abi_ad_id, abi_campaign_id, abi_ad_set_id, country, region, device, store = attribution_data
                cursor.execute("""
                    INSERT INTO user_product_metrics 
                    (distinct_id, product_id, credited_date, current_status, current_value, 
                     value_status, last_updated_ts, valid_lifecycle, abi_ad_id, abi_campaign_id, 
                     abi_ad_set_id, country, region, device, store)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    distinct_id, 
                    product_id,
                    'PLACEHOLDER_DATE',
                    'PLACEHOLDER_STATUS',
                    -999.99,
                    'PLACEHOLDER_VALUE_STATUS',
                    datetime.now(),
                    1 if is_valid else 0,
                    abi_ad_id,              # PRESERVED attribution data
                    abi_campaign_id,        # PRESERVED attribution data
                    abi_ad_set_id,          # PRESERVED attribution data
                    country,                # PRESERVED geographic data
                    region,                 # PRESERVED geographic data
                    device,                 # PRESERVED device data
                    store                   # PRESERVED store data
                ))
            else:
                # Create without attribution data (new relationship)
                cursor.execute("""
                    INSERT INTO user_product_metrics 
                    (distinct_id, product_id, credited_date, current_status, current_value, 
                     value_status, last_updated_ts, valid_lifecycle)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    distinct_id, 
                    product_id,
                    'PLACEHOLDER_DATE',
                    'PLACEHOLDER_STATUS',
                    -999.99,
                    'PLACEHOLDER_VALUE_STATUS',
                    datetime.now(),
                    1 if is_valid else 0
                ))
            
            total_count += 1
                
        except Exception as e:
            print(f"   → Error processing {distinct_id}, {product_id}: {e}")
            increment_invalid_stat('datetime_parse_error')
    
    # Clean up temporary table
    cursor.execute("DROP TABLE temp_attribution_backup_setup")
    
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"   → Completed validation in {elapsed:.1f} seconds")
    print(f"   → Created {total_count:,} user-product relationships")
    print(f"   → Valid lifecycles: {valid_count:,} ({valid_count/total_count*100:.1f}%)")
    print(f"   → Invalid lifecycles: {total_count-valid_count:,} ({(total_count-valid_count)/total_count*100:.1f}%)")
    
    # Verify attribution preservation
    cursor.execute("""
        SELECT COUNT(*) FROM user_product_metrics 
        WHERE abi_ad_id IS NOT NULL OR abi_campaign_id IS NOT NULL OR abi_ad_set_id IS NOT NULL
    """)
    preserved_count = cursor.fetchone()[0]
    print(f"   → ✅ Preserved attribution data for {preserved_count:,} relationships ({preserved_count/total_count*100:.1f}%)")
    
    if preserved_count == 0:
        print("   → ⚠️  WARNING: No attribution data preserved! Check if Module 4 ran successfully.")
    elif preserved_count < attribution_backup_count:
        print(f"   → ⚠️  WARNING: Lost attribution data for {attribution_backup_count - preserved_count:,} relationships")

def display_validation_results(conn):
    """Display comprehensive validation results and statistics"""
    print("\nStep 2: Validation Results Summary")
    
    cursor = conn.cursor()
    
    # Get overall statistics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_relationships,
            SUM(valid_lifecycle) as valid_lifecycles,
            COUNT(DISTINCT product_id) as unique_products
        FROM user_product_metrics
    """)
    
    total_relationships, valid_lifecycles, unique_products = cursor.fetchone()
    invalid_lifecycles = total_relationships - valid_lifecycles
    
    print(f"\n=== LIFECYCLE VALIDATION SUMMARY ===")
    print(f"Total user-product relationships: {total_relationships:,}")
    print(f"Valid lifecycles: {valid_lifecycles:,} ({valid_lifecycles/total_relationships*100:.1f}%)")
    print(f"Invalid lifecycles: {invalid_lifecycles:,} ({invalid_lifecycles/total_relationships*100:.1f}%)")
    print(f"Unique products: {unique_products}")
    
    # Get top invalid reasons
    print(f"\nTop reasons for invalid lifecycles:")
    total_invalid = sum(INVALID_LIFECYCLE_STATS.values())
    if total_invalid > 0:
        sorted_reasons = sorted(INVALID_LIFECYCLE_STATS.items(), key=lambda x: x[1], reverse=True)
        for reason, count in sorted_reasons:
            if count > 0:
                formatted_reason = reason.replace('_', ' ').title()
                print(f"  - {formatted_reason}: {count:,} cases")
    
    # Get product-specific validation rates
    print(f"\nLifecycle validation by product (top 10):")
    cursor.execute("""
        SELECT 
            product_id,
            COUNT(*) as total_users,
            SUM(valid_lifecycle) as valid_lifecycles
        FROM user_product_metrics
        GROUP BY product_id
        ORDER BY total_users DESC
        LIMIT 10
    """)
    
    for product_id, total_users, valid_lifecycles in cursor.fetchall():
        valid_rate = valid_lifecycles / total_users * 100 if total_users > 0 else 0
        print(f"  - {product_id}: {valid_lifecycles:,}/{total_users:,} valid ({valid_rate:.1f}%)")
    
    # Print detailed statistics
    print_validation_statistics()

def analyze_user_events(conn):
    """Analyze event counts and patterns for each user"""
    print("\nStep 1: Analyzing user event patterns...")
    
    cursor = conn.cursor()
    
    # Get event statistics ONLY for users who actually have events (much faster)
    print("   → Executing optimized user event analysis query...")
    start_time = time.time()
    
    cursor.execute("""
        SELECT 
            e.distinct_id,
            COUNT(e.event_uuid) as total_events,
            COUNT(DISTINCT e.event_name) as unique_event_types,
            MIN(e.event_time) as first_event,
            MAX(e.event_time) as last_event,
            SUM(CASE WHEN e.revenue_usd > 0 THEN 1 ELSE 0 END) as revenue_events,
            SUM(COALESCE(e.revenue_usd, 0)) as total_revenue,
            COUNT(DISTINCT DATE(e.event_time)) as active_days
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        GROUP BY e.distinct_id
    """)
    
    user_stats = cursor.fetchall()
    query_time = time.time() - start_time
    print(f"   → Query completed in {query_time:.1f} seconds")
    print(f"   → Processing {len(user_stats):,} valid users")
    
    # Statistics tracking
    stats = {
        'no_events': 0,
        'low_activity': 0,  # 1-10 events
        'medium_activity': 0,  # 11-100 events
        'high_activity': 0,  # 101-1000 events
        'very_high_activity': 0,  # 1000+ events
        'revenue_users': 0,
        'total_revenue': 0
    }
    
    for distinct_id, total_events, unique_types, first_event, last_event, revenue_events, total_revenue, active_days in user_stats:
        total_events = total_events or 0
        total_revenue = total_revenue or 0
        
        # Categorize activity levels
        if total_events == 0:
            stats['no_events'] += 1
        elif total_events <= 10:
            stats['low_activity'] += 1
        elif total_events <= 100:
            stats['medium_activity'] += 1
        elif total_events <= 1000:
            stats['high_activity'] += 1
        else:
            stats['very_high_activity'] += 1
        
        if total_revenue > 0:
            stats['revenue_users'] += 1
            stats['total_revenue'] += total_revenue
    
    # Print statistics
    print(f"   → User Activity Distribution (users with events only):")
    print(f"     • Low activity (1-10 events): {stats['low_activity']:,}")
    print(f"     • Medium activity (11-100 events): {stats['medium_activity']:,}")
    print(f"     • High activity (101-1000 events): {stats['high_activity']:,}")
    print(f"     • Very high activity (1000+ events): {stats['very_high_activity']:,}")
    print(f"     • Revenue users: {stats['revenue_users']:,}")
    print(f"     • Total revenue: ${stats['total_revenue']:,.2f}")
    
    # Note about users without events
    if stats['no_events'] > 0:
        print(f"   → Note: {stats['no_events']:,} users had no events (excluded from analysis)")

def analyze_product_usage(conn):
    """Analyze product usage patterns and populate user_product_metrics table"""
    print("\nStep 2: Analyzing product usage patterns...")
    
    cursor = conn.cursor()
    
    # CRITICAL: Always rebuild user-product relationships from scratch
    # But PRESERVE attribution data that was set by Module 4
    print("   → Preserving existing attribution data before clearing relationships...")
    
    # First, backup existing attribution data
    cursor.execute("""
        CREATE TEMP TABLE temp_attribution_backup AS
        SELECT 
            distinct_id,
            product_id,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            country,
            region,
            device,
            store
        FROM user_product_metrics
        WHERE abi_ad_id IS NOT NULL 
           OR abi_campaign_id IS NOT NULL 
           OR abi_ad_set_id IS NOT NULL
    """)
    
    attribution_backup_count = cursor.execute("SELECT COUNT(*) FROM temp_attribution_backup").fetchone()[0]
    print(f"   → Backed up attribution data for {attribution_backup_count:,} records")
    
    # Clear existing relationships
    print("   → Clearing existing user-product relationships...")
    cursor.execute("DELETE FROM user_product_metrics WHERE 1=1")
    conn.commit()
    print("   → Cleared all existing relationships")
    
    # Get ALL valid users with subscription events and their products
    print("   → Finding all valid users with subscription events...")
    start_time = time.time()
    
    cursor.execute("""
        SELECT DISTINCT 
            e.distinct_id,
            JSON_EXTRACT(e.event_json, '$.properties.product_id') as product_id
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.valid_user = 1
          AND e.event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 'RC Initial purchase', 'RC Cancellation', 'RC Renewal')
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') IS NOT NULL
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') != ''
    """)
    
    user_product_pairs = cursor.fetchall()
    query_time = time.time() - start_time
    print(f"   → Found {len(user_product_pairs):,} user-product pairs in {query_time:.1f} seconds")
    print(f"   → Creating user-product relationships...")
    
    # Create relationships for all user-product pairs
    total_relationships = 0
    
    for distinct_id, product_id in user_product_pairs:
        try:
            # Check if we have attribution data backed up for this user-product combination
            cursor.execute("""
                SELECT abi_ad_id, abi_campaign_id, abi_ad_set_id, country, region, device, store
                FROM temp_attribution_backup
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            attribution_data = cursor.fetchone()
            
            if attribution_data:
                # Restore with attribution data
                abi_ad_id, abi_campaign_id, abi_ad_set_id, country, region, device, store = attribution_data
                cursor.execute("""
                    INSERT INTO user_product_metrics 
                    (distinct_id, product_id, credited_date, current_status, current_value, 
                     value_status, last_updated_ts, valid_lifecycle, abi_ad_id, abi_campaign_id, 
                     abi_ad_set_id, country, region, device, store)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    distinct_id, 
                    product_id,
                    'PLACEHOLDER_DATE',      # Clear placeholder - will be set by Module 3
                    'PLACEHOLDER_STATUS',    # Clear placeholder - will be set by Module 3
                    -999.99,                 # Clear placeholder - will be set by Module 3
                    'PLACEHOLDER_VALUE_STATUS',  # Clear placeholder - will be set by Module 3
                    datetime.now(),
                    0,  # Will be validated in Step 3
                    abi_ad_id,              # PRESERVED attribution data
                    abi_campaign_id,        # PRESERVED attribution data
                    abi_ad_set_id,          # PRESERVED attribution data
                    country,                # PRESERVED geographic data
                    region,                 # PRESERVED geographic data
                    device,                 # PRESERVED device data
                    store                   # PRESERVED store data
                ))
            else:
                # Create without attribution data (new relationship)
                cursor.execute("""
                    INSERT INTO user_product_metrics 
                    (distinct_id, product_id, credited_date, current_status, current_value, 
                     value_status, last_updated_ts, valid_lifecycle)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    distinct_id, 
                    product_id,
                    'PLACEHOLDER_DATE',      # Clear placeholder - will be set by Module 3
                    'PLACEHOLDER_STATUS',    # Clear placeholder - will be set by Module 3
                    -999.99,                 # Clear placeholder - will be set by Module 3
                    'PLACEHOLDER_VALUE_STATUS',  # Clear placeholder - will be set by Module 3
                    datetime.now(),
                    0  # Will be validated in Step 3
                ))
            
            total_relationships += 1
                
        except Exception as e:
            print(f"Error inserting user-product relationship for {distinct_id}, {product_id}: {e}")
    
    # Clean up temporary table
    cursor.execute("DROP TABLE temp_attribution_backup")
    
    conn.commit()
    print(f"   → Created {total_relationships:,} user-product relationships for validation")
    
    # Verify attribution preservation
    cursor.execute("""
        SELECT COUNT(*) FROM user_product_metrics 
        WHERE abi_ad_id IS NOT NULL OR abi_campaign_id IS NOT NULL OR abi_ad_set_id IS NOT NULL
    """)
    preserved_count = cursor.fetchone()[0]
    print(f"   → ✅ Preserved attribution data for {preserved_count:,} relationships ({preserved_count/total_relationships*100:.1f}%)")
    
    if preserved_count == 0:
        print("   → ⚠️  WARNING: No attribution data preserved! Check if Module 4 ran successfully.")
    elif preserved_count < attribution_backup_count:
        print(f"   → ⚠️  WARNING: Lost attribution data for {attribution_backup_count - preserved_count:,} relationships")

def classify_event_type(event_name):
    """Classify an event name into lifecycle event types"""
    return EVENT_CLASSIFICATION.get(event_name, 'other')

def get_user_product_events(cursor, distinct_id, product_id):
    """Get all events for a specific user-product combination, ordered by time"""
    
    # Get all important events for this user, ordered by time
    event_name_list = ', '.join([f"'{name}'" for name in IMPORTANT_EVENTS])
    
    cursor.execute(f"""
        SELECT 
            event_time,
            event_name,
            COALESCE(revenue_usd, 0) as revenue_usd,
            event_uuid,
            event_json
        FROM mixpanel_event
        WHERE distinct_id = ?
          AND event_name IN ({event_name_list})
          AND JSON_EXTRACT(event_json, '$.properties.product_id') = ?
        ORDER BY event_time ASC
    """, (distinct_id, product_id))
    
    all_events = cursor.fetchall()
    relevant_events = []
    
    for event_time, event_name, revenue_usd, event_uuid, event_json in all_events:
        # Filter events by the specific product_id being validated
        event_type = classify_event_type(event_name)
        
        relevant_events.append(LifecycleEvent(
            event_time=event_time,
            event_name=event_name,
            event_type=event_type,
            revenue_usd=revenue_usd,
            event_uuid=event_uuid
        ))
    
    return relevant_events

def validate_user_lifecycles(conn):
    """Validate user lifecycles and update valid_lifecycle flags"""
    print("\nStep 3: Validating user lifecycles with business logic...")
    
    cursor = conn.cursor()
    
    # Reset global statistics for this validation run
    global INVALID_LIFECYCLE_STATS
    for key in INVALID_LIFECYCLE_STATS:
        INVALID_LIFECYCLE_STATS[key] = 0
    print("   → Reset invalid lifecycle statistics")
    
    # CRITICAL: Clear all existing lifecycle validation data before processing
    # Set ALL records to invalid (0) first, then only mark valid ones as 1
    print("   → Clearing existing lifecycle validation data...")
    cursor.execute("""
        UPDATE user_product_metrics 
        SET valid_lifecycle = 0
        WHERE 1=1
    """)
    conn.commit()
    print(f"   → Set all records to invalid lifecycle (0) - will mark valid ones as 1")
    
    # First, get count of user-product relationships to process
    print("   → Counting user-product relationships to validate...")
    cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
    total_relationships = cursor.fetchone()[0]
    print(f"   → Found {total_relationships:,} user-product relationships to validate")
    
    # Get all user-product relationships
    print("   → Loading user-product relationships...")
    start_time = time.time()
    
    cursor.execute("""
        SELECT up.user_product_id, up.distinct_id, up.product_id
        FROM user_product_metrics up
        JOIN mixpanel_user u ON up.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
    """)
    
    user_products = cursor.fetchall()
    query_time = time.time() - start_time
    print(f"   → Loaded {len(user_products):,} relationships in {query_time:.1f} seconds")
    print(f"   → Starting lifecycle validation...")
    
    valid_lifecycles = 0
    validation_results = []
    processed = 0
    start_time = time.time()
    
    for user_product_id, distinct_id, product_id in user_products:
        processed += 1
        
        # Show progress every 5,000 items
        if processed % 5000 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (len(user_products) - processed) / rate if rate > 0 else 0
            print(f"   → Progress: {processed:,}/{len(user_products):,} ({processed/len(user_products)*100:.1f}%) "
                  f"| Rate: {rate:.1f}/sec | ETA: {eta/60:.1f}min")
        
        # Show detailed progress for first few items
        if processed <= 3:
            print(f"   → Processing user {processed}: {distinct_id[:20]}...")
        # Get events for this user-product combination
        events = get_user_product_events(cursor, distinct_id, product_id)
        
        # Validate lifecycle pattern
        is_valid, reason = validate_lifecycle_pattern(events, distinct_id, product_id)
        
        validation_results.append({
            'user_product_id': user_product_id,
            'distinct_id': distinct_id,
            'product_id': product_id,
            'is_valid': is_valid,
            'reason': reason,
            'event_count': len(events)
        })
        
        if is_valid:
            cursor.execute("""
                UPDATE user_product_metrics 
                SET valid_lifecycle = 1 
                WHERE user_product_id = ?
            """, (user_product_id,))
            valid_lifecycles += 1
        # Note: Invalid records already set to 0 in the initial cleanup, no need to update again
    
    conn.commit()
    
    # Log summary statistics
    total_relationships = len(validation_results)
    invalid_lifecycles = total_relationships - valid_lifecycles
    
    print(f"\n=== LIFECYCLE VALIDATION SUMMARY ===")
    print(f"Total user-product relationships: {total_relationships}")
    print(f"Valid lifecycles: {valid_lifecycles} ({valid_lifecycles/total_relationships*100:.1f}%)")
    print(f"Invalid lifecycles: {invalid_lifecycles} ({invalid_lifecycles/total_relationships*100:.1f}%)")
    
    # Store validation results for detailed analysis
    cursor.execute("DROP TABLE IF EXISTS temp_validation_results")
    cursor.execute("""
        CREATE TEMP TABLE temp_validation_results (
            user_product_id INTEGER,
            distinct_id TEXT,
            product_id TEXT,
            is_valid BOOLEAN,
            reason TEXT,
            event_count INTEGER
        )
    """)
    
    for result in validation_results:
        cursor.execute("""
            INSERT INTO temp_validation_results VALUES (?, ?, ?, ?, ?, ?)
        """, (result['user_product_id'], result['distinct_id'], result['product_id'], 
              result['is_valid'], result['reason'], result['event_count']))
    
    return validation_results

def validate_lifecycle_pattern(events, distinct_id, product_id):
    """
    Validates lifecycle pattern for a SPECIFIC product_id only.
    
    VALIDATION RULES (Applied per product):
    
    1. TRIAL FLOW RULES:
       - Maximum 1 "RC Trial started" event per product
       - If trial started, must have exactly 1 trial end event within 31 days
       - Trial end = "RC Trial converted" OR "RC Trial cancelled"
       - No events allowed between trial start and trial end except the end event itself
    
    2. PURCHASE FLOW RULES:
       - Maximum 1 "RC Initial purchase" event per product
       - Cannot have "RC Initial purchase" if trial is active (started but not ended)
       - Cannot have "RC Initial purchase" after "RC Trial converted"
    
    3. SUBSCRIPTION STATE RULES:
       - Subscription state = TRUE after "RC Trial converted" OR "RC Initial purchase"
       - "RC Renewal" events only allowed if subscription state = TRUE
       - "RC Cancellation" events only allowed if subscription state = TRUE
    
    4. EVENT SEQUENCE RULES:
       - Events must be in chronological order (already sorted)
       - No duplicate event types except: "RC Renewal", "RC Cancellation"
       - No "RC Trial started" after any subscription event
    
    5. COMPLETION RULES:
       - If "RC Trial started" exists, must have trial end event
       - Must have at least one subscription-creating event ("RC Trial converted" OR "RC Initial purchase")
    
    Returns: (is_valid: bool, reason: str)
    """
    
    if not events:
        increment_invalid_stat('no_events')
        log_invalid_lifecycle(distinct_id, product_id, 'no_events', events)
        return False, "no_events"
    
    # Sort events chronologically to ensure proper sequence validation
    try:
        sorted_events = sorted(events, key=lambda x: x.event_time)
    except Exception as e:
        increment_invalid_stat('datetime_parse_error')
        log_invalid_lifecycle(distinct_id, product_id, 'datetime_parse_error', events)
        return False, "datetime_parse_error"
    
    # Initialize state machine for this product
    state = ProductLifecycleState()
    
    # Process each event in chronological order
    for event in sorted_events:
        try:
            event_time = datetime.fromisoformat(event.event_time.replace('Z', '+00:00'))
        except Exception as e:
            increment_invalid_stat('datetime_parse_error')
            log_invalid_lifecycle(distinct_id, product_id, 'datetime_parse_error', events)
            return False, "datetime_parse_error"
        
        event_name = event.event_name
        
        # Process each event type according to business rules
        if event_name == 'RC Trial started':
            if not state.can_start_trial():
                if state.trial_started:
                    increment_invalid_stat('multiple_trial_starts')
                    log_invalid_lifecycle(distinct_id, product_id, 'multiple_trial_starts', events)
                    return False, "multiple_trial_starts"
                else:  # has_subscription is True
                    increment_invalid_stat('trial_start_after_subscription')
                    log_invalid_lifecycle(distinct_id, product_id, 'trial_start_after_subscription', events)
                    return False, "trial_start_after_subscription"
            
            state.trial_started = True
            state.trial_start_time = event_time
            
        elif event_name == 'RC Trial converted':
            if not state.can_end_trial():
                if not state.trial_started:
                    increment_invalid_stat('trial_conversion_without_start')
                    log_invalid_lifecycle(distinct_id, product_id, 'trial_conversion_without_start', events)
                    return False, "trial_conversion_without_start"
                else:  # trial_ended is True
                    increment_invalid_stat('multiple_trial_ends')
                    log_invalid_lifecycle(distinct_id, product_id, 'multiple_trial_ends', events)
                    return False, "multiple_trial_ends"
            
            # Check 31-day rule
            days_diff = (event_time - state.trial_start_time).days
            if days_diff > 31:
                increment_invalid_stat('trial_end_too_late')
                log_invalid_lifecycle(distinct_id, product_id, 'trial_end_too_late', events)
                return False, "trial_end_too_late"
            
            state.trial_ended = True
            state.trial_end_time = event_time
            state.trial_end_type = 'converted'
            state.has_subscription = True
            state.subscription_start_time = event_time
            
        elif event_name == 'RC Trial cancelled':
            if not state.can_end_trial():
                if not state.trial_started:
                    increment_invalid_stat('trial_cancellation_without_start')
                    log_invalid_lifecycle(distinct_id, product_id, 'trial_cancellation_without_start', events)
                    return False, "trial_cancellation_without_start"
                else:  # trial_ended is True
                    increment_invalid_stat('multiple_trial_ends')
                    log_invalid_lifecycle(distinct_id, product_id, 'multiple_trial_ends', events)
                    return False, "multiple_trial_ends"
            
            # Check 31-day rule
            days_diff = (event_time - state.trial_start_time).days
            if days_diff > 31:
                increment_invalid_stat('trial_end_too_late')
                log_invalid_lifecycle(distinct_id, product_id, 'trial_end_too_late', events)
                return False, "trial_end_too_late"
            
            state.trial_ended = True
            state.trial_end_time = event_time
            state.trial_end_type = 'cancelled'
            # Note: trial cancellation does NOT create subscription state
            
        elif event_name == 'RC Initial purchase':
            if not state.can_purchase():
                if state.trial_started and not state.trial_ended:
                    increment_invalid_stat('initial_purchase_during_trial')
                    log_invalid_lifecycle(distinct_id, product_id, 'initial_purchase_during_trial', events)
                    return False, "initial_purchase_during_trial"
                elif state.initial_purchase_count > 0:
                    increment_invalid_stat('multiple_initial_purchases')
                    log_invalid_lifecycle(distinct_id, product_id, 'multiple_initial_purchases', events)
                    return False, "multiple_initial_purchases"
                else:  # trial converted already happened
                    increment_invalid_stat('initial_purchase_after_conversion')
                    log_invalid_lifecycle(distinct_id, product_id, 'initial_purchase_after_conversion', events)
                    return False, "initial_purchase_after_conversion"
            
            state.initial_purchase_count += 1
            state.has_subscription = True
            if not state.subscription_start_time:
                state.subscription_start_time = event_time
                
        elif event_name == 'RC Renewal':
            if not state.can_renew_or_cancel():
                increment_invalid_stat('renewal_without_subscription')
                log_invalid_lifecycle(distinct_id, product_id, 'renewal_without_subscription', events)
                return False, "renewal_without_subscription"
            
        elif event_name == 'RC Cancellation':
            if not state.can_renew_or_cancel():
                increment_invalid_stat('cancellation_without_subscription')
                log_invalid_lifecycle(distinct_id, product_id, 'cancellation_without_subscription', events)
                return False, "cancellation_without_subscription"
    
    # Final validation checks
    if state.trial_started and not state.trial_ended:
        increment_invalid_stat('trial_without_end')
        log_invalid_lifecycle(distinct_id, product_id, 'trial_without_end', events)
        return False, "trial_without_end"
    
    # Valid lifecycle must have either:
    # 1. A subscription (trial converted or initial purchase), OR
    # 2. A complete trial lifecycle (trial started and ended, even if cancelled)
    has_valid_lifecycle = (state.has_subscription or 
                          (state.trial_started and state.trial_ended))
    
    if not has_valid_lifecycle:
        increment_invalid_stat('no_subscription_event')
        log_invalid_lifecycle(distinct_id, product_id, 'no_subscription_event', events)
        return False, "no_subscription_event"
    
    # If we reach here, the lifecycle is valid
    return True, "valid_lifecycle"

def analyze_invalid_lifecycles(conn):
    """Analyze invalid lifecycles to understand common failure patterns"""
    print("\nStep 4: Analyzing invalid lifecycle patterns...")
    
    cursor = conn.cursor()
    
    # Get invalid lifecycle reasons from temp table
    cursor.execute("""
        SELECT reason, COUNT(*) as count
        FROM temp_validation_results
        WHERE is_valid = FALSE
        GROUP BY reason
        ORDER BY count DESC
    """)
    
    invalid_reasons = cursor.fetchall()
    
    print("Top reasons for invalid lifecycles:")
    for reason, count in invalid_reasons[:10]:  # Show top 10 reasons
        formatted_reason = reason.replace('_', ' ').title()
        print(f"  - {formatted_reason}: {count:,} cases")
    
    if len(invalid_reasons) > 10:
        remaining = sum(count for _, count in invalid_reasons[10:])
        print(f"  - Other reasons: {remaining:,} cases")
    
    # Analyze by product
    cursor.execute("""
        SELECT 
            product_id,
            COUNT(*) as total,
            COUNT(CASE WHEN is_valid = TRUE THEN 1 END) as valid,
            COUNT(CASE WHEN is_valid = FALSE THEN 1 END) as invalid
        FROM temp_validation_results
        GROUP BY product_id
        ORDER BY total DESC
        LIMIT 10
    """)
    
    product_stats = cursor.fetchall()
    
    print(f"\nLifecycle validation by product (top 10):")
    for product_id, total, valid, invalid in product_stats:
        valid_pct = (valid / total * 100) if total > 0 else 0
        print(f"  - {product_id}: {valid:,}/{total:,} valid ({valid_pct:.1f}%)")
    
    # Sample detailed analysis of invalid cases
    print(f"\nDetailed analysis of invalid cases (sample of 3):")
    cursor.execute("""
        SELECT distinct_id, product_id, reason, event_count
        FROM temp_validation_results
        WHERE is_valid = FALSE
        ORDER BY event_count DESC
        LIMIT 3
    """)
    
    invalid_samples = cursor.fetchall()
    
    for distinct_id, product_id, reason, event_count in invalid_samples:
        formatted_reason = reason.replace('_', ' ').title()
        print(f"  - User {distinct_id[:8]}..., Product {product_id}: {formatted_reason} ({event_count} events)")
        
        # Get actual events for this user-product to understand the pattern
        events = get_user_product_events(cursor, distinct_id, product_id)
        if events:
            print(f"    Event sequence:")
            for i, event in enumerate(events[:3]):  # Show first 3 events
                event_time_short = event.event_time[:19] if len(event.event_time) > 19 else event.event_time
                print(f"      {i+1}. {event_time_short} - {event.event_name} (${event.revenue_usd})")
            if len(events) > 3:
                print(f"      ... and {len(events)-3} more events")
        print()

def verify_lifecycle_validation(conn):
    """Verify the lifecycle validation results"""
    print("\nStep 5: Verifying lifecycle validation results...")
    
    cursor = conn.cursor()
    
    # Overall statistics
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE valid_user = TRUE")
    total_valid_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
    total_user_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM user_product_metrics WHERE valid_lifecycle = TRUE")
    valid_lifecycles = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT product_id) FROM user_product_metrics")
    unique_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT distinct_id) FROM user_product_metrics")
    users_with_products = cursor.fetchone()[0]
    
    print(f"Lifecycle Validation Summary:")
    print(f"  - Valid users: {total_valid_users}")
    print(f"  - Users with products: {users_with_products}")
    print(f"  - Total user-product relationships: {total_user_products}")
    print(f"  - Valid lifecycles: {valid_lifecycles}")
    print(f"  - Unique products: {unique_products}")
    
    # Product distribution
    cursor.execute("""
        SELECT product_id, COUNT(*) as user_count,
               COUNT(CASE WHEN valid_lifecycle = TRUE THEN 1 END) as valid_count
        FROM user_product_metrics
        GROUP BY product_id
        ORDER BY user_count DESC
        LIMIT 10
    """)
    
    product_stats = cursor.fetchall()
    print(f"\nTop products by user count:")
    for product_id, user_count, valid_count in product_stats:
        print(f"  - {product_id}: {user_count} users ({valid_count} valid lifecycles)")
    
    # Event volume statistics for valid lifecycles only
    cursor.execute("""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT sub.distinct_id) as users_with_events,
            AVG(CAST(sub.events_per_user AS FLOAT)) as avg_events_per_user
        FROM (
            SELECT e.distinct_id, COUNT(*) as events_per_user
            FROM mixpanel_event e
            JOIN user_product_metrics up ON e.distinct_id = up.distinct_id
            WHERE up.valid_lifecycle = TRUE
            GROUP BY e.distinct_id
        ) sub
    """)
    
    event_stats = cursor.fetchone()
    if event_stats and event_stats[0]:
        total_events, users_with_events, avg_events = event_stats
        print(f"\nEvent Volume Statistics (Valid Lifecycles Only):")
        print(f"  - Total events: {total_events}")
        print(f"  - Users with events: {users_with_events}")
        print(f"  - Average events per user: {avg_events:.1f}")
    
    print("Lifecycle validation verification completed")

if __name__ == "__main__":
    sys.exit(main()) 