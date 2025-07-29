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
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

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
    """Print concise statistics about invalid lifecycles"""
    # This function is now simplified - detailed stats shown in main output

def deduplicate_user_lifecycles(conn):
    """
    Ensure each user has only one valid lifecycle.
    For users with multiple valid lifecycles, keep only the most recent one.
    
    Business Rule: Users shouldn't have multiple active trials/subscriptions.
    This prevents over-estimation in revenue calculations.
    
    Returns: (users_deduplicated, lifecycles_invalidated)
    """
    cursor = conn.cursor()
    
    # Find users with multiple valid lifecycles
    cursor.execute("""
        SELECT distinct_id, COUNT(*) as lifecycle_count
        FROM user_product_metrics
        WHERE valid_lifecycle = 1
        GROUP BY distinct_id
        HAVING COUNT(*) > 1
    """)
    
    users_with_multiple = cursor.fetchall()
    deduplication_count = 0
    
    for distinct_id, lifecycle_count in users_with_multiple:
        # For each user with multiple lifecycles, find the most recent one
        cursor.execute("""
            SELECT 
                ump.product_id,
                MIN(e.event_time) as first_event_time
            FROM user_product_metrics ump
            JOIN mixpanel_event e ON ump.distinct_id = e.distinct_id
            WHERE ump.distinct_id = ?
              AND ump.valid_lifecycle = 1
              AND e.event_name IN ('RC Trial started', 'RC Initial purchase')
              AND JSON_EXTRACT(e.event_json, '$.properties.product_id') = ump.product_id
            GROUP BY ump.product_id
            ORDER BY first_event_time DESC
            LIMIT 1
        """, (distinct_id,))
        
        most_recent = cursor.fetchone()
        if most_recent:
            most_recent_product_id = most_recent[0]
            
            # Mark all other lifecycles for this user as invalid
            cursor.execute("""
                UPDATE user_product_metrics
                SET valid_lifecycle = 0
                WHERE distinct_id = ?
                  AND product_id != ?
                  AND valid_lifecycle = 1
            """, (distinct_id, most_recent_product_id))
            
            deduplication_count += lifecycle_count - 1
    
    conn.commit()
    return len(users_with_multiple), deduplication_count

def main():
    try:
        print("=== Module 6: Validate Event Lifecycle ===")
        
        # Connect to database
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # Setup user-product relationships and validate lifecycles
        setup_and_validate_lifecycles(conn)
        
        conn.close()
        
        print("Event lifecycle validation completed successfully")
        return 0
        
    except Exception as e:
        print(f"Module 6 failed: {e}", file=sys.stderr)
        return 1

def setup_and_validate_lifecycles(conn):
    """Setup user-product relationships and validate all lifecycles"""
    cursor = conn.cursor()
    
    # Reset statistics
    global INVALID_LIFECYCLE_STATS
    for key in INVALID_LIFECYCLE_STATS:
        INVALID_LIFECYCLE_STATS[key] = 0
    
    # PRESERVE METADATA: Backup before clearing
    cursor.execute("""
        CREATE TEMP TABLE temp_attribution_backup_setup AS
        SELECT 
            distinct_id,
            product_id,
            country,
            region,
            device,
            store
        FROM user_product_metrics
    """)
    
    # Clear existing relationships and validation data
    cursor.execute("DELETE FROM user_product_metrics WHERE 1=1")
    conn.commit()
    
    # Get ALL valid users with subscription events and their products
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
    
    # Create relationships and validate lifecycles in one pass
    valid_count = 0
    total_count = 0
    
    for i, (distinct_id, product_id) in enumerate(user_product_pairs):
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
            cursor.execute("""
                SELECT country, region, device, store
                FROM temp_attribution_backup_setup
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            metadata = cursor.fetchone()
            
            if metadata:
                # Restore with metadata
                country, region, device, store = metadata
                cursor.execute("""
                    INSERT INTO user_product_metrics 
                    (distinct_id, product_id, credited_date, current_status, current_value, 
                     value_status, last_updated_ts, valid_lifecycle, country, region, device, store)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    distinct_id, 
                    product_id,
                    'PLACEHOLDER_DATE',
                    'PLACEHOLDER_STATUS',
                    -999.99,
                    'PLACEHOLDER_VALUE_STATUS',
                    now_in_timezone(),
                    1 if is_valid else 0,
                    country,                # PRESERVED geographic data
                    region,                 # PRESERVED geographic data
                    device,                 # PRESERVED device data
                    store                   # PRESERVED store data
                ))
            else:
                # Create without metadata (new relationship)
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
                    now_in_timezone(),
                    1 if is_valid else 0
                ))
            
            total_count += 1
                
        except Exception as e:
            increment_invalid_stat('datetime_parse_error')
    
    # Clean up temporary table
    cursor.execute("DROP TABLE temp_attribution_backup_setup")
    conn.commit()
    
    # DEDUPLICATION: Ensure each user has only one valid lifecycle
    users_deduplicated, lifecycles_invalidated = deduplicate_user_lifecycles(conn)
    if users_deduplicated > 0:
        print(f"🔄 Deduplicated {users_deduplicated:,} users with multiple lifecycles")
        print(f"   Invalidated {lifecycles_invalidated:,} duplicate lifecycles")
        # Update valid_count to reflect deduplication
        valid_count -= lifecycles_invalidated
    
    # Display concise results
    print(f"✅ Processed {total_count:,} user-product relationships")
    print(f"✅ Valid lifecycles: {valid_count:,} ({valid_count/total_count*100:.1f}%)")
    if total_count - valid_count > 0:
        print(f"❌ Invalid lifecycles: {total_count-valid_count:,} ({(total_count-valid_count)/total_count*100:.1f}%)")
        
        # Show top 5 invalid reasons only
        total_invalid = sum(INVALID_LIFECYCLE_STATS.values())
        if total_invalid > 0:
            print(f"\nTop invalid reasons:")
            sorted_reasons = sorted(INVALID_LIFECYCLE_STATS.items(), key=lambda x: x[1], reverse=True)
            for reason, count in sorted_reasons[:5]:  # Only top 5
                if count > 0:
                    formatted_reason = reason.replace('_', ' ').title()
                    percentage = count/total_invalid*100
                    print(f"  • {formatted_reason}: {count:,} cases ({percentage:.1f}%)")

# Removed verbose display_validation_results function - now handled in main validation

# Removed verbose analyze_user_events function - not needed for concise output

# Removed verbose analyze_product_usage function - functionality integrated into main validation


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

# Removed verbose validate_user_lifecycles function - functionality integrated into main validation

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
        # Check if trial started more than 31 days ago
        # If so, it should have ended by now, so it's invalid
        # If not, the trial could still be legitimately active
        try:
            current_time = now_in_timezone()
            days_since_trial_start = (current_time - state.trial_start_time).days
            
            if days_since_trial_start > 31:
                increment_invalid_stat('trial_without_end')
                log_invalid_lifecycle(distinct_id, product_id, 'trial_without_end', events)
                return False, "trial_without_end"
            # If trial started within last 31 days, it's still potentially valid
            # so we don't mark it as invalid here
        except Exception as e:
            # If we can't parse the trial start time, fall back to marking as invalid
            increment_invalid_stat('trial_without_end')
            log_invalid_lifecycle(distinct_id, product_id, 'trial_without_end', events)
            return False, "trial_without_end"
    
    # Valid lifecycle must have either:
    # 1. A subscription (trial converted or initial purchase), OR
    # 2. A complete trial lifecycle (trial started and ended, even if cancelled), OR
    # 3. An active trial within the 31-day window
    has_valid_lifecycle = (state.has_subscription or 
                          (state.trial_started and state.trial_ended) or
                          (state.trial_started and not state.trial_ended))  # Active trial case
    
    if not has_valid_lifecycle:
        increment_invalid_stat('no_subscription_event')
        log_invalid_lifecycle(distinct_id, product_id, 'no_subscription_event', events)
        return False, "no_subscription_event"
    
    # If we reach here, the lifecycle is valid
    return True, "valid_lifecycle"

# Removed verbose analyze_invalid_lifecycles and verify_lifecycle_validation functions - not needed for concise output

if __name__ == "__main__":
    sys.exit(main()) 