#!/usr/bin/env python3
"""
Module 6: Validate Event Lifecycle

This module validates that user-product lifecycle events follow valid business patterns.
It analyzes event sequences for each user-product combination to identify valid vs invalid
lifecycle patterns according to subscription business rules.

VALIDATION RULES:
- Trial Flow: Trial started → Trial converted/cancelled (within 31 days)
- Purchase Flow: Initial purchase (standalone)
- Subscription Flow: Renewals/Cancellations after conversion or purchase
- No multiple trial starts, orphaned events, or invalid sequences

PERFORMANCE OPTIMIZATIONS:
- Batch processing with efficient database queries
- Single-pass validation and relationship creation
- Minimal memory footprint with streaming processing
"""

import os
import sys
import sqlite3
import time
from datetime import datetime, timedelta
from collections import namedtuple, defaultdict
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configuration
DATABASE_PATH = get_database_path('mixpanel_data')

# Event mapping and data structures
IMPORTANT_EVENTS = {
    "RC Trial started", "RC Trial converted", "RC Cancellation", 
    "RC Initial purchase", "RC Trial cancelled", "RC Renewal"
}

LifecycleEvent = namedtuple('LifecycleEvent', ['event_time', 'event_name', 'event_type', 'revenue_usd', 'event_uuid'])

class ProductLifecycleState:
    """Tracks lifecycle state for validation with business rules"""
    def __init__(self):
        self.trial_started = False
        self.trial_start_time = None
        self.trial_ended = False
        self.has_subscription = False
        self.initial_purchase_count = 0
        
    def can_start_trial(self):
        return not self.trial_started and not self.has_subscription
        
    def can_end_trial(self):
        return self.trial_started and not self.trial_ended
        
    def can_purchase(self):
        return (not self.trial_started or self.trial_ended) and self.initial_purchase_count == 0
        
    def can_renew_or_cancel(self):
        return self.has_subscription

class LifecycleValidator:
    """Main validator class with business logic and statistics tracking"""
    
    def __init__(self):
        self.invalid_stats = defaultdict(int)
        self.total_processed = 0
        self.valid_count = 0
        
    def validate_lifecycle_pattern(self, events, distinct_id, product_id):
        """
        Validates lifecycle pattern for a specific product.
        Returns: (is_valid: bool, reason: str)
        """
        if not events:
            self.invalid_stats['no_events'] += 1
            return False, "no_events"
        
        try:
            sorted_events = sorted(events, key=lambda x: x.event_time)
        except Exception:
            self.invalid_stats['datetime_parse_error'] += 1
            return False, "datetime_parse_error"
        
        state = ProductLifecycleState()
        
        for event in sorted_events:
            try:
                event_time = datetime.fromisoformat(event.event_time.replace('Z', '+00:00'))
            except Exception:
                self.invalid_stats['datetime_parse_error'] += 1
                return False, "datetime_parse_error"
            
            event_name = event.event_name
            
            # Validate each event type according to business rules
            if event_name == 'RC Trial started':
                if not state.can_start_trial():
                    reason = 'multiple_trial_starts' if state.trial_started else 'trial_start_after_subscription'
                    self.invalid_stats[reason] += 1
                    return False, reason
                state.trial_started = True
                state.trial_start_time = event_time
                
            elif event_name == 'RC Trial converted':
                if not state.can_end_trial():
                    reason = 'trial_conversion_without_start' if not state.trial_started else 'multiple_trial_ends'
                    self.invalid_stats[reason] += 1
                    return False, reason
                # Check 31-day rule
                if (event_time - state.trial_start_time).days > 31:
                    self.invalid_stats['trial_end_too_late'] += 1
                    return False, "trial_end_too_late"
                state.trial_ended = True
                state.has_subscription = True
                
            elif event_name == 'RC Trial cancelled':
                if not state.can_end_trial():
                    reason = 'trial_cancellation_without_start' if not state.trial_started else 'multiple_trial_ends'
                    self.invalid_stats[reason] += 1
                    return False, reason
                # Check 31-day rule
                if (event_time - state.trial_start_time).days > 31:
                    self.invalid_stats['trial_end_too_late'] += 1
                    return False, "trial_end_too_late"
                state.trial_ended = True
                
            elif event_name == 'RC Initial purchase':
                if not state.can_purchase():
                    if state.trial_started and not state.trial_ended:
                        reason = 'initial_purchase_during_trial'
                    elif state.initial_purchase_count > 0:
                        reason = 'multiple_initial_purchases'
                    else:
                        reason = 'initial_purchase_after_conversion'
                    self.invalid_stats[reason] += 1
                    return False, reason
                state.initial_purchase_count += 1
                state.has_subscription = True
                
            elif event_name in ['RC Renewal', 'RC Cancellation']:
                if not state.can_renew_or_cancel():
                    reason = 'renewal_without_subscription' if event_name == 'RC Renewal' else 'cancellation_without_subscription'
                    self.invalid_stats[reason] += 1
                    return False, reason
        
        # Final validation checks
        if state.trial_started and not state.trial_ended:
            self.invalid_stats['trial_without_end'] += 1
            return False, "trial_without_end"
        
        # Must have valid lifecycle (subscription or complete trial)
        if not (state.has_subscription or (state.trial_started and state.trial_ended)):
            self.invalid_stats['no_subscription_event'] += 1
            return False, "no_subscription_event"
        
        return True, "valid_lifecycle"
    
    def get_user_product_events(self, cursor, distinct_id, product_id):
        """Get all events for a specific user-product combination"""
        event_name_list = ', '.join([f"'{name}'" for name in IMPORTANT_EVENTS])
        
        cursor.execute(f"""
            SELECT event_time, event_name, COALESCE(revenue_usd, 0) as revenue_usd, 
                   event_uuid, event_json
            FROM mixpanel_event
            WHERE distinct_id = ? AND event_name IN ({event_name_list})
              AND JSON_EXTRACT(event_json, '$.properties.product_id') = ?
            ORDER BY event_time ASC
        """, (distinct_id, product_id))
        
        return [LifecycleEvent(event_time, event_name, None, revenue_usd, event_uuid)
                for event_time, event_name, revenue_usd, event_uuid, _ in cursor.fetchall()]

def main():
    try:
        print("=== Module 6: Validate Event Lifecycle ===")
        
        conn = sqlite3.connect(str(DATABASE_PATH))
        validator = LifecycleValidator()
        
        # Setup and validate in single efficient pass
        total_relationships, valid_lifecycles = setup_and_validate_lifecycles(conn, validator)
        
        # Display clean summary
        display_summary(conn, validator, total_relationships, valid_lifecycles)
        
        conn.close()
        print("Event lifecycle validation completed successfully")
        return 0
        
    except Exception as e:
        print(f"Module 6 failed: {e}", file=sys.stderr)
        return 1

def setup_and_validate_lifecycles(conn, validator):
    """Setup user-product relationships and validate all lifecycles in single pass"""
    cursor = conn.cursor()
    
    # Preserve existing metadata
    cursor.execute("""
        CREATE TEMP TABLE temp_attribution_backup AS
        SELECT distinct_id, product_id, country, region, device, store
        FROM user_product_metrics
    """)
    
    # Clear and rebuild relationships
    cursor.execute("DELETE FROM user_product_metrics WHERE 1=1")
    conn.commit()
    
    # Get all user-product pairs that need validation
    cursor.execute("""
        SELECT DISTINCT e.distinct_id,
               JSON_EXTRACT(e.event_json, '$.properties.product_id') as product_id
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.valid_user = 1
          AND e.event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 
                               'RC Initial purchase', 'RC Cancellation', 'RC Renewal')
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') IS NOT NULL
          AND JSON_EXTRACT(e.event_json, '$.properties.product_id') != ''
    """)
    
    user_product_pairs = cursor.fetchall()
    total_pairs = len(user_product_pairs)
    
    print(f"✅ Processing {total_pairs:,} user-product relationships...")
    
    # Process in batches for better performance
    valid_count = 0
    batch_size = 1000
    start_time = time.time()
    
    for i in range(0, total_pairs, batch_size):
        batch = user_product_pairs[i:i + batch_size]
        batch_valid = 0
        
        for distinct_id, product_id in batch:
            try:
                # Get events and validate lifecycle
                events = validator.get_user_product_events(cursor, distinct_id, product_id)
                is_valid, _ = validator.validate_lifecycle_pattern(events, distinct_id, product_id)
                
                if is_valid:
                    batch_valid += 1
                
                # Restore metadata if available
                cursor.execute("""
                    SELECT country, region, device, store
                    FROM temp_attribution_backup
                    WHERE distinct_id = ? AND product_id = ?
                """, (distinct_id, product_id))
                
                metadata = cursor.fetchone()
                
                if metadata:
                    cursor.execute("""
                        INSERT INTO user_product_metrics 
                        (distinct_id, product_id, credited_date, current_status, current_value, 
                         value_status, last_updated_ts, valid_lifecycle, country, region, device, store)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (distinct_id, product_id, 'PLACEHOLDER_DATE', 'PLACEHOLDER_STATUS', 
                          -999.99, 'PLACEHOLDER_VALUE_STATUS', datetime.now(), 
                          1 if is_valid else 0, *metadata))
                else:
                    cursor.execute("""
                        INSERT INTO user_product_metrics 
                        (distinct_id, product_id, credited_date, current_status, current_value, 
                         value_status, last_updated_ts, valid_lifecycle)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (distinct_id, product_id, 'PLACEHOLDER_DATE', 'PLACEHOLDER_STATUS',
                          -999.99, 'PLACEHOLDER_VALUE_STATUS', datetime.now(), 
                          1 if is_valid else 0))
                
            except Exception as e:
                validator.invalid_stats['datetime_parse_error'] += 1
        
        valid_count += batch_valid
        
        # Show progress for large datasets
        if total_pairs > 10000 and (i + batch_size) % 10000 == 0:
            progress = min(100, (i + batch_size) / total_pairs * 100)
            print(f"   Progress: {progress:.0f}% ({i + batch_size:,}/{total_pairs:,})")
    
    cursor.execute("DROP TABLE temp_attribution_backup")
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ Completed validation in {elapsed:.1f}s")
    
    return total_pairs, valid_count

def display_summary(conn, validator, total_relationships, valid_lifecycles):
    """Display clean, actionable summary"""
    cursor = conn.cursor()
    
    invalid_lifecycles = total_relationships - valid_lifecycles
    
    print(f"\n✅ Processed {total_relationships:,} user-product relationships")
    print(f"✅ Valid lifecycles: {valid_lifecycles:,} ({valid_lifecycles/total_relationships*100:.1f}%)")
    
    if invalid_lifecycles > 0:
        print(f"❌ Invalid lifecycles: {invalid_lifecycles:,} ({invalid_lifecycles/total_relationships*100:.1f}%)")
        
        # Show top 5 invalid reasons only
        sorted_reasons = sorted(validator.invalid_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        if sorted_reasons:
            print(f"\nTop invalid reasons:")
            for reason, count in sorted_reasons:
                percentage = count / invalid_lifecycles * 100
                formatted_reason = reason.replace('_', ' ').replace('rc ', '').title()
                print(f"  • {formatted_reason}: {count:,} cases ({percentage:.1f}%)")
    
    # Show product health summary (top products with issues)
    cursor.execute("""
        SELECT product_id, COUNT(*) as total_users, SUM(valid_lifecycle) as valid_lifecycles
        FROM user_product_metrics
        WHERE valid_lifecycle = 0
        GROUP BY product_id
        HAVING COUNT(*) >= 100
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    
    problem_products = cursor.fetchall()
    if problem_products:
        print(f"\nProducts needing attention (most invalid lifecycles):")
        for product_id, total_invalid, _ in problem_products:
            print(f"  • {product_id}: {total_invalid:,} invalid lifecycles")

if __name__ == "__main__":
    sys.exit(main()) 