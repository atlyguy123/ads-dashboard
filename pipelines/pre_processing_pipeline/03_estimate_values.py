#!/usr/bin/env python3
"""
Estimate Values Module - Pre-processing Pipeline

This module estimates monetary values based on assigned price buckets 
and conversion rates from previous pipeline steps. It implements the 
core value calculation logic for:
- Credited date calculation
- Current status determination  
- Current value calculation
- Value status classification

Ported from mixpanel_processing_stage.py but adapted for the new consolidated schema.
"""

import os
import sys
from typing import Dict, Any, List, Optional, Tuple
import logging
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Main function for the value estimation module.
    """
    logger.info("Starting Pre-processing Pipeline - Estimate Values Module")
    
    try:
        # Initialize the value estimator
        estimator = ValueEstimator()
        
        # Process all users with attribution
        result = estimator.process_attributed_users()
        
        if result['success']:
            logger.info(f"Value estimation completed successfully. Processed {result['users_processed']} users")
            return True
        else:
            logger.error(f"Value estimation failed: {result['error']}")
            return False
        
    except Exception as e:
        logger.error(f"Error in value estimation: {str(e)}")
        return False


class ValueEstimator:
    """
    Main class for estimating user-product values using the consolidated schema.
    
    This class implements the core logic from mixpanel_processing_stage.py but
    adapted to work with the new consolidated database schema.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize the value estimator.
        
        Args:
            db_path: Path to the main database file (auto-detected if None)
        """
        # Auto-detect database path if not provided
        if db_path is None:
            db_path = get_database_path('mixpanel_data')
        self.db_path = db_path
        self.pricing_rules = None
        
        # Error counters for summary reporting
        self.error_counts = {
            'no_subscription_start_event': 0,
            'no_price_bucket_in_database': 0,
            'no_conversion_rates_found': 0,
            'invalid_credited_date': 0,
            'failed_profile_json_parse': 0,
            'failed_user_processing': 0,
            'failed_price_lookup': 0,
            'failed_value_calculation': 0
        }
        
        self._load_pricing_rules()
        logger.info("ValueEstimator initialized")
    
    def _load_pricing_rules(self) -> None:
        """Load pricing rules from data/pricing_rules/pricing_rules.json"""
        try:
            pricing_rules_path = 'data/pricing_rules/pricing_rules.json'
            if os.path.exists(pricing_rules_path):
                with open(pricing_rules_path, 'r') as f:
                    self.pricing_rules = json.load(f)
                logger.info(f"Loaded pricing rules for {len(self.pricing_rules.get('products', {}))} products")
            else:
                logger.warning(f"Pricing rules file not found at {pricing_rules_path}")
                self.pricing_rules = {'products': {}}
        except Exception as e:
            logger.error(f"Failed to load pricing rules: {e}")
            self.pricing_rules = {'products': {}}

    def _get_active_price_for_date(self, product_id: str, country: str, trial_date: str) -> Optional[float]:
        """
        Find active price for product/country on specific date.
        Rules with later start_date override earlier ones, even if end_date is null.
        """
        try:
            if not self.pricing_rules:
                return None
                
            product_rules = self.pricing_rules['products'].get(product_id)
            if not product_rules:
                return None
            
            # Try country-specific rules first, then 'ALL'
            for country_key in [country, 'ALL']:
                if country_key not in product_rules:
                    continue
                    
                rules = product_rules[country_key]['rules']
                
                # Sort by start_date (newest first)
                sorted_rules = sorted(rules, key=lambda x: x['start_date'], reverse=True)
                
                # Find first rule where start_date <= trial_date
                for rule in sorted_rules:
                    if rule['start_date'] <= trial_date:
                        return rule['price_usd']
            
            return None
            
        except Exception as e:
            self.error_counts['failed_price_lookup'] += 1
            return None

    def process_attributed_users(self) -> Dict[str, Any]:
        """
        Process all users with ABI attribution to calculate their values.
        
        Returns:
            Dictionary with processing results
        """
        try:
            start_time = datetime.now()
            
            # CRITICAL: Only clear the three specific value fields, preserve all other data
            logger.info("Clearing only current_status, current_value, and value_status fields...")
            self._clear_value_fields_only()
            
            # Get users with attribution
            attributed_users = self._get_attributed_users()
            if not attributed_users:
                return {
                    'success': True,
                    'users_processed': 0,
                    'message': 'No users with ABI attribution to process'
                }
            
            logger.info(f"Found {len(attributed_users)} users with ABI attribution")
            
            # Process users in batches - optimized for large-scale processing
            total_processed = 0
            total_user_product_pairs = 0
            total_successful = 0
            total_failed = 0
            
            # Use larger batch size for memory efficiency (10k-50k user-product pairs)
            batch_size = 5000  # Start with 5k users, which can generate 10k-50k user-product pairs
            for i in range(0, len(attributed_users), batch_size):
                batch = attributed_users[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(attributed_users) + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} users)")
                
                batch_result = self._process_user_batch(batch)
                total_processed += batch_result['users_processed']
                total_user_product_pairs += batch_result['user_product_pairs']
                total_successful += batch_result['successful_calculations']
                total_failed += batch_result['failed_calculations']
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Display error summary
            self._display_error_summary()
            
            return {
                'success': True,
                'users_processed': total_processed,
                'user_product_pairs': total_user_product_pairs,
                'successful_calculations': total_successful,
                'failed_calculations': total_failed,
                'processing_time': processing_time,
                'error_counts': self.error_counts,
                'message': f'Successfully processed {total_processed} users with {total_user_product_pairs} user-product pairs'
            }
            
        except Exception as e:
            logger.error(f"Error processing attributed users: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _display_error_summary(self):
        """Display summary of all errors encountered during processing"""
        logger.info("=== VALUE ESTIMATION ERROR SUMMARY ===")
        total_errors = sum(self.error_counts.values())
        logger.info(f"Total errors encountered: {total_errors}")
        
        if total_errors > 0:
            for error_type, count in self.error_counts.items():
                if count > 0:
                    logger.info(f"  {error_type}: {count}")
        else:
            logger.info("  No errors encountered during processing")
        logger.info("=====================================")

    def _clear_value_fields_only(self) -> None:
        """Clear ONLY the three value estimation fields that this script is allowed to modify"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # CRITICAL: Only clear the three fields this script is authorized to modify
            # Preserve ALL other data including conversion rates, accuracy scores, etc.
            cursor.execute("""
                UPDATE user_product_metrics 
                SET current_status = 'PLACEHOLDER_STATUS',
                    current_value = 0.00,
                    value_status = 'PLACEHOLDER_VALUE_STATUS',
                    last_updated_ts = datetime('now')
                WHERE 1=1
            """)
            conn.commit()
            
            rows_cleared = cursor.rowcount
            conn.close()
            
            logger.info(f"Cleared ONLY current_status, current_value, and value_status for {rows_cleared} records")
            
        except Exception as e:
            logger.error(f"Failed to clear value fields: {e}")
            raise

    def _get_attributed_users(self) -> List[Dict[str, Any]]:
        """Get ALL users from user_product_metrics table (TEMPORARILY REMOVING FILTERS FOR TESTING)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # TEMPORARILY REMOVED FILTERING - Processing ALL users for testing
            # Original filtering was: u.has_abi_attribution = 1 AND u.valid_user = 1 AND upm.valid_lifecycle = 1
            query = """
                SELECT DISTINCT u.distinct_id, u.profile_json
                FROM mixpanel_user u
                JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            attributed_users = []
            for row in rows:
                distinct_id, profile_json = row
                try:
                    profile = json.loads(profile_json) if profile_json else {}
                    attributed_users.append({
                        'distinct_id': distinct_id,
                        'profile': profile
                    })
                except json.JSONDecodeError:
                    self.error_counts['failed_profile_json_parse'] += 1
                    continue
            
            conn.close()
            return attributed_users
            
        except Exception as e:
            logger.error(f"Failed to get attributed users: {e}")
            return []

    def _process_user_batch(self, users_batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a single batch of users with optimized memory usage"""
        batch_stats = {
            'users_processed': 0,
            'user_product_pairs': 0,
            'successful_calculations': 0,
            'failed_calculations': 0
        }
        
        # Pre-fetch all user events in a single query for the entire batch
        user_ids = [user['distinct_id'] for user in users_batch]
        all_user_events = self._get_batch_user_events(user_ids)
        
        user_product_records = []
        
        for user_data in users_batch:
            distinct_id = user_data['distinct_id']
            profile = user_data['profile']
            
            try:
                # Get user's events from pre-fetched data
                user_events = all_user_events.get(distinct_id, {})
                
                if not user_events:
                    batch_stats['users_processed'] += 1
                    continue
                
                batch_stats['user_product_pairs'] += len(user_events)
                
                # Process each User x Product pair
                for product_id, events in user_events.items():
                    record = self._process_user_product_pair(distinct_id, product_id, profile, events)
                    
                    if record:
                        user_product_records.append(record)
                        batch_stats['successful_calculations'] += 1
                    else:
                        batch_stats['failed_calculations'] += 1
                
                batch_stats['users_processed'] += 1
                
            except Exception as e:
                self.error_counts['failed_user_processing'] += 1
                batch_stats['failed_calculations'] += 1
                continue
        
        # Store all records for this batch in a single transaction
        if user_product_records:
            stored_count = self._store_user_product_records(user_product_records)
        
        return batch_stats

    def _get_batch_user_events(self, user_ids: List[str]) -> Dict[str, Dict[str, List[Dict]]]:
        """Get subscription events for a batch of users, for ALL user-product pairs (TEMPORARILY REMOVING FILTERS)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create placeholder for user IDs
            user_placeholders = ','.join(['?' for _ in user_ids])
            
            # Get all relevant events for ALL user-product pairs (filtering temporarily removed)
            query = f"""
                WITH all_user_products AS (
                    SELECT DISTINCT 
                        upm.distinct_id,
                        upm.product_id
                    FROM user_product_metrics upm
                    WHERE upm.distinct_id IN ({user_placeholders})
                )
                SELECT 
                    e.distinct_id,
                    JSON_EXTRACT(e.event_json, '$.properties.product_id') as product_id,
                    e.event_name,
                    e.event_time,
                    e.revenue_usd,
                    e.refund_flag,
                    e.event_json
                FROM mixpanel_event e
                INNER JOIN all_user_products aup ON e.distinct_id = aup.distinct_id 
                    AND JSON_EXTRACT(e.event_json, '$.properties.product_id') = aup.product_id
                WHERE e.event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 'RC Initial purchase', 'RC Cancellation')
                ORDER BY e.distinct_id, aup.product_id, e.event_time
            """
            
            cursor.execute(query, user_ids)
            rows = cursor.fetchall()
            
            # Group by user_id -> product_id -> events
            batch_user_events = {}
            for row in rows:
                distinct_id, product_id, event_name, event_time, revenue_usd, refund_flag, event_json = row
                
                if distinct_id not in batch_user_events:
                    batch_user_events[distinct_id] = {}
                
                if product_id not in batch_user_events[distinct_id]:
                    batch_user_events[distinct_id][product_id] = []
                
                batch_user_events[distinct_id][product_id].append({
                    'event_name': event_name,
                    'event_time': event_time,
                    'revenue_usd': revenue_usd or 0.0,
                    'refund_flag': refund_flag or 0,
                    'event_json': event_json
                })
            
            conn.close()
            return batch_user_events
            
        except Exception as e:
            logger.error(f"Failed to get batch events for {len(user_ids)} users: {e}")
            return {}

    def _get_user_product_pairs(self, distinct_id: str) -> Dict[str, List[Dict]]:
        """Get subscription events for a user, grouped by product_id, only for products with start events"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # First, find products that have start events for this user
            start_events_query = """
                SELECT DISTINCT JSON_EXTRACT(event_json, '$.properties.product_id') as product_id
                FROM mixpanel_event 
                WHERE distinct_id = ?
                  AND event_name IN ('RC Trial started', 'RC Initial purchase')
                  AND JSON_EXTRACT(event_json, '$.properties.product_id') IS NOT NULL
            """
            
            cursor.execute(start_events_query, (distinct_id,))
            products_with_start_events = set(row[0] for row in cursor.fetchall())
            
            if not products_with_start_events:
                conn.close()
                return {}
            
            # Now get all events for products that have start events
            products_placeholder = ','.join(['?' for _ in products_with_start_events])
            query = f"""
                SELECT 
                    JSON_EXTRACT(event_json, '$.properties.product_id') as product_id,
                    event_name,
                    event_time,
                    revenue_usd,
                    refund_flag,
                    event_json
                FROM mixpanel_event 
                WHERE distinct_id = ?
                  AND event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 'RC Initial purchase', 'RC Cancellation')
                  AND JSON_EXTRACT(event_json, '$.properties.product_id') IN ({products_placeholder})
                ORDER BY event_time
            """
            
            cursor.execute(query, [distinct_id] + list(products_with_start_events))
            rows = cursor.fetchall()
            
            # Group by product_id
            product_pairs = {}
            for row in rows:
                product_id, event_name, event_time, revenue_usd, refund_flag, event_json = row
                
                if product_id not in product_pairs:
                    product_pairs[product_id] = []
                
                product_pairs[product_id].append({
                    'event_name': event_name,
                    'event_time': event_time,
                    'revenue_usd': revenue_usd or 0.0,
                    'refund_flag': refund_flag or 0,
                    'event_json': event_json
                })
            
            conn.close()
            return product_pairs
            
        except Exception as e:
            logger.error(f"Failed to get events for user {distinct_id}: {e}")
            return {}

    def _process_user_product_pair(self, distinct_id: str, product_id: str, profile: Dict, events: List[Dict]) -> Optional[Dict]:
        """
        Process a single User x Product pair to calculate values.
        
        This implements the core logic from mixpanel_processing_stage.py
        """
        try:
            # Find subscription start event - use the LAST conversion event (trial start OR initial purchase)
            # This handles complex user journeys like: trial → cancel → initial purchase
            # NOTE: Edge cases (conversions without start events) are handled by 00_assign_credited_date.py
            start_event = None
            for event in reversed(events):  # Go backwards to find the last conversion event
                if event['event_name'] in ['RC Trial started', 'RC Initial purchase']:
                    start_event = event
                    break
            
            # EDGE CASE: If no start event found, but we have a credited_date (from fallback logic)
            # Create a synthetic start event based on the credited_date
            if not start_event:
                credited_date = self._get_credited_date_from_db(distinct_id, product_id)
                if credited_date and credited_date != 'PLACEHOLDER_DATE':
                    # Create synthetic start event based on credited date
                    start_event = {
                        'event_name': 'RC Trial started',  # Treat fallback as trial start
                        'event_time': credited_date + 'T00:00:00Z',  # Use credited date as start time
                        'revenue_usd': 0,
                        'event_json': json.dumps({'properties': {'product_id': product_id}})
                    }
                    logger.debug(f"Created synthetic start event for {distinct_id} based on credited_date {credited_date}")
            
            if not start_event:
                self.error_counts['no_subscription_start_event'] += 1
                return None
            
            # Calculate credited date
            credited_date = self._calculate_credited_date(start_event)
            if not credited_date:
                self.error_counts['invalid_credited_date'] += 1
                return None
            
            # Calculate current status
            current_status = self._calculate_current_status(events)
            
            # Get price bucket value from database (where Module 01 stored it)
            price_bucket_value = self._get_price_bucket_from_database(distinct_id, product_id)
            
            # Get real conversion metrics from database
            conversion_metrics = self._get_conversion_metrics(distinct_id, product_id)
            
            # Calculate current value and value status
            current_value, value_status = self._calculate_current_value(
                events, start_event, conversion_metrics, price_bucket_value
            )
            
            # CRITICAL: Only return the three fields we're allowed to modify
            # All other fields (conversion rates, accuracy scores, etc.) must be preserved
            record = {
                'distinct_id': distinct_id,
                'product_id': product_id,
                'current_status': current_status,
                'current_value': current_value,
                'value_status': value_status
            }
            
            return record
            
        except Exception as e:
            self.error_counts['failed_user_processing'] += 1
            return None

    def _calculate_credited_date(self, start_event: Dict) -> Optional[str]:
        """
        Calculate the credited date from the start event.
        
        Args:
            start_event: The subscription start event
            
        Returns:
            Credited date in YYYY-MM-DD format or None if invalid
        """
        try:
            credited_date = start_event.get('event_time', '')[:10]  # YYYY-MM-DD format
            if not credited_date or len(credited_date) != 10:
                return None
            return credited_date
        except Exception as e:
            logger.error(f"Error calculating credited date: {e}")
            return None

    def _calculate_current_status(self, events: List[Dict]) -> str:
        """
        Calculate current status based on last chronological event.
        
        This is a direct port of the logic from mixpanel_processing_stage.py
        """
        if not events:
            return 'unknown'
        
        # Sort by event_time to get last event
        sorted_events = sorted(events, key=lambda x: x['event_time'])
        last_event = sorted_events[-1]
        
        event_name = last_event['event_name']
        
        if event_name == 'RC Trial started':
            # Check if this trial is old (>31 days) - if so, it's likely missing follow-up events
            # This should align with Module 6 lifecycle validation (31-day rule)
            try:
                event_date = datetime.strptime(last_event['event_time'][:10], '%Y-%m-%d').date()
                days_since = (datetime.utcnow().date() - event_date).days
                
                if days_since > 31:
                    return 'extended_trial_error'
                else:
                    return 'trial_pending'
            except:
                return 'trial_pending'  # Fallback if date parsing fails
                
        elif event_name == 'RC Trial cancelled':
            # FIX: Check if user converted before cancelling
            # If they converted, treat this as a subscription cancellation, not trial cancellation
            has_converted = any(event['event_name'] == 'RC Trial converted' for event in sorted_events)
            
            if has_converted:
                # Check if this cancellation has negative revenue (refund)
                revenue = last_event.get('revenue_usd', 0)
                is_refund = revenue and float(revenue) < 0
                return 'trial_converted_refunded' if is_refund else 'trial_converted_cancelled'
            else:
                # They cancelled before converting
                return 'trial_cancelled'
        elif event_name == 'RC Trial converted':
            return 'trial_converted'
        elif event_name == 'RC Renewal':
            # RC Renewal needs intelligent lookback to determine what's being renewed
            has_trial_converted = False
            has_initial_purchase = False
            
            for event in reversed(sorted_events[:-1]):  # Skip the last event (current RC Renewal)
                if event['event_name'] == 'RC Trial converted':
                    has_trial_converted = True
                    break
                elif event['event_name'] == 'RC Initial purchase':
                    has_initial_purchase = True
                    break
            
            # Determine status based on subscription origin
            if has_trial_converted:
                return 'trial_converted'
            elif has_initial_purchase:
                return 'initial_purchase'
            else:
                return 'trial_converted'  # Fallback
        elif event_name == 'RC Initial purchase':
            return 'initial_purchase'
        elif event_name == 'RC Cancellation':
            # Check if this is a refund (negative revenue) or cancellation
            revenue = last_event.get('revenue_usd', 0)
            is_refund = revenue and float(revenue) < 0
            
            # Look back through events to find the nearest meaningful subscription event
            meaningful_event = None
            for event in reversed(sorted_events[:-1]):
                if event['event_name'] in ['RC Initial purchase', 'RC Trial converted', 'RC Renewal', 'RC Trial started']:
                    meaningful_event = event
                    break
            
            if meaningful_event:
                meaningful_event_name = meaningful_event['event_name']
                
                if meaningful_event_name == 'RC Initial purchase':
                    return 'purchase_refunded' if is_refund else 'purchase_cancelled'
                elif meaningful_event_name in ['RC Trial converted', 'RC Renewal']:
                    return 'trial_converted_refunded' if is_refund else 'trial_converted_cancelled'
                elif meaningful_event_name == 'RC Trial started':
                    return 'trial_cancelled'
            
            # Fallback for edge cases
            return 'refunded' if is_refund else 'cancelled'
        else:
            return 'unknown'

    def _calculate_current_value(self, events: List[Dict], start_event: Dict, conversion_metrics: Dict, price_bucket: float) -> Tuple[float, str]:
        """
        Calculate current_value and value_status based on time and events.
        
        Implementation follows exact specifications:
        - Trial Users: 3 phases (0-7 days, 8-37 days, 38+ days)
        - Initial Purchase Users: 2 phases (0-30 days, 31+ days)
        - Uses different refund rates for trial conversions vs initial purchases
        """
        try:
            today = datetime.utcnow().date()
            credited_date = datetime.strptime(start_event['event_time'][:10], '%Y-%m-%d').date()
            days_since = (today - credited_date).days
            
            # Get actual revenue values from events
            actual_revenue_from_trial_conversion = 0.0
            actual_revenue_from_initial_purchase = 0.0
            
            for event in events:
                if event['event_name'] == 'RC Trial converted' and event['revenue_usd']:
                    actual_revenue_from_trial_conversion = abs(float(event['revenue_usd']))
                elif event['event_name'] == 'RC Initial purchase' and event['revenue_usd']:
                    actual_revenue_from_initial_purchase = abs(float(event['revenue_usd']))
            
            # Get rates from conversion metrics - use exact field names
            trial_conversion_rate = float(conversion_metrics.get('trial_conversion_rate', 0.0))
            trial_refund_rate = float(conversion_metrics.get('trial_converted_to_refund_rate', 0.0))
            purchase_refund_rate = float(conversion_metrics.get('initial_purchase_to_refund_rate', 0.0))
            
            # Determine current status for refund/cancellation checks
            current_status = self._calculate_current_status(events)
            
            # Determine scenario and calculate value precisely as specified
            if start_event['event_name'] == 'RC Trial started':
                # TRIAL STARTED USERS - 3 PHASES
                
                if 0 <= days_since <= 7:
                    # Phase 1: Days 0-7 (Trial Pending)
                    current_value = price_bucket * trial_conversion_rate * (1 - trial_refund_rate)
                    value_status = "pending_trial"
                    
                elif 8 <= days_since <= 37:
                    # Phase 2: Days 8-37 (Post-Conversion Pre-Refund)
                    # Extract product_id from start_event to ensure we're checking the right product
                    try:
                        start_event_json = json.loads(start_event['event_json'])
                        target_product_id = start_event_json.get('properties', {}).get('product_id', '')
                    except (json.JSONDecodeError, KeyError):
                        target_product_id = ''
                    
                    # Check if user actually converted (has RC Trial converted event for this product)
                    has_converted = any(
                        event['event_name'] == 'RC Trial converted' and
                        self._get_product_id_from_event(event) == target_product_id
                        for event in events
                    )
                    
                    if has_converted:
                        current_value = actual_revenue_from_trial_conversion * (1 - trial_refund_rate)
                    else:
                        current_value = 0.0  # They didn't convert, so value is $0
                    value_status = "post_conversion_pre_refund"
                    
                else:  # days_since >= 38
                    # Phase 3: Days 38+ (Final Value) - Only check refund/cancel status in final phase
                    if current_status in ['trial_converted_refunded', 'trial_cancelled', 'extended_trial_error']:
                        current_value = 0.0  # Refunded users get $0, trial cancelled users never paid, error users never paid
                    else:
                        current_value = actual_revenue_from_trial_conversion  # trial_converted_cancelled users keep their value (they paid)
                    value_status = "final_value"
                    
            else:
                # INITIAL PURCHASE USERS - 2 PHASES
                
                if 0 <= days_since <= 30:
                    # Phase 1: Days 0-30 (Post-Purchase Pre-Refund)
                    if actual_revenue_from_initial_purchase > 0:
                        current_value = actual_revenue_from_initial_purchase * (1 - purchase_refund_rate)
                    else:
                        current_value = price_bucket * (1 - purchase_refund_rate)
                    value_status = "post_purchase_pre_refund"
                    
                else:  # days_since >= 31
                    # Phase 2: Days 31+ (Final Value) - Only check refund status in final phase
                    if current_status == 'purchase_refunded':
                        current_value = 0.0  # Refunded users get $0 (they got money back)
                    else:
                        current_value = actual_revenue_from_initial_purchase  # Cancelled users keep their value (they paid)
                    value_status = "final_value"
            
            return float(current_value), value_status
            
        except Exception as e:
            self.error_counts['failed_value_calculation'] += 1
            return 0.0, "error"

    def _extract_user_properties(self, distinct_id: str, product_id: str, profile: Dict, start_event: Dict) -> Optional[Dict]:
        """Extract user properties needed for value calculation"""
        try:
            # Parse start event JSON
            event_json = json.loads(start_event['event_json'])
            event_props = event_json.get('properties', {})
            
            # Extract properties
            user_properties = {
                'product_id': product_id,
                'app_store': event_props.get('store', ''),
                'country': profile.get('mp_country_code', ''),
                'region': profile.get('mp_region', ''),
            }
            
            # Calculate price bucket using pricing rules
            event_date = start_event.get('event_time', '')[:10]
            price = self._get_active_price_for_date(product_id, user_properties['country'], event_date)
            
            if price is not None:
                user_properties['price_bucket'] = f"${price:.2f}"
            else:
                # Fallback: try to get from event revenue
                revenue = event_props.get('revenue', start_event.get('revenue_usd', 0))
                if revenue and revenue > 0:
                    user_properties['price_bucket'] = f"${abs(float(revenue)):.2f}"
                else:
                    # Last resort: simple product-based mapping
                    user_properties['price_bucket'] = self._get_price_from_product_id(product_id)
            
            return user_properties
            
        except Exception as e:
            logger.error(f"Failed to extract user properties for {distinct_id}: {e}")
            return None

    def _get_price_from_product_id(self, product_id: str) -> str:
        """Get approximate price from product_id (simplified mapping)"""
        # Common product pricing patterns
        if 'yearly' in product_id.lower():
            if '2' in product_id:
                return '$99.99'
            elif '3' in product_id:
                return '$149.99'
            else:
                return '$79.99'
        elif 'monthly' in product_id.lower():
            if '2' in product_id:
                return '$9.99'
            elif '3' in product_id:
                return '$14.99'
            else:
                return '$7.99'
        else:
            return '$9.99'

    def _get_product_id_from_event(self, event: Dict) -> str:
        """Extract product_id from an event"""
        try:
            event_json = json.loads(event['event_json'])
            return event_json.get('properties', {}).get('product_id', '')
        except (json.JSONDecodeError, KeyError):
            return ''

    def _get_credited_date_from_db(self, distinct_id: str, product_id: str) -> Optional[str]:
        """Get credited_date from database for a specific user-product pair"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT credited_date
                FROM user_product_metrics 
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            row = cursor.fetchone()
            conn.close()
            
            return row[0] if row and row[0] else None
                
        except Exception as e:
            logger.error(f"Error getting credited date for {distinct_id}: {e}")
            return None

    def _get_price_bucket_from_database(self, distinct_id: str, product_id: str) -> float:
        """
        Get price bucket from database where Module 01 stored it.
        Only fallback to calculation if not found in database.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT price_bucket
                FROM user_product_metrics 
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            row = cursor.fetchone()
            conn.close()
            
            if row and row[0] is not None and row[0] > 0:
                return float(row[0])
            else:
                # Fallback: calculate if not in database (for the 7,563 missing records)
                self.error_counts['no_price_bucket_in_database'] += 1
                return 9.99  # Default fallback price
                
        except Exception as e:
            self.error_counts['no_price_bucket_in_database'] += 1
            return 9.99  # Default fallback price

    def _get_conversion_metrics(self, distinct_id: str, product_id: str) -> Dict[str, float]:
        """
        Get real conversion metrics from the database for a specific user-product pair.
        These should have been calculated by the assign_conversion_rates module.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT trial_conversion_rate, trial_converted_to_refund_rate, initial_purchase_to_refund_rate, accuracy_score
                FROM user_product_metrics 
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            row = cursor.fetchone()
            conn.close()
            
            if row and row[0] is not None:
                return {
                    'trial_conversion_rate': float(row[0]),
                    'trial_converted_to_refund_rate': float(row[1]) if row[1] is not None else 0.0,
                    'initial_purchase_to_refund_rate': float(row[2]) if row[2] is not None else 0.0
                }
            else:
                # Fallback to default rates if not found
                self.error_counts['no_conversion_rates_found'] += 1
                return {
                    'trial_conversion_rate': 0.25,  # 25% default trial conversion rate
                    'trial_converted_to_refund_rate': 0.20,  # 20% default refund rate for trial conversions
                    'initial_purchase_to_refund_rate': 0.40   # 40% default refund rate for initial purchases
                }
                
        except Exception as e:
            self.error_counts['no_conversion_rates_found'] += 1
            return {
                'trial_conversion_rate': 0.25,
                'trial_converted_to_refund_rate': 0.20,
                'initial_purchase_to_refund_rate': 0.40
            }

    def _store_user_product_records(self, records: List[Dict]) -> int:
        """Update ONLY the value estimation fields: current_status, current_value, value_status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # CRITICAL: Only update the three value estimation fields, preserve all other data
            update_sql = """
                UPDATE user_product_metrics 
                SET current_status = ?,
                    current_value = ?,
                    value_status = ?,
                    last_updated_ts = datetime('now')
                WHERE distinct_id = ? AND product_id = ?
            """
            
            # Prepare data for update - ONLY the three fields we're allowed to modify
            update_data = []
            for record in records:
                update_data.append((
                    record['current_status'],
                    record['current_value'],
                    record['value_status'],
                    record['distinct_id'],
                    record['product_id']
                ))
            
            cursor.executemany(update_sql, update_data)
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            return rows_affected
            
        except Exception as e:
            logger.error(f"Failed to update user product records: {e}")
            return 0


# Additional utility functions for compatibility with the existing codebase

def calculate_credited_date(start_event: Dict) -> Optional[str]:
    """
    Standalone function to calculate the credited date from a subscription start event.
    
    This function can be imported independently for use in other modules.
    
    Args:
        start_event: Dictionary containing subscription start event data with 'event_time' key
        
    Returns:
        Credited date in YYYY-MM-DD format or None if invalid
        
    Example:
        >>> event = {'event_time': '2024-12-01T10:30:00Z', 'event_name': 'RC Trial started'}
        >>> calculate_credited_date(event)
        '2024-12-01'
    """
    try:
        credited_date = start_event.get('event_time', '')[:10]  # YYYY-MM-DD format
        if not credited_date or len(credited_date) != 10:
            return None
        return credited_date
    except Exception as e:
        logger.error(f"Error calculating credited date: {e}")
        return None


def estimate_values(data: pd.DataFrame, estimation_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Estimate monetary values for data records using the new ValueEstimator.
    
    This function provides compatibility with the existing pipeline interface
    while using the new value estimation logic.
    
    Args:
        data: Input DataFrame with user data
        estimation_config: Configuration for value estimation parameters
        
    Returns:
        DataFrame with estimated values
    """
    logger.info("Estimating values using new ValueEstimator")
    
    try:
        # Initialize the value estimator
        estimator = ValueEstimator()
        
        # Process the attributed users
        result = estimator.process_attributed_users()
        
        if result['success']:
            logger.info(f"Successfully processed {result['users_processed']} users")
            # For compatibility, return the input data with a status column
            processed_data = data.copy()
            processed_data['estimation_status'] = 'completed'
            processed_data['users_processed'] = result['users_processed']
            return processed_data
        else:
            logger.error(f"Value estimation failed: {result['error']}")
            processed_data = data.copy()
            processed_data['estimation_status'] = 'failed'
            processed_data['error'] = result['error']
            return processed_data
            
    except Exception as e:
        logger.error(f"Error in estimate_values: {e}")
        processed_data = data.copy()
        processed_data['estimation_status'] = 'error'
        processed_data['error'] = str(e)
        return processed_data


def calculate_ltv(data: pd.DataFrame, ltv_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Calculate Customer Lifetime Value estimates.
    
    This is a placeholder that would integrate with the ValueEstimator
    in a full implementation.
    
    Args:
        data: Input DataFrame with basic value estimates
        ltv_config: Configuration for LTV calculation
        
    Returns:
        DataFrame with LTV estimates
    """
    logger.info("Calculating Customer Lifetime Value estimates (placeholder)")
    
    processed_data = data.copy()
    # This would integrate with the ValueEstimator for real LTV calculations
    processed_data['ltv_estimate'] = processed_data.get('estimated_value', 0) * ltv_config.get('multiplier', 12)
    
    return processed_data


def apply_value_adjustments(data: pd.DataFrame, adjustment_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply adjustments to estimated values based on various factors.
    
    Args:
        data: Input DataFrame with initial value estimates
        adjustment_config: Configuration for value adjustments
        
    Returns:
        DataFrame with adjusted values
    """
    logger.info("Applying value adjustments")
    
    processed_data = data.copy()
    
    # Apply seasonal adjustment if configured
    if 'seasonal_factor' in adjustment_config:
        processed_data['adjusted_value'] = processed_data.get('estimated_value', 0) * adjustment_config['seasonal_factor']
    else:
        processed_data['adjusted_value'] = processed_data.get('estimated_value', 0)
    
    return processed_data


def get_estimation_configuration() -> Dict[str, Any]:
    """
    Get the value estimation configuration.
    
    Returns:
        Dictionary containing estimation configuration
    """
    return {
        'base_value_multiplier': 100,
        'ltv_multiplier': 12,
        'adjustment_factors': {
            'seasonal_factor': 1.1,
            'market_factor': 1.0,
            'competitive_factor': 0.95
        },
        'bucket_multipliers': {
            'low': 0.8,
            'medium': 1.0,
            'high': 1.5
        }
    }


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 