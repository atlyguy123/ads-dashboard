#!/usr/bin/env python3
"""
Assign Conversion Rates Module - Pre-processing Pipeline (REWRITTEN FOR CORRECTNESS & ROBUSTNESS)

This module calculates and assigns conversion rates to user-product records.
It is designed to be robust, correct, and maintainable, addressing flaws in the previous version.

Core Business Logic:
1.  For each valid user-product pair, a cohort of "similar users" is identified.
2.  Similarity is based on a hierarchy of properties:
    - product_id, price_bucket, store, economic_tier, country, region
3.  A progressive fallback mechanism is used to ensure a cohort of at least 12 users:
    - Level 1 (6 props): If cohort < 12, remove 'region'.
    - Level 2 (5 props): If cohort < 12, remove 'country'.
    - Level 3 (4 props): If cohort < 12, remove 'economic_tier'.
    - Level 4 (3 props): Core properties only.
    - If the cohort is still < 12, default rates are used.
4.  Conversion rates are calculated based on the behavior of the final cohort:
    - Cohort Window: Users are considered for a cohort if their start event's 'credited_date'
      is between 53 and 8 days ago (a 45-day window offset for trial periods).
    - Trial Conversion Rate: Matches specific 'RC Trial started' events to subsequent
      'RC Trial converted' events.
    - Refund Rates: Calculated for conversions/purchases that occurred at least 30 days ago
      to provide a complete observation window.
5.  The calculated rates and an accuracy score are written back to the database.
"""

import os
import sys
import sqlite3
import json
import logging
import signal
import time
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Import timezone utilities for consistent timezone handling
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# --- Configuration & Constants ---

# Configure logging for clear, detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Performance and business logic constants
BATCH_SIZE = 5000
MIN_COHORT_SIZE = 12
DEFAULT_RATES = {
    'trial_conversion_rate': 0.25,
    'trial_converted_to_refund_rate': 0.20,
    'initial_purchase_to_refund_rate': 0.40
}
# Order for progressively removing properties to find a large enough cohort
PROPERTY_REMOVAL_ORDER = ['region', 'country', 'economic_tier']

# Event name constants for clarity and maintainability
EVENT_TRIAL_STARTED = 'RC Trial started'
EVENT_INITIAL_PURCHASE = 'RC Initial purchase'
EVENT_TRIAL_CONVERTED = 'RC Trial converted'
EVENT_TRIAL_CANCELLED = 'RC Trial cancelled'
EVENT_CANCELLATION = 'RC Cancellation'

START_EVENTS = [EVENT_TRIAL_STARTED, EVENT_INITIAL_PURCHASE]
RELEVANT_EVENTS = START_EVENTS + [EVENT_TRIAL_CONVERTED, EVENT_TRIAL_CANCELLED, EVENT_CANCELLATION]

# Timeout handling
TIMEOUT_SECONDS = 1800  # 30 minutes
start_time = None

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    """Handle script timeout signal."""
    elapsed = time.time() - start_time if start_time else 0
    error_msg = f"TIMEOUT: Script execution exceeded the {TIMEOUT_SECONDS} second limit."
    logger.error(error_msg)
    raise TimeoutError(error_msg)

# --- Main Execution Block ---

def main():
    """Main function to orchestrate the conversion rate assignment process."""
    global start_time
    start_time = time.time()
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(TIMEOUT_SECONDS)
    
    logger.info("--- Starting Pre-processing: Assign Conversion Rates ---")
    
    try:
        # Use database_utils to get the correct database path
        db_path = get_database_path('mixpanel_data')
        logger.info(f"Using database at: {db_path}")
        
        if not os.path.exists(db_path):
            logger.error(f"Database not found at {db_path}")
            return False
            
        processor = ConversionRateProcessor(db_path)
        success = processor.process_all_user_products()
        
        signal.alarm(0)  # Disable the timeout alarm
        elapsed = time.time() - start_time
        
        if success:
            logger.info(f"--- Conversion Rate Assignment Completed Successfully in {elapsed:.2f} seconds ---")
            # Print final summary
            processor.print_final_summary()
            return True
        else:
            logger.error(f"--- Conversion Rate Assignment Failed after {elapsed:.2f} seconds ---")
            return False
            
    except TimeoutError as e:
        logger.error(f"A timeout error occurred: {str(e)}")
        return False
    except Exception as e:
        signal.alarm(0)
        elapsed = time.time() - start_time if start_time else 0
        logger.error(f"An unexpected error occurred after {elapsed:.2f} seconds: {str(e)}", exc_info=True)
        return False

# --- Core Processor Class ---

class ConversionRateProcessor:
    """
    Handles the logic for calculating and assigning conversion rates.
    This class uses a robust, in-memory data model to ensure correctness and performance.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # Define separate cohort date windows for trials and purchases
        # Trial cohort window: 53 days ago to 8 days ago
        self.trial_cohort_start_date = (now_in_timezone() - timedelta(days=53)).strftime('%Y-%m-%d')
        self.trial_cohort_end_date = (now_in_timezone() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.trial_refund_cutoff = (now_in_timezone() - timedelta(days=38)).strftime('%Y-%m-%d %H:%M:%S')

        # Purchase cohort window: 45 days ago to 0 days ago (today)
        self.purchase_cohort_start_date = (now_in_timezone() - timedelta(days=45)).strftime('%Y-%m-%d')
        self.purchase_cohort_end_date = now_in_timezone().strftime('%Y-%m-%d')
        self.purchase_refund_cutoff = (now_in_timezone() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Trial Cohort Window (for start events based on credited_date): {self.trial_cohort_start_date} to {self.trial_cohort_end_date}")
        logger.info(f"Purchase Cohort Window: {self.purchase_cohort_start_date} to {self.purchase_cohort_end_date}")
        logger.info(f"Trial Refund Cutoff (trial conversions must be before this time): {self.trial_refund_cutoff}")
        logger.info(f"Purchase Refund Cutoff (purchases must be before this time): {self.purchase_refund_cutoff}")

        # --- NEW In-Memory Data Model ---
        # A single, unified data structure to hold all necessary data, replacing the previous brittle caches.
        
        # 1. Stores properties and a complete, time-sorted event list for every relevant user.
        # Format: {'user_id': {'properties': {...}, 'events': [...]}}
        self.users_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'properties': {}, 'events': []})

        # 2. A lookup for user_product_metrics data, keyed by (distinct_id, product_id).
        # This provides instant access to credited_date, price_bucket, store, etc.
        self.user_product_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        
        # 3. Pre-indexed cohort lookup for fast matching
        # This will store users grouped by their properties for fast cohort matching
        self.cohort_index: Dict[str, List[str]] = defaultdict(list)

        # 4. Summary statistics for final reporting
        self.summary_stats = {
            'total_processed': 0,
            'zero_rate_issues': [],
            'accuracy_distribution': defaultdict(int),
            'rate_distributions': {
                'trial_conversion_rate': [],
                'trial_converted_to_refund_rate': [],
                'initial_purchase_to_refund_rate': []
            }
        }

    def process_all_user_products(self) -> bool:
        """Orchestrates the entire processing pipeline."""
        try:
            # Stage 1: Load all necessary data into the robust in-memory model.
            self._load_data_into_memory()
            
            # Stage 2: Get the list of user-product pairs to process.
            target_pairs = self._get_target_user_product_pairs()
            total_pairs = len(target_pairs)
            logger.info(f"Found {total_pairs} valid user-product pairs to process.")
            if total_pairs == 0:
                logger.warning("No user-product pairs found to process. Exiting.")
                return True

            # Stage 3: Process in batches.
            all_updates = []
            for i in range(0, total_pairs, BATCH_SIZE):
                batch_start_time = time.time()
                batch = target_pairs[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                logger.info(f"Processing Batch {batch_num}: pairs {i+1}-{min(i+BATCH_SIZE, total_pairs)} of {total_pairs}")
                
                updates = self._process_batch(batch)
                all_updates.extend(updates)

                batch_time = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} completed in {batch_time:.2f}s. Progress: {min(i+BATCH_SIZE, total_pairs)}/{total_pairs} ({(min(i+BATCH_SIZE, total_pairs)/total_pairs*100):.1f}%)")

            # Stage 4: Perform a single bulk update to the database.
            if all_updates:
                self._batch_update_metrics(all_updates)
            
            # Stage 5: CLEANUP - Assign default rates to any valid users still missing rates
            logger.info("="*60)
            logger.info("🧹 STARTING CLEANUP: Assigning default rates to missed valid users")
            logger.info("="*60)
            self._assign_default_rates_to_missed_users()
            
            return True
        except Exception as e:
            logger.error(f"Error during user processing: {e}", exc_info=True)
            return False
        finally:
            self.conn.close()
            logger.info("Database connection closed.")

    def _load_data_into_memory(self):
        """
        [EXTRACT & TRANSFORM]
        Loads ALL necessary data from the database into the in-memory data model.
        This is the core fix that eliminates the "60-day bug" and brittle caching.
        """
        logger.info("Loading all required data into memory...")
        cursor = self.conn.cursor()

        # Step 1: Fetch all valid user-product metric records. This defines our universe of users and products.
        cursor.execute("""
            SELECT
                upm.user_product_id, upm.distinct_id, upm.product_id, upm.credited_date, upm.price_bucket, upm.store,
                u.economic_tier, u.country, u.region
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE upm.valid_lifecycle = TRUE AND u.valid_user = TRUE
        """)
        all_user_product_data = cursor.fetchall()
        logger.info(f"Fetched {len(all_user_product_data)} valid user-product records.")

        # Step 2: Populate the user_product_lookup and gather all unique user IDs.
        all_relevant_user_ids = set()
        for row in all_user_product_data:
            key = (row['distinct_id'], row['product_id'])
            self.user_product_lookup[key] = dict(row)
            all_relevant_user_ids.add(row['distinct_id'])
        
        logger.info(f"Identified {len(all_relevant_user_ids)} unique users to load.")
        if not all_relevant_user_ids:
            return # No users to process

        # Step 3: Fetch the COMPLETE event history for all relevant users. NO 60-DAY LIMIT.
        # Using parameter substitution to handle a large number of IDs safely.
        placeholders = ','.join('?' for _ in all_relevant_user_ids)
        query = f"""
            SELECT distinct_id, event_name, event_time, event_json, revenue_usd
            FROM mixpanel_event
            WHERE event_name IN ({','.join('?' for _ in RELEVANT_EVENTS)})
            AND distinct_id IN ({placeholders})
        """
        params = RELEVANT_EVENTS + list(all_relevant_user_ids)
        cursor.execute(query, params)
        all_events = cursor.fetchall()
        logger.info(f"Fetched {len(all_events)} relevant events for all users.")

        # Step 4: Structure the data into the self.users_data dictionary.
        # First, populate user properties from the already-fetched user_product_data.
        # We only need to store properties for each user once.
        processed_users = set()
        for row in all_user_product_data:
            user_id = row['distinct_id']
            if user_id not in processed_users:
                self.users_data[user_id]['properties'] = {
                    'economic_tier': row['economic_tier'],
                    'country': row['country'],
                    'region': row['region']
                }
                processed_users.add(user_id)

        # Then, append all events to the corresponding user.
        for event_row in all_events:
            user_id = event_row['distinct_id']
            try:
                event_data = json.loads(event_row['event_json']) if event_row['event_json'] else {}
                properties = event_data.get('properties', {})
                product_id = properties.get('product_id')
                if product_id:  # Only process events with a product_id
                    self.users_data[user_id]['events'].append({
                        'event_name': event_row['event_name'],
                        'event_time': event_row['event_time'],
                        'product_id': product_id,
                        'revenue_usd': event_row['revenue_usd'] or 0
                    })
            except (json.JSONDecodeError, AttributeError):
                continue # Skip malformed event JSON

        # Finally, sort events for each user by time. This is CRITICAL for correct calculations.
        for user_id in self.users_data:
            self.users_data[user_id]['events'].sort(key=lambda x: x['event_time'])

        # Build cohort index for fast matching
        self._build_cohort_index()

        logger.info("In-memory data model constructed successfully.")

    def _build_cohort_index(self):
        """Build an index for fast cohort matching."""
        logger.info("Building cohort index for fast matching...")
        
        # For each user-product combination, create index keys for different property combinations
        for (distinct_id, product_id), props in self.user_product_lookup.items():
            # Check if this user-product is in the trial cohort window (used for cohort matching)
            credited_date = props['credited_date']
            if not (self.trial_cohort_start_date <= credited_date <= self.trial_cohort_end_date):
                continue
                
            # Create keys for different property levels
            base_props = [product_id, props.get('price_bucket', ''), props.get('store', '')]
            all_props = base_props + [props.get('economic_tier', ''), props.get('country', ''), props.get('region', '')]
            
            # Level 1: All 6 properties (very_high accuracy)
            key_6 = '|'.join(str(p) for p in all_props)
            self.cohort_index[f"level_6:{key_6}"].append(distinct_id)
            
            # Level 2: 5 properties (high accuracy) - remove region
            key_5 = '|'.join(str(p) for p in all_props[:-1])
            self.cohort_index[f"level_5:{key_5}"].append(distinct_id)
            
            # Level 3: 4 properties (medium accuracy) - remove region and country
            key_4 = '|'.join(str(p) for p in all_props[:-2])
            self.cohort_index[f"level_4:{key_4}"].append(distinct_id)
            
            # Level 4: 3 properties (low accuracy) - core properties only
            key_3 = '|'.join(str(p) for p in base_props)
            self.cohort_index[f"level_3:{key_3}"].append(distinct_id)
        
        logger.info(f"Cohort index built with {len(self.cohort_index)} property combinations.")

    def _get_target_user_product_pairs(self) -> List[Dict]:
        """Gets the list of user-product pairs that need rate assignments."""
        # We can just return the data we already loaded into the lookup.
        return [
            {
                'user_product_id': val['user_product_id'], # Assuming user_product_id is in the table
                'distinct_id': key[0],
                'product_id': key[1]
            }
            for key, val in self.user_product_lookup.items()
        ]

    def _process_batch(self, batch: List[Dict]) -> List[Tuple]:
        """Processes a single batch of user-product pairs and returns database update tuples."""
        updates = []
        for pair in batch:
            distinct_id = pair['distinct_id']
            product_id = pair['product_id']
            
            # Get the properties for the target user-product pair from our lookup.
            target_props = self.user_product_lookup.get((distinct_id, product_id))
            if not target_props:
                continue # Should not happen if data is loaded correctly, but a safe check.

            # Step 1: Find the best possible cohort using the progressive fallback logic.
            cohort_user_ids, accuracy = self._find_matching_cohort(target_props)
            
            # Step 2: Calculate rates or use defaults.
            if len(cohort_user_ids) >= MIN_COHORT_SIZE:
                rates = self._calculate_rates_from_cohort(cohort_user_ids, product_id)
                final_accuracy = accuracy
            else:
                rates = DEFAULT_RATES
                final_accuracy = 'default'
            
            # Track statistics for summary
            self.summary_stats['total_processed'] += 1
            self.summary_stats['accuracy_distribution'][final_accuracy] += 1
            for rate_name, rate_value in rates.items():
                self.summary_stats['rate_distributions'][rate_name].append(rate_value)
            
            # Step 3: Prepare the update tuple.
            updates.append((
                rates['trial_conversion_rate'],
                rates['trial_converted_to_refund_rate'],
                rates['initial_purchase_to_refund_rate'],
                final_accuracy,
                pair['user_product_id']
            ))
        return updates

    def _find_matching_cohort(self, target_props: Dict) -> Tuple[List[str], str]:
        """
        Finds the best cohort for a target user using the pre-built index for fast lookup.
        """
        # Create the property keys for lookup
        product_id = target_props['product_id']
        base_props = [product_id, target_props.get('price_bucket', ''), target_props.get('store', '')]
        all_props = base_props + [target_props.get('economic_tier', ''), target_props.get('country', ''), target_props.get('region', '')]
        
        # Try each level, starting from most specific
        accuracy_levels = ['very_high', 'high', 'medium', 'low']
        
        # Level 1: All 6 properties (very_high)
        key_6 = '|'.join(str(p) for p in all_props)
        cohort = self.cohort_index.get(f"level_6:{key_6}", [])
        if len(cohort) >= MIN_COHORT_SIZE:
            return cohort, accuracy_levels[0]
        
        # Level 2: 5 properties (high) - remove region
        key_5 = '|'.join(str(p) for p in all_props[:-1])
        cohort = self.cohort_index.get(f"level_5:{key_5}", [])
        if len(cohort) >= MIN_COHORT_SIZE:
            return cohort, accuracy_levels[1]
        
        # Level 3: 4 properties (medium) - remove region and country
        key_4 = '|'.join(str(p) for p in all_props[:-2])
        cohort = self.cohort_index.get(f"level_4:{key_4}", [])
        if len(cohort) >= MIN_COHORT_SIZE:
            return cohort, accuracy_levels[2]
        
        # Level 4: 3 properties (low) - core properties only
        key_3 = '|'.join(str(p) for p in base_props)
        cohort = self.cohort_index.get(f"level_3:{key_3}", [])
        if len(cohort) >= MIN_COHORT_SIZE:
            return cohort, accuracy_levels[3]
                
        return [], 'default'

    def _calculate_rates_from_cohort(self, cohort_user_ids: List[str], product_id: str) -> Dict[str, float]:
        """
        Calculates conversion and refund rates from a given cohort of users.
        This uses the complete, time-sorted event lists for precise, unambiguous calculations.
        """
        # Counters for rate calculations
        trials_in_window = 0
        matched_conversions = 0
        purchases_eligible_for_refund = 0
        purchase_refunds = 0
        conversions_eligible_for_refund = 0
        conversion_refunds = 0

        for user_id in cohort_user_ids:
            user_events = self.users_data[user_id]['events']
            
            # Iterate through events to find trial starts and initial purchases
            for i, start_event in enumerate(user_events):
                if start_event['product_id'] != product_id:
                    continue

                event_name = start_event['event_name']
                event_time = start_event['event_time']
                
                # --- A. Trial Conversion Rate Logic ---
                if event_name == EVENT_TRIAL_STARTED:
                    # Confirm this specific trial start is in our trial cohort window using its credited_date
                    user_product_info = self.user_product_lookup.get((user_id, product_id))
                    if user_product_info and (self.trial_cohort_start_date <= user_product_info['credited_date'] <= self.trial_cohort_end_date):
                        trials_in_window += 1
                        # Look FORWARD in the event list for the first conversion
                        for subsequent_event in user_events[i+1:]:
                            if (subsequent_event['product_id'] == product_id and
                                subsequent_event['event_name'] == EVENT_TRIAL_CONVERTED):
                                matched_conversions += 1
                                break # Count only the first conversion per trial

                # --- B. Refund Rate Logic ---
                if event_name == EVENT_TRIAL_CONVERTED:
                    if event_time < self.trial_refund_cutoff:
                        conversions_eligible_for_refund += 1
                        # Look FORWARD 30 days for a cancellation
                        refund_window_end = (datetime.fromisoformat(event_time) + timedelta(days=30)).isoformat()
                        for subsequent_event in user_events[i+1:]:
                            if subsequent_event['event_time'] > refund_window_end:
                                break # Moved past the 30-day window
                            if (subsequent_event['product_id'] == product_id and
                                subsequent_event['event_name'] == EVENT_CANCELLATION and
                                subsequent_event['revenue_usd'] < 0):
                                conversion_refunds += 1
                                break

                if event_name == EVENT_INITIAL_PURCHASE:
                    if event_time < self.purchase_refund_cutoff:
                        purchases_eligible_for_refund += 1
                        # Look FORWARD 30 days for a cancellation
                        refund_window_end = (datetime.fromisoformat(event_time) + timedelta(days=30)).isoformat()
                        for subsequent_event in user_events[i+1:]:
                            if subsequent_event['event_time'] > refund_window_end:
                                break
                            if (subsequent_event['product_id'] == product_id and
                                subsequent_event['event_name'] == EVENT_CANCELLATION and
                                subsequent_event['revenue_usd'] < 0):
                                purchase_refunds += 1
                                break

        # Calculate final rates, avoiding division by zero
        rates = {
            'trial_conversion_rate': matched_conversions / trials_in_window if trials_in_window > 0 else 0,
            'trial_converted_to_refund_rate': conversion_refunds / conversions_eligible_for_refund if conversions_eligible_for_refund > 0 else 0,
            'initial_purchase_to_refund_rate': purchase_refunds / purchases_eligible_for_refund if purchases_eligible_for_refund > 0 else 0
        }

        # Check for zero rates and collect summary info instead of verbose logging
        if all(rate == 0 for rate in rates.values()) and len(cohort_user_ids) >= MIN_COHORT_SIZE:
            self._collect_zero_rate_summary(product_id, len(cohort_user_ids), {
                'trials_in_window': trials_in_window,
                'matched_conversions': matched_conversions,
                'purchases_eligible_for_refund': purchases_eligible_for_refund,
                'purchase_refunds': purchase_refunds,
                'conversions_eligible_for_refund': conversions_eligible_for_refund,
                'conversion_refunds': conversion_refunds
            })

        return rates

    def _collect_zero_rate_summary(self, product_id: str, cohort_size: int, counters: Dict) -> None:
        """
        Collect summary information about zero rate issues for final reporting.
        """
        issue_summary = {
            'product_id': product_id,
            'cohort_size': cohort_size,
            'trials_in_window': counters['trials_in_window'],
            'matched_conversions': counters['matched_conversions'],
            'conversions_eligible_for_refund': counters['conversions_eligible_for_refund'],
            'conversion_refunds': counters['conversion_refunds'],
            'purchases_eligible_for_refund': counters['purchases_eligible_for_refund'],
            'purchase_refunds': counters['purchase_refunds']
        }
        
        # Determine the primary issue
        if counters['trials_in_window'] == 0:
            issue_summary['primary_issue'] = 'no_trials_in_window'
        elif counters['matched_conversions'] == 0:
            issue_summary['primary_issue'] = 'no_conversions_matched'
        elif counters['conversions_eligible_for_refund'] == 0 and counters['purchases_eligible_for_refund'] == 0:
            issue_summary['primary_issue'] = 'no_events_eligible_for_refund'
        else:
            issue_summary['primary_issue'] = 'other'
        
        self.summary_stats['zero_rate_issues'].append(issue_summary)

    def print_final_summary(self) -> None:
        """Print a comprehensive summary at the end of processing."""
        logger.info("="*60)
        logger.info("📊 CONVERSION RATE ASSIGNMENT SUMMARY")
        logger.info("="*60)
        
        # Basic statistics
        total = self.summary_stats['total_processed']
        logger.info(f"Total user-product pairs processed: {total:,}")
        
        # Accuracy distribution
        logger.info(f"\n📈 Accuracy Distribution:")
        for accuracy, count in sorted(self.summary_stats['accuracy_distribution'].items()):
            percentage = (count / total * 100) if total > 0 else 0
            logger.info(f"   {accuracy:>12}: {count:>6,} ({percentage:5.1f}%)")
        
        # Rate statistics
        logger.info(f"\n📊 Rate Statistics:")
        for rate_name, values in self.summary_stats['rate_distributions'].items():
            if values:
                avg_rate = sum(values) / len(values)
                non_zero = [v for v in values if v > 0]
                non_zero_pct = (len(non_zero) / len(values) * 100) if values else 0
                logger.info(f"   {rate_name}:")
                logger.info(f"      Average: {avg_rate:.3f} | Non-zero: {len(non_zero):,}/{len(values):,} ({non_zero_pct:.1f}%)")
        
        # Zero rate issues summary
        zero_issues = self.summary_stats['zero_rate_issues']
        if zero_issues:
            logger.info(f"\n⚠️  Zero Rate Issues Summary ({len(zero_issues)} cases):")
            
            # Group by primary issue
            issue_groups = defaultdict(list)
            for issue in zero_issues:
                issue_groups[issue['primary_issue']].append(issue)
            
            for issue_type, issues in issue_groups.items():
                logger.info(f"   {issue_type.replace('_', ' ').title()}: {len(issues)} cases")
                
                # Show a few examples
                if issue_type == 'no_trials_in_window':
                    sample_products = [issue['product_id'] for issue in issues[:3]]
                    logger.info(f"      Example products: {', '.join(sample_products[:3])}")
                    logger.info(f"      Cohort sizes: {[issue['cohort_size'] for issue in issues[:3]]}")
                
                elif issue_type == 'no_conversions_matched':
                    trials_found = sum(issue['trials_in_window'] for issue in issues)
                    logger.info(f"      Total trials found: {trials_found}, but no conversions matched")
                
                elif issue_type == 'no_events_eligible_for_refund':
                    logger.info(f"      Events found but none eligible for refund analysis")
        
        else:
            logger.info(f"\n✅ No zero rate issues detected!")
        
        logger.info("="*60)

    def _batch_update_metrics(self, updates: List[Tuple]) -> None:
        """Performs a single, efficient bulk update to the database."""
        logger.info(f"Performing bulk update of {len(updates)} records in the database...")
        cursor = self.conn.cursor()
        try:
            cursor.executemany("""
                UPDATE user_product_metrics 
                SET 
                    trial_conversion_rate = ?,
                    trial_converted_to_refund_rate = ?,
                    initial_purchase_to_refund_rate = ?,
                    accuracy_score = ?
                WHERE user_product_id = ?
            """, updates)
            self.conn.commit()
            logger.info(f"Successfully updated {cursor.rowcount} records.")
        except sqlite3.Error as e:
            logger.error(f"Database error during batch update: {e}")
            self.conn.rollback()

    def _assign_default_rates_to_missed_users(self) -> None:
        """
        CLEANUP PHASE: Assign default conversion rates to valid users who were missed.
        This ensures ALL valid users have conversion rates, regardless of lifecycle validity.
        """
        cursor = self.conn.cursor()
        
        # Find valid users without conversion rates
        cursor.execute("""
            SELECT 
                upm.user_product_id,
                upm.distinct_id,
                upm.product_id,
                upm.valid_lifecycle
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.valid_user = TRUE 
            AND upm.trial_conversion_rate IS NULL
            ORDER BY upm.user_product_id
        """)
        
        missed_users = cursor.fetchall()
        
        if not missed_users:
            logger.info("🎉 No missed users found! All valid users already have conversion rates.")
            return
        
        logger.info(f"🔍 Found {len(missed_users):,} valid users without conversion rates")
        
        # Analyze characteristics
        cursor.execute("""
            SELECT 
                upm.valid_lifecycle,
                COUNT(*) as count
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.valid_user = TRUE 
            AND upm.trial_conversion_rate IS NULL
            GROUP BY upm.valid_lifecycle
        """)
        
        lifecycle_breakdown = cursor.fetchall()
        logger.info(f"📊 Lifecycle validity breakdown:")
        for row in lifecycle_breakdown:
            logger.info(f"   valid_lifecycle={row['valid_lifecycle']}: {row['count']:,} users")
        
        # Assign default rates
        accuracy_score = 'invalid_lifecycle_autoset_to_default'
        logger.info(f"🔧 Assigning default rates with accuracy_score: '{accuracy_score}'")
        logger.info(f"   Default rates: {DEFAULT_RATES}")
        
        try:
            # Prepare batch update
            updates = []
            for user in missed_users:
                updates.append((
                    DEFAULT_RATES['trial_conversion_rate'],
                    DEFAULT_RATES['trial_converted_to_refund_rate'], 
                    DEFAULT_RATES['initial_purchase_to_refund_rate'],
                    accuracy_score,
                    user['user_product_id']
                ))
            
            # Execute batch update
            cursor.executemany("""
                UPDATE user_product_metrics 
                SET 
                    trial_conversion_rate = ?,
                    trial_converted_to_refund_rate = ?,
                    initial_purchase_to_refund_rate = ?,
                    accuracy_score = ?
                WHERE user_product_id = ?
            """, updates)
            
            self.conn.commit()
            logger.info(f"✅ Successfully assigned default rates to {cursor.rowcount:,} missed users")
            
            # Final verification
            cursor.execute("""
                SELECT COUNT(*) as remaining
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                AND upm.trial_conversion_rate IS NULL
            """)
            
            remaining = cursor.fetchone()['remaining']
            if remaining == 0:
                logger.info(f"🎉 SUCCESS! All valid users now have conversion rates!")
            else:
                logger.warning(f"⚠️  Still have {remaining:,} valid users without conversion rates!")
                
        except sqlite3.Error as e:
            logger.error(f"❌ Database error during cleanup: {e}")
            self.conn.rollback()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)