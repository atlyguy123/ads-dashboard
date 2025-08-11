#!/usr/bin/env python3
"""
Module 8: Compute Daily Mixpanel Metrics

This module pre-computes daily metrics for every advertising entity (campaign, adset, ad)
and date combination. It calculates trial users, purchase users, estimated revenue,
and maintains user lists for detailed analysis. This enables lightning-fast dashboard
queries by eliminating complex runtime calculations.

Key Features:
- Pre-computes 6 core metrics for every entity ID and date
- Handles proper user deduplication (COUNT DISTINCT logic)
- Stores user lists as JSON for detailed drill-down analysis
- Calculates estimated revenue using current_value from user_product_metrics
- Includes data quality scoring and validation
- Optimized for dashboard performance and reliability

Dependencies: Requires mixpanel_user, mixpanel_event, user_product_metrics tables
Outputs: Populated daily_mixpanel_metrics table
"""

import sqlite3
import logging
import json
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path
import sys
from datetime import datetime, date, timedelta
from collections import defaultdict

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Note: Country codes are identical between Meta and Mixpanel, no mapping needed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Calculator integration - implement essential calculation methods directly
# This avoids complex import dependencies while maintaining accuracy

class DirectCalculators:
    """Direct implementation of essential calculator methods for pre-computation"""
    
    @staticmethod
    def safe_divide(numerator, denominator, default=0.0):
        """Safe division with default value"""
        try:
            return float(numerator) / float(denominator) if float(denominator) != 0 else default
        except (ValueError, TypeError, ZeroDivisionError):
            return default
    
    @staticmethod
    def calculate_trial_accuracy_ratio(mixpanel_trials, meta_trials):
        """Calculate trial accuracy ratio: (Mixpanel trials / Meta trials) * 100"""
        return DirectCalculators.safe_divide(mixpanel_trials, meta_trials) * 100
    
    @staticmethod
    def calculate_purchase_accuracy_ratio(mixpanel_purchases, meta_purchases):
        """Calculate purchase accuracy ratio: (Mixpanel purchases / Meta purchases) * 100"""
        return DirectCalculators.safe_divide(mixpanel_purchases, meta_purchases) * 100
    
    @staticmethod
    def calculate_estimated_roas(estimated_revenue, spend):
        """Calculate ROAS: revenue / spend"""
        return DirectCalculators.safe_divide(estimated_revenue, spend)
    
    @staticmethod
    def calculate_cost_per_trial(spend, trials):
        """Calculate cost per trial: spend / trials"""
        return DirectCalculators.safe_divide(spend, trials)
    
    @staticmethod
    def calculate_cost_per_purchase(spend, purchases):
        """Calculate cost per purchase: spend / purchases"""
        return DirectCalculators.safe_divide(spend, purchases)
    
    @staticmethod
    def calculate_click_to_trial_rate(trials, clicks):
        """Calculate click to trial rate: (trials / clicks) * 100"""
        return DirectCalculators.safe_divide(trials, clicks) * 100

logger.info("✅ Direct calculator methods loaded successfully")

# Configuration - Use centralized database path discovery
DATABASE_PATH = get_database_path('mixpanel_data')
META_DATABASE_PATH = get_database_path('meta_analytics')

class DailyMetricsProcessor:
    """Processes and computes daily Mixpanel metrics for all entities with Meta integration"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        
        # Add Meta database connection
        self.meta_conn = sqlite3.connect(META_DATABASE_PATH)
        self.meta_cursor = self.meta_conn.cursor()
        
        # Note: Simple mapping approach avoids complex dependencies
        
        self.stats = {
            'date_range_start': None,
            'date_range_end': None,
            'total_dates_processed': 0,
            'campaign_metrics_created': 0,
            'adset_metrics_created': 0,
            'ad_metrics_created': 0,
            'entities_with_trials': 0,
            'total_trial_events': 0,
            'entities_with_purchases': 0,
            'total_purchase_events': 0,
            'total_estimated_revenue': 0.0
        }
    
    def get_data_date_range(self) -> Tuple[date, date]:
        """
        Determine the date range of available event data
        
        Returns:
            Tuple of (start_date, end_date)
        """
        logger.info("Determining data date range...")
        
        self.cursor.execute("""
        SELECT 
            MIN(DATE(e.event_time)) as min_date,
            MAX(DATE(e.event_time)) as max_date
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE event_name IN ('RC Trial started', 'RC Initial purchase')
            AND u.has_abi_attribution = TRUE
        """)
        
        result = self.cursor.fetchone()
        if not result or not result[0]:
            raise RuntimeError("No attributed event data found for trial/purchase events")
        
        start_date = datetime.strptime(result[0], '%Y-%m-%d').date()
        end_date = datetime.strptime(result[1], '%Y-%m-%d').date()
        
        logger.info(f"Data range: {start_date} to {end_date}")
        return start_date, end_date
    
    def compute_daily_metrics_for_entity_type(self, entity_type: str, attribution_column: str) -> int:
        """
        Compute daily metrics for a specific entity type with user deduplication
        
        CRITICAL DEDUPLICATION LOGIC:
        - If a user has multiple trial/purchase events across different days, 
          only count them on the LATEST day within the analysis period
        - Remove them from earlier days to prevent double-counting
        - This ensures accurate user counts and proper funnel analysis
        
        Args:
            entity_type: 'campaign', 'adset', or 'ad'
            attribution_column: Database column name (e.g., 'abi_campaign_id')
            
        Returns:
            Number of metrics records created
        """
        logger.info(f"Computing daily metrics for {entity_type} entities with user deduplication...")
        
        start_date, end_date = self.get_data_date_range()
        
        # Step 1: Get ALL trial events across the entire date range with latest event date per user
        logger.info(f"Analyzing trial events with deduplication logic...")
        trial_dedup_query = f"""
        SELECT 
            u.{attribution_column} as entity_id,
            u.distinct_id,
            MAX(DATE(e.event_time)) as latest_trial_date
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
          AND u.{attribution_column} IS NOT NULL
          AND u.has_abi_attribution = TRUE
        GROUP BY u.{attribution_column}, u.distinct_id
        """
        
        self.cursor.execute(trial_dedup_query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        trial_dedup_results = self.cursor.fetchall()
        
        # Step 2: Get ALL purchase events with deduplication
        logger.info(f"Analyzing purchase events with deduplication logic...")
        purchase_dedup_query = f"""
        SELECT 
            u.{attribution_column} as entity_id,
            u.distinct_id,
            MAX(DATE(e.event_time)) as latest_purchase_date
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.event_name = 'RC Initial purchase'
          AND DATE(e.event_time) BETWEEN ? AND ?
          AND u.{attribution_column} IS NOT NULL
          AND u.has_abi_attribution = TRUE
        GROUP BY u.{attribution_column}, u.distinct_id
        """
        
        self.cursor.execute(purchase_dedup_query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        purchase_dedup_results = self.cursor.fetchall()
        
        # Step 3: Build deduplicated daily metrics structure
        logger.info(f"Building deduplicated daily metrics structure...")
        
        # Group by entity_id and date for trials
        trial_by_entity_date = defaultdict(lambda: defaultdict(list))
        for entity_id, distinct_id, latest_date in trial_dedup_results:
            trial_by_entity_date[entity_id][latest_date].append(distinct_id)
        
        # Group by entity_id and date for purchases  
        purchase_by_entity_date = defaultdict(lambda: defaultdict(list))
        for entity_id, distinct_id, latest_date in purchase_dedup_results:
            purchase_by_entity_date[entity_id][latest_date].append(distinct_id)
        
        # Step 4: Process each date and create metrics
        metrics_created = 0
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logger.debug(f"Processing deduplicated {entity_type} metrics for {date_str}")
            
            # Combine results by entity_id for this specific date
            entity_metrics = defaultdict(lambda: {
                'trial_users_count': 0,
                'trial_users_list': [],
                'purchase_users_count': 0,
                'purchase_users_list': [],
                'estimated_revenue_usd': 0.0
            })
            
            # Process trial data for this date
            for entity_id, date_users in trial_by_entity_date.items():
                if date_str in date_users:
                    users_for_date = date_users[date_str]
                    entity_metrics[entity_id]['trial_users_count'] = len(users_for_date)
                    entity_metrics[entity_id]['trial_users_list'] = users_for_date
            
            # Process purchase data for this date
            for entity_id, date_users in purchase_by_entity_date.items():
                if date_str in date_users:
                    users_for_date = date_users[date_str]
                    entity_metrics[entity_id]['purchase_users_count'] = len(users_for_date)
                    entity_metrics[entity_id]['purchase_users_list'] = users_for_date
            
            # Calculate estimated revenue for entities with trial users
            for entity_id, metrics in entity_metrics.items():
                if metrics['trial_users_list']:
                    revenue = self.calculate_estimated_revenue(metrics['trial_users_list'])
                    metrics['estimated_revenue_usd'] = revenue
            
            # Insert metrics into database (only if there's data for this date)
            if entity_metrics:
                self.insert_daily_metrics(entity_type, current_date, entity_metrics)
                metrics_created += len(entity_metrics)
            
            current_date += timedelta(days=1)
        
        logger.info(f"✅ Created {metrics_created} deduplicated {entity_type} daily metrics")
        return metrics_created
    
    def calculate_estimated_revenue(self, user_list: List[str]) -> float:
        """
        Calculate estimated revenue for a list of users
        
        Args:
            user_list: List of distinct_ids
            
        Returns:
            Total estimated revenue (sum of current_value)
        """
        if not user_list:
            return 0.0
        
        # Create placeholders for IN clause
        placeholders = ','.join(['?' for _ in user_list])
        
        query = f"""
        SELECT COALESCE(SUM(current_value), 0.0) as total_revenue
        FROM user_product_metrics
        WHERE distinct_id IN ({placeholders})
        """
        
        self.cursor.execute(query, user_list)
        result = self.cursor.fetchone()
        return float(result[0]) if result and result[0] else 0.0
    
    def insert_daily_metrics(self, entity_type: str, date_obj: date, entity_metrics: Dict[str, Dict]):
        """
        Insert comprehensive daily metrics for all entities of a given type and date.
        Now includes Meta advertising data and calculated performance metrics.
        
        Args:
            entity_type: 'campaign', 'adset', or 'ad'
            date_obj: Date object
            entity_metrics: Dictionary of entity_id -> metrics
        """
        # Collect Meta advertising data for this date and entity type
        start_date = end_date = date_obj
        meta_data = self.collect_meta_advertising_data(start_date, end_date)
        
        # Prepare bulk insert data
        bulk_insert_data = []
        current_time = datetime.now()
        date_str = date_obj.strftime('%Y-%m-%d')
        
        for entity_id, metrics in entity_metrics.items():
            # Get Meta data for this entity/date combination
            meta_key = (entity_type, entity_id, date_str)
            meta_metrics = meta_data.get(meta_key, {
                'meta_spend': 0, 'meta_impressions': 0, 'meta_clicks': 0,
                'meta_trial_count': 0, 'meta_purchase_count': 0
            })
            
            # Calculate user lifecycle lists
            user_lifecycle = self.calculate_user_lifecycle_data(metrics, date_obj)
            
            # Calculate comprehensive metrics using both Meta and Mixpanel data
            comprehensive_metrics = self.calculate_comprehensive_metrics(meta_metrics, metrics, user_lifecycle)
            
            # Convert user lists to JSON
            trial_users_json = json.dumps(metrics['trial_users_list'])
            purchase_users_json = json.dumps(metrics['purchase_users_list'])
            
            # Prepare complete record for bulk insert
            record = (
                date_str, entity_type, entity_id,
                # Basic Mixpanel metrics
                metrics['trial_users_count'], trial_users_json,
                metrics['purchase_users_count'], purchase_users_json,
                metrics['estimated_revenue_usd'],
                # Meta advertising metrics  
                meta_metrics['meta_spend'], meta_metrics['meta_impressions'], 
                meta_metrics['meta_clicks'], meta_metrics['meta_trial_count'], 
                meta_metrics['meta_purchase_count'],
                # User lifecycle tracking (from specification calculations)
                json.dumps(user_lifecycle['post_trial_user_ids']), 
                json.dumps(user_lifecycle['converted_user_ids']),
                json.dumps(user_lifecycle['trial_refund_user_ids']), 
                json.dumps(user_lifecycle['purchase_refund_user_ids']),
                # Conversion rate metrics (from specification calculations)
                comprehensive_metrics['trial_conversion_rate_estimated'], 
                comprehensive_metrics['trial_conversion_rate_actual'],
                comprehensive_metrics['trial_refund_rate_estimated'], 
                comprehensive_metrics['trial_refund_rate_actual'], 
                comprehensive_metrics['purchase_refund_rate_estimated'], 
                comprehensive_metrics['purchase_refund_rate_actual'],
                # Revenue metrics
                comprehensive_metrics['actual_revenue_usd'], comprehensive_metrics['actual_refunds_usd'],
                comprehensive_metrics['net_actual_revenue_usd'], comprehensive_metrics['adjusted_estimated_revenue_usd'],
                # Performance metrics
                comprehensive_metrics['profit_usd'], comprehensive_metrics['estimated_roas'],
                comprehensive_metrics['trial_accuracy_ratio'], comprehensive_metrics['purchase_accuracy_ratio'],
                # Cost metrics
                comprehensive_metrics['mixpanel_cost_per_trial'], comprehensive_metrics['mixpanel_cost_per_purchase'],
                comprehensive_metrics['meta_cost_per_trial'], comprehensive_metrics['meta_cost_per_purchase'],
                comprehensive_metrics['click_to_trial_rate'],
                # Metadata
                current_time, 1.0  # computed_at, data_quality_score
            )
            
            bulk_insert_data.append(record)
        
        # Bulk insert with comprehensive schema - using correct field names
        insert_query = """
        INSERT OR REPLACE INTO daily_mixpanel_metrics 
        (date, entity_type, entity_id, trial_users_count, trial_user_ids, 
         purchase_users_count, purchase_user_ids, estimated_revenue_usd,
         meta_spend, meta_impressions, meta_clicks, meta_trial_count, meta_purchase_count,
         post_trial_user_ids, converted_user_ids, trial_refund_user_ids, purchase_refund_user_ids,
         trial_conversion_rate_estimated, trial_conversion_rate_actual, trial_refund_rate_estimated, 
         trial_refund_rate_actual, purchase_refund_rate_estimated, purchase_refund_rate_actual,
         actual_revenue_usd, actual_refunds_usd, net_actual_revenue_usd, adjusted_estimated_revenue_usd,
         profit_usd, estimated_roas, trial_accuracy_ratio, purchase_accuracy_ratio,
         mixpanel_cost_per_trial, mixpanel_cost_per_purchase, meta_cost_per_trial, meta_cost_per_purchase,
         click_to_trial_rate, computed_at, data_quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Execute bulk insert
        self.cursor.executemany(insert_query, bulk_insert_data)
        logger.info(f"✅ Bulk inserted {len(bulk_insert_data)} comprehensive {entity_type} metrics for {date_str}")
    
    def insert_breakdown_metrics(self, entity_type: str, date_obj: date, breakdown_type: str, meta_breakdown_data: Dict, mixpanel_breakdown_data: Dict):
        """
        Insert breakdown metrics for all entity-date-breakdown combinations according to specification
        """
        import json
        from datetime import datetime
        
        date_str = date_obj.strftime('%Y-%m-%d')
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        bulk_insert_data = []
        
        logger.info(f"🔍 Processing {breakdown_type} breakdown metrics for {entity_type} entities on {date_str}")
        
        # Get all unique breakdown combinations from both Meta and Mixpanel data
        all_keys = set(meta_breakdown_data.keys()) | set(mixpanel_breakdown_data.keys())
        
        for key in all_keys:
            # Key format: (entity_type, entity_id, date_str, breakdown_value)
            breakdown_entity_type, entity_id, breakdown_date, breakdown_value = key
            
            # Skip if not matching current processing parameters
            if breakdown_entity_type != entity_type or breakdown_date != date_str:
                continue
            
            # Get Meta breakdown metrics for this combination
            meta_metrics = meta_breakdown_data.get(key, {
                'meta_spend': 0, 'meta_impressions': 0, 'meta_clicks': 0,
                'meta_trial_count': 0, 'meta_purchase_count': 0
            })
            
            # Get Mixpanel breakdown metrics for this combination
            mixpanel_metrics = mixpanel_breakdown_data.get(key, {
                'trial_users_count': 0, 'trial_users_list': [],
                'purchase_users_count': 0, 'purchase_users_list': [],
                'estimated_revenue_usd': 0
            })
            
            # Calculate user lifecycle data for this breakdown
            breakdown_user_lifecycle = self.calculate_user_lifecycle_data_breakdown(
                mixpanel_metrics, date_obj, breakdown_type, breakdown_value
            )
            
            # Calculate comprehensive metrics for this breakdown
            breakdown_comprehensive_metrics = self.calculate_comprehensive_metrics(
                meta_metrics, mixpanel_metrics, breakdown_user_lifecycle
            )
            
            # Convert user lists to JSON
            trial_users_json = json.dumps(mixpanel_metrics['trial_users_list'])
            purchase_users_json = json.dumps(mixpanel_metrics['purchase_users_list'])
            
            # Prepare complete breakdown record for bulk insert
            record = (
                entity_type, entity_id, date_str, breakdown_type, breakdown_value,
                # Meta advertising metrics
                meta_metrics['meta_spend'], meta_metrics['meta_impressions'], 
                meta_metrics['meta_clicks'], meta_metrics['meta_trial_count'], 
                meta_metrics['meta_purchase_count'],
                # Mixpanel basic metrics
                mixpanel_metrics['trial_users_count'], mixpanel_metrics['purchase_users_count'],
                # User lists
                trial_users_json, json.dumps(breakdown_user_lifecycle['post_trial_user_ids']),
                json.dumps(breakdown_user_lifecycle['converted_user_ids']), json.dumps(breakdown_user_lifecycle['trial_refund_user_ids']),
                purchase_users_json, json.dumps(breakdown_user_lifecycle['purchase_refund_user_ids']),
                # Conversion rate metrics
                breakdown_comprehensive_metrics['trial_conversion_rate_estimated'], 
                breakdown_comprehensive_metrics['trial_conversion_rate_actual'],
                breakdown_comprehensive_metrics['trial_refund_rate_estimated'], 
                breakdown_comprehensive_metrics['trial_refund_rate_actual'], 
                breakdown_comprehensive_metrics['purchase_refund_rate_estimated'], 
                breakdown_comprehensive_metrics['purchase_refund_rate_actual'],
                # Revenue metrics
                breakdown_comprehensive_metrics['actual_revenue_usd'], breakdown_comprehensive_metrics['actual_refunds_usd'],
                breakdown_comprehensive_metrics['net_actual_revenue_usd'], 
                mixpanel_metrics.get('estimated_revenue_usd', 0),  # Basic estimated revenue for breakdown
                breakdown_comprehensive_metrics['adjusted_estimated_revenue_usd'],
                # Performance metrics
                breakdown_comprehensive_metrics['profit_usd'], breakdown_comprehensive_metrics['estimated_roas'],
                breakdown_comprehensive_metrics['trial_accuracy_ratio'], breakdown_comprehensive_metrics['purchase_accuracy_ratio'],
                # Cost metrics
                breakdown_comprehensive_metrics['mixpanel_cost_per_trial'], breakdown_comprehensive_metrics['mixpanel_cost_per_purchase'],
                breakdown_comprehensive_metrics['meta_cost_per_trial'], breakdown_comprehensive_metrics['meta_cost_per_purchase'],
                breakdown_comprehensive_metrics['click_to_trial_rate'],
                # Metadata
                current_time
            )
            
            bulk_insert_data.append(record)
        
        if bulk_insert_data:
            # Bulk insert breakdown records
            breakdown_insert_query = """
            INSERT OR REPLACE INTO daily_mixpanel_metrics_breakdown 
            (entity_type, entity_id, date, breakdown_type, breakdown_value,
             meta_spend, meta_impressions, meta_clicks, meta_trial_count, meta_purchase_count,
             mixpanel_trial_count, mixpanel_purchase_count,
             trial_user_ids, post_trial_user_ids, converted_user_ids, trial_refund_user_ids,
             purchase_user_ids, purchase_refund_user_ids,
             trial_conversion_rate_estimated, trial_conversion_rate_actual, trial_refund_rate_estimated, 
             trial_refund_rate_actual, purchase_refund_rate_estimated, purchase_refund_rate_actual,
             actual_revenue_usd, actual_refunds_usd, net_actual_revenue_usd, estimated_revenue_usd, adjusted_estimated_revenue_usd,
             profit_usd, estimated_roas, trial_accuracy_ratio, purchase_accuracy_ratio,
             mixpanel_cost_per_trial, mixpanel_cost_per_purchase, meta_cost_per_trial, meta_cost_per_purchase,
             click_to_trial_rate, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Execute bulk insert
            self.cursor.executemany(breakdown_insert_query, bulk_insert_data)
            logger.info(f"✅ Bulk inserted {len(bulk_insert_data)} {breakdown_type} breakdown metrics for {entity_type} entities on {date_str}")
        else:
            logger.warning(f"⚠️ No {breakdown_type} breakdown data found for {entity_type} entities on {date_str}")

    def calculate_user_lifecycle_data_breakdown(self, metrics: Dict[str, Any], date_obj: date, breakdown_type: str, breakdown_value: str) -> Dict[str, Any]:
        """
        Calculate user lifecycle data for breakdown-specific users according to specification
        Same logic as main calculation but filtered by breakdown criteria
        """
        from datetime import timedelta, date
        
        trial_users = metrics['trial_users_list']
        purchase_users = metrics['purchase_users_list']
        
        # Calculate 7 days ago from TODAY (not record date) for post-trial logic
        today = date.today()
        post_trial_cutoff = today - timedelta(days=7)
        
        # Additional WHERE clause for breakdown filtering
        if breakdown_type == 'country':
            breakdown_filter = "AND u.country = ?"
            breakdown_params = [breakdown_value]
        elif breakdown_type == 'region':
            breakdown_filter = "AND u.region = ?"
            breakdown_params = [breakdown_value]
        else:
            # Default - no additional filtering
            breakdown_filter = ""
            breakdown_params = []
        
        # Post-trial users: Those whose trial started at least 7 days ago (breakdown-specific)
        post_trial_users = []
        if trial_users:
            post_trial_query = f"""
            SELECT DISTINCT e.distinct_id
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
            WHERE e.distinct_id IN ({','.join(['?' for _ in trial_users])})
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) <= ?
              AND u.has_abi_attribution = TRUE
              {breakdown_filter}
            """
            
            params = trial_users + [post_trial_cutoff.strftime('%Y-%m-%d')] + breakdown_params
            self.cursor.execute(post_trial_query, params)
            post_trial_users = [row[0] for row in self.cursor.fetchall()]
        
        # Converted users: Post-trial users who have 'RC Trial converted' events
        converted_users = []
        if post_trial_users:
            converted_query = f"""
            SELECT DISTINCT e.distinct_id
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
            WHERE e.distinct_id IN ({','.join(['?' for _ in post_trial_users])})
              AND e.event_name = 'RC Trial converted'
              {breakdown_filter}
            """
            
            params = post_trial_users + breakdown_params
            self.cursor.execute(converted_query, params)
            converted_users = [row[0] for row in self.cursor.fetchall()]
        
        # Trial refund users: Converted users who have refund events
        trial_refund_users = []
        if converted_users:
            trial_refund_query = f"""
            SELECT DISTINCT e.distinct_id
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
            WHERE e.distinct_id IN ({','.join(['?' for _ in converted_users])})
              AND (e.event_name LIKE '%cancel%' OR e.event_name = 'RC Cancellation')
              AND e.revenue_usd < 0
              {breakdown_filter}
            """
            
            params = converted_users + breakdown_params
            self.cursor.execute(trial_refund_query, params)
            trial_refund_users = [row[0] for row in self.cursor.fetchall()]
        
        # Purchase refund users: Purchase users who have refund events  
        purchase_refund_users = []
        if purchase_users:
            purchase_refund_query = f"""
            SELECT DISTINCT e.distinct_id  
            FROM mixpanel_event e
            JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
            WHERE e.distinct_id IN ({','.join(['?' for _ in purchase_users])})
              AND (e.event_name LIKE '%cancel%' OR e.event_name = 'RC Cancellation')
              AND e.revenue_usd < 0
              {breakdown_filter}
            """
            
            params = purchase_users + breakdown_params
            self.cursor.execute(purchase_refund_query, params)
            purchase_refund_users = [row[0] for row in self.cursor.fetchall()]
        
        return {
            'post_trial_user_ids': post_trial_users,
            'converted_user_ids': converted_users,
            'trial_refund_user_ids': trial_refund_users,
            'purchase_refund_user_ids': purchase_refund_users
        }

    def compute_all_daily_metrics(self):
        """Compute daily metrics for all entity types"""
        logger.info("Computing daily metrics for all entity types...")
        
        # Clear existing metrics (fresh computation)
        self.cursor.execute("DELETE FROM daily_mixpanel_metrics")
        self.cursor.execute("DELETE FROM daily_mixpanel_metrics_breakdown")
        self.conn.commit()
        
        # Entity type configurations
        entity_configs = [
            ('campaign', 'abi_campaign_id'),
            ('adset', 'abi_ad_set_id'),
            ('ad', 'abi_ad_id')
        ]
        
        total_metrics = 0
        
        for entity_type, attribution_column in entity_configs:
            try:
                metrics_count = self.compute_daily_metrics_for_entity_type(entity_type, attribution_column)
                total_metrics += metrics_count
                
                # Update stats
                if entity_type == 'campaign':
                    self.stats['campaign_metrics_created'] = metrics_count
                elif entity_type == 'adset':
                    self.stats['adset_metrics_created'] = metrics_count
                elif entity_type == 'ad':
                    self.stats['ad_metrics_created'] = metrics_count
                
            except Exception as e:
                logger.error(f"Failed to compute {entity_type} metrics: {e}")
                self.conn.rollback()
                raise
        
        self.conn.commit()
        
        # Compute breakdown metrics according to specification (country and region only)
        logger.info("🔍 Computing breakdown metrics for country and region dimensions...")
        breakdown_metrics = 0
        
        start_date, end_date = self.get_data_date_range()
        breakdown_types = ['country', 'region']  # Specification: only country and region, no device
        
        from datetime import timedelta
        
        for breakdown_type in breakdown_types:
            try:
                logger.info(f"Processing {breakdown_type} breakdown metrics...")
                
                # Collect Meta breakdown data
                meta_breakdown_data = self.collect_meta_breakdown_data(start_date, end_date, breakdown_type)
                
                # Collect Mixpanel breakdown data  
                mixpanel_breakdown_data = self.collect_mixpanel_breakdown_data(start_date, end_date, breakdown_type)
                
                # Process breakdown metrics for each entity type
                for entity_type, _ in entity_configs:
                    # Process each date in the range
                    current_date = start_date
                    while current_date <= end_date:
                        self.insert_breakdown_metrics(
                            entity_type, current_date, breakdown_type, 
                            meta_breakdown_data, mixpanel_breakdown_data
                        )
                        current_date += timedelta(days=1)
                
                logger.info(f"✅ Completed {breakdown_type} breakdown processing")
                
            except Exception as e:
                logger.error(f"Failed to compute {breakdown_type} breakdown metrics: {e}")
                # Continue with other breakdown types rather than failing completely
                continue
        
        self.conn.commit()
        
        # Count total breakdown records created
        self.cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics_breakdown")
        breakdown_metrics = self.cursor.fetchone()[0]
        logger.info(f"✅ Created {breakdown_metrics} breakdown metrics records")
        
        # Update global stats
        self.stats['date_range_start'] = start_date
        self.stats['date_range_end'] = end_date
        self.stats['total_dates_processed'] = (end_date - start_date).days + 1
        self.stats['breakdown_metrics_created'] = breakdown_metrics
        
        # Calculate totals
        self.calculate_summary_stats()
        
        logger.info(f"✅ Successfully computed {total_metrics} daily metrics + {breakdown_metrics} breakdown metrics records")
    
    def calculate_summary_stats(self):
        """Calculate summary statistics with robust JSON handling"""
        # Total unique trial users - use safer approach
        self.cursor.execute("""
        SELECT COUNT(DISTINCT entity_id) as entities_with_trials,
               SUM(trial_users_count) as total_trial_events
        FROM daily_mixpanel_metrics 
        WHERE trial_users_count > 0
        """)
        result = self.cursor.fetchone()
        self.stats['entities_with_trials'] = result[0] if result else 0
        self.stats['total_trial_events'] = result[1] if result else 0
        
        # Total unique purchase users - use safer approach  
        self.cursor.execute("""
        SELECT COUNT(DISTINCT entity_id) as entities_with_purchases,
               SUM(purchase_users_count) as total_purchase_events
        FROM daily_mixpanel_metrics 
        WHERE purchase_users_count > 0
        """)
        result = self.cursor.fetchone()
        self.stats['entities_with_purchases'] = result[0] if result else 0
        self.stats['total_purchase_events'] = result[1] if result else 0
        
        # Total estimated revenue
        self.cursor.execute("""
        SELECT COALESCE(SUM(estimated_revenue_usd), 0.0)
        FROM daily_mixpanel_metrics
        """)
        result = self.cursor.fetchone()
        self.stats['total_estimated_revenue'] = float(result[0]) if result else 0.0
    
    def validate_metrics(self):
        """Validate the computed daily metrics"""
        logger.info("Validating daily metrics...")
        
        # Check total metrics count
        self.cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics")
        total_metrics = self.cursor.fetchone()[0]
        
        if total_metrics == 0:
            logger.error("❌ No daily metrics found")
            return False
        
        logger.info(f"✅ Total daily metrics records: {total_metrics}")
        
        # Check metrics by entity type
        self.cursor.execute("""
        SELECT entity_type, COUNT(*) as metric_count
        FROM daily_mixpanel_metrics
        GROUP BY entity_type
        ORDER BY entity_type
        """)
        
        entity_counts = self.cursor.fetchall()
        for entity_type, count in entity_counts:
            logger.info(f"  {entity_type}: {count} metrics")
        
        # Check for data quality issues
        self.cursor.execute("""
        SELECT AVG(data_quality_score) as avg_quality
        FROM daily_mixpanel_metrics
        WHERE data_quality_score IS NOT NULL
        """)
        avg_quality = self.cursor.fetchone()[0]
        if avg_quality:
            logger.info(f"✅ Average data quality score: {avg_quality:.2f}")
        
        # Sample check: verify some metrics look reasonable
        self.cursor.execute("""
        SELECT entity_type, entity_id, date, trial_users_count, purchase_users_count, estimated_revenue_usd
        FROM daily_mixpanel_metrics
        WHERE trial_users_count > 0
        ORDER BY trial_users_count DESC
        LIMIT 5
        """)
        
        samples = self.cursor.fetchall()
        logger.info("Sample daily metrics:")
        for entity_type, entity_id, date, trials, purchases, revenue in samples:
            logger.info(f"  {entity_type} {entity_id} on {date}: {trials} trials, {purchases} purchases, ${revenue:.2f} revenue")
        
        return True
    
    def collect_meta_advertising_data(self, start_date: date, end_date: date) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        """
        Query Meta Analytics database for advertising spend, impressions, clicks, etc.
        
        Returns:
            Dict mapping (entity_type, entity_id, date) to meta metrics
        """
        logger.info("Collecting Meta advertising data...")
        
        meta_data = {}
        
        # Query for all entity levels (campaign, adset, ad) from Meta database
        query = """
        SELECT 
            ad_id,
            adset_id, 
            campaign_id,
            date,
            SUM(spend) as meta_spend,
            SUM(impressions) as meta_impressions,
            SUM(clicks) as meta_clicks,
            SUM(meta_trials) as meta_trial_count,
            SUM(meta_purchases) as meta_purchase_count
        FROM ad_performance_daily 
        WHERE date BETWEEN ? AND ?
        GROUP BY ad_id, adset_id, campaign_id, date
        """
        
        self.meta_cursor.execute(query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        for row in self.meta_cursor.fetchall():
            ad_id, adset_id, campaign_id, date_str, spend, impressions, clicks, trials, purchases = row
            
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Store data for each entity level
            entity_data = [
                ('ad', ad_id),
                ('adset', adset_id), 
                ('campaign', campaign_id)
            ]
            
            for entity_type, entity_id in entity_data:
                if entity_id:  # Skip if entity_id is None
                    key = (entity_type, entity_id, date_str)
                    
                    if key not in meta_data:
                        meta_data[key] = {
                            'meta_spend': 0.0,
                            'meta_impressions': 0,
                            'meta_clicks': 0,
                            'meta_trial_count': 0,
                            'meta_purchase_count': 0
                        }
                    
                    # Aggregate data for this entity
                    meta_data[key]['meta_spend'] += float(spend or 0)
                    meta_data[key]['meta_impressions'] += int(impressions or 0)
                    meta_data[key]['meta_clicks'] += int(clicks or 0)
                    meta_data[key]['meta_trial_count'] += int(trials or 0)
                    meta_data[key]['meta_purchase_count'] += int(purchases or 0)
        
        logger.info(f"Collected Meta data for {len(meta_data)} entity-date combinations")
        return meta_data
    
    def collect_meta_breakdown_data(self, start_date: date, end_date: date, breakdown_type: str) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
        """
        Query Meta breakdown tables for country/device/region data
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection 
            breakdown_type: 'country', 'device', or 'region'
            
        Returns:
            Dict mapping (entity_type, entity_id, date, breakdown_value) to meta metrics
        """
        logger.info(f"Collecting Meta {breakdown_type} breakdown data...")
        
        # Map breakdown type to table name
        table_map = {
            'country': 'ad_performance_daily_country',
            'region': 'ad_performance_daily_region'
        }
        
        table = table_map.get(breakdown_type)
        if not table:
            logger.warning(f"Unknown breakdown type: {breakdown_type}")
            return {}
        
        breakdown_data = {}
        
        # Query breakdown table
        query = f"""
        SELECT 
            ad_id,
            adset_id,
            campaign_id, 
            date,
            {breakdown_type},
            SUM(spend) as meta_spend,
            SUM(impressions) as meta_impressions,
            SUM(clicks) as meta_clicks,
            SUM(meta_trials) as meta_trial_count,
            SUM(meta_purchases) as meta_purchase_count
        FROM {table}
        WHERE date BETWEEN ? AND ?
        GROUP BY ad_id, adset_id, campaign_id, date, {breakdown_type}
        """
        
        self.meta_cursor.execute(query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        for row in self.meta_cursor.fetchall():
            ad_id, adset_id, campaign_id, date_str, breakdown_value, spend, impressions, clicks, trials, purchases = row
            
            # Store data for each entity level
            entity_data = [
                ('ad', ad_id),
                ('adset', adset_id),
                ('campaign', campaign_id)
            ]
            
            for entity_type, entity_id in entity_data:
                if entity_id and breakdown_value:  # Skip if entity_id or breakdown_value is None
                    key = (entity_type, entity_id, date_str, breakdown_value)
                    
                    if key not in breakdown_data:
                        breakdown_data[key] = {
                            'meta_spend': 0.0,
                            'meta_impressions': 0,
                            'meta_clicks': 0,
                            'meta_trial_count': 0,
                            'meta_purchase_count': 0
                        }
                    
                    # Aggregate data for this entity-breakdown combination
                    breakdown_data[key]['meta_spend'] += float(spend or 0)
                    breakdown_data[key]['meta_impressions'] += int(impressions or 0)
                    breakdown_data[key]['meta_clicks'] += int(clicks or 0)
                    breakdown_data[key]['meta_trial_count'] += int(trials or 0)
                    breakdown_data[key]['meta_purchase_count'] += int(purchases or 0)
        
        logger.info(f"Collected Meta {breakdown_type} breakdown data for {len(breakdown_data)} entity-date-breakdown combinations")
        return breakdown_data
    
    def collect_mixpanel_breakdown_data(self, start_date: date, end_date: date, breakdown_type: str) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
        """
        Query Mixpanel data with breakdown dimensions (country, device, region)
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            breakdown_type: 'country', 'device', or 'region'
            
        Returns:
            Dict mapping (entity_type, entity_id, date, breakdown_value) to mixpanel metrics
        """
        logger.info(f"Collecting Mixpanel {breakdown_type} breakdown data...")
        
        # Map breakdown type to Mixpanel field
        field_map = {
            'country': 'country',
            'region': 'region'
        }
        
        mixpanel_field = field_map.get(breakdown_type)
        if not mixpanel_field:
            logger.warning(f"Unknown breakdown type: {breakdown_type}")
            return {}
        
        breakdown_data = {}
        
        # Entity type configurations
        entity_configs = [
            ('campaign', 'abi_campaign_id'),
            ('adset', 'abi_ad_set_id'),
            ('ad', 'abi_ad_id')
        ]
        
        for entity_type, attribution_column in entity_configs:
            # Query for trial events with breakdown
            trial_query = f"""
            SELECT 
                {attribution_column} as entity_id,
                DATE(event_time) as date,
                {mixpanel_field} as breakdown_value,
                COUNT(DISTINCT distinct_id) as trial_count,
                GROUP_CONCAT(DISTINCT distinct_id) as trial_user_ids
            FROM mixpanel_event
            WHERE event_name = 'RC Trial started'
              AND {attribution_column} IS NOT NULL
              AND {mixpanel_field} IS NOT NULL
              AND DATE(event_time) BETWEEN ? AND ?
            GROUP BY {attribution_column}, DATE(event_time), {mixpanel_field}
            """
            
            self.cursor.execute(trial_query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            
            for row in self.cursor.fetchall():
                entity_id, date_str, breakdown_value, trial_count, trial_user_ids = row
                
                # No mapping needed - Meta and Mixpanel use same country codes
                mapped_breakdown_value = breakdown_value
                
                key = (entity_type, entity_id, date_str, mapped_breakdown_value)
                
                if key not in breakdown_data:
                    breakdown_data[key] = {
                        'mixpanel_trial_count': 0,
                        'mixpanel_purchase_count': 0,
                        'trial_user_ids': [],
                        'purchase_user_ids': []
                    }
                
                breakdown_data[key]['mixpanel_trial_count'] = trial_count
                breakdown_data[key]['trial_user_ids'] = trial_user_ids.split(',') if trial_user_ids else []
            
            # Query for purchase events with breakdown
            purchase_query = f"""
            SELECT 
                {attribution_column} as entity_id,
                DATE(event_time) as date,
                {mixpanel_field} as breakdown_value,
                COUNT(DISTINCT distinct_id) as purchase_count,
                GROUP_CONCAT(DISTINCT distinct_id) as purchase_user_ids
            FROM mixpanel_event
            WHERE event_name IN ('RC Initial purchase', 'RC Trial converted')
              AND {attribution_column} IS NOT NULL
              AND {mixpanel_field} IS NOT NULL
              AND DATE(event_time) BETWEEN ? AND ?
            GROUP BY {attribution_column}, DATE(event_time), {mixpanel_field}
            """
            
            self.cursor.execute(purchase_query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            
            for row in self.cursor.fetchall():
                entity_id, date_str, breakdown_value, purchase_count, purchase_user_ids = row
                
                # No mapping needed - Meta and Mixpanel use same country codes
                mapped_breakdown_value = breakdown_value
                
                key = (entity_type, entity_id, date_str, mapped_breakdown_value)
                
                if key not in breakdown_data:
                    breakdown_data[key] = {
                        'mixpanel_trial_count': 0,
                        'mixpanel_purchase_count': 0,
                        'trial_user_ids': [],
                        'purchase_user_ids': []
                    }
                
                breakdown_data[key]['mixpanel_purchase_count'] = purchase_count
                breakdown_data[key]['purchase_user_ids'] = purchase_user_ids.split(',') if purchase_user_ids else []
        
        logger.info(f"Collected Mixpanel {breakdown_type} breakdown data for {len(breakdown_data)} entity-date-breakdown combinations")
        return breakdown_data
    
    def calculate_user_lifecycle_data(self, metrics: Dict[str, Any], date_obj: date) -> Dict[str, Any]:
        """
        Calculate user lifecycle lists according to specification:
        - post_trial_user_ids: Users whose trials started ≥7 days ago 
        - converted_user_ids: Users with 'RC Trial converted' events
        - trial_refund_user_ids: Users with trial refund events
        - purchase_refund_user_ids: Users with purchase refund events
        """
        from datetime import timedelta, date
        
        trial_users = metrics['trial_users_list']
        purchase_users = metrics['purchase_users_list']
        
        # Calculate 7 days ago from TODAY (not record date) for post-trial logic
        today = date.today()
        post_trial_cutoff = today - timedelta(days=7)
        
        # Post-trial users: Those whose trial started at least 7 days ago
        post_trial_query = """
        SELECT DISTINCT e.distinct_id
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE e.distinct_id IN ({})
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) <= ?
          AND u.has_abi_attribution = TRUE
        """.format(','.join(['?' for _ in trial_users]))
        
        if trial_users:
            self.cursor.execute(post_trial_query, trial_users + [post_trial_cutoff.strftime('%Y-%m-%d')])
            post_trial_users = [row[0] for row in self.cursor.fetchall()]
        else:
            post_trial_users = []
        
        # Converted users: Post-trial users who have 'RC Trial converted' events
        converted_users = []
        if post_trial_users:
            converted_query = """
            SELECT DISTINCT e.distinct_id
            FROM mixpanel_event e
            WHERE e.distinct_id IN ({})
              AND e.event_name = 'RC Trial converted'
            """.format(','.join(['?' for _ in post_trial_users]))
            
            self.cursor.execute(converted_query, post_trial_users)
            converted_users = [row[0] for row in self.cursor.fetchall()]
        
        # Trial refund users: Converted users who have refund events
        trial_refund_users = []
        if converted_users:
            trial_refund_query = """
            SELECT DISTINCT e.distinct_id
            FROM mixpanel_event e
            WHERE e.distinct_id IN ({})
              AND (e.event_name LIKE '%cancel%' OR e.event_name = 'RC Cancellation')
              AND e.revenue_usd < 0
            """.format(','.join(['?' for _ in converted_users]))
            
            self.cursor.execute(trial_refund_query, converted_users)
            trial_refund_users = [row[0] for row in self.cursor.fetchall()]
        
        # Purchase refund users: Purchase users who have refund events  
        purchase_refund_users = []
        if purchase_users:
            purchase_refund_query = """
            SELECT DISTINCT e.distinct_id  
            FROM mixpanel_event e
            WHERE e.distinct_id IN ({})
              AND (e.event_name LIKE '%cancel%' OR e.event_name = 'RC Cancellation')
              AND e.revenue_usd < 0
            """.format(','.join(['?' for _ in purchase_users]))
            
            self.cursor.execute(purchase_refund_query, purchase_users)
            purchase_refund_users = [row[0] for row in self.cursor.fetchall()]
        
        return {
            'post_trial_user_ids': post_trial_users,
            'converted_user_ids': converted_users,
            'trial_refund_user_ids': trial_refund_users,
            'purchase_refund_user_ids': purchase_refund_users
        }
    
    def calculate_conversion_rates(self, metrics: Dict[str, Any], user_lifecycle: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate conversion rates according to specification:
        - trial_conversion_rate_estimated: AVG(user_product_metrics.trial_conversion_rate)
        - trial_conversion_rate_actual: (converted_users / post_trial_users) * 100
        - trial_refund_rate_estimated: AVG(user_product_metrics.trial_converted_to_refund_rate)
        - trial_refund_rate_actual: (trial_refund_users / converted_users) * 100
        - purchase_refund_rate_estimated: AVG(user_product_metrics.initial_purchase_to_refund_rate)
        - purchase_refund_rate_actual: (purchase_refund_users / purchase_users) * 100
        """
        trial_users = metrics['trial_users_list']
        purchase_users = metrics['purchase_users_list']
        post_trial_users = user_lifecycle['post_trial_user_ids']
        converted_users = user_lifecycle['converted_user_ids']
        trial_refund_users = user_lifecycle['trial_refund_user_ids']
        purchase_refund_users = user_lifecycle['purchase_refund_user_ids']
        
        # Trial conversion rate estimated
        trial_conversion_rate_estimated = 0.0
        if trial_users:
            trial_est_query = """
            SELECT AVG(trial_conversion_rate)
            FROM user_product_metrics
            WHERE distinct_id IN ({})
            """.format(','.join(['?' for _ in trial_users]))
            
            self.cursor.execute(trial_est_query, trial_users)
            result = self.cursor.fetchone()
            trial_conversion_rate_estimated = float(result[0]) if result and result[0] else 0.0
        
        # Trial conversion rate actual
        trial_conversion_rate_actual = 0.0
        if post_trial_users:
            trial_conversion_rate_actual = (len(converted_users) / len(post_trial_users)) * 100
        
        # Trial refund rate estimated
        trial_refund_rate_estimated = 0.0
        if converted_users:
            trial_refund_est_query = """
            SELECT AVG(trial_converted_to_refund_rate)
            FROM user_product_metrics
            WHERE distinct_id IN ({})
            """.format(','.join(['?' for _ in converted_users]))
            
            self.cursor.execute(trial_refund_est_query, converted_users)
            result = self.cursor.fetchone()
            trial_refund_rate_estimated = float(result[0]) if result and result[0] else 0.0
        
        # Trial refund rate actual
        trial_refund_rate_actual = 0.0
        if converted_users:
            trial_refund_rate_actual = (len(trial_refund_users) / len(converted_users)) * 100
        
        # Purchase refund rate estimated
        purchase_refund_rate_estimated = 0.0
        if purchase_users:
            purchase_refund_est_query = """
            SELECT AVG(initial_purchase_to_refund_rate)
            FROM user_product_metrics
            WHERE distinct_id IN ({})
            """.format(','.join(['?' for _ in purchase_users]))
            
            self.cursor.execute(purchase_refund_est_query, purchase_users)
            result = self.cursor.fetchone()
            purchase_refund_rate_estimated = float(result[0]) if result and result[0] else 0.0
        
        # Purchase refund rate actual
        purchase_refund_rate_actual = 0.0
        if purchase_users:
            purchase_refund_rate_actual = (len(purchase_refund_users) / len(purchase_users)) * 100
        
        return {
            'trial_conversion_rate_estimated': trial_conversion_rate_estimated,
            'trial_conversion_rate_actual': trial_conversion_rate_actual,
            'trial_refund_rate_estimated': trial_refund_rate_estimated,
            'trial_refund_rate_actual': trial_refund_rate_actual,
            'purchase_refund_rate_estimated': purchase_refund_rate_estimated,
            'purchase_refund_rate_actual': purchase_refund_rate_actual
        }
    
    def calculate_actual_revenue_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate actual revenue metrics according to specification:
        - actual_revenue_usd: SUM revenue from 'RC Initial purchase', 'RC Trial converted'
        - actual_refunds_usd: SUM ABS(revenue) from cancellation events
        """
        all_users = metrics['trial_users_list'] + metrics['purchase_users_list']
        
        actual_revenue_usd = 0.0
        actual_refunds_usd = 0.0
        
        if all_users:
            # Actual revenue from purchase and trial conversion events
            revenue_query = """
            SELECT COALESCE(SUM(revenue_usd), 0)
            FROM mixpanel_event
            WHERE distinct_id IN ({})
              AND event_name IN ('RC Initial purchase', 'RC Trial converted')
              AND revenue_usd > 0
            """.format(','.join(['?' for _ in all_users]))
            
            self.cursor.execute(revenue_query, all_users)
            result = self.cursor.fetchone()
            actual_revenue_usd = float(result[0]) if result else 0.0
            
            # Actual refunds from cancellation events
            refunds_query = """
            SELECT COALESCE(SUM(ABS(revenue_usd)), 0)
            FROM mixpanel_event
            WHERE distinct_id IN ({})
              AND (event_name LIKE '%cancel%' OR event_name = 'RC Cancellation')
              AND revenue_usd < 0
            """.format(','.join(['?' for _ in all_users]))
            
            self.cursor.execute(refunds_query, all_users)
            result = self.cursor.fetchone()
            actual_refunds_usd = float(result[0]) if result else 0.0
        
        return {
            'actual_revenue_usd': actual_revenue_usd,
            'actual_refunds_usd': actual_refunds_usd,
            'net_actual_revenue_usd': actual_revenue_usd - actual_refunds_usd
        }
    
    def calculate_comprehensive_metrics(self, meta_data: Dict[str, Any], mixpanel_data: Dict[str, Any], user_lifecycle: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all metrics using direct calculator methods
        
        Args:
            meta_data: Meta advertising metrics dict
            mixpanel_data: Mixpanel event metrics dict
            
        Returns:
            Dict with all calculated metrics
        """
        # Extract values
        spend = float(meta_data.get('meta_spend', 0) or 0)
        impressions = int(meta_data.get('meta_impressions', 0) or 0)
        clicks = int(meta_data.get('meta_clicks', 0) or 0)
        meta_trials = int(meta_data.get('meta_trial_count', 0) or 0)
        meta_purchases = int(meta_data.get('meta_purchase_count', 0) or 0)
        
        mixpanel_trials = int(mixpanel_data.get('trial_users_count', 0) or 0)
        mixpanel_purchases = int(mixpanel_data.get('purchase_users_count', 0) or 0)
        estimated_revenue = float(mixpanel_data.get('estimated_revenue_usd', 0) or 0)
        
        # Calculate conversion rates using specification logic
        conversion_rates = self.calculate_conversion_rates(mixpanel_data, user_lifecycle)
        
        # Calculate actual revenue using specification logic
        actual_revenue_metrics = self.calculate_actual_revenue_metrics(mixpanel_data)
        
        # Calculate accuracy ratios
        trial_accuracy_ratio = DirectCalculators.calculate_trial_accuracy_ratio(mixpanel_trials, meta_trials)
        purchase_accuracy_ratio = DirectCalculators.calculate_purchase_accuracy_ratio(mixpanel_purchases, meta_purchases)
        
        # Calculate adjusted estimated revenue according to spec
        # Use trial accuracy ratio if more trials than purchases, otherwise purchase accuracy
        primary_accuracy_ratio = trial_accuracy_ratio if mixpanel_trials >= mixpanel_purchases else purchase_accuracy_ratio
        adjusted_estimated_revenue = estimated_revenue / (primary_accuracy_ratio / 100) if primary_accuracy_ratio > 0 else estimated_revenue
        
        # Combine all metrics according to specification
        metrics = {
            # Accuracy metrics
            'trial_accuracy_ratio': trial_accuracy_ratio,
            'purchase_accuracy_ratio': purchase_accuracy_ratio,
            
            # Conversion rates (from specification calculations)
            **conversion_rates,
            
            # Revenue metrics (from specification calculations)
            **actual_revenue_metrics,
            'adjusted_estimated_revenue_usd': adjusted_estimated_revenue,
            
            # ROAS and performance metrics
            'estimated_roas': DirectCalculators.calculate_estimated_roas(adjusted_estimated_revenue, spend),
            'profit_usd': adjusted_estimated_revenue - spend,
            
            # Cost metrics
            'mixpanel_cost_per_trial': DirectCalculators.calculate_cost_per_trial(spend, mixpanel_trials),
            'mixpanel_cost_per_purchase': DirectCalculators.calculate_cost_per_purchase(spend, mixpanel_purchases),
            'meta_cost_per_trial': DirectCalculators.calculate_cost_per_trial(spend, meta_trials),
            'meta_cost_per_purchase': DirectCalculators.calculate_cost_per_purchase(spend, meta_purchases),
            
            # Rate metrics
            'click_to_trial_rate': DirectCalculators.calculate_click_to_trial_rate(mixpanel_trials, clicks)
        }
        
        return metrics
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()
    
    def close_connections(self):
        """Close database connections"""
        if hasattr(self, 'meta_conn'):
            self.meta_conn.close()

def main():
    """Main execution function"""
    try:
        logger.info("=== Module 8: Compute Daily Mixpanel Metrics ===")
        logger.info("Pre-computing daily metrics for all advertising entities...")
        
        # Connect to database
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        
        # Verify required tables exist
        cursor = conn.cursor()
        required_tables = [
            'daily_mixpanel_metrics', 
            'mixpanel_user', 
            'mixpanel_event',
            'user_product_metrics'
        ]
        
        for table in required_tables:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table,)
            )
            if not cursor.fetchone():
                raise RuntimeError(f"Required table '{table}' not found. Run prior pipeline modules first.")
        
        # Verify Meta Analytics database availability
        try:
            meta_conn = sqlite3.connect(META_DATABASE_PATH)
            meta_cursor = meta_conn.cursor()
            meta_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ad_performance_daily'")
            if not meta_cursor.fetchone():
                raise RuntimeError("Meta Analytics database not available or missing required tables. Run meta pipeline first.")
            meta_conn.close()
            logger.info("✅ Meta Analytics database validation passed")
        except Exception as e:
            raise RuntimeError(f"Meta Analytics database integration required but unavailable: {e}")
        
        # Process daily metrics
        processor = DailyMetricsProcessor(conn)
        processor.compute_all_daily_metrics()
        
        # Validate results
        if not processor.validate_metrics():
            raise RuntimeError("Daily metrics validation failed")
        
        # Display final statistics
        stats = processor.get_stats()
        logger.info("=== Daily Metrics Computation Statistics ===")
        logger.info(f"Date range: {stats['date_range_start']} to {stats['date_range_end']}")
        logger.info(f"Total dates processed: {stats['total_dates_processed']}")
        logger.info(f"Campaign metrics created: {stats['campaign_metrics_created']}")
        logger.info(f"Ad set metrics created: {stats['adset_metrics_created']}")
        logger.info(f"Ad metrics created: {stats['ad_metrics_created']}")
        logger.info(f"Entities with trial events: {stats['entities_with_trials']}")
        logger.info(f"Total trial events (deduplicated): {stats['total_trial_events']}")
        logger.info(f"Entities with purchase events: {stats['entities_with_purchases']}")
        logger.info(f"Total purchase events (deduplicated): {stats['total_purchase_events']}")
        logger.info(f"Total estimated revenue: ${stats['total_estimated_revenue']:.2f}")
        
        # Clean up connections
        processor.close_connections()
        conn.close()
        
        logger.info("✅ Module 8 completed successfully")
        logger.info("Daily metrics are ready for lightning-fast dashboard queries")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Module 8 failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())