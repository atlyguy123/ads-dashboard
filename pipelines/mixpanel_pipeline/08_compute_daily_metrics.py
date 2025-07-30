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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - Use centralized database path discovery
DATABASE_PATH = get_database_path('mixpanel_data')

class DailyMetricsProcessor:
    """Processes and computes daily Mixpanel metrics for all entities"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
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
            MIN(DATE(event_time)) as min_date,
            MAX(DATE(event_time)) as max_date
        FROM mixpanel_event
        WHERE event_name IN ('RC Trial started', 'RC Initial purchase')
        """)
        
        result = self.cursor.fetchone()
        if not result or not result[0]:
            raise RuntimeError("No event data found for trial/purchase events")
        
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
        Insert daily metrics for all entities of a given type and date
        
        Args:
            entity_type: 'campaign', 'adset', or 'ad'
            date_obj: Date object
            entity_metrics: Dictionary of entity_id -> metrics
        """
        insert_query = """
        INSERT OR REPLACE INTO daily_mixpanel_metrics 
        (date, entity_type, entity_id, trial_users_count, trial_users_list, 
         purchase_users_count, purchase_users_list, estimated_revenue_usd, 
         computed_at, data_quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        current_time = datetime.now()
        date_str = date_obj.strftime('%Y-%m-%d')
        
        for entity_id, metrics in entity_metrics.items():
            # Convert user lists to JSON
            trial_users_json = json.dumps(metrics['trial_users_list'])
            purchase_users_json = json.dumps(metrics['purchase_users_list'])
            
            # Calculate data quality score (simple heuristic)
            data_quality = self.calculate_data_quality_score(metrics)
            
            self.cursor.execute(insert_query, (
                date_str,
                entity_type,
                entity_id,
                metrics['trial_users_count'],
                trial_users_json,
                metrics['purchase_users_count'],
                purchase_users_json,
                metrics['estimated_revenue_usd'],
                current_time,
                data_quality
            ))
    
    def calculate_data_quality_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate a simple data quality score for metrics
        
        Args:
            metrics: Dictionary containing computed metrics
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        score = 1.0
        
        # Reduce score if no trial users (suspicious for active campaigns)
        if metrics['trial_users_count'] == 0:
            score -= 0.2
        
        # Reduce score if revenue calculation seems off
        if metrics['trial_users_count'] > 0 and metrics['estimated_revenue_usd'] == 0:
            score -= 0.1
        
        # Reduce score if purchase users > trial users (impossible)
        if metrics['purchase_users_count'] > metrics['trial_users_count']:
            score -= 0.3
        
        return max(0.0, score)
    
    def compute_all_daily_metrics(self):
        """Compute daily metrics for all entity types"""
        logger.info("Computing daily metrics for all entity types...")
        
        # Clear existing metrics (fresh computation)
        self.cursor.execute("DELETE FROM daily_mixpanel_metrics")
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
        
        # Update global stats
        start_date, end_date = self.get_data_date_range()
        self.stats['date_range_start'] = start_date
        self.stats['date_range_end'] = end_date
        self.stats['total_dates_processed'] = (end_date - start_date).days + 1
        
        # Calculate totals
        self.calculate_summary_stats()
        
        logger.info(f"✅ Successfully computed {total_metrics} daily metrics records")
    
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()

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
        
        conn.close()
        
        logger.info("✅ Module 8 completed successfully")
        logger.info("Daily metrics are ready for lightning-fast dashboard queries")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Module 8 failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())