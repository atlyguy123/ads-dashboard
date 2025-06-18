"""
Analytics Query Service

This service handles fast dashboard data retrieval with JOIN queries between
meta_analytics.db and mixpanel_analytics.db using ABI attribution fields.
"""

import sqlite3
import logging
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

# Add the project root to the Python path for database utilities
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.database_utils import get_database_path

logger = logging.getLogger(__name__)


@dataclass
class QueryConfig:
    """Configuration for analytics queries"""
    breakdown: str  # 'all', 'country', 'region', 'device'
    start_date: str
    end_date: str
    group_by: Optional[str] = None  # 'campaign', 'adset', 'ad'
    include_mixpanel: bool = True


class AnalyticsQueryService:
    """Service for executing analytics queries across multiple databases"""
    
    def __init__(self, 
                 meta_db_path: Optional[str] = None,
                 mixpanel_db_path: Optional[str] = None,
                 mixpanel_analytics_db_path: Optional[str] = None):
        # Use centralized database path discovery or provided paths
        try:
            self.meta_db_path = meta_db_path or get_database_path('meta_analytics')
        except Exception:
            # Fallback for meta_analytics if not found (might not exist yet)
            self.meta_db_path = meta_db_path or ""
            
        self.mixpanel_db_path = mixpanel_db_path or get_database_path('mixpanel_data')
        self.mixpanel_analytics_db_path = mixpanel_analytics_db_path or get_database_path('mixpanel_data')
        
        # Table mapping based on breakdown parameter
        self.table_mapping = {
            'all': 'ad_performance_daily',
            'country': 'ad_performance_daily_country', 
            'region': 'ad_performance_daily_region',
            'device': 'ad_performance_daily_device'
        }
    
    def get_table_name(self, breakdown: str) -> str:
        """Get the appropriate table name based on breakdown parameter"""
        return self.table_mapping.get(breakdown, 'ad_performance_daily')
    
    def execute_analytics_query(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute analytics query with JOIN between meta and mixpanel data
        
        Args:
            config: Query configuration
            
        Returns:
            Dictionary with flat array of hierarchical data
        """
        try:
            # Get the appropriate table name
            table_name = self.get_table_name(config.breakdown)
            
            # Build and execute the main query based on hierarchy level
            if config.group_by == 'campaign':
                structured_data = self._get_campaign_level_data(config, table_name)
            elif config.group_by == 'adset':
                structured_data = self._get_adset_level_data(config, table_name)
            else:  # ad level
                structured_data = self._get_ad_level_data(config, table_name)
            
            return {
                'success': True,
                'data': structured_data,
                'metadata': {
                    'query_config': config.__dict__,
                    'table_used': table_name,
                    'record_count': len(structured_data),
                    'date_range': f"{config.start_date} to {config.end_date}",
                    'generated_at': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing analytics query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': datetime.now().isoformat()
                }
            }
    
    def _get_campaign_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get campaign-level aggregated data with adset and ad children"""
        
        # Step 1: Get campaign-level aggregated meta data
        campaign_query = f"""
        SELECT 
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND campaign_id IS NOT NULL
        GROUP BY campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        campaigns = self._execute_meta_query(campaign_query, [config.start_date, config.end_date])
        
        # Step 2: Get adset-level data for each campaign
        for campaign in campaigns:
            campaign_id = campaign['campaign_id']
            
            # Get adsets for this campaign
            adset_query = f"""
            SELECT 
                adset_id,
                adset_name,
                campaign_id,
                campaign_name,
                SUM(spend) as spend,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(meta_trials) as meta_trials_started,
                SUM(meta_purchases) as meta_purchases
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
              AND campaign_id = ?
              AND adset_id IS NOT NULL
            GROUP BY adset_id, adset_name, campaign_id, campaign_name
            ORDER BY SUM(spend) DESC
            """
            
            adsets = self._execute_meta_query(adset_query, [config.start_date, config.end_date, campaign_id])
            
            # Get ads for each adset
            for adset in adsets:
                adset_id = adset['adset_id']
                
                ad_query = f"""
                SELECT 
                    ad_id,
                    ad_name,
                    adset_id,
                    adset_name,
                    campaign_id,
                    campaign_name,
                    SUM(spend) as spend,
                    SUM(impressions) as impressions,
                    SUM(clicks) as clicks,
                    SUM(meta_trials) as meta_trials_started,
                    SUM(meta_purchases) as meta_purchases
                FROM {table_name}
                WHERE date BETWEEN ? AND ?
                  AND adset_id = ?
                  AND ad_id IS NOT NULL
                GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
                ORDER BY SUM(spend) DESC
                """
                
                ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date, adset_id])
                
                # Add mixpanel data to ads
                if config.include_mixpanel:
                    self._add_mixpanel_data_to_records(ads, config)
                
                # Format ads and add required fields
                formatted_ads = []
                for ad in ads:
                    formatted_ad = self._format_record(ad, 'ad')
                    formatted_ads.append(formatted_ad)
                
                adset['children'] = formatted_ads
            
            # Add mixpanel data to adsets
            if config.include_mixpanel:
                self._add_mixpanel_data_to_records(adsets, config)
            
            # Format adsets
            formatted_adsets = []
            for adset in adsets:
                formatted_adset = self._format_record(adset, 'adset')
                if 'children' not in formatted_adset:
                    formatted_adset['children'] = []
                formatted_adsets.append(formatted_adset)
            
            campaign['children'] = formatted_adsets
        
        # Add mixpanel data to campaigns
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(campaigns, config)
        
        # Format campaigns
        formatted_campaigns = []
        for campaign in campaigns:
            formatted_campaign = self._format_record(campaign, 'campaign')
            if 'children' not in formatted_campaign:
                formatted_campaign['children'] = []
            formatted_campaigns.append(formatted_campaign)
        
        return formatted_campaigns
    
    def _get_adset_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get adset-level aggregated data with ad children"""
        
        # Get adset-level aggregated meta data
        adset_query = f"""
        SELECT 
            adset_id,
            adset_name,
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND adset_id IS NOT NULL
        GROUP BY adset_id, adset_name, campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        adsets = self._execute_meta_query(adset_query, [config.start_date, config.end_date])
        
        # Get ads for each adset
        for adset in adsets:
            adset_id = adset['adset_id']
            
            ad_query = f"""
            SELECT 
                ad_id,
                ad_name,
                adset_id,
                adset_name,
                campaign_id,
                campaign_name,
                SUM(spend) as spend,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(meta_trials) as meta_trials_started,
                SUM(meta_purchases) as meta_purchases
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
              AND adset_id = ?
              AND ad_id IS NOT NULL
            GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
            ORDER BY SUM(spend) DESC
            """
            
            ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date, adset_id])
            
            # Add mixpanel data to ads
            if config.include_mixpanel:
                self._add_mixpanel_data_to_records(ads, config)
            
            # Format ads
            formatted_ads = []
            for ad in ads:
                formatted_ad = self._format_record(ad, 'ad')
                formatted_ads.append(formatted_ad)
            
            adset['children'] = formatted_ads
        
        # Add mixpanel data to adsets
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(adsets, config)
        
        # Format adsets
        formatted_adsets = []
        for adset in adsets:
            formatted_adset = self._format_record(adset, 'adset')
            if 'children' not in formatted_adset:
                formatted_adset['children'] = []
            formatted_adsets.append(formatted_adset)
        
        return formatted_adsets
    
    def _get_ad_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get ad-level data (flat, no children)"""
        
        ad_query = f"""
        SELECT 
            ad_id,
            ad_name,
            adset_id,
            adset_name,
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND ad_id IS NOT NULL
        GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date])
        
        # Add mixpanel data to ads
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(ads, config)
        
        # Format ads
        formatted_ads = []
        for ad in ads:
            formatted_ad = self._format_record(ad, 'ad')
            formatted_ads.append(formatted_ad)
        
        return formatted_ads
    
    def _execute_meta_query(self, query: str, params: List) -> List[Dict[str, Any]]:
        """Execute query against meta analytics database"""
        try:
            conn = sqlite3.connect(self.meta_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            
            conn.close()
            return data
            
        except Exception as e:
            logger.error(f"Error executing meta query: {e}")
            raise
    
    def _add_mixpanel_data_to_records(self, records: List[Dict[str, Any]], config: QueryConfig):
        """Add mixpanel metrics to records using direct attribution"""
        try:
            # Get all unique entity IDs for batch query
            campaign_ids = set()
            adset_ids = set()
            ad_ids = set()
            
            for record in records:
                if record.get('campaign_id'):
                    campaign_ids.add(record['campaign_id'])
                if record.get('adset_id'):
                    adset_ids.add(record['adset_id'])
                if record.get('ad_id'):
                    ad_ids.add(record['ad_id'])
            
            # Connect to mixpanel analytics database
            conn = sqlite3.connect(self.mixpanel_analytics_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get mixpanel metrics grouped by attribution
            mixpanel_data = {}
            
            # Campaign-level metrics
            if campaign_ids:
                campaign_placeholders = ','.join(['?' for _ in campaign_ids])
                campaign_query = f"""
                SELECT 
                    abi_campaign_id,
                    -- Trial metrics (users who started with trials)
                    COUNT(CASE WHEN current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_started,
                    COUNT(CASE WHEN current_status = 'trial_pending' THEN 1 END) as mixpanel_trials_in_progress,
                    COUNT(CASE WHEN current_status IN ('trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_ended,
                    
                    -- Purchase metrics (initial purchases + converted trials)
                    COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as mixpanel_purchases,
                    COUNT(CASE WHEN current_status = 'trial_converted' THEN 1 END) as mixpanel_converted_amount,
                    
                    -- Revenue metrics (sum of current_value attributed to credited_date)
                    SUM(CASE WHEN current_status != 'refunded' THEN current_value ELSE 0 END) as mixpanel_revenue_usd,
                    SUM(CASE WHEN current_status = 'refunded' THEN ABS(current_value) ELSE 0 END) as mixpanel_refunds_usd,
                    SUM(current_value) as estimated_revenue_usd,
                    
                    -- Net conversions (purchases minus refunds)
                    (COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) - 
                     COUNT(CASE WHEN current_status = 'refunded' THEN 1 END)) as mixpanel_conversions_net_refunds,
                    
                    -- Segment accuracy metrics
                    AVG(CASE WHEN accuracy_score IS NOT NULL THEN 
                        CASE accuracy_score 
                            WHEN 'Very High' THEN 5
                            WHEN 'High' THEN 4  
                            WHEN 'Medium' THEN 3
                            WHEN 'Low' THEN 2
                            WHEN 'Very Low' THEN 1
                            ELSE 3
                        END
                    END) as segment_accuracy_average,
                    
                    -- Conversion rates (weighted averages)
                    AVG(trial_conversion_rate) as avg_trial_conversion_rate,
                    AVG(trial_converted_to_refund_rate) as avg_trial_refund_rate,
                    AVG(initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                    
                    -- User counts for calculations
                    COUNT(DISTINCT distinct_id) as total_attributed_users
                    
                FROM user_product_metrics
                WHERE abi_campaign_id IN ({campaign_placeholders})
                  AND credited_date BETWEEN ? AND ?
                GROUP BY abi_campaign_id
                """
                
                cursor.execute(campaign_query, list(campaign_ids) + [config.start_date, config.end_date])
                for row in cursor.fetchall():
                    mixpanel_data[f"campaign_{row['abi_campaign_id']}"] = dict(row)
            
            # AdSet-level metrics
            if adset_ids:
                adset_placeholders = ','.join(['?' for _ in adset_ids])
                adset_query = f"""
                SELECT 
                    abi_ad_set_id,
                    -- Trial metrics (users who started with trials)
                    COUNT(CASE WHEN current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_started,
                    COUNT(CASE WHEN current_status = 'trial_pending' THEN 1 END) as mixpanel_trials_in_progress,
                    COUNT(CASE WHEN current_status IN ('trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_ended,
                    
                    -- Purchase metrics (initial purchases + converted trials)
                    COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as mixpanel_purchases,
                    COUNT(CASE WHEN current_status = 'trial_converted' THEN 1 END) as mixpanel_converted_amount,
                    
                    -- Revenue metrics (sum of current_value attributed to credited_date)
                    SUM(CASE WHEN current_status != 'refunded' THEN current_value ELSE 0 END) as mixpanel_revenue_usd,
                    SUM(CASE WHEN current_status = 'refunded' THEN ABS(current_value) ELSE 0 END) as mixpanel_refunds_usd,
                    SUM(current_value) as estimated_revenue_usd,
                    
                    -- Net conversions (purchases minus refunds)
                    (COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) - 
                     COUNT(CASE WHEN current_status = 'refunded' THEN 1 END)) as mixpanel_conversions_net_refunds,
                    
                    -- Segment accuracy metrics
                    AVG(CASE WHEN accuracy_score IS NOT NULL THEN 
                        CASE accuracy_score 
                            WHEN 'Very High' THEN 5
                            WHEN 'High' THEN 4  
                            WHEN 'Medium' THEN 3
                            WHEN 'Low' THEN 2
                            WHEN 'Very Low' THEN 1
                            ELSE 3
                        END
                    END) as segment_accuracy_average,
                    
                    -- Conversion rates (weighted averages)
                    AVG(trial_conversion_rate) as avg_trial_conversion_rate,
                    AVG(trial_converted_to_refund_rate) as avg_trial_refund_rate,
                    AVG(initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                    
                    -- User counts for calculations
                    COUNT(DISTINCT distinct_id) as total_attributed_users
                    
                FROM user_product_metrics
                WHERE abi_ad_set_id IN ({adset_placeholders})
                  AND credited_date BETWEEN ? AND ?
                GROUP BY abi_ad_set_id
                """
                
                cursor.execute(adset_query, list(adset_ids) + [config.start_date, config.end_date])
                for row in cursor.fetchall():
                    mixpanel_data[f"adset_{row['abi_ad_set_id']}"] = dict(row)
            
            # Ad-level metrics
            if ad_ids:
                ad_placeholders = ','.join(['?' for _ in ad_ids])
                ad_query = f"""
                SELECT 
                    abi_ad_id,
                    -- Trial metrics (users who started with trials)
                    COUNT(CASE WHEN current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_started,
                    COUNT(CASE WHEN current_status = 'trial_pending' THEN 1 END) as mixpanel_trials_in_progress,
                    COUNT(CASE WHEN current_status IN ('trial_cancelled', 'trial_converted') THEN 1 END) as mixpanel_trials_ended,
                    
                    -- Purchase metrics (initial purchases + converted trials)
                    COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as mixpanel_purchases,
                    COUNT(CASE WHEN current_status = 'trial_converted' THEN 1 END) as mixpanel_converted_amount,
                    
                    -- Revenue metrics (sum of current_value attributed to credited_date)
                    SUM(CASE WHEN current_status != 'refunded' THEN current_value ELSE 0 END) as mixpanel_revenue_usd,
                    SUM(CASE WHEN current_status = 'refunded' THEN ABS(current_value) ELSE 0 END) as mixpanel_refunds_usd,
                    SUM(current_value) as estimated_revenue_usd,
                    
                    -- Net conversions (purchases minus refunds)
                    (COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) - 
                     COUNT(CASE WHEN current_status = 'refunded' THEN 1 END)) as mixpanel_conversions_net_refunds,
                    
                    -- Segment accuracy metrics
                    AVG(CASE WHEN accuracy_score IS NOT NULL THEN 
                        CASE accuracy_score 
                            WHEN 'Very High' THEN 5
                            WHEN 'High' THEN 4  
                            WHEN 'Medium' THEN 3
                            WHEN 'Low' THEN 2
                            WHEN 'Very Low' THEN 1
                            ELSE 3
                        END
                    END) as segment_accuracy_average,
                    
                    -- Conversion rates (weighted averages)
                    AVG(trial_conversion_rate) as avg_trial_conversion_rate,
                    AVG(trial_converted_to_refund_rate) as avg_trial_refund_rate,
                    AVG(initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                    
                    -- User counts for calculations
                    COUNT(DISTINCT distinct_id) as total_attributed_users
                    
                FROM user_product_metrics
                WHERE abi_ad_id IN ({ad_placeholders})
                  AND credited_date BETWEEN ? AND ?
                GROUP BY abi_ad_id
                """
                
                cursor.execute(ad_query, list(ad_ids) + [config.start_date, config.end_date])
                for row in cursor.fetchall():
                    mixpanel_data[f"ad_{row['abi_ad_id']}"] = dict(row)
            
            conn.close()
            
            # Merge mixpanel data with records
            for record in records:
                # Try to find mixpanel data for this record
                mixpanel_metrics = None
                
                if record.get('ad_id'):
                    mixpanel_metrics = mixpanel_data.get(f"ad_{record['ad_id']}")
                elif record.get('adset_id'):
                    mixpanel_metrics = mixpanel_data.get(f"adset_{record['adset_id']}")
                elif record.get('campaign_id'):
                    mixpanel_metrics = mixpanel_data.get(f"campaign_{record['campaign_id']}")
                
                if mixpanel_metrics:
                    record.update(mixpanel_metrics)
                else:
                    # Set default values for all metrics
                    record.update({
                        'mixpanel_trials_started': 0,
                        'mixpanel_trials_in_progress': 0,
                        'mixpanel_trials_ended': 0,
                        'mixpanel_purchases': 0,
                        'mixpanel_converted_amount': 0,
                        'mixpanel_revenue_usd': 0.0,
                        'mixpanel_refunds_usd': 0.0,
                        'estimated_revenue_usd': 0.0,
                        'mixpanel_conversions_net_refunds': 0,
                        'segment_accuracy_average': None,
                        'avg_trial_conversion_rate': 0.0,
                        'avg_trial_refund_rate': 0.0,
                        'avg_purchase_refund_rate': 0.0,
                        'total_attributed_users': 0
                    })
            
        except Exception as e:
            logger.error(f"Error adding mixpanel data: {e}")
            # Set default values for all records on error
            for record in records:
                record.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_trials_in_progress': 0,
                    'mixpanel_trials_ended': 0,
                    'mixpanel_purchases': 0,
                    'mixpanel_converted_amount': 0,
                    'mixpanel_revenue_usd': 0.0,
                    'mixpanel_refunds_usd': 0.0,
                    'estimated_revenue_usd': 0.0,
                    'mixpanel_conversions_net_refunds': 0,
                    'segment_accuracy_average': None,
                    'avg_trial_conversion_rate': 0.0,
                    'avg_trial_refund_rate': 0.0,
                    'avg_purchase_refund_rate': 0.0,
                    'total_attributed_users': 0
                })
    
    def _format_record(self, record: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
        """Format a record with the expected structure for the frontend"""
        
        # Create unique ID based on entity type
        if entity_type == 'campaign':
            record_id = f"campaign_{record.get('campaign_id', 'unknown')}"
            name = record.get('campaign_name', 'Unknown Campaign')
        elif entity_type == 'adset':
            record_id = f"adset_{record.get('adset_id', 'unknown')}"
            name = record.get('adset_name', 'Unknown Ad Set')
        else:  # ad
            record_id = f"ad_{record.get('ad_id', 'unknown')}"
            name = record.get('ad_name', 'Unknown Ad')
        
        # Helper function to format accuracy score
        def format_accuracy_score(avg_score):
            if not avg_score:
                return "Unknown"
            if avg_score >= 4.5:
                return "Very High"
            elif avg_score >= 3.5:
                return "High"
            elif avg_score >= 2.5:
                return "Medium"
            elif avg_score >= 1.5:
                return "Low"
            else:
                return "Very Low"
        
        # Build the formatted record with all expected fields
        formatted = {
            'id': record_id,
            'type': entity_type,
            'name': name,
            'campaign_name': record.get('campaign_name', ''),
            'adset_name': record.get('adset_name', ''),
            'ad_name': record.get('ad_name', ''),
            
            # Meta metrics (already aggregated)
            'spend': float(record.get('spend', 0) or 0),
            'impressions': int(record.get('impressions', 0) or 0),
            'clicks': int(record.get('clicks', 0) or 0),
            'meta_trials_started': int(record.get('meta_trials_started', 0) or 0),
            'meta_purchases': int(record.get('meta_purchases', 0) or 0),
            
            # Mixpanel trial metrics
            'mixpanel_trials_started': int(record.get('mixpanel_trials_started', 0) or 0),
            'mixpanel_trials_in_progress': int(record.get('mixpanel_trials_in_progress', 0) or 0),
            'mixpanel_trials_ended': int(record.get('mixpanel_trials_ended', 0) or 0),
            
            # Mixpanel purchase metrics
            'mixpanel_purchases': int(record.get('mixpanel_purchases', 0) or 0),
            'mixpanel_converted_amount': int(record.get('mixpanel_converted_amount', 0) or 0),
            'mixpanel_conversions_net_refunds': int(record.get('mixpanel_conversions_net_refunds', 0) or 0),
            
            # Mixpanel revenue metrics
            'mixpanel_revenue_usd': float(record.get('mixpanel_revenue_usd', 0) or 0),
            'mixpanel_refunds_usd': float(record.get('mixpanel_refunds_usd', 0) or 0),
            'estimated_revenue_usd': float(record.get('estimated_revenue_usd', 0) or 0),
            
            # Segment accuracy
            'segment_accuracy_average': format_accuracy_score(record.get('segment_accuracy_average')),
            
            # Attribution stats
            'total_attributed_users': int(record.get('total_attributed_users', 0) or 0),
        }
        
        # Calculate derived metrics according to spec (all limited to 2 decimal places)
        spend = formatted['spend']
        mixpanel_trials = formatted['mixpanel_trials_started']
        mixpanel_purchases = formatted['mixpanel_purchases']
        meta_trials = formatted['meta_trials_started']
        meta_purchases = formatted['meta_purchases']
        clicks = formatted['clicks']
        
        # Accuracy ratios (as percentages, limited to 2 decimal places)
        if meta_trials > 0:
            formatted['trial_accuracy_ratio'] = round((mixpanel_trials / meta_trials) * 100, 2)
        else:
            formatted['trial_accuracy_ratio'] = 0.0
            
        if meta_purchases > 0:
            formatted['purchase_accuracy_ratio'] = round((mixpanel_purchases / meta_purchases) * 100, 2)
        else:
            formatted['purchase_accuracy_ratio'] = 0.0
        
        # ROAS calculation with trial accuracy ratio adjustment
        if spend > 0 and formatted['trial_accuracy_ratio'] > 0:
            # Adjust estimated revenue by dividing by trial accuracy ratio (to account for Meta/Mixpanel dropoff)
            # ROAS = (estimated_revenue / trial_accuracy_ratio) / spend
            adjusted_revenue = formatted['estimated_revenue_usd'] / (formatted['trial_accuracy_ratio'] / 100)
            formatted['estimated_roas'] = round(adjusted_revenue / spend, 2)
        elif spend > 0:
            # Fallback to standard calculation if no trial accuracy ratio
            formatted['estimated_roas'] = round(formatted['estimated_revenue_usd'] / spend, 2)
        else:
            formatted['estimated_roas'] = 0.0
        
        # Cost calculations (limited to 2 decimal places)
        if mixpanel_trials > 0:
            formatted['mixpanel_cost_per_trial'] = round(spend / mixpanel_trials, 2)
        else:
            formatted['mixpanel_cost_per_trial'] = 0.0
            
        if mixpanel_purchases > 0:
            formatted['mixpanel_cost_per_purchase'] = round(spend / mixpanel_purchases, 2)
        else:
            formatted['mixpanel_cost_per_purchase'] = 0.0
            
        if meta_trials > 0:
            formatted['meta_cost_per_trial'] = round(spend / meta_trials, 2)
        else:
            formatted['meta_cost_per_trial'] = 0.0
            
        if meta_purchases > 0:
            formatted['meta_cost_per_purchase'] = round(spend / meta_purchases, 2)
        else:
            formatted['meta_cost_per_purchase'] = 0.0
        
        # Rate calculations (limited to 2 decimal places)
        if clicks > 0:
            formatted['click_to_trial_rate'] = round((mixpanel_trials / clicks) * 100, 2)
        else:
            formatted['click_to_trial_rate'] = 0.0
        
        # CONVERSION RATES: Pull directly from database (DO NOT calculate)
        # These come from the segment matcher and are already percentages in the database
        formatted['trial_conversion_rate'] = round(float(record.get('avg_trial_conversion_rate', 0) or 0) * 100, 2)
        formatted['trial_to_purchase_rate'] = formatted['trial_conversion_rate']  # Same metric
        formatted['avg_trial_refund_rate'] = round(float(record.get('avg_trial_refund_rate', 0) or 0) * 100, 2)
        formatted['purchase_refund_rate'] = round(float(record.get('avg_purchase_refund_rate', 0) or 0) * 100, 2)
        
        # Profit calculation (limited to 2 decimal places)
        formatted['profit'] = round(formatted['estimated_revenue_usd'] - spend, 2)
        
        # CRITICAL: Preserve children array if it exists (for hierarchical structure)
        if 'children' in record:
            formatted['children'] = record['children']
        
        return formatted
    
    def get_chart_data(self, config: QueryConfig, entity_type: str, entity_id: str) -> Dict[str, Any]:
        """Get detailed daily metrics for sparkline charts - combines Meta and Mixpanel data by date"""
        try:
            table_name = self.get_table_name(config.breakdown)
            
            # Build WHERE clause for Meta data based on entity type
            if entity_type == 'campaign':
                meta_where = "campaign_id = ?"
                mixpanel_attr_field = "abi_campaign_id"
            elif entity_type == 'adset':
                meta_where = "adset_id = ?"
                mixpanel_attr_field = "abi_ad_set_id"
            elif entity_type == 'ad':
                meta_where = "ad_id = ?"
                mixpanel_attr_field = "abi_ad_id"
            else:
                raise ValueError(f"Invalid entity_type: {entity_type}")
            
            # Get daily Meta data
            meta_query = f"""
            SELECT date,
                   SUM(spend) as daily_spend,
                   SUM(impressions) as daily_impressions,
                   SUM(clicks) as daily_clicks,
                   SUM(meta_trials) as daily_meta_trials,
                   SUM(meta_purchases) as daily_meta_purchases
            FROM {table_name}
            WHERE {meta_where} AND date BETWEEN ? AND ?
            GROUP BY date
            ORDER BY date ASC
            """
            
            meta_data = self._execute_meta_query(meta_query, [entity_id, config.start_date, config.end_date])
            
            # Get daily Mixpanel data (attributed to credited_date)
            mixpanel_conn = sqlite3.connect(self.mixpanel_analytics_db_path)
            mixpanel_conn.row_factory = sqlite3.Row
            
            mixpanel_query = f"""
            SELECT 
                credited_date as date,
                -- Trial metrics by credited date
                COUNT(CASE WHEN current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as daily_mixpanel_trials,
                
                -- Purchase metrics by credited date
                COUNT(CASE WHEN current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as daily_mixpanel_purchases,
                COUNT(CASE WHEN current_status = 'trial_converted' THEN 1 END) as daily_mixpanel_conversions,
                
                -- Revenue metrics by credited date (sum of current_value attributed to this date)
                SUM(CASE WHEN current_status != 'refunded' THEN current_value ELSE 0 END) as daily_mixpanel_revenue,
                SUM(CASE WHEN current_status = 'refunded' THEN ABS(current_value) ELSE 0 END) as daily_mixpanel_refunds,
                SUM(current_value) as daily_estimated_revenue,
                
                -- User count for statistical significance
                COUNT(DISTINCT distinct_id) as daily_attributed_users
                
            FROM user_product_metrics
            WHERE {mixpanel_attr_field} = ? 
              AND credited_date BETWEEN ? AND ?
            GROUP BY credited_date
            ORDER BY credited_date ASC
            """
            
            cursor = mixpanel_conn.cursor()
            cursor.execute(mixpanel_query, [entity_id, config.start_date, config.end_date])
            mixpanel_data = [dict(row) for row in cursor.fetchall()]
            mixpanel_conn.close()
            
            # Merge Meta and Mixpanel data by date
            daily_data = {}
            
            # Add Meta data
            for row in meta_data:
                date = row['date']
                daily_data[date] = {
                    'date': date,
                    'daily_spend': float(row.get('daily_spend', 0) or 0),
                    'daily_impressions': int(row.get('daily_impressions', 0) or 0),
                    'daily_clicks': int(row.get('daily_clicks', 0) or 0),
                    'daily_meta_trials': int(row.get('daily_meta_trials', 0) or 0),
                    'daily_meta_purchases': int(row.get('daily_meta_purchases', 0) or 0),
                    # Initialize Mixpanel fields
                    'daily_mixpanel_trials': 0,
                    'daily_mixpanel_purchases': 0,
                    'daily_mixpanel_conversions': 0,
                    'daily_mixpanel_revenue': 0.0,
                    'daily_mixpanel_refunds': 0.0,
                    'daily_estimated_revenue': 0.0,
                    'daily_attributed_users': 0
                }
            
            # Add Mixpanel data
            for row in mixpanel_data:
                date = row['date']
                if date in daily_data:
                    # Update existing date with Mixpanel data
                    daily_data[date].update({
                        'daily_mixpanel_trials': int(row.get('daily_mixpanel_trials', 0) or 0),
                        'daily_mixpanel_purchases': int(row.get('daily_mixpanel_purchases', 0) or 0),
                        'daily_mixpanel_conversions': int(row.get('daily_mixpanel_conversions', 0) or 0),
                        'daily_mixpanel_revenue': float(row.get('daily_mixpanel_revenue', 0) or 0),
                        'daily_mixpanel_refunds': float(row.get('daily_mixpanel_refunds', 0) or 0),
                        'daily_estimated_revenue': float(row.get('daily_estimated_revenue', 0) or 0),
                        'daily_attributed_users': int(row.get('daily_attributed_users', 0) or 0)
                    })
                else:
                    # Create new date entry (Mixpanel data without Meta data)
                    daily_data[date] = {
                        'date': date,
                        'daily_spend': 0.0,
                        'daily_impressions': 0,
                        'daily_clicks': 0,
                        'daily_meta_trials': 0,
                        'daily_meta_purchases': 0,
                        'daily_mixpanel_trials': int(row.get('daily_mixpanel_trials', 0) or 0),
                        'daily_mixpanel_purchases': int(row.get('daily_mixpanel_purchases', 0) or 0),
                        'daily_mixpanel_conversions': int(row.get('daily_mixpanel_conversions', 0) or 0),
                        'daily_mixpanel_revenue': float(row.get('daily_mixpanel_revenue', 0) or 0),
                        'daily_mixpanel_refunds': float(row.get('daily_mixpanel_refunds', 0) or 0),
                        'daily_estimated_revenue': float(row.get('daily_estimated_revenue', 0) or 0),
                        'daily_attributed_users': int(row.get('daily_attributed_users', 0) or 0)
                    }
            
            # Calculate OVERALL accuracy ratio for the entire period (trials + purchases combined)
            total_meta_conversions = sum(d['daily_meta_trials'] + d['daily_meta_purchases'] for d in daily_data.values())
            total_mixpanel_conversions = sum(d['daily_mixpanel_trials'] + d['daily_mixpanel_purchases'] for d in daily_data.values())
            
            # Calculate the period-wide accuracy ratio
            overall_accuracy_ratio = 0.0
            if total_meta_conversions > 0:
                overall_accuracy_ratio = total_mixpanel_conversions / total_meta_conversions
            
            # Calculate daily derived metrics (ROAS, etc.)
            chart_data = []
            for date in sorted(daily_data.keys()):
                day_data = daily_data[date]
                
                # Calculate daily ROAS using the overall period accuracy ratio
                if day_data['daily_spend'] > 0:
                    # Base revenue from Mixpanel for this day
                    base_revenue = day_data['daily_estimated_revenue']
                    
                    # Apply overall accuracy ratio adjustment
                    if overall_accuracy_ratio > 0 and overall_accuracy_ratio != 1.0:
                        # Divide by accuracy ratio to account for Meta/Mixpanel discrepancy
                        # E.g., if Mixpanel shows 50% of Meta conversions, divide revenue by 0.5 (multiply by 2)
                        adjusted_revenue = base_revenue / overall_accuracy_ratio
                        day_data['daily_roas'] = round(adjusted_revenue / day_data['daily_spend'], 2)
                    else:
                        # Use standard calculation if no valid accuracy ratio
                        day_data['daily_roas'] = round(base_revenue / day_data['daily_spend'], 2)
                else:
                    day_data['daily_roas'] = 0.0
                
                # Store the accuracy ratio for debugging
                day_data['period_accuracy_ratio'] = overall_accuracy_ratio
                
                # Calculate daily profit (limited to 2 decimal places)
                day_data['daily_profit'] = round(day_data['daily_estimated_revenue'] - day_data['daily_spend'], 2)
                
                # For sparkline coloring: use conversion count for statistical significance
                day_data['conversions_for_coloring'] = day_data['daily_mixpanel_conversions']
                
                chart_data.append(day_data)
            
            return {
                'success': True,
                'chart_data': chart_data,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'date_range': f"{config.start_date} to {config.end_date}",
                'total_days': len(chart_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting chart data: {e}")
            return {
                'success': False,
                'error': str(e)
            } 