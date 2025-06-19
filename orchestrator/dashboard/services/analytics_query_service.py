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
project_root = Path(__file__).parent.parent.parent.parent
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
        Execute analytics query with comprehensive error handling and fallback logic
        """
        try:
            logger.info(f"🔍 EXECUTE_ANALYTICS_QUERY CALLED - NEW CODE VERSION")
            # Get the appropriate table name based on breakdown
            table_name = self.get_table_name(config.breakdown)
            logger.info(f"🔍 Table name: {table_name}")
            
            # Check if Meta ad performance tables have data
            meta_data_count = self._get_meta_data_count(table_name)
            logger.info(f"🔍 Meta data count: {meta_data_count}")
            
            if meta_data_count == 0:
                logger.info(f"🔍 No data found in {table_name}, falling back to Mixpanel-only data")
                return self._execute_mixpanel_only_query(config)
            
            # Original logic for when Meta data exists
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
    
    def _get_meta_data_count(self, table_name: str) -> int:
        """Check if Meta ad performance table has any data"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self._execute_meta_query(query, [])
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Error checking Meta data count: {e}")
            return 0
    
    def _execute_mixpanel_only_query(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute analytics query using only Mixpanel data when Meta ad performance tables are empty
        """
        try:
            # Get data directly from Mixpanel tables
            if config.group_by == 'campaign':
                structured_data = self._get_mixpanel_campaign_data(config)
            elif config.group_by == 'adset':
                structured_data = self._get_mixpanel_adset_data(config)
            else:  # ad level
                structured_data = self._get_mixpanel_ad_data(config)
            
            return {
                'success': True,
                'data': structured_data,
                'metadata': {
                    'query_config': config.__dict__,
                    'table_used': 'mixpanel_user + mixpanel_event (Mixpanel-only mode)',
                    'record_count': len(structured_data),
                    'date_range': f"{config.start_date} to {config.end_date}",
                    'generated_at': datetime.now().isoformat(),
                    'data_source': 'mixpanel_only'
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing Mixpanel-only query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': datetime.now().isoformat()
                }
            }
    
    def _get_mixpanel_campaign_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get campaign-level data from Mixpanel only - CORRECTED to use user_product_metrics for revenue"""
        
        # Get campaign-level event data (trials and purchases)
        campaign_event_query = """
        SELECT 
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_campaign_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_campaign_id
        ORDER BY e.abi_campaign_id
        """
        
        # Get campaign-level revenue data from user_product_metrics with JOIN (CORRECTED)
        campaign_revenue_query = """
        SELECT 
            e.abi_campaign_id as campaign_id,
            SUM(upm.current_value) as estimated_revenue_usd
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_campaign_id IS NOT NULL
          AND upm.credited_date BETWEEN ? AND ?
        GROUP BY e.abi_campaign_id
        """
        
        # Execute event query
        event_params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date   # main user filter
        ]
        
        campaigns = self._execute_mixpanel_query(campaign_event_query, event_params)
        
        # Execute revenue query
        revenue_params = [
            config.start_date, config.end_date   # credited_date filter
        ]
        
        revenue_data = self._execute_mixpanel_query(campaign_revenue_query, revenue_params)
        
        # Create revenue lookup map
        revenue_map = {row['campaign_id']: row['estimated_revenue_usd'] for row in revenue_data}
        
        # Format campaigns with default Meta values and CORRECTED revenue
        formatted_campaigns = []
        for campaign in campaigns:
            campaign_id = campaign['campaign_id']
            estimated_revenue = float(revenue_map.get(campaign_id, 0))
            
            formatted_campaign = {
                'id': f"campaign_{campaign['campaign_id']}",
                'entity_type': 'campaign',
                'campaign_id': campaign['campaign_id'],
                'campaign_name': campaign['campaign_name'] or 'Unknown Campaign',
                'name': campaign['campaign_name'] or 'Unknown Campaign',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(campaign['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(campaign['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': estimated_revenue,  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(campaign['total_users'] or 0),
                'new_users': int(campaign['new_users'] or 0),
                'children': []
            }
            formatted_campaigns.append(formatted_campaign)
        
        # Sort by revenue (descending)
        formatted_campaigns.sort(key=lambda x: x['estimated_revenue_usd'], reverse=True)
        
        return formatted_campaigns
    
    def _get_mixpanel_adset_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get adset-level data from Mixpanel only - CORRECTED to use user_product_metrics for revenue"""
        
        # Get adset-level event data (trials and purchases)
        adset_event_query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY e.abi_ad_set_id
        """
        
        # Get adset-level revenue data from user_product_metrics with JOIN (CORRECTED)
        adset_revenue_query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            SUM(upm.current_value) as estimated_revenue_usd
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id IS NOT NULL
          AND upm.credited_date BETWEEN ? AND ?
        GROUP BY e.abi_ad_set_id
        """
        
        # Execute event query
        event_params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date   # main user filter
        ]
        
        adsets = self._execute_mixpanel_query(adset_event_query, event_params)
        
        # Execute revenue query
        revenue_params = [
            config.start_date, config.end_date   # credited_date filter
        ]
        
        revenue_data = self._execute_mixpanel_query(adset_revenue_query, revenue_params)
        
        # Create revenue lookup map
        revenue_map = {row['adset_id']: row['estimated_revenue_usd'] for row in revenue_data}
        
        # Format adsets with default Meta values and CORRECTED revenue
        formatted_adsets = []
        for adset in adsets:
            adset_id = adset['adset_id']
            estimated_revenue = float(revenue_map.get(adset_id, 0))
            
            formatted_adset = {
                'id': f"adset_{adset['adset_id']}",
                'entity_type': 'adset',
                'adset_id': adset['adset_id'],
                'adset_name': adset['adset_name'] or 'Unknown Adset',
                'campaign_id': adset['campaign_id'],
                'campaign_name': adset['campaign_name'] or 'Unknown Campaign',
                'name': adset['adset_name'] or 'Unknown Adset',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(adset['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(adset['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': estimated_revenue,  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(adset['total_users'] or 0),
                'new_users': int(adset['new_users'] or 0),
                'children': []
            }
            formatted_adsets.append(formatted_adset)
        
        # Sort by revenue (descending)
        formatted_adsets.sort(key=lambda x: x['estimated_revenue_usd'], reverse=True)
        
        return formatted_adsets
    
    def _get_mixpanel_ad_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get ad-level data from Mixpanel only - CORRECTED to use user_product_metrics for revenue"""
        
        # Get ad-level event data (trials and purchases)
        ad_event_query = """
        SELECT 
            u.abi_ad_id as ad_id,
            'Unknown Ad' as ad_name,
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY u.abi_ad_id, e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY u.abi_ad_id
        """
        
        # Get ad-level revenue data from user_product_metrics with JOIN (CORRECTED)
        ad_revenue_query = """
        SELECT 
            u.abi_ad_id as ad_id,
            SUM(upm.current_value) as estimated_revenue_usd
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.abi_ad_id IS NOT NULL
          AND upm.credited_date BETWEEN ? AND ?
        GROUP BY u.abi_ad_id
        """
        
        # Execute event query
        event_params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date   # main user filter
        ]
        
        ads = self._execute_mixpanel_query(ad_event_query, event_params)
        
        # Execute revenue query
        revenue_params = [
            config.start_date, config.end_date   # credited_date filter
        ]
        
        revenue_data = self._execute_mixpanel_query(ad_revenue_query, revenue_params)
        
        # Create revenue lookup map
        revenue_map = {row['ad_id']: row['estimated_revenue_usd'] for row in revenue_data}
        
        # Format ads with default Meta values and CORRECTED revenue
        formatted_ads = []
        for ad in ads:
            ad_id = ad['ad_id']
            estimated_revenue = float(revenue_map.get(ad_id, 0))
            
            formatted_ad = {
                'id': f"ad_{ad['ad_id']}",
                'entity_type': 'ad',
                'ad_id': ad['ad_id'],
                'ad_name': ad['ad_name'] or 'Unknown Ad',
                'adset_id': ad['adset_id'],
                'adset_name': ad['adset_name'] or 'Unknown Adset',
                'campaign_id': ad['campaign_id'],
                'campaign_name': ad['campaign_name'] or 'Unknown Campaign',
                'name': ad['ad_name'] or 'Unknown Ad',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(ad['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(ad['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # CORRECTED: Use revenue from user_product_metrics
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': estimated_revenue,  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(ad['total_users'] or 0),
                'new_users': int(ad['new_users'] or 0),
                'children': []
            }
            formatted_ads.append(formatted_ad)
        
        # Sort by revenue (descending)
        formatted_ads.sort(key=lambda x: x['estimated_revenue_usd'], reverse=True)
        
        return formatted_ads
    
    def _execute_mixpanel_query(self, query: str, params: List) -> List[Dict[str, Any]]:
        """Execute query against Mixpanel database"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                return results
                
        except Exception as e:
            logger.error(f"Error executing Mixpanel query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
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
            
            # Format adsets and aggregate Mixpanel data from ad children
            formatted_adsets = []
            for adset in adsets:
                # Initialize adset Mixpanel fields to 0
                adset.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_purchases': 0,
                    'mixpanel_revenue_usd': 0.0,
                    'estimated_revenue_usd': 0.0,
                    'total_attributed_users': 0
                })
                
                # Aggregate from formatted ad children
                for ad in adset.get('children', []):
                    adset['mixpanel_trials_started'] += ad.get('mixpanel_trials_started', 0)
                    adset['mixpanel_purchases'] += ad.get('mixpanel_purchases', 0)
                    adset['mixpanel_revenue_usd'] += ad.get('mixpanel_revenue_usd', 0)
                    adset['estimated_revenue_usd'] += ad.get('estimated_revenue_usd', 0)
                    adset['total_attributed_users'] += ad.get('total_attributed_users', 0)
                
                formatted_adset = self._format_record(adset, 'adset')
                if 'children' not in formatted_adset:
                    formatted_adset['children'] = []
                formatted_adsets.append(formatted_adset)
            
            campaign['children'] = formatted_adsets
        
        # Aggregate Mixpanel data from adset children to campaigns
        for campaign in campaigns:
            # Initialize campaign Mixpanel fields to 0
            campaign.update({
                'mixpanel_trials_started': 0,
                'mixpanel_purchases': 0,
                'mixpanel_revenue_usd': 0.0,
                'estimated_revenue_usd': 0.0,
                'total_attributed_users': 0
            })
            
            # Aggregate from adset children
            for adset in campaign.get('children', []):
                campaign['mixpanel_trials_started'] += adset.get('mixpanel_trials_started', 0)
                campaign['mixpanel_purchases'] += adset.get('mixpanel_purchases', 0)
                campaign['mixpanel_revenue_usd'] += adset.get('mixpanel_revenue_usd', 0)
                campaign['estimated_revenue_usd'] += adset.get('estimated_revenue_usd', 0)
                campaign['total_attributed_users'] += adset.get('total_attributed_users', 0)
        
        # Format campaigns (now with aggregated Mixpanel data from children)
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
        
        # Aggregate Mixpanel data from ad children to adsets
        for adset in adsets:
            # Initialize adset Mixpanel fields to 0
            adset.update({
                'mixpanel_trials_started': 0,
                'mixpanel_purchases': 0,
                'mixpanel_revenue_usd': 0.0,
                'estimated_revenue_usd': 0.0,
                'total_attributed_users': 0
            })
            
            # Aggregate from formatted ad children
            for ad in adset.get('children', []):
                adset['mixpanel_trials_started'] += ad.get('mixpanel_trials_started', 0)
                adset['mixpanel_purchases'] += ad.get('mixpanel_purchases', 0)
                adset['mixpanel_revenue_usd'] += ad.get('mixpanel_revenue_usd', 0)
                adset['estimated_revenue_usd'] += ad.get('estimated_revenue_usd', 0)
                adset['total_attributed_users'] += ad.get('total_attributed_users', 0)
        
        # Format adsets (now with aggregated Mixpanel data from children)
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
        """
        Add mixpanel metrics to records using CORRECTED revenue calculation from user_product_metrics table.
        Trial/Purchase counts from events, but revenue from user_product_metrics.current_value by credited_date.
        """
        if not config.include_mixpanel or not records:
            # Initialize Mixpanel fields to 0 if not included
            def initialize_mixpanel_fields(record):
                record.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_purchases': 0,
                    'mixpanel_revenue_usd': 0.0,
                    'estimated_revenue_usd': 0.0,
                    'trial_accuracy_ratio': 0.0,
                    'total_attributed_users': 0
                })
                if 'children' in record:
                    for child in record['children']:
                        initialize_mixpanel_fields(child)
            
            for record in records:
                initialize_mixpanel_fields(record)
            return

        logger.info(f"🔍 Adding Mixpanel data to {len(records)} records using CORRECTED revenue calculation...")
        logger.info(f"📅 Date range: {config.start_date} to {config.end_date}")
        logger.info(f"🎯 Group by: {config.group_by}, Breakdown: {config.breakdown}")

        # DEBUG: Log the structure of records to understand the hierarchy
        def debug_record_structure(items, level=0):
            indent = "  " * level
            for i, item in enumerate(items):
                item_type = "UNKNOWN"
                item_id = "NO_ID"
                
                if item.get('campaign_id'):
                    item_type = "CAMPAIGN"
                    item_id = item['campaign_id']
                elif item.get('adset_id'):
                    item_type = "ADSET"  
                    item_id = item['adset_id']
                elif item.get('ad_id'):
                    item_type = "AD"
                    item_id = item['ad_id']
                
                children_count = len(item.get('children', []))
                logger.info(f"{indent}📊 [{i}] {item_type}: {item_id} (children: {children_count})")
                
                if 'children' in item and item['children']:
                    debug_record_structure(item['children'], level + 1)
        
        logger.info("🔍 DEBUG: Record structure:")
        debug_record_structure(records)

        # Step 1: Collect all unique ad_ids from the entire hierarchy
        all_ad_ids = set()
        def collect_ad_ids(items):
            for item in items:
                if item.get('ad_id'):
                    all_ad_ids.add(item['ad_id'])
                    logger.debug(f"🎯 Found ad_id: {item['ad_id']}")
                if item.get('children'):
                    collect_ad_ids(item['children'])
        
        collect_ad_ids(records)
        logger.info(f"🎯 Collected {len(all_ad_ids)} unique ad_ids from records")
        
        if len(all_ad_ids) > 0:
            # Log first few ad_ids for verification
            sample_ads = list(all_ad_ids)[:5]
            logger.info(f"📝 Sample ad_ids: {sample_ads}")

        if not all_ad_ids:
            logger.warning("❌ No ad_ids found in records to query Mixpanel with. Skipping Mixpanel enrichment.")
            logger.warning("💡 This usually means:")
            logger.warning("   1. No Meta data for the selected date range")
            logger.warning("   2. Query is grouped at campaign/adset level without drilling down to ads")
            logger.warning("   3. Meta data structure is missing ad-level records")
            return

        # Step 2A: Get trial and purchase counts from events (KEEP EXISTING WORKING LOGIC)
        event_data_map = {}
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                ad_placeholders = ','.join(['?' for _ in all_ad_ids])
                # Query events for trial and purchase counts (keeping existing working logic)
                events_query = f"""
                SELECT 
                    u.abi_ad_id,
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started,
                    COUNT(DISTINCT CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_purchases,
                    COUNT(DISTINCT u.distinct_id) as total_attributed_users
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_ad_id IN ({ad_placeholders})
                  AND u.has_abi_attribution = TRUE
                GROUP BY u.abi_ad_id
                """
                
                events_params = [
                    config.start_date, config.end_date,  # trial date filter
                    config.start_date, config.end_date,  # purchase date filter
                    *list(all_ad_ids)
                ]
                
                logger.info(f"🔍 Executing Mixpanel events query for {len(all_ad_ids)} ad_ids")
                cursor.execute(events_query, events_params)
                
                results = cursor.fetchall()
                logger.info(f"🎯 Events query returned {len(results)} rows")
                
                for row in results:
                    ad_id = row['abi_ad_id']
                    event_data_map[ad_id] = dict(row)
        
        except Exception as e:
            logger.error(f"Error fetching Mixpanel event data: {e}", exc_info=True)
        
        # Step 2B: Get CORRECTED revenue from user_product_metrics by credited_date (LIKE SPARKLINE CODE)
        revenue_data_map = {}
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                ad_placeholders = ','.join(['?' for _ in all_ad_ids])
                # CORRECTED revenue query using JOIN between user_product_metrics and mixpanel_user
                revenue_query = f"""
                SELECT 
                    u.abi_ad_id,
                    SUM(upm.current_value) as estimated_revenue_usd
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_ad_id IN ({ad_placeholders})
                  AND upm.credited_date BETWEEN ? AND ?
                GROUP BY u.abi_ad_id
                """
                
                revenue_params = [
                    *list(all_ad_ids),
                    config.start_date, config.end_date  # credited_date filter
                ]
                
                logger.info(f"🔍 Executing CORRECTED revenue query using user_product_metrics and credited_date")
                cursor.execute(revenue_query, revenue_params)
                
                results = cursor.fetchall()
                logger.info(f"💰 Revenue query returned {len(results)} rows")
                
                for row in results:
                    ad_id = row['abi_ad_id']
                    revenue_data_map[ad_id] = dict(row)
                    logger.debug(f"💰 Ad {ad_id}: ${row['estimated_revenue_usd']:.2f} from user_product_metrics")
        
        except Exception as e:
            logger.error(f"Error fetching Mixpanel revenue data: {e}", exc_info=True)

        # Step 3: Combine event and revenue data
        mixpanel_data_map = {}
        for ad_id in all_ad_ids:
            event_data = event_data_map.get(ad_id, {})
            revenue_data = revenue_data_map.get(ad_id, {})
            
            mixpanel_data_map[ad_id] = {
                'mixpanel_trials_started': event_data.get('mixpanel_trials_started', 0),
                'mixpanel_purchases': event_data.get('mixpanel_purchases', 0),
                'total_attributed_users': event_data.get('total_attributed_users', 0),
                'estimated_revenue_usd': revenue_data.get('estimated_revenue_usd', 0.0)
            }
        
        logger.info(f"✅ Successfully combined event and revenue data for {len(mixpanel_data_map)} ads")

        # Step 4: Apply data to records (keep existing aggregation logic)
        def process_and_aggregate(items):
            for item in items:
                # Initialize Mixpanel metrics to 0
                item.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_purchases': 0,
                    'mixpanel_revenue_usd': 0.0,
                    'estimated_revenue_usd': 0.0,
                    'total_attributed_users': 0
                })

                if item.get('ad_id'):
                    # It's an ad, get data directly from the combined map
                    ad_metrics = mixpanel_data_map.get(item['ad_id'])
                    if ad_metrics:
                        item.update({
                            'mixpanel_trials_started': int(ad_metrics.get('mixpanel_trials_started', 0)),
                            'mixpanel_purchases': int(ad_metrics.get('mixpanel_purchases', 0)),
                            'mixpanel_revenue_usd': float(ad_metrics.get('estimated_revenue_usd', 0)),  # Use corrected revenue
                            'estimated_revenue_usd': float(ad_metrics.get('estimated_revenue_usd', 0)),
                            'total_attributed_users': int(ad_metrics.get('total_attributed_users', 0))
                        })
                        logger.debug(f"✅ Ad {item['ad_id']}: {item['mixpanel_trials_started']} trials, {item['mixpanel_purchases']} purchases, ${item['estimated_revenue_usd']:.2f} revenue")
                    else:
                        logger.debug(f"❌ Ad {item['ad_id']}: No Mixpanel data found")
                        
                elif 'children' in item and item['children']:
                    # It's a campaign or adset, process children first then aggregate
                    process_and_aggregate(item['children'])
                    
                    # Aggregate metrics from children
                    for child in item['children']:
                        item['mixpanel_trials_started'] += child.get('mixpanel_trials_started', 0)
                        item['mixpanel_purchases'] += child.get('mixpanel_purchases', 0)
                        item['mixpanel_revenue_usd'] += child.get('mixpanel_revenue_usd', 0)
                        item['estimated_revenue_usd'] += child.get('estimated_revenue_usd', 0)
                        item['total_attributed_users'] += child.get('total_attributed_users', 0)
                    
                    entity_type = 'campaign' if item.get('campaign_id') else 'adset'
                    entity_id = item.get('campaign_id') or item.get('adset_id')
                    logger.debug(f"✅ {entity_type.title()} {entity_id}: Aggregated from {len(item['children'])} children - {item['mixpanel_trials_started']} trials, {item['mixpanel_purchases']} purchases, ${item['estimated_revenue_usd']:.2f} revenue")

                # Add derived metrics
                meta_trials = item.get('meta_trials_started', 0)
                mixpanel_trials = item.get('mixpanel_trials_started', 0)
                item['trial_accuracy_ratio'] = (mixpanel_trials / meta_trials) * 100 if meta_trials > 0 else 0.0
        
        process_and_aggregate(records)
        
        # Log final summary
        total_trials = sum(record.get('mixpanel_trials_started', 0) for record in records)
        total_purchases = sum(record.get('mixpanel_purchases', 0) for record in records)
        total_revenue = sum(record.get('estimated_revenue_usd', 0) for record in records)
        logger.info(f"🎯 FINAL: Added Mixpanel data totaling {total_trials} trials, {total_purchases} purchases, ${total_revenue:.2f} revenue (CORRECTED)")
    
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
            print(f"🔍 GET_CHART_DATA CALLED:")
            print(f"   config: {config}")
            print(f"   entity_type: {entity_type}")
            print(f"   entity_id: {entity_id}")
            print(f"   meta_db_path: {self.meta_db_path}")
            print(f"   mixpanel_db_path: {self.mixpanel_db_path}")
            
            table_name = self.get_table_name(config.breakdown)
            print(f"   table_name: {table_name}")
            
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
            
            print(f"   meta_where: {meta_where}")
            print(f"   mixpanel_attr_field: {mixpanel_attr_field}")
            
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
            print(f"🔍 META QUERY RESULT: {len(meta_data)} rows")
            if len(meta_data) > 0:
                print(f"   first meta row: {meta_data[0]}")
            
            # Get daily Mixpanel data (attributed to credited_date)
            mixpanel_conn = sqlite3.connect(self.mixpanel_analytics_db_path)
            mixpanel_conn.row_factory = sqlite3.Row
            print(f"🔍 MIXPANEL CONNECTION: {self.mixpanel_analytics_db_path}")
            
            mixpanel_query = f"""
            SELECT 
                upm.credited_date as date,
                -- Trial metrics by credited date
                COUNT(CASE WHEN upm.current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as daily_mixpanel_trials,
                
                -- Purchase metrics by credited date
                COUNT(CASE WHEN upm.current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as daily_mixpanel_purchases,
                COUNT(CASE WHEN upm.current_status = 'trial_converted' THEN 1 END) as daily_mixpanel_conversions,
                
                -- Revenue metrics by credited date (sum of current_value attributed to this date)
                SUM(CASE WHEN upm.current_status != 'refunded' THEN upm.current_value ELSE 0 END) as daily_mixpanel_revenue,
                SUM(CASE WHEN upm.current_status = 'refunded' THEN ABS(upm.current_value) ELSE 0 END) as daily_mixpanel_refunds,
                SUM(upm.current_value) as daily_estimated_revenue,
                
                -- User count for statistical significance
                COUNT(DISTINCT upm.distinct_id) as daily_attributed_users
                
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.{mixpanel_attr_field} = ? 
              AND upm.credited_date BETWEEN ? AND ?
            GROUP BY upm.credited_date
            ORDER BY upm.credited_date ASC
            """
            
            cursor = mixpanel_conn.cursor()
            print(f"🔍 EXECUTING MIXPANEL QUERY:")
            print(f"   mixpanel_query: {mixpanel_query}")
            print(f"   params: [{entity_id}, {config.start_date}, {config.end_date}]")
            cursor.execute(mixpanel_query, [entity_id, config.start_date, config.end_date])
            mixpanel_data = [dict(row) for row in cursor.fetchall()]
            print(f"🔍 MIXPANEL QUERY RESULT: {len(mixpanel_data)} rows")
            if len(mixpanel_data) > 0:
                print(f"   first mixpanel row: {mixpanel_data[0]}")
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