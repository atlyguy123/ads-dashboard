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

# Import the breakdown mapping service
from .breakdown_mapping_service import BreakdownMappingService, BreakdownData

# Import the modular calculator system
from ..calculators import (
    CalculationInput, 
    RevenueCalculators,
    ROASCalculators,
    AccuracyCalculators,
    CostCalculators,
    RateCalculators,
    DatabaseCalculators
)

logger = logging.getLogger(__name__)


@dataclass
class QueryConfig:
    """Configuration for analytics queries"""
    breakdown: str  # 'all', 'country', 'region', 'device'
    start_date: str
    end_date: str
    group_by: Optional[str] = None  # 'campaign', 'adset', 'ad'
    include_mixpanel: bool = True
    enable_breakdown_mapping: bool = True  # New: Enable Meta-Mixpanel mapping


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
        
        # 🔥 CRITICAL FIX: Initialize breakdown mapping service with the CORRECT Meta database path
        # The breakdown service needs meta_analytics.db for Meta data, not mixpanel_data.db!
        self.breakdown_service = BreakdownMappingService(self.mixpanel_db_path, meta_db_path=self.meta_db_path)
        
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
        Enhanced with breakdown mapping support
        """
        try:
            logger.info(f"🔍 EXECUTE_ANALYTICS_QUERY CALLED - BREAKDOWN MAPPING VERSION")
            # Get the appropriate table name based on breakdown
            table_name = self.get_table_name(config.breakdown)
            logger.info(f"🔍 Table name: {table_name}, Breakdown: {config.breakdown}")
            
            # Check if this is a breakdown query that needs mapping
            if config.enable_breakdown_mapping and config.breakdown in ['country', 'device']:
                return self._execute_breakdown_query_with_mapping(config)
            
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
                    'generated_at': datetime.now().isoformat(),
                    'breakdown_mapping_enabled': config.enable_breakdown_mapping
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
    
    def _execute_breakdown_query_with_mapping(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute breakdown query with Meta-Mixpanel mapping
        This provides unified breakdown data with proper mapping between platforms
        """
        try:
            logger.info(f"🔍 Executing breakdown query with mapping: {config.breakdown}")
            
            # Get breakdown data using the mapping service
            breakdown_data = self.breakdown_service.get_breakdown_data(
                breakdown_type=config.breakdown,
                start_date=config.start_date,
                end_date=config.end_date,
                group_by=config.group_by or 'campaign'
            )
            
            # Convert BreakdownData objects to dashboard-compatible format
            structured_data = []
            
            for bd in breakdown_data:
                # Map each breakdown data to our standard format
                base_entity = {
                    'id': f"{config.breakdown}_{bd.breakdown_value}_{bd.meta_data.get('campaign_id', 'unknown')}",
                    'entity_type': config.breakdown,
                    'campaign_id': bd.meta_data.get('campaign_id'),
                    'campaign_name': bd.meta_data.get('campaign_name'),
                    'name': bd.breakdown_value,  # This is the breakdown value (country name, device type, etc.)
                    'breakdown_value': bd.breakdown_value,  # 🔥 CRITICAL FIX: Add breakdown_value to top level
                    
                    # Meta metrics
                    'spend': bd.meta_data.get('spend', 0),
                    'impressions': bd.meta_data.get('impressions', 0),
                    'clicks': bd.meta_data.get('clicks', 0),
                    'meta_trials_started': bd.meta_data.get('meta_trials', 0),
                    'meta_purchases': bd.meta_data.get('meta_purchases', 0),
                    
                    # Mixpanel metrics
                    'mixpanel_trials_started': bd.mixpanel_data.get('mixpanel_trials', 0),
                    'mixpanel_purchases': bd.mixpanel_data.get('mixpanel_purchases', 0),
                    'mixpanel_revenue_usd': bd.mixpanel_data.get('mixpanel_revenue', 0),
                    'mixpanel_revenue_net': bd.mixpanel_data.get('mixpanel_revenue', 0),  # Same as gross for now
                    'estimated_revenue_adjusted': bd.mixpanel_data.get('mixpanel_revenue', 0),  # Use actual revenue
                    'profit': bd.mixpanel_data.get('mixpanel_revenue', 0) - bd.meta_data.get('spend', 0),
                    
                    # Calculated metrics
                    'estimated_roas': bd.combined_metrics.get('estimated_roas', 0),
                    'trial_accuracy_ratio': bd.combined_metrics.get('trial_accuracy_ratio', 0),
                    'purchase_accuracy_ratio': bd.combined_metrics.get('purchase_accuracy_ratio', 0),
                    
                    # Breakdown-specific data
                    'breakdowns': [{
                        'type': config.breakdown,
                        'values': [{
                            'name': bd.breakdown_value,
                            'meta_value': bd.meta_data.get(config.breakdown),
                            'mixpanel_value': bd.breakdown_value,
                            **bd.meta_data,
                            **bd.mixpanel_data,
                            **bd.combined_metrics
                        }]
                    }],
                    
                    # Additional metadata
                    'total_users': bd.mixpanel_data.get('total_users', 0),
                    'children': []
                }
                
                structured_data.append(base_entity)
            
            return {
                'success': True,
                'data': structured_data,
                'metadata': {
                    'query_config': config.__dict__,
                    'table_used': f'breakdown_mapping_{config.breakdown}',
                    'record_count': len(structured_data),
                    'date_range': f"{config.start_date} to {config.end_date}",
                    'generated_at': datetime.now().isoformat(),
                    'breakdown_mapping_enabled': True,
                    'breakdown_type': config.breakdown
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing breakdown query with mapping: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': datetime.now().isoformat()
                }
            }
    
    def discover_breakdown_mappings(self) -> Dict[str, Any]:
        """
        Discover unmapped breakdown values and return mapping suggestions
        """
        try:
            return self.breakdown_service.discover_and_update_mappings()
        except Exception as e:
            logger.error(f"Error discovering breakdown mappings: {e}")
            return {'unmapped_countries': [], 'unmapped_devices': []}
    
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
        
        # Get campaign-level event data (trials, purchases, and revenue)
        campaign_event_query = """
        SELECT 
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            COALESCE(SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN e.revenue_usd ELSE 0 END), 0) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_campaign_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
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
            config.start_date, config.end_date,  # revenue filter
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
                'mixpanel_revenue_usd': float(campaign.get('mixpanel_revenue_usd', 0) or 0),  # ACTUAL revenue from Mixpanel events
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # ESTIMATED revenue from user_product_metrics
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
        
        # Get adset-level event data (trials, purchases, and revenue)
        adset_event_query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            COALESCE(SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN e.revenue_usd ELSE 0 END), 0) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
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
            config.start_date, config.end_date,  # revenue filter
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
                'mixpanel_revenue_usd': float(adset.get('mixpanel_revenue_usd', 0) or 0),  # ACTUAL revenue from Mixpanel events
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # ESTIMATED revenue from user_product_metrics
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
        
        # Get ad-level event data (trials, purchases, and revenue)
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
            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            COALESCE(SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN e.revenue_usd ELSE 0 END), 0) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY u.abi_ad_id, e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
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
            config.start_date, config.end_date,  # revenue filter
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
                'mixpanel_revenue_usd': float(ad.get('mixpanel_revenue_usd', 0) or 0),  # ACTUAL revenue from Mixpanel events
                
                # Calculated metrics
                'estimated_revenue_usd': estimated_revenue,  # ESTIMATED revenue from user_product_metrics
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
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_purchases,
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
        
        # Step 2A: Get ACTUAL Mixpanel revenue from purchase events (using the WORKING logic)
        actual_revenue_data_map = {}
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                ad_placeholders = ','.join(['?' for _ in all_ad_ids])
                # Query for ACTUAL Mixpanel revenue from purchase events (CORRECTED EVENT TYPES)
                actual_revenue_query = f"""
                SELECT 
                    u.abi_ad_id,
                    COALESCE(SUM(CASE 
                        WHEN me.event_name = 'RC Initial purchase' 
                             AND DATE(me.event_time) BETWEEN ? AND ?
                        THEN me.revenue_usd 
                        ELSE 0 
                    END), 0) as actual_mixpanel_revenue_usd,
                    COALESCE(SUM(CASE 
                        WHEN me.event_name = 'RC Cancellation'
                             AND DATE(me.event_time) BETWEEN ? AND ?
                        THEN ABS(me.revenue_usd)
                        ELSE 0
                    END), 0) as actual_mixpanel_refunds_usd
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event me ON u.distinct_id = me.distinct_id
                WHERE u.abi_ad_id IN ({ad_placeholders})
                  AND u.has_abi_attribution = TRUE
                GROUP BY u.abi_ad_id
                """
                
                actual_revenue_params = [
                    config.start_date, config.end_date,  # Revenue window
                    config.start_date, config.end_date,  # Refunds window
                    *list(all_ad_ids)
                ]
                
                logger.info(f"🔍 Executing ACTUAL Mixpanel revenue query using WORKING logic")
                cursor.execute(actual_revenue_query, actual_revenue_params)
                
                results = cursor.fetchall()
                logger.info(f"💰 Actual revenue query returned {len(results)} rows")
                
                for row in results:
                    ad_id = row['abi_ad_id']
                    actual_revenue_data_map[ad_id] = dict(row)
                    logger.debug(f"💰 Ad {ad_id}: ${row['actual_mixpanel_revenue_usd']:.2f} actual revenue, ${row['actual_mixpanel_refunds_usd']:.2f} refunds from Mixpanel events")
        
        except Exception as e:
            logger.error(f"Error fetching ACTUAL Mixpanel revenue data from events: {e}", exc_info=True)

        # Step 2B: Get ESTIMATED revenue from user_product_metrics by credited_date (lifecycle predictions)
        estimated_revenue_data_map = {}
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                ad_placeholders = ','.join(['?' for _ in all_ad_ids])
                # Query for ESTIMATED revenue from user lifecycle predictions
                estimated_revenue_query = f"""
                SELECT 
                    u.abi_ad_id,
                    SUM(upm.current_value) as estimated_revenue_usd
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_ad_id IN ({ad_placeholders})
                  AND upm.credited_date BETWEEN ? AND ?
                GROUP BY u.abi_ad_id
                """
                
                estimated_revenue_params = [
                    *list(all_ad_ids),
                    config.start_date, config.end_date  # credited_date filter
                ]
                
                logger.info(f"🔍 Executing ESTIMATED revenue query from user_product_metrics by credited_date")
                cursor.execute(estimated_revenue_query, estimated_revenue_params)
                
                results = cursor.fetchall()
                logger.info(f"💰 Estimated revenue query returned {len(results)} rows")
                
                for row in results:
                    ad_id = row['abi_ad_id']
                    estimated_revenue_data_map[ad_id] = dict(row)
                    logger.debug(f"💰 Ad {ad_id}: ${row['estimated_revenue_usd']:.2f} estimated revenue from user_product_metrics")
        
        except Exception as e:
            logger.error(f"Error fetching ESTIMATED revenue data from user_product_metrics: {e}", exc_info=True)

        # Step 3: Combine event, actual revenue, and estimated revenue data
        mixpanel_data_map = {}
        for ad_id in all_ad_ids:
            event_data = event_data_map.get(ad_id, {})
            actual_revenue_data = actual_revenue_data_map.get(ad_id, {})
            estimated_revenue_data = estimated_revenue_data_map.get(ad_id, {})
            
            mixpanel_data_map[ad_id] = {
                # Event metrics
                'mixpanel_trials_started': event_data.get('mixpanel_trials_started', 0),
                'mixpanel_purchases': event_data.get('mixpanel_purchases', 0),
                'total_attributed_users': event_data.get('total_attributed_users', 0),
                
                # ACTUAL revenue from Mixpanel purchase events (with 8-day logic)
                'actual_mixpanel_revenue_usd': actual_revenue_data.get('actual_mixpanel_revenue_usd', 0.0),
                'actual_mixpanel_refunds_usd': actual_revenue_data.get('actual_mixpanel_refunds_usd', 0.0),
                
                # ESTIMATED revenue from user lifecycle predictions
                'estimated_revenue_usd': estimated_revenue_data.get('estimated_revenue_usd', 0.0)
            }
        
        logger.info(f"✅ Successfully combined event and revenue data for {len(mixpanel_data_map)} ads")

        # Step 4: Apply data to records with proper separation of actual vs estimated revenue
        def process_and_aggregate(items):
            for item in items:
                # Initialize Mixpanel metrics to 0 with CLEAR separation of actual vs estimated
                item.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_purchases': 0,
                    
                    # ACTUAL revenue from Mixpanel purchase events (with 8-day logic)
                    'mixpanel_revenue_usd': 0.0,  # This is now ACTUAL revenue from events
                    'mixpanel_refunds_usd': 0.0,  # This is now ACTUAL refunds from events
                    
                    # ESTIMATED revenue from user lifecycle predictions
                    'estimated_revenue_usd': 0.0,  # This remains estimated revenue
                    
                    'total_attributed_users': 0
                })

                if item.get('ad_id'):
                    # It's an ad, get data directly from the combined map
                    ad_metrics = mixpanel_data_map.get(item['ad_id'])
                    if ad_metrics:
                        item.update({
                            'mixpanel_trials_started': int(ad_metrics.get('mixpanel_trials_started', 0)),
                            'mixpanel_purchases': int(ad_metrics.get('mixpanel_purchases', 0)),
                            
                            # ACTUAL revenue metrics from Mixpanel events
                            'mixpanel_revenue_usd': float(ad_metrics.get('actual_mixpanel_revenue_usd', 0)),
                            'mixpanel_refunds_usd': float(ad_metrics.get('actual_mixpanel_refunds_usd', 0)),
                            
                            # ESTIMATED revenue from lifecycle predictions
                            'estimated_revenue_usd': float(ad_metrics.get('estimated_revenue_usd', 0)),
                            
                            'total_attributed_users': int(ad_metrics.get('total_attributed_users', 0))
                        })
                        logger.debug(f"✅ Ad {item['ad_id']}: {item['mixpanel_trials_started']} trials, {item['mixpanel_purchases']} purchases, ACTUAL: ${item['mixpanel_revenue_usd']:.2f}, ESTIMATED: ${item['estimated_revenue_usd']:.2f}")
                    else:
                        logger.debug(f"❌ Ad {item['ad_id']}: No Mixpanel data found")
                        
                elif 'children' in item and item['children']:
                    # It's a campaign or adset, process children first then aggregate
                    process_and_aggregate(item['children'])
                    
                    # Aggregate metrics from children (preserving actual vs estimated separation)
                    for child in item['children']:
                        item['mixpanel_trials_started'] += child.get('mixpanel_trials_started', 0)
                        item['mixpanel_purchases'] += child.get('mixpanel_purchases', 0)
                        
                        # ACTUAL revenue aggregation
                        item['mixpanel_revenue_usd'] += child.get('mixpanel_revenue_usd', 0)
                        item['mixpanel_refunds_usd'] += child.get('mixpanel_refunds_usd', 0)
                        
                        # ESTIMATED revenue aggregation
                        item['estimated_revenue_usd'] += child.get('estimated_revenue_usd', 0)
                        
                        item['total_attributed_users'] += child.get('total_attributed_users', 0)
                    
                    entity_type = 'campaign' if item.get('campaign_id') else 'adset'
                    entity_id = item.get('campaign_id') or item.get('adset_id')
                    logger.debug(f"✅ {entity_type.title()} {entity_id}: Aggregated from {len(item['children'])} children - {item['mixpanel_trials_started']} trials, {item['mixpanel_purchases']} purchases, ACTUAL: ${item['mixpanel_revenue_usd']:.2f}, ESTIMATED: ${item['estimated_revenue_usd']:.2f}")

                # Add derived metrics
                meta_trials = item.get('meta_trials_started', 0)
                mixpanel_trials = item.get('mixpanel_trials_started', 0)
                item['trial_accuracy_ratio'] = (mixpanel_trials / meta_trials) * 100 if meta_trials > 0 else 0.0
        
        process_and_aggregate(records)
        
        # Log final summary
        total_trials = sum(record.get('mixpanel_trials_started', 0) for record in records)
        total_purchases = sum(record.get('mixpanel_purchases', 0) for record in records)
        total_actual_revenue = sum(record.get('mixpanel_revenue_usd', 0) for record in records)
        total_estimated_revenue = sum(record.get('estimated_revenue_usd', 0) for record in records)
        logger.info(f"🎯 FINAL: Added Mixpanel data totaling {total_trials} trials, {total_purchases} purchases, ACTUAL: ${total_actual_revenue:.2f}, ESTIMATED: ${total_estimated_revenue:.2f} (FIXED SEPARATION)")
    
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
        
        # === USE MODULAR CALCULATOR SYSTEM ===
        # Create standardized input for all calculations
        calc_input = CalculationInput(raw_record=record)
        
        # Calculate all derived metrics using the calculator functions
        formatted['trial_accuracy_ratio'] = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
        formatted['purchase_accuracy_ratio'] = AccuracyCalculators.calculate_purchase_accuracy_ratio(calc_input)
        
        # ROAS calculation with accuracy adjustment
        formatted['estimated_roas'] = ROASCalculators.calculate_estimated_roas(calc_input)
        formatted['performance_impact_score'] = ROASCalculators.calculate_performance_impact_score(calc_input)
        
        # Cost calculations
        formatted['mixpanel_cost_per_trial'] = CostCalculators.calculate_mixpanel_cost_per_trial(calc_input)
        formatted['mixpanel_cost_per_purchase'] = CostCalculators.calculate_mixpanel_cost_per_purchase(calc_input)
        formatted['meta_cost_per_trial'] = CostCalculators.calculate_meta_cost_per_trial(calc_input)
        formatted['meta_cost_per_purchase'] = CostCalculators.calculate_meta_cost_per_purchase(calc_input)
        
        # Rate calculations
        formatted['click_to_trial_rate'] = RateCalculators.calculate_click_to_trial_rate(calc_input)
        
        # Database pass-through calculations (conversion rates)
        formatted['trial_conversion_rate'] = DatabaseCalculators.calculate_trial_conversion_rate(calc_input)
        formatted['trial_to_purchase_rate'] = DatabaseCalculators.calculate_trial_to_purchase_rate(calc_input)
        formatted['avg_trial_refund_rate'] = DatabaseCalculators.calculate_avg_trial_refund_rate(calc_input)
        formatted['purchase_refund_rate'] = DatabaseCalculators.calculate_purchase_refund_rate(calc_input)
        
        # Revenue calculations
        formatted['mixpanel_revenue_net'] = RevenueCalculators.calculate_mixpanel_revenue_net(calc_input)
        formatted['profit'] = RevenueCalculators.calculate_profit(calc_input)
        
        # NEW: Add accuracy-adjusted estimated revenue for frontend display
        formatted['estimated_revenue_adjusted'] = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(calc_input)
        
        # CRITICAL: Preserve children array if it exists (for hierarchical structure)
        if 'children' in record:
            formatted['children'] = record['children']
        
        return formatted
    
    def get_chart_data(self, config: QueryConfig, entity_type: str, entity_id: str) -> Dict[str, Any]:
        """Get detailed daily metrics for sparkline charts - ALWAYS returns exactly 14 days ending on config.end_date"""
        try:
            table_name = self.get_table_name(config.breakdown)
            
            # Calculate the exact 14-day period ending on config.end_date
            end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=13)  # 13 days back + end date = 14 days total
            
            # Calculate expanded date range for activity analysis (1 week before and after)
            expanded_start_date = start_date - timedelta(days=7)  # 1 week before display period
            expanded_end_date = end_date + timedelta(days=7)      # 1 week after display period
            
            # Format dates for queries
            chart_start_date = start_date.strftime('%Y-%m-%d')
            chart_end_date = config.end_date
            expanded_start_str = expanded_start_date.strftime('%Y-%m-%d')
            expanded_end_str = expanded_end_date.strftime('%Y-%m-%d')
            
            logger.info(f"📊 CHART DATA: Showing 14 days from {chart_start_date} to {chart_end_date}")
            logger.info(f"📊 ACTIVITY ANALYSIS: Checking spend activity from {expanded_start_str} to {expanded_end_str}")
            
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
            
            # Get daily Meta data for the EXPANDED period to determine activity range
            expanded_meta_query = f"""
            SELECT date,
                   SUM(spend) as daily_spend
            FROM {table_name}
            WHERE {meta_where} AND date BETWEEN ? AND ? AND spend > 0
            GROUP BY date
            ORDER BY date ASC
            """
            
            expanded_meta_data = self._execute_meta_query(expanded_meta_query, [entity_id, expanded_start_str, expanded_end_str])
            
            # Determine first and last spend dates from expanded data
            first_spend_date = None
            last_spend_date = None
            
            if expanded_meta_data:
                spend_dates = [row['date'] for row in expanded_meta_data if row['daily_spend'] > 0]
                if spend_dates:
                    first_spend_date = min(spend_dates)
                    last_spend_date = max(spend_dates)
            
            logger.info(f"📊 ACTIVITY PERIOD: First spend: {first_spend_date}, Last spend: {last_spend_date}")
            
            # Get daily Meta data for the 14-day display period
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
            
            meta_data = self._execute_meta_query(meta_query, [entity_id, chart_start_date, chart_end_date])
            
            # Get daily Mixpanel data for the 14-day period (attributed to credited_date)
            mixpanel_conn = sqlite3.connect(self.mixpanel_analytics_db_path)
            mixpanel_conn.row_factory = sqlite3.Row
            
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
            cursor.execute(mixpanel_query, [entity_id, chart_start_date, chart_end_date])
            mixpanel_data = [dict(row) for row in cursor.fetchall()]
            mixpanel_conn.close()
            
            # Generate ALL 14 days, filling missing days with zeros
            daily_data = {}
            current_date = start_date
            
            # Initialize all 14 days with zero values and activity status
            for i in range(14):
                date_str = current_date.strftime('%Y-%m-%d')
                
                # Determine if this day should be grey (inactive)
                is_inactive = False
                if first_spend_date and last_spend_date:
                    # Grey if before first spend or after last spend
                    is_inactive = date_str < first_spend_date or date_str > last_spend_date
                elif first_spend_date:
                    # Only first spend found, grey before first spend
                    is_inactive = date_str < first_spend_date
                elif last_spend_date:
                    # Only last spend found, grey after last spend
                    is_inactive = date_str > last_spend_date
                else:
                    # No spend found in expanded period, all days are inactive
                    is_inactive = True
                
                daily_data[date_str] = {
                    'date': date_str,
                    'daily_spend': 0.0,
                    'daily_impressions': 0,
                    'daily_clicks': 0,
                    'daily_meta_trials': 0,
                    'daily_meta_purchases': 0,
                    'daily_mixpanel_trials': 0,
                    'daily_mixpanel_purchases': 0,
                    'daily_mixpanel_conversions': 0,
                    'daily_mixpanel_revenue': 0.0,
                    'daily_mixpanel_refunds': 0.0,
                    'daily_estimated_revenue': 0.0,
                    'daily_attributed_users': 0,
                    'is_inactive': is_inactive  # New field for frontend styling
                }
                current_date += timedelta(days=1)
            
            # Overlay actual Meta data where it exists
            for row in meta_data:
                date = row['date']
                if date in daily_data:
                    daily_data[date].update({
                        'daily_spend': float(row.get('daily_spend', 0) or 0),
                        'daily_impressions': int(row.get('daily_impressions', 0) or 0),
                        'daily_clicks': int(row.get('daily_clicks', 0) or 0),
                        'daily_meta_trials': int(row.get('daily_meta_trials', 0) or 0),
                        'daily_meta_purchases': int(row.get('daily_meta_purchases', 0) or 0)
                    })
            
            # Overlay actual Mixpanel data where it exists
            for row in mixpanel_data:
                date = row['date']
                if date in daily_data:
                    daily_data[date].update({
                        'daily_mixpanel_trials': int(row.get('daily_mixpanel_trials', 0) or 0),
                        'daily_mixpanel_purchases': int(row.get('daily_mixpanel_purchases', 0) or 0),
                        'daily_mixpanel_conversions': int(row.get('daily_mixpanel_conversions', 0) or 0),
                        'daily_mixpanel_revenue': float(row.get('daily_mixpanel_revenue', 0) or 0),
                        'daily_mixpanel_refunds': float(row.get('daily_mixpanel_refunds', 0) or 0),
                        'daily_estimated_revenue': float(row.get('daily_estimated_revenue', 0) or 0),
                        'daily_attributed_users': int(row.get('daily_attributed_users', 0) or 0)
                    })
            
            # Calculate accuracy ratio for the entire period
            total_meta_trials = sum(d['daily_meta_trials'] for d in daily_data.values())
            total_mixpanel_trials = sum(d['daily_mixpanel_trials'] for d in daily_data.values())
            total_meta_purchases = sum(d['daily_meta_purchases'] for d in daily_data.values())
            total_mixpanel_purchases = sum(d['daily_mixpanel_purchases'] for d in daily_data.values())
            
            # Determine event priority and accuracy ratio
            if total_mixpanel_trials == 0 and total_mixpanel_purchases == 0:
                event_priority = 'trials'
                overall_accuracy_ratio = 0.0
            elif total_mixpanel_trials > total_mixpanel_purchases:
                event_priority = 'trials'
                overall_accuracy_ratio = total_mixpanel_trials / total_meta_trials if total_meta_trials > 0 else 0.0
            elif total_mixpanel_purchases > total_mixpanel_trials:
                event_priority = 'purchases'
                overall_accuracy_ratio = total_mixpanel_purchases / total_meta_purchases if total_meta_purchases > 0 else 0.0
            else:
                event_priority = 'equal'
                overall_accuracy_ratio = total_mixpanel_trials / total_meta_trials if total_meta_trials > 0 else 0.0
            
            logger.info(f"📊 CHART ACCURACY: {event_priority} priority, {overall_accuracy_ratio:.3f} ratio")
            
            # Calculate daily metrics using the modular calculator system for ALL 14 days
            chart_data = []
            for date in sorted(daily_data.keys()):  # This will always be exactly 14 days
                day_data = daily_data[date]
                
                # Map daily fields to standard calculator field names
                calculator_record = {
                    'spend': day_data['daily_spend'],
                    'estimated_revenue_usd': day_data['daily_estimated_revenue'],
                    'mixpanel_revenue_usd': day_data.get('daily_mixpanel_revenue', 0),
                    'mixpanel_refunds_usd': day_data.get('daily_mixpanel_refunds', 0),
                    'mixpanel_trials_started': day_data.get('daily_mixpanel_trials', 0),
                    'meta_trials_started': day_data.get('daily_meta_trials', 0),
                    'mixpanel_purchases': day_data.get('daily_mixpanel_purchases', 0),
                    'meta_purchases': day_data.get('daily_meta_purchases', 0),
                    **day_data  # Keep original daily fields for reference
                }
                
                # Use the modular calculator system for ROAS and profit calculations
                calc_input = CalculationInput(raw_record=calculator_record)
                day_data['daily_roas'] = ROASCalculators.calculate_estimated_roas(calc_input)
                day_data['daily_profit'] = RevenueCalculators.calculate_profit(calc_input)
                
                # Store accuracy ratio and event priority for tooltips
                day_data['period_accuracy_ratio'] = overall_accuracy_ratio
                day_data['event_priority'] = event_priority
                day_data['conversions_for_coloring'] = day_data['daily_mixpanel_conversions']
                
                chart_data.append(day_data)
            
            logger.info(f"📊 CHART RESULT: {len(chart_data)} days from {chart_data[0]['date']} to {chart_data[-1]['date']}")
            
            return {
                'success': True,
                'chart_data': chart_data,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'date_range': f"{chart_start_date} to {chart_end_date}",
                'total_days': len(chart_data),
                'period_info': f"14-day period ending {chart_end_date}",
                'activity_analysis': {
                    'expanded_range': f"{expanded_start_str} to {expanded_end_str}",
                    'first_spend_date': first_spend_date,
                    'last_spend_date': last_spend_date,
                    'inactive_days_count': sum(1 for d in chart_data if d.get('is_inactive', False))
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting chart data: {e}")
            return {
                'success': False,
                'error': str(e)
            } 