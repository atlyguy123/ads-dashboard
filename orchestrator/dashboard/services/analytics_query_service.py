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

from utils.database_utils import get_database_path, get_database_connection

# Import timezone utilities for consistent timezone handling
from ...utils.timezone_utils import now_in_timezone

# Import breakdown data type (service will be imported lazily)
from .breakdown_mapping_service import BreakdownData

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
        
        # 🔥 CRITICAL FIX: Make breakdown service initialization LAZY to prevent Railway startup failures
        # Instead of initializing immediately, store the path and create the service only when needed
        self._breakdown_service = None
        self._breakdown_service_initialized = False
        
        # Table mapping based on breakdown parameter
        self.table_mapping = {
            'all': 'ad_performance_daily',
            'country': 'ad_performance_daily_country', 
            'region': 'ad_performance_daily_region',
            'device': 'ad_performance_daily_device'
        }
    
    @property
    def breakdown_service(self):
        """
        Lazy initialization of BreakdownMappingService to prevent Railway startup failures.
        
        Only creates the service when it's actually needed, and handles any initialization
        errors gracefully by returning None (which disables breakdown functionality).
        """
        if not self._breakdown_service_initialized:
            try:
                # Import here to avoid circular imports
                from .breakdown_mapping_service import BreakdownMappingService
                
                # Only initialize if we have valid database paths
                if self.mixpanel_db_path and self.meta_db_path:
                    logger.info(f"🔧 Initializing BreakdownMappingService: mixpanel={self.mixpanel_db_path}, meta={self.meta_db_path}")
                    self._breakdown_service = BreakdownMappingService(
                        self.mixpanel_db_path, 
                        meta_db_path=self.meta_db_path
                    )
                    logger.info("✅ BreakdownMappingService initialized successfully")
                else:
                    logger.warning(f"❌ Cannot initialize BreakdownMappingService: missing paths (mixpanel={bool(self.mixpanel_db_path)}, meta={bool(self.meta_db_path)})")
                    self._breakdown_service = None
                    
            except Exception as e:
                logger.warning(f"❌ Failed to initialize BreakdownMappingService: {e}")
                logger.info("🔧 Breakdown functionality will be disabled, but analytics will continue to work")
                self._breakdown_service = None
                
            self._breakdown_service_initialized = True
            
        return self._breakdown_service
    
    def get_table_name(self, breakdown: str) -> str:
        """Get the appropriate table name based on breakdown parameter"""
        return self.table_mapping.get(breakdown, 'ad_performance_daily')
    
    def execute_analytics_query(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute analytics query with proper hierarchical structure
        
        When breakdown is requested, maintains hierarchy and enriches it with breakdown data
        instead of replacing the hierarchy with flat breakdown records.
        """
        try:
            logger.info(f"🔍 Executing analytics query: breakdown={config.breakdown}, group_by={config.group_by}")
            
            # Get the appropriate table name for the breakdown
            table_name = self.get_table_name(config.breakdown)
            
            # Check if Meta data exists in the primary table
            meta_data_count = self._get_meta_data_count(table_name)
            logger.info(f"📊 Meta data count in {table_name}: {meta_data_count}")
            
            # CRITICAL FIX: Always get hierarchical data first
            if meta_data_count == 0:
                # Use Mixpanel-only data with hierarchical structure
                hierarchical_result = self._execute_mixpanel_only_query(config)
            else:
                # Use Meta + Mixpanel data with hierarchical structure
                hierarchical_result = self._execute_hierarchical_query(config, table_name)
            
            # Check if we got valid hierarchical data
            if not hierarchical_result.get('success') or not hierarchical_result.get('data'):
                return hierarchical_result
            
            # CRITICAL FIX: If breakdown is requested AND we have hierarchical data, 
            # enrich the hierarchy with breakdown data instead of replacing it
            if config.breakdown != 'all' and config.enable_breakdown_mapping:
                logger.info(f"🔍 Enriching hierarchical data with {config.breakdown} breakdown data")
                enriched_result = self._enrich_hierarchical_data_with_breakdowns(
                    hierarchical_result, config
                )
                return enriched_result
            
            # Return the hierarchical data as-is for 'all' breakdown
            return hierarchical_result
            
        except Exception as e:
            logger.error(f"Error executing analytics query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': now_in_timezone().isoformat()
                }
            }
    
    def _execute_hierarchical_query(self, config: QueryConfig, table_name: str) -> Dict[str, Any]:
        """
        Execute hierarchical query maintaining campaign->adset->ad structure
        """
        try:
            logger.info(f"🔍 Executing hierarchical query: {config.group_by} level")
            
            # Execute the appropriate hierarchical query based on group_by
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
                    'generated_at': now_in_timezone().isoformat(),
                    'data_source': 'hierarchical_with_meta'
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing hierarchical query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': now_in_timezone().isoformat()
                }
            }
    
    def _enrich_hierarchical_data_with_breakdowns(self, hierarchical_result: Dict[str, Any], config: QueryConfig) -> Dict[str, Any]:
        """
        Enrich existing hierarchical data with breakdown information
        
        This maintains the campaign->adset->ad structure while adding breakdown data
        under each entity as requested by the user.
        """
        try:
            logger.info(f"🔍 Enriching hierarchy with {config.breakdown} breakdown data")
            
            # CRITICAL FIX: Check if breakdown service is available before using it
            if not self.breakdown_service:
                logger.warning("⚠️ Breakdown service not available, skipping breakdown enrichment")
                return hierarchical_result
            
            # CRITICAL FIX: Get breakdown data for ALL hierarchy levels, not just group_by level
            # This enables breakdown data to appear at all levels (campaign, adset, and ad)
            breakdown_data = self.breakdown_service.get_breakdown_data(
                breakdown_type=config.breakdown,
                start_date=config.start_date,
                end_date=config.end_date,
                fetch_all_levels=True  # NEW: Fetch data for all levels
            )
            
            # Convert breakdown data to a lookup structure keyed by entity ID
            breakdown_lookup = {}
            for bd in breakdown_data:
                # FILTER OUT INVALID BREAKDOWN VALUES
                breakdown_value = bd.breakdown_value
                if not breakdown_value or breakdown_value.lower() in ['unknown', 'null', '', 'total']:
                    continue  # Skip invalid/irrelevant breakdown values
                
                # Create keys for different entity levels
                campaign_id = bd.meta_data.get('campaign_id')
                adset_id = bd.meta_data.get('adset_id')
                ad_id = bd.meta_data.get('ad_id')
                
                # Store breakdown data by the most specific available ID
                if ad_id:
                    key = f"ad_{ad_id}"
                elif adset_id:
                    key = f"adset_{adset_id}"
                elif campaign_id:
                    key = f"campaign_{campaign_id}"
                else:
                    continue  # Skip if no ID available
                
                if key not in breakdown_lookup:
                    breakdown_lookup[key] = []
                
                # CRITICAL FIX: Use the same calculation system as parent entities
                # Create a fake record with breakdown data for calculations
                breakdown_record = {
                    'spend': bd.meta_data.get('spend', 0),
                    'impressions': bd.meta_data.get('impressions', 0),
                    'clicks': bd.meta_data.get('clicks', 0),
                    'meta_trials_started': bd.meta_data.get('meta_trials', 0),
                    'meta_purchases': bd.meta_data.get('meta_purchases', 0),
                    'mixpanel_trials_started': bd.mixpanel_data.get('mixpanel_trials', 0),
                    'mixpanel_purchases': bd.mixpanel_data.get('mixpanel_purchases', 0),
                    'mixpanel_revenue_usd': bd.mixpanel_data.get('mixpanel_revenue', 0),
                    'total_attributed_users': bd.mixpanel_data.get('total_users', 0),
                    'estimated_revenue_usd': bd.mixpanel_data.get('estimated_revenue', 0),  # CRITICAL ADD: Use estimated revenue from breakdown service
                    
                    # CRITICAL ADD: Include rate fields from breakdown data for DatabaseCalculators
                    'avg_trial_conversion_rate': bd.mixpanel_data.get('avg_trial_conversion_rate', 0),
                    'avg_trial_refund_rate': bd.mixpanel_data.get('avg_trial_refund_rate', 0),
                    'avg_purchase_refund_rate': bd.mixpanel_data.get('avg_purchase_refund_rate', 0),
                    
                    # Add other fields that might be needed for calculations
                    'mixpanel_trials_in_progress': 0,
                    'mixpanel_trials_ended': 0,
                    'mixpanel_converted_amount': 0,
                    'mixpanel_conversions_net_refunds': 0,
                    'mixpanel_refunds_usd': 0,
                    'segment_accuracy_average': None
                }
                
                # Use the same calculation system as _format_record
                calc_input = CalculationInput(
                    raw_record=breakdown_record,
                    config=config.__dict__ if config else None,
                    start_date=config.start_date if config else None,
                    end_date=config.end_date if config else None
                )
                
                # Extract entity IDs for identification
                campaign_id = bd.meta_data.get('campaign_id')
                adset_id = bd.meta_data.get('adset_id') 
                ad_id = bd.meta_data.get('ad_id')
                
                # Calculate all the same metrics as parent entities
                calculated_breakdown = {
                    'type': config.breakdown,
                    'name': breakdown_value,
                    'meta_value': bd.meta_data.get(config.breakdown),
                    'mixpanel_value': breakdown_value,
                    
                    # Entity identification fields (needed for sparkline)
                    'id': f"{breakdown_value}_{campaign_id or adset_id or ad_id}",  # Unique ID for breakdown
                    'entity_type': 'campaign' if campaign_id else ('adset' if adset_id else 'ad'),
                    'campaign_id': campaign_id,
                    'adset_id': adset_id,
                    'ad_id': ad_id,
                    
                    # Base metrics (same as parent)
                    'spend': float(breakdown_record.get('spend', 0) or 0),
                    'impressions': int(breakdown_record.get('impressions', 0) or 0),
                    'clicks': int(breakdown_record.get('clicks', 0) or 0),
                    'meta_trials_started': int(breakdown_record.get('meta_trials_started', 0) or 0),
                    'meta_purchases': int(breakdown_record.get('meta_purchases', 0) or 0),
                    'mixpanel_trials_started': int(breakdown_record.get('mixpanel_trials_started', 0) or 0),
                    'mixpanel_purchases': int(breakdown_record.get('mixpanel_purchases', 0) or 0),
                    'mixpanel_revenue_usd': float(breakdown_record.get('mixpanel_revenue_usd', 0) or 0),
                    'total_attributed_users': int(breakdown_record.get('total_attributed_users', 0) or 0),
                    'estimated_revenue_usd': float(breakdown_record.get('estimated_revenue_usd', 0) or 0),  # CRITICAL ADD: Include estimated revenue
                    
                    # Calculated metrics (same calculation system as parent)
                    'trial_accuracy_ratio': AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input) / 100.0,  # Convert to decimal for frontend
                    'purchase_accuracy_ratio': AccuracyCalculators.calculate_purchase_accuracy_ratio(calc_input) / 100.0,  # Convert to decimal for frontend
                    'estimated_roas': ROASCalculators.calculate_estimated_roas(calc_input),
                    'performance_impact_score': ROASCalculators.calculate_performance_impact_score(calc_input),
                    'estimated_revenue_adjusted': RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(calc_input),  # CRITICAL ADD: Missing field
                    'mixpanel_cost_per_trial': CostCalculators.calculate_mixpanel_cost_per_trial(calc_input),
                    'mixpanel_cost_per_purchase': CostCalculators.calculate_mixpanel_cost_per_purchase(calc_input),
                    'meta_cost_per_trial': CostCalculators.calculate_meta_cost_per_trial(calc_input),
                    'meta_cost_per_purchase': CostCalculators.calculate_meta_cost_per_purchase(calc_input),
                    'click_to_trial_rate': RateCalculators.calculate_click_to_trial_rate(calc_input),
                    'trial_conversion_rate': DatabaseCalculators.calculate_trial_conversion_rate(calc_input),
                    'trial_to_purchase_rate': DatabaseCalculators.calculate_trial_to_purchase_rate(calc_input),
                    'avg_trial_refund_rate': DatabaseCalculators.calculate_avg_trial_refund_rate(calc_input),
                    'purchase_refund_rate': DatabaseCalculators.calculate_purchase_refund_rate(calc_input),
                    'mixpanel_revenue_net': RevenueCalculators.calculate_mixpanel_revenue_net(calc_input),
                    'profit': RevenueCalculators.calculate_profit(calc_input),
                }
                
                breakdown_lookup[key].append(calculated_breakdown)
            
            # Recursively enrich the hierarchical data with breakdown information
            def enrich_entity(entity, level='campaign'):
                """Recursively add breakdown data to entities"""
                entity_type = level
                entity_id = None
                
                if level == 'campaign':
                    entity_id = entity.get('campaign_id')
                elif level == 'adset':
                    entity_id = entity.get('adset_id')
                elif level == 'ad':
                    entity_id = entity.get('ad_id')
                
                # Add breakdown data if available for this entity
                if entity_id:
                    lookup_key = f"{entity_type}_{entity_id}"
                    if lookup_key in breakdown_lookup:
                        entity['breakdowns'] = [{
                            'type': config.breakdown,
                            'values': breakdown_lookup[lookup_key]
                        }]
                
                # Recursively process children
                if 'children' in entity and entity['children']:
                    next_level = 'adset' if level == 'campaign' else 'ad'
                    for child in entity['children']:
                        enrich_entity(child, next_level)
            
            # Enrich all top-level entities
            enriched_data = hierarchical_result['data'].copy()
            
            # CRITICAL FIX: Start enrichment at the correct level based on group_by
            starting_level = config.group_by or 'campaign'
            for entity in enriched_data:
                enrich_entity(entity, starting_level)
            
            # Update the result with enriched data
            enriched_result = hierarchical_result.copy()
            enriched_result['data'] = enriched_data
            enriched_result['metadata']['breakdown_enriched'] = True
            enriched_result['metadata']['breakdown_type'] = config.breakdown
            enriched_result['metadata']['breakdown_records_added'] = len(breakdown_data)
            
            logger.info(f"✅ Successfully enriched {len(enriched_data)} entities with {config.breakdown} breakdown data")
            return enriched_result
            
        except Exception as e:
            logger.error(f"Error enriching hierarchical data with breakdowns: {e}", exc_info=True)
            # Return original hierarchical data if enrichment fails
            hierarchical_result['metadata']['breakdown_enrichment_failed'] = str(e)
            return hierarchical_result
    
    def discover_breakdown_mappings(self) -> Dict[str, Any]:
        """
        Discover unmapped breakdown values and return mapping suggestions
        """
        try:
            # CRITICAL FIX: Check if breakdown service is available
            if not self.breakdown_service:
                logger.warning("⚠️ Breakdown service not available, cannot discover mappings")
                return {'unmapped_countries': [], 'unmapped_devices': []}
                
            return self.breakdown_service.discover_and_update_mappings()
        except Exception as e:
            logger.error(f"Error discovering breakdown mappings: {e}")
            return {'unmapped_countries': [], 'unmapped_devices': []}
    
    def _get_meta_data_count(self, table_name: str, start_date: str = None, end_date: str = None) -> int:
        """Check if Meta ad performance table has data for the specified date range"""
        try:
            if start_date and end_date:
                # Check for data within the specific date range
                query = f"SELECT COUNT(*) as count FROM {table_name} WHERE date BETWEEN ? AND ?"
                result = self._execute_meta_query(query, [start_date, end_date])
                logger.info(f"📊 Meta data count in {table_name} for {start_date} to {end_date}: {result[0]['count'] if result else 0}")
            else:
                # Fallback to checking all data in table
                query = f"SELECT COUNT(*) as count FROM {table_name}"
                result = self._execute_meta_query(query, [])
                logger.info(f"📊 Meta data count in {table_name} (all dates): {result[0]['count'] if result else 0}")
            
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
                    'generated_at': now_in_timezone().isoformat(),
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
                    'generated_at': now_in_timezone().isoformat()
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
                    formatted_ad = self._format_record(ad, 'ad', config)
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
                
                formatted_adset = self._format_record(adset, 'adset', config)
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
            formatted_campaign = self._format_record(campaign, 'campaign', config)
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
                formatted_ad = self._format_record(ad, 'ad', config)
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
            formatted_adset = self._format_record(adset, 'adset', config)
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
            formatted_ad = self._format_record(ad, 'ad', config)
            formatted_ads.append(formatted_ad)
        
        return formatted_ads
    
    def _execute_meta_query(self, query: str, params: List) -> List[Dict[str, Any]]:
        """Execute query against meta analytics database with graceful fallback"""
        try:
            # Check if meta database path is valid and database exists
            if not self.meta_db_path or not Path(self.meta_db_path).exists():
                logger.info(f"Meta analytics database not available at {self.meta_db_path}, returning empty results")
                return []
            
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
            logger.warning(f"Error executing meta query, falling back to empty results: {e}")
            # Return empty results instead of raising exception
            return []
    
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
                    'total_attributed_users': 0,
                    # CRITICAL ADD: Initialize rate fields
                    'avg_trial_conversion_rate': 0.0,
                    'avg_trial_refund_rate': 0.0,
                    'avg_purchase_refund_rate': 0.0
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

        # Step 2C: Get AVERAGE CONVERSION AND REFUND RATES from user_product_metrics (CRITICAL ADD)
        rate_data_map = {}
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                ad_placeholders = ','.join(['?' for _ in all_ad_ids])
                # Query for AVERAGE rates from user lifecycle predictions
                rates_query = f"""
                SELECT 
                    u.abi_ad_id,
                    AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                    AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                    AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                    COUNT(DISTINCT upm.distinct_id) as users_with_rates
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_ad_id IN ({ad_placeholders})
                  AND upm.credited_date BETWEEN ? AND ?
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND upm.trial_converted_to_refund_rate IS NOT NULL  
                  AND upm.initial_purchase_to_refund_rate IS NOT NULL
                GROUP BY u.abi_ad_id
                """
                
                rates_params = [
                    *list(all_ad_ids),
                    config.start_date, config.end_date  # credited_date filter
                ]
                
                logger.info(f"🔍 Executing STEP 2C: RATE queries from user_product_metrics by credited_date")
                cursor.execute(rates_query, rates_params)
                
                results = cursor.fetchall()
                logger.info(f"📊 Step 2C rate query returned {len(results)} rows")
                
                for row in results:
                    ad_id = row['abi_ad_id']
                    rate_data_map[ad_id] = dict(row)
                    logger.debug(f"📊 Ad {ad_id}: {row['avg_trial_conversion_rate']:.3f} trial conv, {row['avg_trial_refund_rate']:.3f} trial refund, {row['avg_purchase_refund_rate']:.3f} purchase refund rates")
        
        except Exception as e:
            logger.error(f"Error fetching STEP 2C rate data from user_product_metrics: {e}", exc_info=True)

        # Step 3: Combine event, actual revenue, estimated revenue, and rate data
        mixpanel_data_map = {}
        for ad_id in all_ad_ids:
            event_data = event_data_map.get(ad_id, {})
            actual_revenue_data = actual_revenue_data_map.get(ad_id, {})
            estimated_revenue_data = estimated_revenue_data_map.get(ad_id, {})
            rate_data = rate_data_map.get(ad_id, {})
            
            mixpanel_data_map[ad_id] = {
                # Event metrics
                'mixpanel_trials_started': event_data.get('mixpanel_trials_started', 0),
                'mixpanel_purchases': event_data.get('mixpanel_purchases', 0),
                'total_attributed_users': event_data.get('total_attributed_users', 0),
                
                # ACTUAL revenue from Mixpanel purchase events (with 8-day logic)
                'actual_mixpanel_revenue_usd': actual_revenue_data.get('actual_mixpanel_revenue_usd', 0.0),
                'actual_mixpanel_refunds_usd': actual_revenue_data.get('actual_mixpanel_refunds_usd', 0.0),
                
                # ESTIMATED revenue from user lifecycle predictions
                'estimated_revenue_usd': estimated_revenue_data.get('estimated_revenue_usd', 0.0),
                
                # AVERAGE rates from user lifecycle predictions
                'avg_trial_conversion_rate': rate_data.get('avg_trial_conversion_rate', 0.0),
                'avg_trial_refund_rate': rate_data.get('avg_trial_refund_rate', 0.0),
                'avg_purchase_refund_rate': rate_data.get('avg_purchase_refund_rate', 0.0),
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
                    
                    # AVERAGE rates from user lifecycle predictions (CRITICAL ADD)
                    'avg_trial_conversion_rate': 0.0,
                    'avg_trial_refund_rate': 0.0,
                    'avg_purchase_refund_rate': 0.0,
                    
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
                            
                            # AVERAGE rates from lifecycle predictions (CRITICAL ADD)
                            'avg_trial_conversion_rate': float(ad_metrics.get('avg_trial_conversion_rate', 0)),
                            'avg_trial_refund_rate': float(ad_metrics.get('avg_trial_refund_rate', 0)),
                            'avg_purchase_refund_rate': float(ad_metrics.get('avg_purchase_refund_rate', 0)),
                            
                            'total_attributed_users': int(ad_metrics.get('total_attributed_users', 0))
                        })
                        logger.debug(f"✅ Ad {item['ad_id']}: {item['mixpanel_trials_started']} trials, {item['mixpanel_purchases']} purchases, ACTUAL: ${item['mixpanel_revenue_usd']:.2f}, ESTIMATED: ${item['estimated_revenue_usd']:.2f}")
                    else:
                        logger.debug(f"❌ Ad {item['ad_id']}: No Mixpanel data found")
                        
                elif 'children' in item and item['children']:
                    # It's a campaign or adset, process children first then aggregate
                    process_and_aggregate(item['children'])
                    
                    # Aggregate metrics from children (preserving actual vs estimated separation)
                    total_users_for_rates = 0
                    weighted_trial_conv_sum = 0
                    weighted_trial_refund_sum = 0 
                    weighted_purchase_refund_sum = 0
                    
                    for child in item['children']:
                        item['mixpanel_trials_started'] += child.get('mixpanel_trials_started', 0)
                        item['mixpanel_purchases'] += child.get('mixpanel_purchases', 0)
                        
                        # ACTUAL revenue aggregation
                        item['mixpanel_revenue_usd'] += child.get('mixpanel_revenue_usd', 0)
                        item['mixpanel_refunds_usd'] += child.get('mixpanel_refunds_usd', 0)
                        
                        # ESTIMATED revenue aggregation
                        item['estimated_revenue_usd'] += child.get('estimated_revenue_usd', 0)
                        
                        item['total_attributed_users'] += child.get('total_attributed_users', 0)
                        
                        # WEIGHTED rate aggregation (CRITICAL ADD)
                        child_users = child.get('total_attributed_users', 0)
                        if child_users > 0:
                            weighted_trial_conv_sum += child.get('avg_trial_conversion_rate', 0) * child_users
                            weighted_trial_refund_sum += child.get('avg_trial_refund_rate', 0) * child_users
                            weighted_purchase_refund_sum += child.get('avg_purchase_refund_rate', 0) * child_users
                            total_users_for_rates += child_users
                    
                    # Calculate weighted average rates for parent entity
                    if total_users_for_rates > 0:
                        item['avg_trial_conversion_rate'] = weighted_trial_conv_sum / total_users_for_rates
                        item['avg_trial_refund_rate'] = weighted_trial_refund_sum / total_users_for_rates
                        item['avg_purchase_refund_rate'] = weighted_purchase_refund_sum / total_users_for_rates
                    else:
                        item['avg_trial_conversion_rate'] = 0.0
                        item['avg_trial_refund_rate'] = 0.0
                        item['avg_purchase_refund_rate'] = 0.0
                    
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
    
    def _format_record(self, record: Dict[str, Any], entity_type: str, config: QueryConfig = None) -> Dict[str, Any]:
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
            
            # CRITICAL FIX: Preserve entity ID fields for breakdown enrichment
            'campaign_id': record.get('campaign_id'),
            'adset_id': record.get('adset_id'), 
            'ad_id': record.get('ad_id'),
            
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
        # CRITICAL FIX: Ensure rate fields are available for DatabaseCalculators
        enhanced_record = dict(record)
        enhanced_record.update({
            'avg_trial_conversion_rate': record.get('avg_trial_conversion_rate', 0.0),
            'avg_trial_refund_rate': record.get('avg_trial_refund_rate', 0.0),
            'avg_purchase_refund_rate': record.get('avg_purchase_refund_rate', 0.0)
        })
        
        calc_input = CalculationInput(
            raw_record=enhanced_record,
            config=config.__dict__ if config else None,
            start_date=config.start_date if config else None,
            end_date=config.end_date if config else None
        )
        
        # Calculate all derived metrics using the calculator functions
        formatted['trial_accuracy_ratio'] = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input) / 100.0  # Convert to decimal for frontend
        formatted['purchase_accuracy_ratio'] = AccuracyCalculators.calculate_purchase_accuracy_ratio(calc_input) / 100.0  # Convert to decimal for frontend
        
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
        # CRITICAL FIX: Calculate rates directly from database for this entity
        trial_conv_rate, trial_refund_rate, purchase_refund_rate = self._calculate_entity_rates(entity_type, record, config)
        
        formatted['trial_conversion_rate'] = trial_conv_rate
        formatted['trial_to_purchase_rate'] = trial_conv_rate  # Same as trial conversion rate
        formatted['avg_trial_refund_rate'] = trial_refund_rate
        formatted['purchase_refund_rate'] = purchase_refund_rate
        
        # Revenue calculations
        formatted['mixpanel_revenue_net'] = RevenueCalculators.calculate_mixpanel_revenue_net(calc_input)
        formatted['profit'] = RevenueCalculators.calculate_profit(calc_input)
        
        # NEW: Add accuracy-adjusted estimated revenue for frontend display
        formatted['estimated_revenue_adjusted'] = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(calc_input)
        
        # CRITICAL: Preserve children array if it exists (for hierarchical structure)
        if 'children' in record:
            formatted['children'] = record['children']
        
        return formatted
    
    def _calculate_entity_rates(self, entity_type: str, record: Dict[str, Any], config: QueryConfig = None) -> tuple:
        """
        Calculate conversion rates directly from database for the specific entity
        
        Returns:
            tuple: (trial_conversion_rate, trial_refund_rate, purchase_refund_rate) as percentages (0-100)
        """
        try:
            # Get entity ID based on type
            if entity_type == 'campaign':
                entity_id = record.get('campaign_id')
                entity_field = 'u.abi_campaign_id'
            elif entity_type == 'adset':
                entity_id = record.get('adset_id')
                entity_field = 'u.abi_ad_set_id'
            elif entity_type == 'ad':
                entity_id = record.get('ad_id')
                entity_field = 'u.abi_ad_id'
            else:
                return 0.0, 0.0, 0.0
            
            if not entity_id:
                return 0.0, 0.0, 0.0
            
            # Use config dates or default to recent period
            if config:
                start_date = config.start_date
                end_date = config.end_date
            else:
                from datetime import datetime, timedelta
                end_date = now_in_timezone().date().strftime('%Y-%m-%d')
                start_date = (now_in_timezone().date() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Query database for rates
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                rates_query = f"""
                SELECT 
                    AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                    AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                    AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                    COUNT(DISTINCT upm.distinct_id) as total_users
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE {entity_field} = ?
                  AND upm.credited_date BETWEEN ? AND ?
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND upm.trial_converted_to_refund_rate IS NOT NULL  
                  AND upm.initial_purchase_to_refund_rate IS NOT NULL
                """
                
                cursor.execute(rates_query, [entity_id, start_date, end_date])
                result = cursor.fetchone()
                
                if result and result['total_users'] > 0:
                    # Convert from decimals to percentages (0-100)
                    trial_conv = (result['avg_trial_conversion_rate'] or 0) * 100
                    trial_refund = (result['avg_trial_refund_rate'] or 0) * 100
                    purchase_refund = (result['avg_purchase_refund_rate'] or 0) * 100
                    
                    # Ensure rates are within 0-100% range
                    trial_conv = max(0.0, min(100.0, trial_conv))
                    trial_refund = max(0.0, min(100.0, trial_refund))
                    purchase_refund = max(0.0, min(100.0, purchase_refund))
                    
                    logger.debug(f"✅ {entity_type} {entity_id}: {trial_conv:.1f}%/{trial_refund:.1f}%/{purchase_refund:.1f}% from {result['total_users']} users")
                    return trial_conv, trial_refund, purchase_refund
                else:
                    return 0.0, 0.0, 0.0
                    
        except Exception as e:
            logger.error(f"Error calculating entity rates for {entity_type} {entity_id}: {e}")
            return 0.0, 0.0, 0.0

    def get_chart_data(self, config: QueryConfig, entity_type: str, entity_id: str) -> Dict[str, Any]:
        """Get detailed daily metrics for sparkline charts - ALWAYS returns exactly 14 days ending on config.end_date"""
        try:
            # Check if this is a breakdown entity (format: "US_120217904661980178")
            is_breakdown_entity = '_' in entity_id and not entity_id.startswith(('campaign_', 'adset_', 'ad_'))
            
            if is_breakdown_entity:
                # Parse breakdown entity ID
                breakdown_value, parent_entity_id = entity_id.split('_', 1)
                logger.info(f"📊 BREAKDOWN CHART: {breakdown_value} breakdown for {entity_type} {parent_entity_id}")
                return self._get_breakdown_chart_data(config, entity_type, parent_entity_id, breakdown_value)
            
            # Regular entity chart data
            table_name = self.get_table_name(config.breakdown)
            
            # Calculate the exact 14-day period ending on config.end_date
            end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
            display_start_date = end_date - timedelta(days=13)  # 13 days back + end date = 14 days total
            
            # Calculate data fetch period: no additional days needed for 1-day rolling calculations
            data_start_date = display_start_date  # Start from display period start
            
            # Calculate expanded date range for activity analysis (1 week before and after)
            expanded_start_date = display_start_date - timedelta(days=7)  # 1 week before display period
            expanded_end_date = end_date + timedelta(days=7)      # 1 week after display period
            
            # Format dates for queries
            chart_start_date = data_start_date.strftime('%Y-%m-%d')  # Fetch from display start
            chart_end_date = config.end_date
            display_start_str = display_start_date.strftime('%Y-%m-%d')  # Display period start
            expanded_start_str = expanded_start_date.strftime('%Y-%m-%d')
            expanded_end_str = expanded_end_date.strftime('%Y-%m-%d')
            
            # Chart data configuration validated
            
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
            
            # Activity period determined
            
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
            
            # Generate ALL data fetch days (14 total display days), filling missing days with zeros
            daily_data = {}
            current_date = data_start_date
            
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
            
            # Calculate daily metrics using the modular calculator system for ALL 16 days
            all_data = []
            for date in sorted(daily_data.keys()):  # This will be exactly 16 days
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
                
                all_data.append(day_data)
            
            # Calculate rolling 1-day ROAS for each day in the full dataset
            for i, day_data in enumerate(all_data):
                # Calculate rolling window (current day only)
                rolling_days = [day_data]  # Only current day
                
                # Sum spend and accuracy-adjusted revenue for the rolling window (just current day)
                rolling_spend = day_data['daily_spend']
                # Use accuracy-adjusted revenue from daily calculation for consistency
                rolling_revenue = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(
                    CalculationInput(raw_record={
                        'estimated_revenue_usd': day_data['daily_estimated_revenue'],
                        'mixpanel_trials_started': day_data['daily_mixpanel_trials'],
                        'mixpanel_purchases': day_data['daily_mixpanel_purchases'],
                        'meta_trials_started': day_data['daily_meta_trials'],
                        'meta_purchases': day_data['daily_meta_purchases']
                    })
                )
                rolling_conversions = day_data['daily_mixpanel_purchases']
                rolling_trials = day_data['daily_mixpanel_trials']
                rolling_meta_trials = day_data['daily_meta_trials']
                
                # Calculate rolling ROAS
                if rolling_spend > 0:
                    rolling_roas = rolling_revenue / rolling_spend
                else:
                    rolling_roas = 0.0
                
                # Add rolling metrics to day data
                day_data['rolling_1d_roas'] = round(rolling_roas, 2)
                day_data['rolling_1d_spend'] = rolling_spend
                day_data['rolling_1d_revenue'] = rolling_revenue
                day_data['rolling_1d_conversions'] = rolling_conversions
                day_data['rolling_1d_trials'] = rolling_trials
                day_data['rolling_1d_meta_trials'] = rolling_meta_trials
                day_data['rolling_window_days'] = len(rolling_days)  # For tooltip info
            
            # Extract only the 14-day display period (no need to skip days for 1-day rolling)
            chart_data = all_data[-14:]  # Return only the last 14 days for display
            
            # Chart calculation completed
            
            return {
                'success': True,
                'chart_data': chart_data,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'date_range': f"{display_start_str} to {chart_end_date}",
                'total_days': len(chart_data),
                'period_info': f"14-day period ending {chart_end_date}",
                'rolling_calculation_info': f"Used {len(all_data)}-day dataset for 1-day rolling averages",
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
    
    def _get_breakdown_chart_data(self, config: QueryConfig, entity_type: str, parent_entity_id: str, breakdown_value: str) -> Dict[str, Any]:
        """Get chart data for a specific breakdown value (e.g., US breakdown for a campaign)"""
        try:
            # CRITICAL FIX: Use the lazy-loaded breakdown service instead of creating a new instance
            # This prevents Railway initialization failures
            if not self.breakdown_service:
                logger.warning(f"⚠️ Breakdown service not available, cannot get chart data for {breakdown_value}")
                return {
                    'success': False,
                    'error': 'Breakdown functionality is not available at this time'
                }
            
            breakdown_service = self.breakdown_service
            
            # Determine which breakdown table to use based on config.breakdown
            if config.breakdown == 'country':
                breakdown_table = 'ad_performance_daily_country'
                breakdown_field = 'country'
                mixpanel_filter_field = 'country'
                mixpanel_filter_value = breakdown_value  # Use original country code
            elif config.breakdown == 'device':
                breakdown_table = 'ad_performance_daily_device'
                breakdown_field = 'device'
                # For device, we need to map Meta device to Mixpanel store
                device_mapping = breakdown_service.get_device_mapping(breakdown_value)
                if not device_mapping:
                    raise ValueError(f"No device mapping found for {breakdown_value}")
                mixpanel_filter_field = 'store'
                mixpanel_filter_value = device_mapping['store']  # Use Mixpanel store value
            else:
                raise ValueError(f"Breakdown chart data not supported for breakdown type: {config.breakdown}")
            
            # Calculate date ranges (no extra days needed for 1-day rolling)
            end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
            display_start_date = end_date - timedelta(days=13)
            data_start_date = display_start_date  # No extra days for 1-day rolling
            expanded_start_date = display_start_date - timedelta(days=7)
            expanded_end_date = end_date + timedelta(days=7)
            
            chart_start_date = data_start_date.strftime('%Y-%m-%d')
            chart_end_date = config.end_date
            expanded_start_str = expanded_start_date.strftime('%Y-%m-%d')
            expanded_end_str = expanded_end_date.strftime('%Y-%m-%d')
            
            # Build WHERE clause for Meta breakdown data
            if entity_type == 'campaign':
                meta_where = f"campaign_id = ? AND {breakdown_field} = ?"
                mixpanel_attr_field = "abi_campaign_id"
            elif entity_type == 'adset':
                meta_where = f"adset_id = ? AND {breakdown_field} = ?"
                mixpanel_attr_field = "abi_ad_set_id"
            elif entity_type == 'ad':
                meta_where = f"ad_id = ? AND {breakdown_field} = ?"
                mixpanel_attr_field = "abi_ad_id"
            else:
                raise ValueError(f"Invalid entity_type: {entity_type}")
            
            # Get daily Meta breakdown data for activity analysis
            expanded_meta_query = f"""
            SELECT date, SUM(spend) as daily_spend
            FROM {breakdown_table}
            WHERE {meta_where} AND date BETWEEN ? AND ? AND spend > 0
            GROUP BY date
            ORDER BY date ASC
            """
            
            expanded_meta_data = self._execute_meta_query(expanded_meta_query, 
                [parent_entity_id, breakdown_value, expanded_start_str, expanded_end_str])
            
            # Determine activity period
            first_spend_date = None
            last_spend_date = None
            if expanded_meta_data:
                spend_dates = [row['date'] for row in expanded_meta_data if row['daily_spend'] > 0]
                if spend_dates:
                    first_spend_date = min(spend_dates)
                    last_spend_date = max(spend_dates)
            
            # Get daily Meta breakdown data for chart period
            meta_query = f"""
            SELECT date,
                   SUM(spend) as daily_spend,
                   SUM(impressions) as daily_impressions,
                   SUM(clicks) as daily_clicks,
                   SUM(meta_trials) as daily_meta_trials,
                   SUM(meta_purchases) as daily_meta_purchases
            FROM {breakdown_table}
            WHERE {meta_where} AND date BETWEEN ? AND ?
            GROUP BY date
            ORDER BY date ASC
            """
            
            meta_data = self._execute_meta_query(meta_query, 
                [parent_entity_id, breakdown_value, chart_start_date, chart_end_date])
            
            # Get daily Mixpanel breakdown data
            mixpanel_conn = sqlite3.connect(self.mixpanel_analytics_db_path)
            mixpanel_conn.row_factory = sqlite3.Row
            
            mixpanel_query = f"""
            SELECT 
                upm.credited_date as date,
                COUNT(CASE WHEN upm.current_status IN ('trial_pending', 'trial_cancelled', 'trial_converted') THEN 1 END) as daily_mixpanel_trials,
                COUNT(CASE WHEN upm.current_status IN ('initial_purchase', 'trial_converted') THEN 1 END) as daily_mixpanel_purchases,
                COUNT(CASE WHEN upm.current_status = 'trial_converted' THEN 1 END) as daily_mixpanel_conversions,
                SUM(CASE WHEN upm.current_status != 'refunded' THEN upm.current_value ELSE 0 END) as daily_mixpanel_revenue,
                SUM(CASE WHEN upm.current_status = 'refunded' THEN ABS(upm.current_value) ELSE 0 END) as daily_mixpanel_refunds,
                SUM(upm.current_value) as daily_estimated_revenue,
                COUNT(DISTINCT upm.distinct_id) as daily_attributed_users
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.{mixpanel_attr_field} = ? AND u.{mixpanel_filter_field} = ?
              AND upm.credited_date BETWEEN ? AND ?
            GROUP BY upm.credited_date
            ORDER BY upm.credited_date ASC
            """
            
            cursor = mixpanel_conn.cursor()
            cursor.execute(mixpanel_query, [parent_entity_id, mixpanel_filter_value, chart_start_date, chart_end_date])
            mixpanel_data = [dict(row) for row in cursor.fetchall()]
            mixpanel_conn.close()
            
            # Generate daily data structure (same as regular chart data)
            daily_data = {}
            current_date = data_start_date
            
            for i in range(14):  # 14 days total
                date_str = current_date.strftime('%Y-%m-%d')
                
                # Determine if this day should be grey (inactive)
                is_inactive = False
                if first_spend_date and last_spend_date:
                    is_inactive = date_str < first_spend_date or date_str > last_spend_date
                elif first_spend_date:
                    is_inactive = date_str < first_spend_date
                elif last_spend_date:
                    is_inactive = date_str > last_spend_date
                else:
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
                    'is_inactive': is_inactive
                }
                current_date += timedelta(days=1)
            
            # Overlay actual data
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
            
            # Calculate rolling ROAS using the same system as regular chart data
            from ..calculators.base_calculators import CalculationInput
            from ..calculators.roas_calculators import ROASCalculators
            from ..calculators.revenue_calculators import RevenueCalculators
            
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
            
            # Calculate daily metrics using the modular calculator system for ALL 20 days
            all_data = []
            for date in sorted(daily_data.keys()):
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
                
                all_data.append(day_data)
            
            # Calculate rolling 3-day ROAS for each day in the full dataset
            for i, day_data in enumerate(all_data):
                # Calculate rolling window (this day + up to 2 days back)
                rolling_start_idx = max(0, i - 2)
                rolling_days = all_data[rolling_start_idx:i + 1]
                
                # Sum spend and accuracy-adjusted revenue for the rolling window
                rolling_spend = sum(d['daily_spend'] for d in rolling_days)
                rolling_revenue = sum(RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(
                    CalculationInput(raw_record={
                        'estimated_revenue_usd': d['daily_estimated_revenue'],
                        'mixpanel_trials_started': d['daily_mixpanel_trials'],
                        'mixpanel_purchases': d['daily_mixpanel_purchases'],
                        'meta_trials_started': d['daily_meta_trials'],
                        'meta_purchases': d['daily_meta_purchases']
                    })
                ) for d in rolling_days)
                rolling_conversions = sum(d['daily_mixpanel_purchases'] for d in rolling_days)
                rolling_trials = sum(d['daily_mixpanel_trials'] for d in rolling_days)
                rolling_meta_trials = sum(d['daily_meta_trials'] for d in rolling_days)
                
                # Calculate rolling ROAS
                if rolling_spend > 0:
                    rolling_roas = rolling_revenue / rolling_spend
                else:
                    rolling_roas = 0.0
                
                # Add rolling metrics to day data
                day_data['rolling_1d_roas'] = round(rolling_roas, 2)
                day_data['rolling_1d_spend'] = rolling_spend
                day_data['rolling_1d_revenue'] = rolling_revenue
                day_data['rolling_1d_conversions'] = rolling_conversions
                day_data['rolling_1d_trials'] = rolling_trials
                day_data['rolling_1d_meta_trials'] = rolling_meta_trials
                day_data['rolling_window_days'] = len(rolling_days)
            
            # Extract only the 14-day display period (no need to skip days for 1-day rolling)
            chart_data = all_data  # Return all 14 days for display
            
            logger.info(f"📊 BREAKDOWN CHART RESULT: {len(chart_data)} display days for {breakdown_value} breakdown")
            
            return {
                'success': True,
                'chart_data': chart_data,
                'metadata': {
                    'entity_type': entity_type,
                    'entity_id': parent_entity_id,
                    'breakdown_type': config.breakdown,
                    'breakdown_value': breakdown_value,
                    'period_days': 14,
                    'rolling_window_days': 1,
                    'generated_at': now_in_timezone().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting breakdown chart data: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'chart_data': []
            } 

    def get_user_details_for_tooltip(self, entity_type: str, entity_id: str, start_date: str, end_date: str, 
                                    breakdown: str = 'all', breakdown_value: str = None) -> Dict[str, Any]:
        """
        Get individual user details for tooltip display on conversion rates
        
        Returns user-level breakdown of trial conversion rates, trial refund rates, and purchase refund rates
        for the specified entity and date range.
        """
        try:
            logger.info(f"🔍 Getting user details for tooltip: {entity_type} {entity_id}, {start_date} to {end_date}, breakdown={breakdown}")
            
            # Extract the actual entity ID from the prefixed ID format (e.g., "campaign_123" -> "123")
            if entity_id.startswith(f"{entity_type}_"):
                actual_entity_id = entity_id[len(f"{entity_type}_"):]
            else:
                actual_entity_id = entity_id
            
            # Build the query based on entity type and breakdown
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Base query to get user-level rate data
                if entity_type == 'campaign':
                    entity_field = 'u.abi_campaign_id'
                elif entity_type == 'adset':
                    entity_field = 'u.abi_ad_set_id'
                else:  # ad
                    entity_field = 'u.abi_ad_id'
                
                # Add breakdown filter if specified
                breakdown_filter = ""
                breakdown_params = []
                if breakdown == 'country' and breakdown_value:
                    breakdown_filter = "AND u.country = ?"
                    breakdown_params.append(breakdown_value)
                
                # Query for individual user rate data - ENHANCED: Include accuracy score and product_id, NO LIMIT
                user_details_query = f"""
                SELECT 
                    upm.distinct_id,
                    u.country,
                    u.region,
                    upm.store as device_category,
                    upm.current_status,
                    upm.trial_conversion_rate,
                    upm.trial_converted_to_refund_rate,
                    upm.initial_purchase_to_refund_rate,
                    upm.current_value,
                    upm.credited_date,
                    upm.price_bucket,
                    u.economic_tier,
                    upm.accuracy_score,
                    upm.product_id
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE {entity_field} = ?
                  AND upm.credited_date BETWEEN ? AND ?
                  {breakdown_filter}
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND upm.trial_converted_to_refund_rate IS NOT NULL  
                  AND upm.initial_purchase_to_refund_rate IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = upm.distinct_id 
                      AND e.event_name = 'RC Trial started'
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
                ORDER BY upm.trial_conversion_rate DESC
                """
                
                query_params = [actual_entity_id, start_date, end_date] + breakdown_params + [start_date, end_date]
                cursor.execute(user_details_query, query_params)
                user_records = [dict(row) for row in cursor.fetchall()]
                
                # Calculate summary statistics
                if user_records:
                    trial_conversion_rates = [r['trial_conversion_rate'] for r in user_records if r['trial_conversion_rate'] is not None]
                    trial_refund_rates = [r['trial_converted_to_refund_rate'] for r in user_records if r['trial_converted_to_refund_rate'] is not None]
                    purchase_refund_rates = [r['initial_purchase_to_refund_rate'] for r in user_records if r['initial_purchase_to_refund_rate'] is not None]
                    
                    summary_stats = {
                        'total_users': len(user_records),
                        'avg_trial_conversion_rate': (sum(trial_conversion_rates) / len(trial_conversion_rates) * 100) if trial_conversion_rates else 0,
                        'avg_trial_refund_rate': (sum(trial_refund_rates) / len(trial_refund_rates) * 100) if trial_refund_rates else 0,
                        'avg_purchase_refund_rate': (sum(purchase_refund_rates) / len(purchase_refund_rates) * 100) if purchase_refund_rates else 0,
                        'total_estimated_revenue': sum(r['current_value'] for r in user_records if r['current_value']),
                        'breakdown_applied': breakdown,
                        'breakdown_value': breakdown_value
                    }
                else:
                    summary_stats = {
                        'total_users': 0,
                        'avg_trial_conversion_rate': 0,
                        'avg_trial_refund_rate': 0,
                        'avg_purchase_refund_rate': 0,
                        'total_estimated_revenue': 0,
                        'breakdown_applied': breakdown,
                        'breakdown_value': breakdown_value
                    }
                
                # Format user records for display (convert rates to percentages) - ENHANCED: Include accuracy score and product_id
                formatted_users = []
                for record in user_records:  # Return ALL users (no limit)
                    formatted_users.append({
                        'distinct_id': record['distinct_id'],  # Full ID for copy functionality
                        'country': record['country'] or 'N/A',
                        'region': record['region'] or 'N/A',
                        'device_category': record['device_category'] or 'N/A',
                        'status': record['current_status'],
                        'trial_conversion_rate': round(record['trial_conversion_rate'] * 100, 1) if record['trial_conversion_rate'] else 0,
                        'trial_refund_rate': round(record['trial_converted_to_refund_rate'] * 100, 1) if record['trial_converted_to_refund_rate'] else 0,
                        'purchase_refund_rate': round(record['initial_purchase_to_refund_rate'] * 100, 1) if record['initial_purchase_to_refund_rate'] else 0,
                        'estimated_value': round(record['current_value'], 2) if record['current_value'] else 0,
                        'credited_date': record['credited_date'],
                        'price_bucket': f"${record['price_bucket']:.2f}" if record['price_bucket'] else 'N/A',
                        'economic_tier': record['economic_tier'] or 'N/A',
                        'accuracy_score': record['accuracy_score'] or 'N/A',
                        'product_id': record['product_id'] or 'N/A'
                    })
                
                logger.info(f"✅ Retrieved user details: {summary_stats['total_users']} users, avg rates: {summary_stats['avg_trial_conversion_rate']:.1f}%/{summary_stats['avg_trial_refund_rate']:.1f}%/{summary_stats['avg_purchase_refund_rate']:.1f}%")
                
                return {
                    'success': True,
                    'summary': summary_stats,
                    'users': formatted_users,
                    'entity_info': {
                        'entity_type': entity_type,
                        'entity_id': entity_id,
                        'actual_entity_id': actual_entity_id
                    },
                    'date_range': f"{start_date} to {end_date}",
                    'generated_at': now_in_timezone().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting user details for tooltip: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'entity_info': {
                    'entity_type': entity_type,
                    'entity_id': entity_id
                },
                'generated_at': now_in_timezone().isoformat()
            }

    def get_earliest_meta_date(self) -> str:
        """Get the earliest date available in meta analytics database"""
        try:
            # Connect to meta analytics database
            conn = sqlite3.connect(self.meta_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Query all performance tables to find the absolute earliest date
            tables = ['ad_performance_daily', 'ad_performance_daily_country', 
                     'ad_performance_daily_region', 'ad_performance_daily_device']
            
            earliest_dates = []
            
            for table in tables:
                try:
                    query = f"SELECT MIN(date) as earliest_date FROM {table} WHERE date IS NOT NULL AND date != ''"
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    if result and result['earliest_date']:
                        earliest_dates.append(result['earliest_date'])
                        
                except sqlite3.Error as e:
                    logger.warning(f"Could not query {table}: {e}")
                    continue
            
            conn.close()
            
            # Return the earliest date found across all tables
            if earliest_dates:
                return min(earliest_dates)
            else:
                # Fallback if no data found
                return '2025-01-01'
                
        except Exception as e:
            logger.error(f"Error getting earliest meta date: {e}", exc_info=True)
            # Return fallback date
            return '2025-01-01'

    def get_available_date_range(self) -> Dict[str, Any]:
        """Get available date range from analytics data"""
        try:
            logger.info("Getting available date range for analytics data")
            earliest_date = self.get_earliest_meta_date()
            latest_date = now_in_timezone().strftime('%Y-%m-%d')
            
            return {
                'success': True,
                'data': {
                    'earliest_date': earliest_date,
                    'latest_date': latest_date
                },
                'timestamp': now_in_timezone().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting available date range: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'data': {
                    'earliest_date': '2025-01-01',
                    'latest_date': now_in_timezone().strftime('%Y-%m-%d')
                }
            }

    def get_segment_performance(self, filters: Dict[str, Any], sort_column: str = 'trial_conversion_rate', 
                              sort_direction: str = 'desc') -> Dict[str, Any]:
        """
        Get segment performance data for conversion rate analysis
        
        Returns unique segments based on conversion rate cohort properties:
        - product_id, price_bucket, store, economic_tier, country, region
        - Shows user count, conversion rates, and accuracy level for each segment
        """
        try:
            logger.info(f"Getting segment performance data with filters: {filters}")
            
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Build the base query to get unique segments
                # Group by the key segment properties used in conversion rate calculation
                base_query = """
                SELECT 
                    upm.product_id,
                    upm.store,
                    u.country,
                    u.region,
                    upm.price_bucket,
                    upm.accuracy_score,
                    upm.assignment_type,
                    COUNT(DISTINCT upm.distinct_id) as user_count,
                    AVG(upm.trial_conversion_rate) as trial_conversion_rate,
                    AVG(upm.trial_converted_to_refund_rate) as trial_converted_to_refund_rate,
                    AVG(upm.initial_purchase_to_refund_rate) as initial_purchase_to_refund_rate,
                    MIN(upm.credited_date) as earliest_credited_date,
                    MAX(upm.credited_date) as latest_credited_date
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND upm.trial_converted_to_refund_rate IS NOT NULL  
                  AND upm.initial_purchase_to_refund_rate IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                """
                
                # Add filters
                filter_conditions = []
                params = []
                
                if filters.get('product_id'):
                    filter_conditions.append("upm.product_id = ?")
                    params.append(filters['product_id'])
                    
                if filters.get('store'):
                    filter_conditions.append("upm.store = ?")
                    params.append(filters['store'])
                    
                if filters.get('country'):
                    filter_conditions.append("u.country = ?")
                    params.append(filters['country'])
                    
                if filters.get('region'):
                    filter_conditions.append("u.region = ?")
                    params.append(filters['region'])
                    
                if filters.get('price_bucket'):
                    filter_conditions.append("upm.price_bucket = ?")
                    params.append(float(filters['price_bucket']))
                    
                if filters.get('accuracy_score'):
                    # Handle comma-separated accuracy scores for multi-select
                    accuracy_scores = [score.strip() for score in filters['accuracy_score'].split(',') if score.strip()]
                    if accuracy_scores:
                        placeholders = ','.join(['?'] * len(accuracy_scores))
                        filter_conditions.append(f"upm.accuracy_score IN ({placeholders})")
                        params.extend(accuracy_scores)
                
                # Add the filter conditions to the query
                if filter_conditions:
                    base_query += " AND " + " AND ".join(filter_conditions)
                
                # Group by segment properties
                base_query += """
                GROUP BY 
                    upm.product_id,
                    upm.store,
                    u.country,
                    u.region,
                    upm.price_bucket,
                    upm.accuracy_score,
                    upm.assignment_type
                """
                
                # Add having clause for min user count filter
                if filters.get('min_user_count', 0) > 0:
                    base_query += " HAVING COUNT(DISTINCT upm.distinct_id) >= ?"
                    params.append(filters['min_user_count'])
                
                # Add ordering
                valid_sort_columns = [
                    'product_id', 'store', 'country', 'region',
                    'user_count', 'trial_conversion_rate', 'trial_converted_to_refund_rate',
                    'initial_purchase_to_refund_rate', 'accuracy_score', 'price_bucket'
                ]
                
                if sort_column in valid_sort_columns:
                    order_direction = 'DESC' if sort_direction.lower() == 'desc' else 'ASC'
                    base_query += f" ORDER BY {sort_column} {order_direction}"
                else:
                    base_query += " ORDER BY trial_conversion_rate DESC"
                
                # Limit results to prevent overwhelming response
                base_query += " LIMIT 1000"
                
                logger.info(f"Executing segment query with {len(params)} parameters")
                cursor.execute(base_query, params)
                segment_records = [dict(row) for row in cursor.fetchall()]
                
                # Get filter options for the UI dropdowns
                filter_options = self._get_segment_filter_options(cursor)
                
                logger.info(f"✅ Retrieved {len(segment_records)} segments")
                
                return {
                    'success': True,
                    'data': {
                        'segments': segment_records,
                        'filter_options': filter_options
                    },
                    'metadata': {
                        'total_segments': len(segment_records),
                        'filters_applied': filters,
                        'sort_column': sort_column,
                        'sort_direction': sort_direction,
                        'generated_at': now_in_timezone().isoformat()
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting segment performance data: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'data': {
                    'segments': [],
                    'filter_options': {}
                }
            }
    
    def _get_segment_filter_options(self, cursor) -> Dict[str, List[Any]]:
        """Get available filter options for segment analysis dropdowns"""
        try:
            filter_options = {
                'product_ids': [],
                'stores': [],
                'countries': [],
                'regions': [],
                'price_buckets': [],
                'accuracy_scores': []
            }
            
            # Get unique product IDs
            cursor.execute("""
                SELECT DISTINCT upm.product_id 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND upm.product_id IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY upm.product_id
            """)
            filter_options['product_ids'] = [row[0] for row in cursor.fetchall()]
            
            # Get unique stores
            cursor.execute("""
                SELECT DISTINCT upm.store 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND upm.store IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY upm.store
            """)
            filter_options['stores'] = [row[0] for row in cursor.fetchall()]
            
            # Get unique countries
            cursor.execute("""
                SELECT DISTINCT u.country 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND u.country IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY u.country
            """)
            filter_options['countries'] = [row[0] for row in cursor.fetchall()]
            
            # Get unique regions
            cursor.execute("""
                SELECT DISTINCT u.region 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND u.region IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY u.region
            """)
            filter_options['regions'] = [row[0] for row in cursor.fetchall()]
            
            # Get unique price buckets
            cursor.execute("""
                SELECT DISTINCT upm.price_bucket 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND upm.price_bucket IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY upm.price_bucket
            """)
            filter_options['price_buckets'] = [row[0] for row in cursor.fetchall()]
            
            # Get unique accuracy scores
            cursor.execute("""
                SELECT DISTINCT upm.accuracy_score 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.valid_user = TRUE 
                  AND upm.accuracy_score IS NOT NULL
                  AND upm.assignment_type NOT LIKE '%inherited%'
                ORDER BY 
                    CASE upm.accuracy_score
                        WHEN 'very_high' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                        WHEN 'default' THEN 5
                        ELSE 6
                    END
            """)
            filter_options['accuracy_scores'] = [row[0] for row in cursor.fetchall()]
            
            return filter_options
            
        except Exception as e:
            logger.error(f"Error getting segment filter options: {e}", exc_info=True)
            return {
                'product_ids': [],
                'stores': [],
                'countries': [],
                'regions': [],
                'price_buckets': [],
                'accuracy_scores': []
            }

    def get_overview_roas_chart_data(self, start_date: str, end_date: str, breakdown: str = 'all') -> Dict[str, Any]:
        """
        Get overview ROAS sparkline data for dashboard summary
        
        This aggregates data across all campaigns to show overall performance trends
        """
        try:
            # Getting overview ROAS chart data
            
            # Calculate date range for chart period
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Calculate how many days we need
            date_diff = (end_dt - start_dt).days + 1
            # Generate date range for overview period
            
            # Generate all dates in the requested range
            daily_data = {}
            current_dt = start_dt
            
            # Initialize all days with zero values
            while current_dt <= end_dt:
                date_str = current_dt.strftime('%Y-%m-%d')
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
                    'is_inactive': True  # Will be set to False if we have data
                }
                current_dt += timedelta(days=1)
            
            # ✅ STEP 1: Get aggregated Meta data from correct table (ad_performance_daily)
            # Step 1: Query Meta data
            
            # Determine which table to use based on breakdown
            table_name = self.get_table_name(breakdown)  # Uses existing table mapping logic
            
            meta_query = f"""
                SELECT 
                    date,
                    SUM(spend) as daily_spend,
                    SUM(impressions) as daily_impressions,
                    SUM(clicks) as daily_clicks,
                    SUM(meta_trials) as daily_meta_trials,
                    SUM(meta_purchases) as daily_meta_purchases
                FROM {table_name}
                WHERE date BETWEEN ? AND ?
                GROUP BY date
                ORDER BY date
            """
            
            meta_data = self._execute_meta_query(meta_query, [start_date, end_date])
            logger.info(f"📊 Retrieved {len(meta_data)} days of Meta data")
            
            # ✅ STEP 2: Get aggregated Mixpanel data from correct tables
            # Step 2: Query Mixpanel data
            
            # Get estimated revenue from user_product_metrics (attributed by credited_date)
            with get_database_connection('mixpanel_data') as mixpanel_conn:
                mixpanel_conn.row_factory = sqlite3.Row
                mixpanel_cursor = mixpanel_conn.cursor()
                
                # Query for estimated revenue from user lifecycle predictions
                revenue_query = """
                SELECT 
                    upm.credited_date as date,
                    SUM(upm.current_value) as daily_estimated_revenue,
                    COUNT(DISTINCT upm.distinct_id) as daily_attributed_users
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.credited_date BETWEEN ? AND ?
                  AND u.has_abi_attribution = TRUE
                GROUP BY upm.credited_date
                ORDER BY upm.credited_date
                """
                
                mixpanel_cursor.execute(revenue_query, [start_date, end_date])
                revenue_data = [dict(row) for row in mixpanel_cursor.fetchall()]
                
                # Query for trial/purchase events aggregated by event date
                events_query = """
                SELECT 
                    DATE(e.event_time) as date,
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' THEN u.distinct_id END) as daily_mixpanel_trials,
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' THEN u.distinct_id END) as daily_mixpanel_purchases,
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' THEN u.distinct_id END) as daily_mixpanel_conversions,
                    COALESCE(SUM(CASE WHEN e.event_name = 'RC Initial purchase' THEN e.revenue_usd ELSE 0 END), 0) as daily_mixpanel_revenue,
                    COALESCE(SUM(CASE WHEN e.event_name = 'RC Cancellation' THEN ABS(e.revenue_usd) ELSE 0 END), 0) as daily_mixpanel_refunds
                FROM mixpanel_event e
                JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
                WHERE DATE(e.event_time) BETWEEN ? AND ?
                  AND u.has_abi_attribution = TRUE
                  AND e.event_name IN ('RC Trial started', 'RC Initial purchase', 'RC Cancellation')
                GROUP BY DATE(e.event_time)
                ORDER BY DATE(e.event_time)
                """
                
                mixpanel_cursor.execute(events_query, [start_date, end_date])
                events_data = [dict(row) for row in mixpanel_cursor.fetchall()]
                
                logger.info(f"📊 Retrieved {len(revenue_data)} days of revenue data and {len(events_data)} days of events data")
            
            # Combine revenue and events data
            mixpanel_data = {}
            for row in revenue_data:
                date = row['date']
                mixpanel_data[date] = dict(row)
            
            for row in events_data:
                date = row['date']
                if date in mixpanel_data:
                    mixpanel_data[date].update(row)
                else:
                    mixpanel_data[date] = dict(row)
            
            # ✅ STEP 3: Overlay actual data onto the date framework
            # Overlay Meta data
            for row in meta_data:
                date = row['date']
                if date in daily_data:
                    daily_data[date].update({
                        'daily_spend': float(row.get('daily_spend', 0) or 0),
                        'daily_impressions': int(row.get('daily_impressions', 0) or 0),
                        'daily_clicks': int(row.get('daily_clicks', 0) or 0),
                        'daily_meta_trials': int(row.get('daily_meta_trials', 0) or 0),
                        'daily_meta_purchases': int(row.get('daily_meta_purchases', 0) or 0),
                        'is_inactive': False  # Has data
                    })
            
            # Overlay Mixpanel data (now a dictionary)
            for date, row in mixpanel_data.items():
                if date in daily_data:
                    daily_data[date].update({
                        'daily_mixpanel_trials': int(row.get('daily_mixpanel_trials', 0) or 0),
                        'daily_mixpanel_purchases': int(row.get('daily_mixpanel_purchases', 0) or 0),
                        'daily_mixpanel_conversions': int(row.get('daily_mixpanel_conversions', 0) or 0),
                        'daily_mixpanel_revenue': float(row.get('daily_mixpanel_revenue', 0) or 0),
                        'daily_mixpanel_refunds': float(row.get('daily_mixpanel_refunds', 0) or 0),
                        'daily_estimated_revenue': float(row.get('daily_estimated_revenue', 0) or 0),
                        'daily_attributed_users': int(row.get('daily_attributed_users', 0) or 0),
                        'is_inactive': False  # Has data
                    })
            
            # ✅ STEP 4: Calculate rolling ROAS using the same system as individual charts
            from ..calculators.base_calculators import CalculationInput
            from ..calculators.roas_calculators import ROASCalculators
            from ..calculators.revenue_calculators import RevenueCalculators
            
            # Calculate overall accuracy ratio for the period
            total_mixpanel_trials = sum(d['daily_mixpanel_trials'] for d in daily_data.values())
            total_meta_trials = sum(d['daily_meta_trials'] for d in daily_data.values())
            total_mixpanel_purchases = sum(d['daily_mixpanel_purchases'] for d in daily_data.values())
            total_meta_purchases = sum(d['daily_meta_purchases'] for d in daily_data.values())
            
            # Determine event priority and accuracy ratio
            if total_mixpanel_trials == 0 and total_mixpanel_purchases == 0:
                event_priority = 'trials'
                overall_accuracy_ratio = 0.0
            elif total_mixpanel_trials > total_mixpanel_purchases:
                event_priority = 'trials'
                overall_accuracy_ratio = (total_mixpanel_trials / total_meta_trials) if total_meta_trials > 0 else 0.0
            else:
                event_priority = 'purchases'
                overall_accuracy_ratio = (total_mixpanel_purchases / total_meta_purchases) if total_meta_purchases > 0 else 0.0
            
            # Overview accuracy calculated
            
            # Convert to list and calculate rolling metrics
            all_data = []
            for date in sorted(daily_data.keys()):
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
                
                all_data.append(day_data)
            
            # Calculate rolling 1-day ROAS for each day in the full dataset
            for i, day_data in enumerate(all_data):
                # Calculate rolling window (current day only)
                rolling_days = [day_data]  # Only current day
                
                # Sum spend and accuracy-adjusted revenue for the rolling window (just current day)
                rolling_spend = day_data['daily_spend']
                rolling_revenue = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(
                    CalculationInput(raw_record={
                        'estimated_revenue_usd': day_data['daily_estimated_revenue'],
                        'mixpanel_trials_started': day_data['daily_mixpanel_trials'],
                        'mixpanel_purchases': day_data['daily_mixpanel_purchases'],
                        'meta_trials_started': day_data['daily_meta_trials'],
                        'meta_purchases': day_data['daily_meta_purchases']
                    })
                )
                rolling_conversions = day_data['daily_mixpanel_purchases']
                rolling_trials = day_data['daily_mixpanel_trials']
                rolling_meta_trials = day_data['daily_meta_trials']
                
                # Calculate rolling ROAS
                if rolling_spend > 0:
                    rolling_roas = rolling_revenue / rolling_spend
                else:
                    rolling_roas = 0.0
                
                # Add rolling metrics to day data
                day_data['rolling_1d_roas'] = round(rolling_roas, 2)
                day_data['rolling_1d_spend'] = rolling_spend
                day_data['rolling_1d_revenue'] = rolling_revenue
                day_data['rolling_1d_conversions'] = rolling_conversions
                day_data['rolling_1d_trials'] = rolling_trials
                day_data['rolling_1d_meta_trials'] = rolling_meta_trials
                day_data['rolling_window_days'] = len(rolling_days)  # For tooltip info
            
            # Return all the data (no need to limit since we only generate the requested range)
            chart_data = all_data
            
            # Summary logging
            total_days_with_data = sum(1 for d in chart_data if not d.get('is_inactive', False))
            total_spend = sum(d['daily_spend'] for d in chart_data)
            total_revenue = sum(d['daily_estimated_revenue'] for d in chart_data)
            avg_daily_roas = sum(d['rolling_1d_roas'] for d in chart_data) / len(chart_data) if chart_data else 0
            
                    # Overview chart data completed
            
            return {
                'success': True,
                'chart_data': chart_data,
                'entity_type': 'overview',
                'entity_id': 'all_campaigns',
                'date_range': f"{start_date} to {end_date}",
                'total_days': len(chart_data),
                'active_days': total_days_with_data,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'avg_roas': avg_daily_roas,
                'period_info': f"Overview data for {len(chart_data)}-day period from {start_date} to {end_date}",
                'rolling_calculation_info': f"1-day rolling averages calculated for all {len(chart_data)} days",
                'breakdown': breakdown,
                'metadata': {
                    'table_used': table_name,
                    'accuracy_ratio': overall_accuracy_ratio,
                    'event_priority': event_priority,
                    'generated_at': now_in_timezone().isoformat()
                }
            }
                
        except Exception as e:
            logger.error(f"❌ Error getting overview ROAS chart data: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'chart_data': [],
                'metadata': {
                    'generated_at': now_in_timezone().isoformat(),
                    'error_details': str(e),
                    'date_range_requested': f"{start_date} to {end_date}",
                    'breakdown_requested': breakdown
                }
            }