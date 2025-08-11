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

# Import calculator classes for performance calculations
from ..calculators.base_calculators import CalculationInput
from ..calculators.roas_calculators import ROASCalculators

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
        
        # ENHANCED FIX: Store fallback paths for more robust initialization
        self._fallback_paths_checked = False
        
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
                import os
                
                # Enhanced diagnostics for debugging
                logger.info(f"🔧 BREAKDOWN SERVICE INITIALIZATION DEBUG:")
                logger.info(f"   mixpanel_db_path: {self.mixpanel_db_path}")
                logger.info(f"   meta_db_path: {self.meta_db_path}")
                logger.info(f"   mixpanel_db_exists: {os.path.exists(self.mixpanel_db_path) if self.mixpanel_db_path else False}")
                logger.info(f"   meta_db_exists: {os.path.exists(self.meta_db_path) if self.meta_db_path else False}")
                
                # Try to initialize with available paths
                if self.mixpanel_db_path and self.meta_db_path:
                    # Check if both databases exist
                    if os.path.exists(self.mixpanel_db_path) and os.path.exists(self.meta_db_path):
                        logger.info(f"🔧 Initializing BreakdownMappingService: mixpanel={self.mixpanel_db_path}, meta={self.meta_db_path}")
                        self._breakdown_service = BreakdownMappingService(
                            self.mixpanel_db_path, 
                            meta_db_path=self.meta_db_path
                        )
                        logger.info("✅ BreakdownMappingService initialized successfully")
                    else:
                        logger.warning(f"❌ Database files not found - trying fallback initialization")
                        # Try fallback initialization with None paths (service will handle its own path discovery)
                        try:
                            self._breakdown_service = BreakdownMappingService()
                            logger.info("✅ BreakdownMappingService initialized with fallback path discovery")
                        except Exception as fallback_error:
                            logger.warning(f"❌ Fallback initialization also failed: {fallback_error}")
                            self._breakdown_service = None
                else:
                    logger.warning(f"❌ Cannot initialize BreakdownMappingService: missing paths (mixpanel={bool(self.mixpanel_db_path)}, meta={bool(self.meta_db_path)})")
                    logger.warning(f"   This will disable breakdown functionality (country/device breakdown icons won't appear)")
                    # Try one last fallback with automatic path discovery
                    try:
                        logger.info("🔧 Attempting automatic path discovery for BreakdownMappingService...")
                        self._breakdown_service = BreakdownMappingService()
                        logger.info("✅ BreakdownMappingService initialized with automatic path discovery")
                    except Exception as auto_discovery_error:
                        logger.warning(f"❌ Automatic path discovery failed: {auto_discovery_error}")
                        self._breakdown_service = None
                    
            except Exception as e:
                logger.error(f"❌ Failed to initialize BreakdownMappingService: {e}")
                logger.error(f"   Error type: {type(e).__name__}")
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
            
            # 🎯 ARCHITECTURE DECISION: 
            # For ALL queries (campaign, adset, ad), ALWAYS use pre-computed data for Mixpanel metrics
            # This ensures accurate user deduplication and prevents missing ads
            # But still maintain hierarchical structure for drill-down capability
            if config.group_by in ['campaign', 'adset', 'ad']:
                logger.info(f"🎯 {config.group_by.upper()} QUERY DETECTED - Using hybrid approach (pre-computed Mixpanel + Meta data)")
                hierarchical_result = self._execute_mixpanel_only_query(config)
            elif meta_data_count == 0:
                logger.info(f"📊 No Meta data - Using Mixpanel-only data")
                hierarchical_result = self._execute_mixpanel_only_query(config)
            else:
                logger.info(f"📊 Meta data available - Using hierarchical approach")
                hierarchical_result = self._execute_hierarchical_query(config, table_name)
            
            # Check if we got valid hierarchical data
            if not hierarchical_result.get('success') or not hierarchical_result.get('data'):
                return hierarchical_result
            
            # PERFORMANCE OPTIMIZATION: Batch calculate all entity rates before processing
            logger.info("⚡ Collecting entities for batch rate calculation...")
            all_entities = self._collect_all_entities_from_hierarchy(hierarchical_result['data'])
            
            if all_entities:
                logger.info(f"⚡ Batch calculating rates for {len(all_entities)} entities...")
                self._rates_cache = self._batch_calculate_entity_rates(all_entities, config)
                logger.info(f"⚡ Cached rates for {len(self._rates_cache)} entities")
            else:
                self._rates_cache = {}
            
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
        """Get campaign-level data using HYBRID approach (pre-computed Mixpanel + Meta data)"""
        
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if daily_mixpanel_metrics exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
                if not cursor.fetchone():
                    logger.error("daily_mixpanel_metrics table not found. Run pipeline to generate pre-computed data.")
                    return []
                
                # FIXED: Use subquery approach with canonical names
                precomputed_query = """
                SELECT 
                    campaign_data.entity_id as campaign_id,
                    COALESCE(nm.canonical_name, 'Unknown Campaign (' || campaign_data.entity_id || ')') as campaign_name,
                    campaign_data.total_users,
                    campaign_data.total_users as new_users,
                    campaign_data.mixpanel_trials_started,
                    campaign_data.mixpanel_purchases,
                    campaign_data.mixpanel_revenue_usd,
                    campaign_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as mixpanel_revenue_usd,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'campaign'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) campaign_data
                LEFT JOIN id_name_mapping nm ON campaign_data.entity_id = nm.entity_id AND nm.entity_type = 'campaign'
                ORDER BY campaign_data.estimated_revenue_usd DESC
                """
                
                cursor.execute(precomputed_query, [config.start_date, config.end_date])
                results = cursor.fetchall()
                
                if results:
                    logger.info(f"📊 CAMPAIGN HYBRID: Found {len(results)} campaigns with pre-computed data")
                    
                    # Now fetch Meta data for the same campaigns and merge it
                    campaign_ids = [row['campaign_id'] for row in results]
                    meta_data = self._get_meta_data_for_campaigns(campaign_ids, config)
                    
                    # Format results with both Mixpanel and Meta data
                    formatted_campaigns = []
                    for row in results:
                        campaign_id = row['campaign_id']
                        meta_info = meta_data.get(campaign_id, {})
                        
                        # Calculate metrics
                        mixpanel_trials = int(row['mixpanel_trials_started'])
                        mixpanel_purchases = int(row['mixpanel_purchases'])
                        meta_trials = meta_info.get('meta_trials_started', 0)
                        meta_purchases = meta_info.get('meta_purchases', 0)
                        
                        # Accuracy ratios
                        # Special case: If meta_trials = 0 but mixpanel_trials > 0, treat as 100% accuracy (1.0)
                        if meta_trials == 0 and mixpanel_trials > 0:
                            trial_accuracy_ratio = 1.0  # 100% accuracy for calculations
                        else:
                            trial_accuracy_ratio = (mixpanel_trials / meta_trials) if meta_trials > 0 else 0.0
                        purchase_accuracy_ratio = (mixpanel_purchases / meta_purchases) if meta_purchases > 0 else 0.0
                        trial_conversion_rate = (mixpanel_purchases / mixpanel_trials) if mixpanel_trials > 0 else 0.0
                        
                        # Financial metrics WITH ADJUSTMENT
                        spend = meta_info.get('spend', 0.0)
                        estimated_revenue_raw = float(row['estimated_revenue_usd'])
                        estimated_revenue_adjusted = (estimated_revenue_raw / trial_accuracy_ratio) if trial_accuracy_ratio > 0 else estimated_revenue_raw
                        estimated_roas = (estimated_revenue_adjusted / spend) if spend > 0 else 0.0
                        profit = estimated_revenue_adjusted - spend
                        
                        # Refund rates (placeholder for now)
                        trial_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        purchase_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        
                        formatted_campaign = {
                            'id': f"campaign_{campaign_id}",
                            'entity_type': 'campaign',
                            'campaign_id': campaign_id,
                            'campaign_name': row['campaign_name'],
                            'name': row['campaign_name'],
                            
                            # Meta metrics from regular Meta tables
                            'spend': spend,
                            'impressions': meta_info.get('impressions', 0),
                            'clicks': meta_info.get('clicks', 0),
                            'meta_trials_started': meta_trials,
                            'meta_purchases': meta_purchases,
                            
                            # Mixpanel metrics from pre-computed data
                            'mixpanel_trials_started': mixpanel_trials,
                            'mixpanel_purchases': mixpanel_purchases,
                            'mixpanel_revenue_usd': float(row['mixpanel_revenue_usd']),
                            
                            # ✅ FIXED: Adjusted revenue and accuracy ratios
                            'estimated_revenue_usd': estimated_revenue_adjusted,  # This is the ADJUSTED revenue
                            'estimated_revenue_adjusted': estimated_revenue_adjusted,
                            'estimated_roas': estimated_roas,
                            'profit': profit,
                            'trial_accuracy_ratio': trial_accuracy_ratio,
                            'purchase_accuracy_ratio': purchase_accuracy_ratio,  # ✅ FIXED: Now calculated properly
                            
                            # ✅ FIXED: Rate calculations
                            'avg_trial_conversion_rate': trial_conversion_rate,
                            'trial_conversion_rate': trial_conversion_rate,
                            'conversion_rate': trial_conversion_rate,
                            'avg_trial_refund_rate': trial_refund_rate,  # ✅ FIXED: Now provided
                            'avg_purchase_refund_rate': purchase_refund_rate,  # ✅ FIXED: Now provided
                            'trial_refund_rate': trial_refund_rate,
                            'purchase_refund_rate': purchase_refund_rate,
                            
                            # Additional info
                            'total_users': int(row['total_users']),
                            'new_users': int(row['new_users']),
                            'children': []
                        }
                        formatted_campaigns.append(formatted_campaign)
                    
                    # 🔄 HIERARCHICAL EXPANSION: Fetch child adsets for each campaign
                    logger.info(f"🔄 FETCHING CHILD ADSETS for {len(formatted_campaigns)} campaigns")
                    
                    for campaign in formatted_campaigns:
                        campaign_id = campaign['campaign_id']
                        
                        # Get adsets for this campaign using pre-computed data + Meta
                        adset_children = self._get_child_adsets_for_campaign(campaign_id, config)
                        campaign['children'] = adset_children
                        
                        logger.debug(f"Campaign {campaign_id}: {len(adset_children)} child adsets")
                    
                    logger.info(f"✅ Returning {len(formatted_campaigns)} hybrid campaigns with hierarchical children")
                    return formatted_campaigns
                else:
                    logger.warning("No pre-computed campaign data found for date range. Run pipeline to generate data.")
                    return []
                    
        except Exception as e:
            logger.error(f"Error loading hybrid campaign data: {e}")
            return []
    
    def _get_mixpanel_adset_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get adset-level data from Mixpanel using ONLY pre-computed metrics (fast & accurate)"""
        
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if daily_mixpanel_metrics exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
                if not cursor.fetchone():
                    logger.error("daily_mixpanel_metrics table not found. Run pipeline to generate pre-computed data.")
                    return []
                
                # FIXED: Use subquery to avoid JOIN multiplication issues
                precomputed_query = """
        SELECT 
                    adset_data.entity_id as adset_id,
                    COALESCE(nm.canonical_name, 'Unknown Adset (' || adset_data.entity_id || ')') as adset_name,
                    COALESCE(hm.campaign_id, '') as campaign_id,
                    COALESCE(cm.canonical_name, 'Unknown Campaign (' || COALESCE(hm.campaign_id, 'No ID') || ')') as campaign_name,
                    adset_data.total_users,
                    adset_data.total_users as new_users,
                    adset_data.mixpanel_trials_started,
                    adset_data.mixpanel_purchases,
                    adset_data.mixpanel_revenue_usd,
                    adset_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as mixpanel_revenue_usd,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'adset'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) adset_data
                LEFT JOIN id_name_mapping nm ON adset_data.entity_id = nm.entity_id AND nm.entity_type = 'adset'
                LEFT JOIN (
                    SELECT DISTINCT adset_id, campaign_id 
                    FROM id_hierarchy_mapping 
                    WHERE adset_id IS NOT NULL
                ) hm ON adset_data.entity_id = hm.adset_id
                LEFT JOIN id_name_mapping cm ON hm.campaign_id = cm.entity_id AND cm.entity_type = 'campaign'
                ORDER BY adset_data.estimated_revenue_usd DESC
                """
                
                # Debug the exact query being executed
                params = [config.start_date, config.end_date]
                logger.info(f"🔍 EXECUTING ADSET QUERY:")
                logger.info(f"   📅 START DATE: {params[0]}")
                logger.info(f"   📅 END DATE: {params[1]}")
                logger.info(f"   🔍 SQL: {precomputed_query.strip()}")
                
                cursor.execute(precomputed_query, params)
                results = cursor.fetchall()
                
                if results:
                    logger.info("=" * 80)
                    logger.info(f"🎯 PRE-COMPUTED ADSET DATA RESULTS ({len(results)} adsets)")
                    logger.info(f"📅 Date Range: {config.start_date} to {config.end_date}")
                    logger.info("=" * 80)
                    
                    target_found = False
                    for row in results:
                        adset_id = row['adset_id']
                        trials = int(row['mixpanel_trials_started'])
                        purchases = int(row['mixpanel_purchases'])
                        revenue = float(row['estimated_revenue_usd'])
                        
                        # Special highlight for our target ad set
                        if adset_id == "120223331225270178":
                            target_found = True
                            logger.info(f"🎯🎯🎯 TARGET AD SET FOUND! 🎯🎯🎯")
                            logger.info(f"📊 AD SET ID: {adset_id}")
                            logger.info(f"📊 AD SET NAME: {row['adset_name']}")
                            logger.info(f"   ├─ TRIALS: {trials}")
                            logger.info(f"   ├─ PURCHASES: {purchases}")
                            logger.info(f"   └─ REVENUE: ${revenue:.2f}")
                            logger.info(f"   🎯 Expected: 47 trials | Actual: {trials} trials")
                            if trials == 47:
                                logger.info(f"   ✅ PERFECT! Dashboard should now show 47 trials!")
                            else:
                                logger.info(f"   ❌ STILL WRONG! Expected 47, got {trials}")
                        else:
                            logger.info(f"📊 AD SET: {adset_id} | TRIALS: {trials} | PURCHASES: {purchases} | REVENUE: ${revenue:.2f}")
                        
                    if target_found:
                        logger.info(f"🎯 Using pre-computed data for target adset - dashboard should show 47 trials now!")
                    else:
                        logger.info(f"❌ Target adset 120223331225270178 NOT found in pre-computed data!")
                    
                    logger.info("=" * 80)
                    
                    # Now fetch Meta data for the same adsets and merge it
                    adset_ids = [row['adset_id'] for row in results]
                    meta_data = self._get_meta_data_for_adsets(adset_ids, config)
                    
                    # Format results for frontend consumption with both Mixpanel and Meta data
                    formatted_adsets = []
                    for row in results:
                        adset_id = row['adset_id']
                        meta_info = meta_data.get(adset_id, {})
                        
                        # Calculate accuracy ratio and other metrics
                        mixpanel_trials = int(row['mixpanel_trials_started'])
                        mixpanel_purchases = int(row['mixpanel_purchases'])
                        meta_trials = meta_info.get('meta_trials_started', 0)
                        meta_purchases = meta_info.get('meta_purchases', 0)
                        
                        # Core ratios
                        # Special case: If meta_trials = 0 but mixpanel_trials > 0, treat as 100% accuracy (1.0)
                        if meta_trials == 0 and mixpanel_trials > 0:
                            trial_accuracy_ratio = 1.0  # 100% accuracy for calculations
                        else:
                            trial_accuracy_ratio = (mixpanel_trials / meta_trials) if meta_trials > 0 else 0.0
                        purchase_accuracy_ratio = (mixpanel_purchases / meta_purchases) if meta_purchases > 0 else 0.0
                        trial_conversion_rate = (mixpanel_purchases / mixpanel_trials) if mixpanel_trials > 0 else 0.0
                        
                        # Financial metrics WITH ADJUSTMENT
                        spend = meta_info.get('spend', 0.0)
                        estimated_revenue_raw = float(row['estimated_revenue_usd'])
                        estimated_revenue_adjusted = (estimated_revenue_raw / trial_accuracy_ratio) if trial_accuracy_ratio > 0 else estimated_revenue_raw
                        estimated_roas = (estimated_revenue_adjusted / spend) if spend > 0 else 0.0
                        profit = estimated_revenue_adjusted - spend
                        
                        # Calculate performance impact score using modular calculator system
                        calc_record = {
                            'spend': spend,
                            'estimated_revenue_usd': estimated_revenue_adjusted,
                            'mixpanel_trials_started': mixpanel_trials,
                            'meta_trials_started': meta_trials,
                            'mixpanel_purchases': mixpanel_purchases,
                            'meta_purchases': meta_purchases
                        }
                        calc_input = CalculationInput(
                            raw_record=calc_record,
                            config=config.__dict__ if config else None,
                            start_date=config.start_date if config else None,
                            end_date=config.end_date if config else None
                        )
                        
                        # Calculate performance impact score 
                        performance_impact_score = ROASCalculators.calculate_performance_impact_score(calc_input)
                        
                        # DETAILED DEBUG: Let's see what's happening with the calculation
                        logger.info(f"🔍 DETAILED DEBUG for adset {adset_id}:")
                        logger.info(f"   💰 Raw spend from meta_info: ${meta_info.get('spend', 0.0)}")
                        logger.info(f"   💰 Final spend variable: ${spend}")
                        logger.info(f"   💵 Raw estimated_revenue_usd from row: ${row['estimated_revenue_usd']}")
                        logger.info(f"   💵 Calculated estimated_revenue_adjusted: ${estimated_revenue_adjusted}")
                        logger.info(f"   💵 Raw mixpanel_revenue_usd from row: ${row['mixpanel_revenue_usd']}")
                        logger.info(f"   📊 Trial accuracy ratio: {trial_accuracy_ratio}")
                        logger.info(f"   📈 Manual ROAS calculation: ${estimated_revenue_adjusted} / ${spend} = {estimated_roas}")
                        logger.info(f"   🎯 Performance impact score from calculator: {performance_impact_score}")
                        logger.info(f"   📅 Date range: {config.start_date} to {config.end_date}")
                        
                        # MANUAL VERIFICATION: Calculate what it SHOULD be
                        if spend > 0 and estimated_revenue_adjusted > 0:
                            manual_roas = estimated_revenue_adjusted / spend
                            manual_capped_roas = min(manual_roas, 4.0)
                            manual_base_score = spend * (manual_capped_roas ** 2)
                            logger.info(f"   ✅ MANUAL CALCULATION CHECK:")
                            logger.info(f"      ROAS: {manual_roas:.4f}")
                            logger.info(f"      Capped ROAS: {manual_capped_roas:.4f}")
                            logger.info(f"      Base score (spend × capped_roas²): ${spend} × {manual_capped_roas:.4f}² = ${manual_base_score:.2f}")
                        
                        # FALLBACK: If estimated revenue is zero but we have actual Mixpanel revenue, use that for ROAS calculation
                        if performance_impact_score == 0.0 and estimated_revenue_adjusted == 0.0 and spend > 0:
                            mixpanel_revenue = float(row['mixpanel_revenue_usd'])
                            if mixpanel_revenue > 0:
                                fallback_roas = mixpanel_revenue / spend
                                capped_fallback_roas = min(fallback_roas, 4.0)
                                # SIMPLIFIED: Just spend × ROAS² (no time scaling)
                                performance_impact_score = spend * (capped_fallback_roas ** 2)
                                logger.info(f"🔄 FALLBACK: spend=${spend} × (ROAS={fallback_roas:.4f})² = ${performance_impact_score:.2f}")
                        
                        # Final debug log if still zero
                        if performance_impact_score == 0.0:
                            logger.warning(f"⚠️ ZERO PERFORMANCE IMPACT for adset {adset_id}: spend=${spend}, est_rev=${estimated_revenue_adjusted}, mp_rev=${row['mixpanel_revenue_usd']}")
                        
                        # Refund rates (placeholder for now)
                        trial_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        purchase_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        
                        # Log calculations for target adset
                        if adset_id == "120223331225270178":
                            logger.info(f"🧮 TARGET ADSET CALCULATIONS:")
                            logger.info(f"   🎯 Mixpanel Trials: {mixpanel_trials}")
                            logger.info(f"   🎯 Meta Trials: {meta_trials}")
                            logger.info(f"   📊 Trial Accuracy Ratio: {trial_accuracy_ratio:.3f}")
                            logger.info(f"   📊 Purchase Accuracy Ratio: {purchase_accuracy_ratio:.3f}")
                            logger.info(f"   💰 Spend: ${spend:.2f}")
                            logger.info(f"   💵 Revenue (Raw): ${estimated_revenue_raw:.2f}")
                            logger.info(f"   💵 Revenue (Adjusted): ${estimated_revenue_adjusted:.2f}")
                            logger.info(f"   📈 ROAS: {estimated_roas:.2f}")
                            logger.info(f"   💸 Profit: ${profit:.2f}")
                        
                        formatted_adset = {
                            'id': f"adset_{adset_id}",
                            'entity_type': 'adset',
                            'adset_id': adset_id,
                            'adset_name': row['adset_name'],
                            'campaign_id': row['campaign_id'],
                            'campaign_name': row['campaign_name'],
                            'name': row['adset_name'],
                            
                            # Meta metrics from regular Meta tables
                            'spend': spend,
                            'impressions': meta_info.get('impressions', 0),
                            'clicks': meta_info.get('clicks', 0),
                            'meta_trials_started': meta_trials,
                            'meta_purchases': meta_purchases,
                            
                            # Mixpanel metrics from pre-computed data (accurate)
                            'mixpanel_trials_started': mixpanel_trials,
                            'mixpanel_purchases': mixpanel_purchases,
                            'mixpanel_revenue_usd': float(row['mixpanel_revenue_usd']),
                            
                            # ✅ FIXED: Adjusted revenue and accuracy ratios
                            'estimated_revenue_usd': estimated_revenue_adjusted,  # This is the ADJUSTED revenue
                            'estimated_revenue_adjusted': estimated_revenue_adjusted,  # Frontend expects this field
                            'estimated_roas': estimated_roas,
                            'performance_impact_score': performance_impact_score,  # ✅ FIXED: Now calculated using ROASCalculators
                            'profit': profit,
                            'trial_accuracy_ratio': trial_accuracy_ratio,
                            'purchase_accuracy_ratio': purchase_accuracy_ratio,  # ✅ FIXED: Now calculated properly
                            
                            # ✅ FIXED: Rate calculations
                            'avg_trial_conversion_rate': trial_conversion_rate,
                            'trial_conversion_rate': trial_conversion_rate,
                            'conversion_rate': trial_conversion_rate,
                            'avg_trial_refund_rate': trial_refund_rate,  # ✅ FIXED: Now provided
                            'avg_purchase_refund_rate': purchase_refund_rate,  # ✅ FIXED: Now provided
                            'trial_refund_rate': trial_refund_rate,
                            'purchase_refund_rate': purchase_refund_rate,
                            
                            # Additional info
                            'total_users': int(row['total_users']),
                            'new_users': int(row['new_users']),
                            'children': []
                        }
                        formatted_adsets.append(formatted_adset)
                    
                    # 🔄 HIERARCHICAL EXPANSION: Fetch child ads for each adset
                    logger.info(f"🔄 FETCHING CHILD ADS for {len(formatted_adsets)} adsets")
                    
                    for adset in formatted_adsets:
                        adset_id = adset['adset_id']
                        
                        # Get ads for this adset using pre-computed data + Meta
                        ad_children = self._get_child_ads_for_adset(adset_id, config)
                        adset['children'] = ad_children
                        
                        logger.debug(f"Adset {adset_id}: {len(ad_children)} child ads")
                    
                    logger.info(f"✅ Returning {len(formatted_adsets)} hybrid adsets with hierarchical children")
                    return formatted_adsets
                else:
                    logger.warning("No pre-computed adset data found for date range. Run pipeline to generate data.")
                    return []
                    
        except Exception as e:
            logger.error(f"Error loading pre-computed adset data: {e}")
            return []
    
    def _get_meta_data_for_adsets(self, adset_ids: List[str], config: QueryConfig) -> Dict[str, Dict]:
        """Get Meta data for specific adset IDs to merge with pre-computed Mixpanel data"""
        if not adset_ids:
            return {}
        
        try:
            meta_data = {}
            
            # Get table name for Meta data
            table_name = self.get_table_name(config.breakdown)
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in adset_ids])
            
            meta_query = f"""
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
              AND adset_id IN ({placeholders})
              AND adset_id IS NOT NULL
            GROUP BY adset_id, adset_name, campaign_id, campaign_name
            """
            
            params = [config.start_date, config.end_date] + adset_ids
            meta_results = self._execute_meta_query(meta_query, params)
            
            logger.info(f"🔗 META DATA FETCH: Found {len(meta_results)} adsets with Meta data")
            
            for row in meta_results:
                adset_id = row['adset_id']
                meta_data[adset_id] = {
                    'spend': float(row.get('spend', 0) or 0),
                    'impressions': int(row.get('impressions', 0) or 0),
                    'clicks': int(row.get('clicks', 0) or 0),
                    'meta_trials_started': int(row.get('meta_trials_started', 0) or 0),
                    'meta_purchases': int(row.get('meta_purchases', 0) or 0)
                }
                
                # Log some key metrics
                if adset_id == "120223331225270178":
                    logger.info(f"🎯 TARGET ADSET META DATA:")
                    logger.info(f"   💰 SPEND: ${meta_data[adset_id]['spend']:.2f}")
                    logger.info(f"   📊 META TRIALS: {meta_data[adset_id]['meta_trials_started']}")
                    logger.info(f"   🎯 META PURCHASES: {meta_data[adset_id]['meta_purchases']}")
            
            return meta_data
            
        except Exception as e:
            logger.error(f"Error fetching Meta data for adsets: {e}")
            return {}
    
    def _get_meta_data_for_campaigns(self, campaign_ids: List[str], config: QueryConfig) -> Dict[str, Dict]:
        """Get Meta data for specific campaign IDs to merge with pre-computed Mixpanel data"""
        if not campaign_ids:
            return {}
        
        try:
            meta_data = {}
            
            # Get table name for Meta data
            table_name = self.get_table_name(config.breakdown)
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in campaign_ids])
            
            meta_query = f"""
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
              AND campaign_id IN ({placeholders})
              AND campaign_id IS NOT NULL
            GROUP BY campaign_id, campaign_name
            """
            
            params = [config.start_date, config.end_date] + campaign_ids
            meta_results = self._execute_meta_query(meta_query, params)
            
            logger.info(f"🔗 CAMPAIGN META FETCH: Found {len(meta_results)} campaigns with Meta data")
            
            for row in meta_results:
                campaign_id = row['campaign_id']
                meta_data[campaign_id] = {
                    'spend': float(row.get('spend', 0) or 0),
                    'impressions': int(row.get('impressions', 0) or 0),
                    'clicks': int(row.get('clicks', 0) or 0),
                    'meta_trials_started': int(row.get('meta_trials_started', 0) or 0),
                    'meta_purchases': int(row.get('meta_purchases', 0) or 0)
                }
            
            return meta_data
            
        except Exception as e:
            logger.error(f"Error fetching Meta data for campaigns: {e}")
            return {}
    
    def _get_meta_data_for_ads(self, ad_ids: List[str], config: QueryConfig) -> Dict[str, Dict]:
        """Get Meta data for specific ad IDs to merge with pre-computed Mixpanel data"""
        if not ad_ids:
            return {}
        
        try:
            meta_data = {}
            
            # Get table name for Meta data
            table_name = self.get_table_name(config.breakdown)
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in ad_ids])
            
            meta_query = f"""
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
              AND ad_id IN ({placeholders})
              AND ad_id IS NOT NULL
            GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
            """
            
            params = [config.start_date, config.end_date] + ad_ids
            meta_results = self._execute_meta_query(meta_query, params)
            
            logger.info(f"🔗 AD META FETCH: Found {len(meta_results)} ads with Meta data")
            
            for row in meta_results:
                ad_id = row['ad_id']
                meta_data[ad_id] = {
                    'spend': float(row.get('spend', 0) or 0),
                    'impressions': int(row.get('impressions', 0) or 0),
                    'clicks': int(row.get('clicks', 0) or 0),
                    'meta_trials_started': int(row.get('meta_trials_started', 0) or 0),
                    'meta_purchases': int(row.get('meta_purchases', 0) or 0)
                }
            
            return meta_data
            
        except Exception as e:
            logger.error(f"Error fetching Meta data for ads: {e}")
            return {}
    
    def _get_mixpanel_ad_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get ad-level data using HYBRID approach (pre-computed Mixpanel + Meta data)"""
        
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if daily_mixpanel_metrics exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
                if not cursor.fetchone():
                    logger.error("daily_mixpanel_metrics table not found. Run pipeline to generate pre-computed data.")
                    return []
                
                # FIXED: Use subquery approach with canonical names and hierarchy
                precomputed_query = """
                SELECT 
                    ad_data.entity_id as ad_id,
                    COALESCE(nm.canonical_name, 'Unknown Ad (' || ad_data.entity_id || ')') as ad_name,
                    COALESCE(hm.adset_id, '') as adset_id,
                    COALESCE(am.canonical_name, 'Unknown Adset (' || COALESCE(hm.adset_id, 'No ID') || ')') as adset_name,
                    COALESCE(hm.campaign_id, '') as campaign_id,
                    COALESCE(cm.canonical_name, 'Unknown Campaign (' || COALESCE(hm.campaign_id, 'No ID') || ')') as campaign_name,
                    ad_data.total_users,
                    ad_data.total_users as new_users,
                    ad_data.mixpanel_trials_started,
                    ad_data.mixpanel_purchases,
                    ad_data.mixpanel_revenue_usd,
                    ad_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as mixpanel_revenue_usd,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'ad'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) ad_data
                LEFT JOIN id_name_mapping nm ON ad_data.entity_id = nm.entity_id AND nm.entity_type = 'ad'
                LEFT JOIN (
                    SELECT DISTINCT ad_id, adset_id, campaign_id 
                    FROM id_hierarchy_mapping 
                    WHERE ad_id IS NOT NULL
                ) hm ON ad_data.entity_id = hm.ad_id
                LEFT JOIN id_name_mapping am ON hm.adset_id = am.entity_id AND am.entity_type = 'adset'
                LEFT JOIN id_name_mapping cm ON hm.campaign_id = cm.entity_id AND cm.entity_type = 'campaign'
                ORDER BY ad_data.estimated_revenue_usd DESC
                """
                
                cursor.execute(precomputed_query, [config.start_date, config.end_date])
                results = cursor.fetchall()
                
                if results:
                    logger.info(f"📊 AD HYBRID: Found {len(results)} ads with pre-computed data")
                    
                    # Now fetch Meta data for the same ads and merge it
                    ad_ids = [row['ad_id'] for row in results]
                    meta_data = self._get_meta_data_for_ads(ad_ids, config)
                    
                    # Format results with both Mixpanel and Meta data
                    formatted_ads = []
                    for row in results:
                        ad_id = row['ad_id']
                        meta_info = meta_data.get(ad_id, {})
                        
                        # Calculate metrics
                        mixpanel_trials = int(row['mixpanel_trials_started'])
                        mixpanel_purchases = int(row['mixpanel_purchases'])
                        meta_trials = meta_info.get('meta_trials_started', 0)
                        meta_purchases = meta_info.get('meta_purchases', 0)
                        
                        # Accuracy ratios
                        # Special case: If meta_trials = 0 but mixpanel_trials > 0, treat as 100% accuracy (1.0)
                        if meta_trials == 0 and mixpanel_trials > 0:
                            trial_accuracy_ratio = 1.0  # 100% accuracy for calculations
                        else:
                            trial_accuracy_ratio = (mixpanel_trials / meta_trials) if meta_trials > 0 else 0.0
                        purchase_accuracy_ratio = (mixpanel_purchases / meta_purchases) if meta_purchases > 0 else 0.0
                        trial_conversion_rate = (mixpanel_purchases / mixpanel_trials) if mixpanel_trials > 0 else 0.0
                        
                        # Financial metrics WITH ADJUSTMENT
                        spend = meta_info.get('spend', 0.0)
                        estimated_revenue_raw = float(row['estimated_revenue_usd'])
                        estimated_revenue_adjusted = (estimated_revenue_raw / trial_accuracy_ratio) if trial_accuracy_ratio > 0 else estimated_revenue_raw
                        estimated_roas = (estimated_revenue_adjusted / spend) if spend > 0 else 0.0
                        profit = estimated_revenue_adjusted - spend
                        
                        # Refund rates (placeholder for now)
                        trial_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        purchase_refund_rate = 0.0  # TODO: Calculate from actual refund data
                        
                        formatted_ad = {
                            'id': f"ad_{ad_id}",
                            'entity_type': 'ad',
                            'ad_id': ad_id,
                            'ad_name': row['ad_name'],
                            'adset_id': row['adset_id'],
                            'adset_name': row['adset_name'],
                            'campaign_id': row['campaign_id'],
                            'campaign_name': row['campaign_name'],
                            'name': row['ad_name'],
                            
                            # Meta metrics from regular Meta tables
                            'spend': spend,
                            'impressions': meta_info.get('impressions', 0),
                            'clicks': meta_info.get('clicks', 0),
                            'meta_trials_started': meta_trials,
                            'meta_purchases': meta_purchases,
                            
                            # Mixpanel metrics from pre-computed data
                            'mixpanel_trials_started': mixpanel_trials,
                            'mixpanel_purchases': mixpanel_purchases,
                            'mixpanel_revenue_usd': float(row['mixpanel_revenue_usd']),
                            
                            # ✅ FIXED: Adjusted revenue and accuracy ratios
                            'estimated_revenue_usd': estimated_revenue_adjusted,  # This is the ADJUSTED revenue
                            'estimated_revenue_adjusted': estimated_revenue_adjusted,
                            'estimated_roas': estimated_roas,
                            'profit': profit,
                            'trial_accuracy_ratio': trial_accuracy_ratio,
                            'purchase_accuracy_ratio': purchase_accuracy_ratio,  # ✅ FIXED: Now calculated properly
                            
                            # ✅ FIXED: Rate calculations
                            'avg_trial_conversion_rate': trial_conversion_rate,
                            'trial_conversion_rate': trial_conversion_rate,
                            'conversion_rate': trial_conversion_rate,
                            'avg_trial_refund_rate': trial_refund_rate,  # ✅ FIXED: Now provided
                            'avg_purchase_refund_rate': purchase_refund_rate,  # ✅ FIXED: Now provided
                            'trial_refund_rate': trial_refund_rate,
                            'purchase_refund_rate': purchase_refund_rate,
                            
                            # Additional info
                            'total_users': int(row['total_users']),
                            'new_users': int(row['new_users']),
                            'children': []
                        }
                        formatted_ads.append(formatted_ad)
                    
                    logger.info(f"✅ Returning {len(formatted_ads)} hybrid ads")
                    return formatted_ads
                else:
                    logger.warning("No pre-computed ad data found for date range. Run pipeline to generate data.")
                    return []
                    
        except Exception as e:
            logger.error(f"Error loading hybrid ad data: {e}")
            return []
    
    def _get_child_adsets_for_campaign(self, campaign_id: str, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get child adsets for a specific campaign using hybrid approach"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get adsets that belong to this campaign from pre-computed data
                adset_query = """
                SELECT 
                    adset_data.entity_id as adset_id,
                    COALESCE(nm.canonical_name, 'Unknown Adset (' || adset_data.entity_id || ')') as adset_name,
                    adset_data.total_users,
                    adset_data.mixpanel_trials_started,
                    adset_data.mixpanel_purchases,
                    adset_data.mixpanel_revenue_usd,
                    adset_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as mixpanel_revenue_usd,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'adset'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) adset_data
                LEFT JOIN id_name_mapping nm ON adset_data.entity_id = nm.entity_id AND nm.entity_type = 'adset'
                LEFT JOIN id_hierarchy_mapping hm ON adset_data.entity_id = hm.adset_id
                WHERE hm.campaign_id = ?
                ORDER BY adset_data.estimated_revenue_usd DESC
                """
                
                cursor.execute(adset_query, [config.start_date, config.end_date, campaign_id])
                results = cursor.fetchall()
                
                if not results:
                    return []
                
                # Get Meta data for these adsets
                adset_ids = [row['adset_id'] for row in results]
                meta_data = self._get_meta_data_for_adsets(adset_ids, config)
                
                # Format adset children using _format_record for consistency
                child_adsets = []
                for row in results:
                    adset_id = row['adset_id']
                    meta_info = meta_data.get(adset_id, {})
                    
                    # Build raw record with all necessary fields for _format_record
                    raw_child_record = {
                        'adset_id': adset_id,
                        'adset_name': row['adset_name'],
                        'campaign_id': campaign_id,
                        'spend': meta_info.get('spend', 0.0),
                        'impressions': meta_info.get('impressions', 0),
                        'clicks': meta_info.get('clicks', 0),
                        'meta_trials_started': meta_info.get('meta_trials_started', 0),
                        'meta_purchases': meta_info.get('meta_purchases', 0),
                        'mixpanel_trials_started': int(row['mixpanel_trials_started']),
                        'mixpanel_purchases': int(row['mixpanel_purchases']),
                        'mixpanel_revenue_usd': float(row['mixpanel_revenue_usd']),
                        'estimated_revenue_usd': float(row['estimated_revenue_usd']),
                        'total_attributed_users': int(row['total_users']),
                        'children': []  # Adsets can have ad children, populated separately if needed
                    }
                    
                    # Use _format_record to get consistent calculations including refund rates
                    formatted_child = self._format_record(raw_child_record, 'adset', config)
                    child_adsets.append(formatted_child)
                
                return child_adsets
                
        except Exception as e:
            logger.error(f"Error fetching child adsets for campaign {campaign_id}: {e}")
            return []
    
    def _get_child_ads_for_adset(self, adset_id: str, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get child ads for a specific adset using hybrid approach"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get ads that belong to this adset from pre-computed data
                ad_query = """
                SELECT 
                    ad_data.entity_id as ad_id,
                    COALESCE(nm.canonical_name, 'Unknown Ad (' || ad_data.entity_id || ')') as ad_name,
                    ad_data.total_users,
                    ad_data.mixpanel_trials_started,
                    ad_data.mixpanel_purchases,
                    ad_data.mixpanel_revenue_usd,
                    ad_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as mixpanel_revenue_usd,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'ad'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) ad_data
                LEFT JOIN id_name_mapping nm ON ad_data.entity_id = nm.entity_id AND nm.entity_type = 'ad'
                LEFT JOIN id_hierarchy_mapping hm ON ad_data.entity_id = hm.ad_id
                WHERE hm.adset_id = ?
                ORDER BY ad_data.estimated_revenue_usd DESC
                """
                
                cursor.execute(ad_query, [config.start_date, config.end_date, adset_id])
                results = cursor.fetchall()
                
                if not results:
                    return []
                
                # Get Meta data for these ads
                ad_ids = [row['ad_id'] for row in results]
                meta_data = self._get_meta_data_for_ads(ad_ids, config)
                
                # Format ad children using _format_record for consistency
                child_ads = []
                for row in results:
                    ad_id = row['ad_id']
                    meta_info = meta_data.get(ad_id, {})
                    
                    # Build raw record with all necessary fields for _format_record
                    raw_child_record = {
                        'ad_id': ad_id,
                        'ad_name': row['ad_name'],
                        'adset_id': adset_id,
                        'spend': meta_info.get('spend', 0.0),
                        'impressions': meta_info.get('impressions', 0),
                        'clicks': meta_info.get('clicks', 0),
                        'meta_trials_started': meta_info.get('meta_trials_started', 0),
                        'meta_purchases': meta_info.get('meta_purchases', 0),
                        'mixpanel_trials_started': int(row['mixpanel_trials_started']),
                        'mixpanel_purchases': int(row['mixpanel_purchases']),
                        'mixpanel_revenue_usd': float(row['mixpanel_revenue_usd']),
                        'estimated_revenue_usd': float(row['estimated_revenue_usd']),
                        'total_attributed_users': int(row['total_users']),
                        'children': []  # Ads don't have children
                    }
                    
                    # Use _format_record to get consistent calculations including refund rates
                    formatted_child = self._format_record(raw_child_record, 'ad', config)
                    child_ads.append(formatted_child)
                
                return child_ads
                
        except Exception as e:
            logger.error(f"Error fetching child ads for adset {adset_id}: {e}")
            return []
    
    def _get_precomputed_sparkline_data(self, entity_type: str, entity_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get pre-computed daily Mixpanel data for sparkline charts from daily_mixpanel_metrics"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if daily_mixpanel_metrics exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
                if not cursor.fetchone():
                    logger.warning("daily_mixpanel_metrics table not found. Sparkline will use fallback.")
                    return []
                
                sparkline_query = """
                SELECT 
                    date,
                    trial_users_count,
                    purchase_users_count,
                    estimated_revenue_usd
                FROM daily_mixpanel_metrics
                WHERE entity_type = ? 
                  AND entity_id = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """
                
                cursor.execute(sparkline_query, [entity_type, entity_id, start_date, end_date])
                results = cursor.fetchall()
                
                logger.info(f"📊 SPARKLINE MIXPANEL: Retrieved {len(results)} days from daily_mixpanel_metrics for {entity_type} {entity_id}")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error getting pre-computed sparkline data for {entity_type} {entity_id}: {e}")
            return []
    
    def _get_meta_sparkline_data(self, entity_type: str, entity_id: str, start_date: str, end_date: str, breakdown: str) -> List[Dict[str, Any]]:
        """Get Meta data for sparkline charts (real-time from Meta tables)"""
        try:
            # Get table name for Meta data
            table_name = self.get_table_name(breakdown)
            
            # Build WHERE clause based on entity type
            if entity_type == 'campaign':
                meta_where = "campaign_id = ?"
            elif entity_type == 'adset':
                meta_where = "adset_id = ?"
            elif entity_type == 'ad':
                meta_where = "ad_id = ?"
            else:
                raise ValueError(f"Invalid entity_type: {entity_type}")
            
            meta_query = f"""
            SELECT 
                date,
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
            
            meta_results = self._execute_meta_query(meta_query, [entity_id, start_date, end_date])
            logger.info(f"📊 SPARKLINE META: Retrieved {len(meta_results)} days from {table_name} for {entity_type} {entity_id}")
            
            return meta_results
            
        except Exception as e:
            logger.error(f"Error getting Meta sparkline data for {entity_type} {entity_id}: {e}")
            return []
    
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
            
            with sqlite3.connect(self.meta_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                # Convert to list of dictionaries
                data = [dict(row) for row in results]
                
                return data
            
        except Exception as e:
            logger.warning(f"Error executing meta query, falling back to empty results: {e}")
            # Return empty results instead of raising exception
            return []
    
    def _get_precomputed_mixpanel_data(self, config: QueryConfig, ad_ids: List[str]) -> Dict[str, Dict]:
        """Get pre-computed Mixpanel metrics with user deduplication (fast & accurate)"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if daily_mixpanel_metrics exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
                if not cursor.fetchone():
                    return {}
                
                placeholders = ','.join(['?' for _ in ad_ids])
                query = f"""
                SELECT 
                    entity_id as ad_id,
                    SUM(trial_users_count) as mixpanel_trials_started,
                    SUM(purchase_users_count) as mixpanel_purchases,
                    SUM(estimated_revenue_usd) as estimated_revenue_usd
                FROM daily_mixpanel_metrics
                WHERE entity_type = 'ad'
                  AND entity_id IN ({placeholders})
                  AND date BETWEEN ? AND ?
                GROUP BY entity_id
                """
                
                params = [*ad_ids, config.start_date, config.end_date]
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                if not results:
                    return {}
                
                data_map = {}
                for row in results:
                    ad_id = row['ad_id']
                    trials = int(row['mixpanel_trials_started'])
                    data_map[ad_id] = {
                        'mixpanel_trials_started': trials,
                        'mixpanel_purchases': int(row['mixpanel_purchases']),
                        'total_attributed_users': trials,
                        'estimated_revenue_usd': float(row['estimated_revenue_usd']),
                        'actual_mixpanel_revenue_usd': 0.0,
                        'actual_mixpanel_refunds_usd': 0.0,
                        'avg_trial_conversion_rate': 0.0,
                        'avg_trial_refund_rate': 0.0,
                        'avg_purchase_refund_rate': 0.0
                    }
                
                logger.info(f"📊 FOUND PRE-COMPUTED DATA FOR {len(data_map)} ADS:")
                for ad_id, data in data_map.items():
                    logger.info(f"   🎯 {ad_id}: {data['mixpanel_trials_started']} trials")
                logger.info("")
                logger.info(f"Using pre-computed data for {len(data_map)} ads")
                return data_map
                
        except Exception as e:
            logger.warning(f"Pre-computed data unavailable: {e}")
            return {}
    
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

        logger.info(f"Adding Mixpanel data to {len(records)} records")
        
        # Debug: Show what type of records we're processing (hierarchical aggregation path)
        logger.info(f"🔍 HIERARCHICAL AGGREGATION - Processing {len(records)} records")
        logger.info(f"📝 Note: Adset queries now use direct pre-computed data (not this path)")
        
        entity_types = [record.get('entity_type', 'unknown') for record in records]
        logger.info(f"   📊 Entity types: {list(set(entity_types))}")
        logger.info("")

        # Collect all unique ad_ids from records hierarchy

        # Step 1: Collect all unique ad_ids from the entire hierarchy
        all_ad_ids = set()
        def collect_ad_ids(items):
            for item in items:
                if item.get('ad_id'):
                    all_ad_ids.add(item['ad_id'])
                if item.get('children'):
                    collect_ad_ids(item['children'])
        
        collect_ad_ids(records)

        # Debug: Show which ad IDs we found in the Meta records
        logger.info(f"🔍 COLLECTED AD IDs FROM META RECORDS:")
        logger.info(f"   📊 Total ad_ids found in Meta data: {len(all_ad_ids)}")
        logger.info(f"   🎯 Ad IDs: {sorted(list(all_ad_ids))}")
        
        logger.info("")

        if not all_ad_ids:
            logger.warning("No ad_ids found in records. Skipping Mixpanel enrichment.")
            return

        # Use ONLY pre-computed data (no expensive real-time calculations)
        mixpanel_data_map = self._get_precomputed_mixpanel_data(config, list(all_ad_ids))
        
        if not mixpanel_data_map:
            logger.warning("No pre-computed mixpanel data available. Run pipeline to generate data.")
            return

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
                # Special case: If meta_trials = 0 but mixpanel_trials > 0, treat as 100% accuracy
                if meta_trials == 0 and mixpanel_trials > 0:
                    item['trial_accuracy_ratio'] = 1.0  # 100% accuracy as decimal for frontend consistency
                else:
                    item['trial_accuracy_ratio'] = (mixpanel_trials / meta_trials) if meta_trials > 0 else 0.0
        
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
            campaign_id = record.get('campaign_id', 'unknown')
            record_id = f"campaign_{campaign_id}"
            name = record.get('campaign_name', f'Unknown Campaign ({campaign_id})')
        elif entity_type == 'adset':
            adset_id = record.get('adset_id', 'unknown')
            record_id = f"adset_{adset_id}"
            name = record.get('adset_name', f'Unknown Ad Set ({adset_id})')
        else:  # ad
            ad_id = record.get('ad_id', 'unknown')
            record_id = f"ad_{ad_id}"
            name = record.get('ad_name', f'Unknown Ad ({ad_id})')
        
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
            'entity_type': entity_type,  # ✅ FIXED: Use consistent field name
            'type': entity_type,  # Keep for backwards compatibility
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
        
        # 🚀 OPTIMIZED: Use pre-computed data instead of expensive calculators
        if 'trial_accuracy_ratio' in record:
            # Use pre-computed values directly (already in decimal format)
            formatted['trial_accuracy_ratio'] = float(record.get('trial_accuracy_ratio', 0))
            formatted['purchase_accuracy_ratio'] = float(record.get('purchase_accuracy_ratio', 0))
            formatted['estimated_roas'] = float(record.get('estimated_roas', 0))
            formatted['performance_impact_score'] = float(record.get('estimated_roas', 0))  # Use ROAS as performance score
        else:
            # Fallback to expensive calculators only if pre-computed data not available
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
        # PERFORMANCE OPTIMIZATION: Use cached rates if available, fallback to individual calculation
        entity_id = record.get(f'{entity_type}_id')
        if hasattr(self, '_rates_cache') and entity_id in self._rates_cache:
            trial_conv_rate, trial_refund_rate, purchase_refund_rate = self._rates_cache[entity_id]
        else:
            # Fallback to individual calculation for missing entities
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
    
    def _batch_calculate_entity_rates(self, entities: List[Dict[str, Any]], config: QueryConfig = None) -> Dict[str, tuple]:
        """
        Batch calculate conversion rates for multiple entities in a single database query.
        
        Args:
            entities: List of dicts with 'entity_type', 'entity_id' keys
            config: Query configuration with date range
            
        Returns:
            Dict mapping entity_id to (trial_conversion_rate, trial_refund_rate, purchase_refund_rate)
        """
        if not entities:
            return {}
            
        try:
            # Use config dates or default to recent period
            if config:
                start_date = config.start_date
                end_date = config.end_date
            else:
                from datetime import datetime, timedelta
                end_date = now_in_timezone().date().strftime('%Y-%m-%d')
                start_date = (now_in_timezone().date() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Group entities by type for optimized queries
            campaigns = [e['entity_id'] for e in entities if e['entity_type'] == 'campaign' and e['entity_id']]
            adsets = [e['entity_id'] for e in entities if e['entity_type'] == 'adset' and e['entity_id']]
            ads = [e['entity_id'] for e in entities if e['entity_type'] == 'ad' and e['entity_id']]
            
            rates_cache = {}
            
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Batch query for campaigns
                if campaigns:
                    campaign_placeholders = ','.join(['?' for _ in campaigns])
                    campaign_query = f"""
                    SELECT 
                        u.abi_campaign_id as entity_id,
                        AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                        AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                        AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                        COUNT(DISTINCT upm.distinct_id) as total_users
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE u.abi_campaign_id IN ({campaign_placeholders})
                      AND upm.credited_date BETWEEN ? AND ?
                      AND upm.trial_conversion_rate IS NOT NULL
                      AND upm.trial_converted_to_refund_rate IS NOT NULL  
                      AND upm.initial_purchase_to_refund_rate IS NOT NULL
                    GROUP BY u.abi_campaign_id
                    """
                    cursor.execute(campaign_query, campaigns + [start_date, end_date])
                    for result in cursor.fetchall():
                        if result['total_users'] > 0:
                            entity_id = result['entity_id']
                            trial_conv = max(0.0, min(100.0, (result['avg_trial_conversion_rate'] or 0) * 100))
                            trial_refund = max(0.0, min(100.0, (result['avg_trial_refund_rate'] or 0) * 100))
                            purchase_refund = max(0.0, min(100.0, (result['avg_purchase_refund_rate'] or 0) * 100))
                            rates_cache[entity_id] = (trial_conv, trial_refund, purchase_refund)
                
                # Batch query for adsets
                if adsets:
                    adset_placeholders = ','.join(['?' for _ in adsets])
                    adset_query = f"""
                    SELECT 
                        u.abi_ad_set_id as entity_id,
                        AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                        AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                        AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                        COUNT(DISTINCT upm.distinct_id) as total_users
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE u.abi_ad_set_id IN ({adset_placeholders})
                      AND upm.credited_date BETWEEN ? AND ?
                      AND upm.trial_conversion_rate IS NOT NULL
                      AND upm.trial_converted_to_refund_rate IS NOT NULL  
                      AND upm.initial_purchase_to_refund_rate IS NOT NULL
                    GROUP BY u.abi_ad_set_id
                    """
                    cursor.execute(adset_query, adsets + [start_date, end_date])
                    for result in cursor.fetchall():
                        if result['total_users'] > 0:
                            entity_id = result['entity_id']
                            trial_conv = max(0.0, min(100.0, (result['avg_trial_conversion_rate'] or 0) * 100))
                            trial_refund = max(0.0, min(100.0, (result['avg_trial_refund_rate'] or 0) * 100))
                            purchase_refund = max(0.0, min(100.0, (result['avg_purchase_refund_rate'] or 0) * 100))
                            rates_cache[entity_id] = (trial_conv, trial_refund, purchase_refund)
                
                # Batch query for ads
                if ads:
                    ad_placeholders = ','.join(['?' for _ in ads])
                    ad_query = f"""
                    SELECT 
                        u.abi_ad_id as entity_id,
                        AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                        AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                        AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                        COUNT(DISTINCT upm.distinct_id) as total_users
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE u.abi_ad_id IN ({ad_placeholders})
                      AND upm.credited_date BETWEEN ? AND ?
                      AND upm.trial_conversion_rate IS NOT NULL
                      AND upm.trial_converted_to_refund_rate IS NOT NULL  
                      AND upm.initial_purchase_to_refund_rate IS NOT NULL
                    GROUP BY u.abi_ad_id
                    """
                    cursor.execute(ad_query, ads + [start_date, end_date])
                    for result in cursor.fetchall():
                        if result['total_users'] > 0:
                            entity_id = result['entity_id']
                            trial_conv = max(0.0, min(100.0, (result['avg_trial_conversion_rate'] or 0) * 100))
                            trial_refund = max(0.0, min(100.0, (result['avg_trial_refund_rate'] or 0) * 100))
                            purchase_refund = max(0.0, min(100.0, (result['avg_purchase_refund_rate'] or 0) * 100))
                            rates_cache[entity_id] = (trial_conv, trial_refund, purchase_refund)
            
            logger.info(f"✅ Batch calculated rates for {len(rates_cache)}/{len(entities)} entities in 3 queries (vs {len(entities)} individual queries)")
            return rates_cache
            
        except Exception as e:
            logger.error(f"Error in batch calculating entity rates: {e}")
            return {}
    
    def _collect_all_entities_from_hierarchy(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Recursively collect all entities (campaigns, adsets, ads) from hierarchical data.
        
        Returns:
            List of dicts with 'entity_type' and 'entity_id' keys
        """
        entities = []
        
        def collect_entities_recursive(items, level='campaign'):
            for item in items:
                # Determine entity type and ID
                if level == 'campaign':
                    entity_id = item.get('campaign_id')
                    entity_type = 'campaign'
                elif level == 'adset':
                    entity_id = item.get('adset_id')
                    entity_type = 'adset'
                elif level == 'ad':
                    entity_id = item.get('ad_id')
                    entity_type = 'ad'
                else:
                    continue
                
                # Add entity if it has an ID
                if entity_id:
                    entities.append({
                        'entity_type': entity_type,
                        'entity_id': entity_id
                    })
                
                # Recursively process children
                if 'children' in item and item['children']:
                    next_level = 'adset' if level == 'campaign' else 'ad'
                    collect_entities_recursive(item['children'], next_level)
        
        collect_entities_recursive(data)
        return entities
    
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
        """Get detailed daily metrics for sparkline charts using PRE-COMPUTED data - ALWAYS returns exactly 14 days ending on config.end_date"""
        try:
            # Check if this is a breakdown entity (format: "US_120217904661980178")
            is_breakdown_entity = '_' in entity_id and not entity_id.startswith(('campaign_', 'adset_', 'ad_'))
            
            if is_breakdown_entity:
                # Parse breakdown entity ID
                breakdown_value, parent_entity_id = entity_id.split('_', 1)
                logger.info(f"📊 BREAKDOWN CHART: {breakdown_value} breakdown for {entity_type} {parent_entity_id}")
                return self._get_breakdown_chart_data(config, entity_type, parent_entity_id, breakdown_value)
            
            logger.info(f"🔄 SPARKLINE: Using HYBRID approach for {entity_type} {entity_id}")
            
            # Calculate the exact 14-day period ending on config.end_date
            end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
            display_start_date = end_date - timedelta(days=13)  # 13 days back + end date = 14 days total
            
            # Format dates for queries
            chart_start_date = display_start_date.strftime('%Y-%m-%d')  # 14 days start
            chart_end_date = config.end_date
            
            # ✅ STEP 1: Get PRE-COMPUTED Mixpanel data from daily_mixpanel_metrics
            mixpanel_data = self._get_precomputed_sparkline_data(entity_type, entity_id, chart_start_date, chart_end_date)
            
            # ✅ STEP 2: Get real-time Meta data 
            meta_data = self._get_meta_sparkline_data(entity_type, entity_id, chart_start_date, chart_end_date, config.breakdown)
            
            # ✅ STEP 3: Generate 14-day framework and merge data
            daily_data = {}
            current_date = display_start_date
            
            # Initialize all 14 days with zero values
            for i in range(14):
                date_str = current_date.strftime('%Y-%m-%d')
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
                    'is_inactive': False  # Will be updated based on spend data
                }
                current_date += timedelta(days=1)
            
            # ✅ STEP 4: Overlay Meta data
            for row in meta_data:
                date = row['date']
                if date in daily_data:
                    daily_data[date].update({
                        'daily_spend': float(row.get('daily_spend', 0) or 0),
                        'daily_impressions': int(row.get('daily_impressions', 0) or 0),
                        'daily_clicks': int(row.get('daily_clicks', 0) or 0),
                        'daily_meta_trials': int(row.get('daily_meta_trials', 0) or 0),
                        'daily_meta_purchases': int(row.get('daily_meta_purchases', 0) or 0),
                        'is_inactive': False  # Has activity
                    })
            
            # ✅ STEP 5: Overlay PRE-COMPUTED Mixpanel data
            for row in mixpanel_data:
                date = row['date']
                if date in daily_data:
                    daily_data[date].update({
                        'daily_mixpanel_trials': int(row.get('trial_users_count', 0) or 0),
                        'daily_mixpanel_purchases': int(row.get('purchase_users_count', 0) or 0),
                        'daily_mixpanel_conversions': int(row.get('purchase_users_count', 0) or 0),  # Same as purchases
                        'daily_mixpanel_revenue': float(row.get('estimated_revenue_usd', 0) or 0),  # Raw estimated revenue
                        'daily_mixpanel_refunds': 0.0,  # TODO: Add to pre-computed data if needed
                        'daily_estimated_revenue': float(row.get('adjusted_estimated_revenue_usd', 0) or 0),  # ✅ FIX: Use pre-computed ADJUSTED revenue
                        'daily_attributed_users': int(row.get('trial_users_count', 0) or 0),  # Use trial users as base
                        'is_inactive': False  # Has activity
                    })
            
            logger.info(f"🔄 SPARKLINE HYBRID: Using pre-computed Mixpanel + real-time Meta for {entity_type} {entity_id}")
            
            # ✅ STEP 6: Calculate ROAS and profit using PRE-COMPUTED adjusted revenue
            all_data = []
            for date in sorted(daily_data.keys()):  # This will be exactly 14 days
                day_data = daily_data[date]
                
                # Calculate accuracy ratios for display purposes only (no longer used for adjustment)
                daily_mixpanel_trials = day_data['daily_mixpanel_trials']
                daily_mixpanel_purchases = day_data['daily_mixpanel_purchases']
                daily_meta_trials = day_data['daily_meta_trials']
                daily_meta_purchases = day_data['daily_meta_purchases']
                
                # Daily accuracy ratios (for debugging/logging only)
                daily_trial_accuracy = (daily_mixpanel_trials / daily_meta_trials) if daily_meta_trials > 0 else 0.0
                daily_purchase_accuracy = (daily_mixpanel_purchases / daily_meta_purchases) if daily_meta_purchases > 0 else 0.0
                
                # ✅ USE PRE-COMPUTED ADJUSTED REVENUE (no further adjustment needed)
                adjusted_revenue = day_data['daily_estimated_revenue']  # Already adjusted in pre-computation
                
                # ✅ CALCULATE ROAS using PRE-ADJUSTED revenue
                spend = day_data['daily_spend']
                daily_roas = (adjusted_revenue / spend) if spend > 0 else 0.0
                daily_profit = adjusted_revenue - spend
                

                
                # Update day data with calculated values
                day_data.update({
                    'daily_roas': daily_roas,
                    'daily_profit': daily_profit,
                    'daily_adjusted_revenue': adjusted_revenue,  # Pre-computed adjusted revenue
                    'daily_trial_accuracy': daily_trial_accuracy,
                    'daily_purchase_accuracy': daily_purchase_accuracy,
                    'conversions_for_coloring': day_data['daily_mixpanel_conversions']
                })
                
                all_data.append(day_data)
            
            # ✅ STEP 7: Calculate rolling metrics for sparkline display
            for i, day_data in enumerate(all_data):
                # For sparklines, we use 1-day rolling (just current day)
                rolling_spend = day_data['daily_spend']
                rolling_revenue = day_data['daily_adjusted_revenue']  # Use our calculated adjusted revenue
                rolling_roas = day_data['daily_roas']  # Use our calculated ROAS
                rolling_conversions = day_data['daily_mixpanel_purchases']
                rolling_trials = day_data['daily_mixpanel_trials']
                rolling_meta_trials = day_data['daily_meta_trials']
                
                # Add rolling metrics to day data (required by frontend)
                day_data['rolling_1d_roas'] = round(rolling_roas, 2)
                day_data['rolling_1d_spend'] = rolling_spend
                day_data['rolling_1d_revenue'] = rolling_revenue
                day_data['rolling_1d_conversions'] = rolling_conversions
                day_data['rolling_1d_trials'] = rolling_trials
                day_data['rolling_1d_meta_trials'] = rolling_meta_trials
                day_data['rolling_window_days'] = 1  # Always 1 day for individual entity sparklines
            
            # Return all 14 days for display
            chart_data = all_data
            
            # Chart calculation completed
            
            return {
                'success': True,
                'chart_data': chart_data,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'date_range': f"{chart_start_date} to {chart_end_date}",
                'total_days': len(chart_data),
                'period_info': f"14-day period ending {chart_end_date} using HYBRID approach",
                'rolling_calculation_info': f"1-day rolling averages with adjusted revenue calculations",
                'metadata': {
                    'approach': 'hybrid_precomputed_mixpanel_plus_realtime_meta',
                    'mixpanel_source': 'daily_mixpanel_metrics',
                    'meta_source': config.breakdown,
                    'adjustment_method': 'daily_accuracy_ratio_per_day',
                    'generated_at': now_in_timezone().isoformat()
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
            
            # ✅ FIXED: Get daily Mixpanel breakdown data from PRE-COMPUTED table
            with sqlite3.connect(self.mixpanel_analytics_db_path) as mixpanel_conn:
                mixpanel_conn.row_factory = sqlite3.Row
                
                mixpanel_query = f"""
                SELECT 
                    date,
                    mixpanel_trial_count as daily_mixpanel_trials,
                    mixpanel_purchase_count as daily_mixpanel_purchases,
                    mixpanel_purchase_count as daily_mixpanel_conversions,
                    actual_revenue_usd as daily_mixpanel_revenue,
                    actual_refunds_usd as daily_mixpanel_refunds,
                    adjusted_estimated_revenue_usd as daily_estimated_revenue,
                    mixpanel_trial_count as daily_attributed_users
                FROM daily_mixpanel_metrics_breakdown
                WHERE entity_type = ? AND entity_id = ? AND breakdown_type = ? AND breakdown_value = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """
                
                cursor = mixpanel_conn.cursor()
                cursor.execute(mixpanel_query, [entity_type, parent_entity_id, config.breakdown, breakdown_value, chart_start_date, chart_end_date])
                mixpanel_data = [dict(row) for row in cursor.fetchall()]
            
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
            
            # ✅ FIXED: Calculate ROAS using PRE-COMPUTED adjusted revenue (no double adjustment)
            
            # Calculate accuracy ratios for display purposes only
            total_meta_trials = sum(d['daily_meta_trials'] for d in daily_data.values())
            total_mixpanel_trials = sum(d['daily_mixpanel_trials'] for d in daily_data.values())
            total_meta_purchases = sum(d['daily_meta_purchases'] for d in daily_data.values())
            total_mixpanel_purchases = sum(d['daily_mixpanel_purchases'] for d in daily_data.values())
            
            # Determine event priority (for display only)
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
            
            # ✅ Calculate daily metrics using PRE-COMPUTED adjusted revenue
            all_data = []
            for date in sorted(daily_data.keys()):
                day_data = daily_data[date]
                
                # ✅ USE PRE-COMPUTED ADJUSTED REVENUE (no further adjustment needed)
                adjusted_revenue = day_data['daily_estimated_revenue']  # Already adjusted in pre-computation
                spend = day_data['daily_spend']
                
                # Calculate ROAS and profit directly
                day_data['daily_roas'] = (adjusted_revenue / spend) if spend > 0 else 0.0
                day_data['daily_profit'] = adjusted_revenue - spend
                
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
                
                # ✅ FIXED: Sum spend and PRE-COMPUTED adjusted revenue for the rolling window
                rolling_spend = sum(d['daily_spend'] for d in rolling_days)
                rolling_revenue = sum(d['daily_estimated_revenue'] for d in rolling_days)  # Already adjusted
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
                                    breakdown: str = 'all', breakdown_value: str = None, metric_type: str = 'trial_conversion_rate') -> Dict[str, Any]:
        """
        Get individual user details for tooltip display on conversion rates using daily_mixpanel_metrics as source of truth
        
        Returns BOTH estimated and actual user-level breakdowns:
        - Estimated: All users from trial_users_list/purchase_users_list (current logic)
        - Actual: Users who had time to convert (8+ days for trials, 31+ days for purchases) and current_value > 0
        """
        try:
            logger.info(f"🔍 Getting DUAL user details for tooltip: {entity_type} {entity_id}, {start_date} to {end_date}, breakdown={breakdown}, metric_type={metric_type}")
            
            # Extract the actual entity ID from the prefixed ID format (e.g., "campaign_123" -> "123")
            if entity_id.startswith(f"{entity_type}_"):
                actual_entity_id = entity_id[len(f"{entity_type}_"):]
            else:
                actual_entity_id = entity_id
            
            # Determine which user list to use based on metric type
            if metric_type in ['trial_conversion_rate', 'avg_trial_refund_rate']:
                user_list_column = 'trial_users_list'
                is_trial_metric = True
                logger.info(f"📊 Using trial users for metric type: {metric_type}")
            elif metric_type == 'purchase_refund_rate':
                user_list_column = 'purchase_users_list'
                is_trial_metric = False
                logger.info(f"📊 Using purchase users for metric type: {metric_type}")
            else:
                # Default to trial users for unknown metric types
                user_list_column = 'trial_users_list'
                is_trial_metric = True
                logger.warning(f"⚠️ Unknown metric type '{metric_type}', defaulting to trial users")
            
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # STEP 1: Get distinct_ids from daily_mixpanel_metrics (source of truth)
                logger.info(f"🔍 Querying daily_mixpanel_metrics for {entity_type} {actual_entity_id}")
                
                daily_metrics_query = f"""
                SELECT date, {user_list_column} 
                FROM daily_mixpanel_metrics 
                WHERE entity_type = ? 
                  AND entity_id = ?
                  AND date BETWEEN ? AND ?
                  AND {user_list_column} IS NOT NULL
                  AND {user_list_column} != ''
                  AND {user_list_column} != '[]'
                ORDER BY date
                """
                
                cursor.execute(daily_metrics_query, [entity_type, actual_entity_id, start_date, end_date])
                daily_records = [dict(row) for row in cursor.fetchall()]
                
                logger.info(f"📅 Found {len(daily_records)} days with {user_list_column} data")
                
                # STEP 2: Extract and deduplicate distinct_ids from JSON arrays
                all_distinct_ids = set()
                for record in daily_records:
                    user_list_json = record[user_list_column]
                    if user_list_json:
                        try:
                            user_list = json.loads(user_list_json)
                            if isinstance(user_list, list):
                                all_distinct_ids.update(user_list)
                                logger.debug(f"📋 Date {record['date']}: {len(user_list)} users, running total: {len(all_distinct_ids)}")
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(f"⚠️ Failed to parse {user_list_column} for date {record['date']}: {e}")
                
                logger.info(f"✅ Deduplicated to {len(all_distinct_ids)} unique users from daily_mixpanel_metrics")
                
                if not all_distinct_ids:
                    logger.warning(f"⚠️ No users found in {user_list_column} for {entity_type} {actual_entity_id}")
                    empty_result = {
                        'success': True,
                        'estimated': {
                            'summary': {
                                'total_users': 0,
                                'avg_trial_conversion_rate': 0,
                                'avg_trial_refund_rate': 0,
                                'avg_purchase_refund_rate': 0,
                                'total_estimated_revenue': 0,
                                'breakdown_applied': breakdown,
                                'breakdown_value': breakdown_value
                            },
                            'users': []
                        },
                        'actual': {
                            'summary': {
                                'total_users': 0,
                                'avg_trial_conversion_rate': 0,
                                'avg_trial_refund_rate': 0,
                                'avg_purchase_refund_rate': 0,
                                'total_estimated_revenue': 0,
                                'breakdown_applied': breakdown,
                                'breakdown_value': breakdown_value
                            },
                            'users': []
                        },
                        'entity_info': {
                            'entity_type': entity_type,
                            'entity_id': entity_id,
                            'actual_entity_id': actual_entity_id
                        },
                        'date_range': f"{start_date} to {end_date}",
                        'generated_at': now_in_timezone().isoformat()
                    }
                    return empty_result
                
                # STEP 3: Query user_product_metrics for only these specific users
                distinct_ids_list = list(all_distinct_ids)
                placeholders = ','.join(['?' for _ in distinct_ids_list])
                
                # Add breakdown filter if specified
                breakdown_filter = ""
                breakdown_params = []
                if breakdown == 'country' and breakdown_value:
                    breakdown_filter = "AND u.country = ?"
                    breakdown_params.append(breakdown_value)
                
                user_details_query = f"""
                WITH most_recent_trials AS (
                    SELECT 
                        upm.distinct_id,
                        upm.country,
                        upm.region,
                        upm.store as device_category,
                        upm.current_status,
                        upm.trial_conversion_rate,
                        upm.trial_converted_to_refund_rate,
                        upm.initial_purchase_to_refund_rate,
                        upm.current_value,
                        upm.value_status,
                        upm.credited_date,
                        upm.price_bucket,
                        upm.accuracy_score,
                        upm.product_id,
                        ROW_NUMBER() OVER (PARTITION BY upm.distinct_id ORDER BY upm.credited_date DESC) as row_num
                    FROM user_product_metrics upm
                    WHERE upm.distinct_id IN ({placeholders})
                      AND upm.trial_conversion_rate IS NOT NULL
                      AND upm.trial_converted_to_refund_rate IS NOT NULL  
                      AND upm.initial_purchase_to_refund_rate IS NOT NULL
                )
                SELECT 
                    mrt.distinct_id,
                    u.country,
                    u.region,
                    mrt.device_category,
                    mrt.current_status,
                    mrt.trial_conversion_rate,
                    mrt.trial_converted_to_refund_rate,
                    mrt.initial_purchase_to_refund_rate,
                    mrt.current_value,
                    mrt.value_status,
                    mrt.credited_date,
                    mrt.price_bucket,
                    u.economic_tier,
                    mrt.accuracy_score,
                    mrt.product_id
                FROM most_recent_trials mrt
                JOIN mixpanel_user u ON mrt.distinct_id = u.distinct_id
                WHERE mrt.row_num = 1
                  {breakdown_filter}
                ORDER BY mrt.trial_conversion_rate DESC
                """
                
                query_params = distinct_ids_list + breakdown_params
                
                cursor.execute(user_details_query, query_params)
                user_records = [dict(row) for row in cursor.fetchall()]
                
                logger.info(f"📊 Retrieved {len(user_records)} users (most recent trial per user) from user_product_metrics")
                
                # ========================================
                # STEP 4: SEPARATE INTO ESTIMATED vs ACTUAL 
                # ========================================
                from datetime import datetime, timedelta
                today = now_in_timezone().date()
                
                estimated_users = user_records.copy()  # All users for estimated calculation
                actual_users = []  # Filtered users for actual calculation
                
                # Filter actual users based on RC Trial started event timestamps (7+ days ago)
                with sqlite3.connect(self.mixpanel_db_path) as event_conn:
                    event_conn.row_factory = sqlite3.Row
                    event_cursor = event_conn.cursor()
                    
                    for record in user_records:
                        distinct_id = record.get('distinct_id', '')
                        product_id = record.get('product_id', '')
                        
                        if distinct_id and product_id:
                            try:
                                # Get RC Trial started event timestamp for this user/product
                                trial_start_query = """
                                SELECT event_time 
                                FROM mixpanel_event 
                                WHERE distinct_id = ? 
                                  AND JSON_EXTRACT(event_json, '$.properties.product_id') = ?
                                  AND event_name = 'RC Trial started'
                                ORDER BY event_time DESC
                                LIMIT 1
                                """
                                event_cursor.execute(trial_start_query, (distinct_id, product_id))
                                result = event_cursor.fetchone()
                                
                                if result:
                                    event_time_str = result['event_time']
                                    # Parse event_time (handle both datetime and date formats)
                                    try:
                                        if 'T' in event_time_str or ' ' in event_time_str:
                                            trial_start_date = datetime.fromisoformat(event_time_str.replace('Z', '')).date()
                                        else:
                                            trial_start_date = datetime.strptime(event_time_str, '%Y-%m-%d').date()
                                        
                                        # Only include users whose trial started 7+ days ago
                                        days_since_trial = (today - trial_start_date).days
                                        if days_since_trial >= 7:
                                            actual_users.append(record)
                                        else:
                                            logger.debug(f"🕐 User {distinct_id} trial too recent: {days_since_trial} days ago")
                                    except ValueError as ve:
                                        # Skip users with invalid event timestamp
                                        logger.debug(f"📅 User {distinct_id} invalid timestamp: {event_time_str} - {ve}")
                                        continue
                                else:
                                    logger.debug(f"👻 User {distinct_id} has no RC Trial started event")
                            except Exception as e:
                                logger.warning(f"Error checking trial start for user {distinct_id}: {e}")
                                continue
                
                logger.info(f"🔍 FILTERING RESULTS:")
                logger.info(f"   📊 Estimated users (all): {len(estimated_users)}")
                logger.info(f"   📊 Actual users (time-filtered): {len(actual_users)}")
                
                # DEBUG: If there's a discrepancy, log which users were filtered out
                if len(estimated_users) != len(actual_users):
                    estimated_ids = {r['distinct_id'] for r in estimated_users}
                    actual_ids = {r['distinct_id'] for r in actual_users}
                    missing_ids = estimated_ids - actual_ids
                    logger.warning(f"⚠️ {len(missing_ids)} users filtered out: {list(missing_ids)[:5]}...")  # Show first 5
                
                # Calculate summary statistics for ESTIMATED users
                def calculate_summary_stats(users_list):
                    if users_list:
                        trial_conversion_rates = [r['trial_conversion_rate'] for r in users_list if r['trial_conversion_rate'] is not None]
                        trial_refund_rates = [r['trial_converted_to_refund_rate'] for r in users_list if r['trial_converted_to_refund_rate'] is not None]
                        purchase_refund_rates = [r['initial_purchase_to_refund_rate'] for r in users_list if r['initial_purchase_to_refund_rate'] is not None]
                        
                        return {
                            'total_users': len(users_list),
                            'avg_trial_conversion_rate': (sum(trial_conversion_rates) / len(trial_conversion_rates) * 100) if trial_conversion_rates else 0,
                            'avg_trial_refund_rate': (sum(trial_refund_rates) / len(trial_refund_rates) * 100) if trial_refund_rates else 0,
                            'avg_purchase_refund_rate': (sum(purchase_refund_rates) / len(purchase_refund_rates) * 100) if purchase_refund_rates else 0,
                            'total_estimated_revenue': sum(r['current_value'] for r in users_list if r['current_value']),
                            'breakdown_applied': breakdown,
                            'breakdown_value': breakdown_value
                        }
                    else:
                        return {
                            'total_users': 0,
                            'avg_trial_conversion_rate': 0,
                            'avg_trial_refund_rate': 0,
                            'avg_purchase_refund_rate': 0,
                            'total_estimated_revenue': 0,
                            'breakdown_applied': breakdown,
                            'breakdown_value': breakdown_value
                        }
                
                # Calculate summary for actual conversions (binary logic, OPTIMIZED with batch queries)
                def calculate_actual_conversion_stats(users_list):
                    if not users_list:
                        return {
                            'total_users': 0,
                            'converted_users': 0,
                            'avg_trial_conversion_rate': 0,
                            'avg_trial_refund_rate': 0,
                            'avg_purchase_refund_rate': 0,
                            'total_estimated_revenue': 0,
                            'breakdown_applied': breakdown,
                            'breakdown_value': breakdown_value
                        }
                    
                    # OPTIMIZED: Batch query for all user/product conversions and refunds
                    user_product_pairs = [(r['distinct_id'], r['product_id']) for r in users_list if r.get('distinct_id') and r.get('product_id')]
                    
                    if not user_product_pairs:
                        return {
                            'total_users': 0,
                            'converted_users': 0,
                            'avg_trial_conversion_rate': 0,
                            'avg_trial_refund_rate': 0,
                            'avg_purchase_refund_rate': 0,
                            'total_estimated_revenue': 0,
                            'breakdown_applied': breakdown,
                            'breakdown_value': breakdown_value
                        }
                    
                    # Create placeholders for batch query
                    placeholders = ','.join([f"(?,?)" for _ in user_product_pairs])
                    query_params = [item for pair in user_product_pairs for item in pair]
                    
                    # Single batch query for all events
                    batch_events_query = f"""
                    SELECT 
                        distinct_id,
                        JSON_EXTRACT(event_json, '$.properties.product_id') as product_id,
                        event_name,
                        CASE WHEN revenue_usd < 0 THEN 1 ELSE 0 END as is_refund
                    FROM mixpanel_event 
                    WHERE (distinct_id, JSON_EXTRACT(event_json, '$.properties.product_id')) IN (VALUES {placeholders})
                      AND event_name IN ('RC Trial converted', 'RC Initial purchase', 'RC Cancellation')
                    """
                    
                    cursor = conn.cursor()
                    cursor.execute(batch_events_query, query_params)
                    all_events = cursor.fetchall()
                    
                    # Process results with TWO-PASS logic to fix refund categorization
                    converted_users = set()
                    trial_converted_users = set() 
                    purchase_users = set()
                    refund_events = []  # Store refund events for second pass
                    
                    # PASS 1: Collect all conversions and purchases first
                    for event in all_events:
                        user_product = (event['distinct_id'], event['product_id'])
                        event_name = event['event_name']
                        is_refund = event['is_refund']
                        
                        if event_name == 'RC Trial converted':
                            converted_users.add(user_product)
                            trial_converted_users.add(user_product)
                        elif event_name == 'RC Initial purchase':
                            purchase_users.add(user_product)
                        elif event_name == 'RC Cancellation' and is_refund:
                            # Store refund events for second pass
                            refund_events.append(user_product)
                    
                    # PASS 2: Now categorize refunds based on complete conversion/purchase sets
                    trial_refunded_users = set()
                    purchase_refunded_users = set()
                    
                    for user_product in refund_events:
                        if user_product in trial_converted_users:
                            trial_refunded_users.add(user_product)
                        if user_product in purchase_users:
                            purchase_refunded_users.add(user_product)
                    
                    total_eligible = len(users_list)
                    converted_count = len(converted_users)
                    
                    # Calculate actual conversion rates
                    actual_conversion_rate = (converted_count / total_eligible * 100) if total_eligible > 0 else 0
                    
                    # Calculate binary refund rates
                    trial_conversion_count = len(trial_converted_users)
                    trial_refund_count = len(trial_refunded_users)
                    avg_trial_refund = (trial_refund_count / trial_conversion_count * 100) if trial_conversion_count > 0 else 0
                    
                    purchase_count = len(purchase_users)
                    purchase_refund_count = len(purchase_refunded_users)
                    avg_purchase_refund = (purchase_refund_count / purchase_count * 100) if purchase_count > 0 else 0
                    
                    # Get converted user records for revenue calculation
                    converted_user_records = [r for r in users_list if (r['distinct_id'], r['product_id']) in converted_users]
                    
                    # Create cohort-specific user lists for display
                    trial_converted_user_records = [r for r in users_list if (r['distinct_id'], r['product_id']) in trial_converted_users]
                    purchase_user_records = [r for r in users_list if (r['distinct_id'], r['product_id']) in purchase_users]
                    
                    return {
                        'total_users': total_eligible,  # Show all time-eligible users (matching modal display)
                        'converted_users': converted_count,  # Track how many actually converted  
                        'avg_trial_conversion_rate': actual_conversion_rate,
                        'avg_trial_refund_rate': avg_trial_refund,
                        'avg_purchase_refund_rate': avg_purchase_refund,
                        'total_estimated_revenue': sum(r['current_value'] for r in converted_user_records if r['current_value']),
                        'breakdown_applied': breakdown,
                        'breakdown_value': breakdown_value,
                        # Add specific counts for refund rate display
                        'trial_refunded_users': trial_refund_count,
                        'trial_converted_users': trial_conversion_count,
                        'purchase_refunded_users': purchase_refund_count,
                        'purchase_users': purchase_count,
                        # Add cohort-specific user lists
                        'trial_converted_user_records': trial_converted_user_records,
                        'purchase_user_records': purchase_user_records
                    }
                
                estimated_summary = calculate_summary_stats(estimated_users)
                actual_summary = calculate_actual_conversion_stats(actual_users)
                
                # Format user records for display (convert rates to percentages)
                def format_user_records(users_list):
                    formatted_users = []
                    for record in users_list:
                        formatted_users.append({
                            'distinct_id': record['distinct_id'],
                            'country': record['country'] or 'N/A',
                            'region': record['region'] or 'N/A',
                            'device_category': record['device_category'] or 'N/A',
                            'status': record['current_status'],
                            'trial_conversion_rate': round(record['trial_conversion_rate'] * 100, 1) if record['trial_conversion_rate'] else 0,
                            'trial_refund_rate': round(record['trial_converted_to_refund_rate'] * 100, 1) if record['trial_converted_to_refund_rate'] else 0,
                            'purchase_refund_rate': round(record['initial_purchase_to_refund_rate'] * 100, 1) if record['initial_purchase_to_refund_rate'] else 0,
                            'estimated_value': round(record['current_value'], 2) if record['current_value'] else 0,
                            'value_status': record['value_status'] or 'N/A',
                            'credited_date': record['credited_date'],
                            'price_bucket': f"${record['price_bucket']:.2f}" if record['price_bucket'] else 'N/A',
                            'economic_tier': record['economic_tier'] or 'N/A',
                            'accuracy_score': record['accuracy_score'] or 'N/A',
                            'product_id': record['product_id'] or 'N/A'
                        })
                    return formatted_users
                
                logger.info(f"✅ ESTIMATED: {estimated_summary['total_users']} users, avg rates: {estimated_summary['avg_trial_conversion_rate']:.1f}%/{estimated_summary['avg_trial_refund_rate']:.1f}%/{estimated_summary['avg_purchase_refund_rate']:.1f}%")
                logger.info(f"✅ ACTUAL: {actual_summary['total_users']} eligible, {actual_summary['converted_users']} converted ({actual_summary['avg_trial_conversion_rate']:.1f}%), rates: {actual_summary['avg_trial_refund_rate']:.1f}%/{actual_summary['avg_purchase_refund_rate']:.1f}%")
                
                # Select appropriate user list for actual mode based on metric type
                if metric_type == 'avg_trial_refund_rate':
                    actual_users_for_display = actual_summary['trial_converted_user_records']
                elif metric_type == 'purchase_refund_rate':
                    actual_users_for_display = actual_summary['purchase_user_records']
                else:  # trial_conversion_rate
                    actual_users_for_display = actual_users
                
                return {
                    'success': True,
                    'estimated': {
                        'summary': estimated_summary,
                        'users': format_user_records(estimated_users)
                    },
                    'actual': {
                        'summary': actual_summary,
                        'users': format_user_records(actual_users_for_display)  # Show cohort that matches summary count
                    },
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
        """
        DEPRECATED: Moved to dashboard_refresh_service.py
        Get the earliest date available in meta analytics database
        """
        try:
            logger.warning("DEPRECATED: analytics_query_service.get_earliest_meta_date() called. Use dashboard_refresh_service.get_earliest_meta_date() instead!")
            # Connect to meta analytics database
            with sqlite3.connect(self.meta_db_path) as conn:
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
        """
        DEPRECATED: Moved to dashboard_refresh_service.py
        Use dashboard_refresh_service.get_available_date_range() instead.
        Get available date range from analytics data
        """
        try:
            logger.warning("DEPRECATED: analytics_query_service.get_available_date_range() called. Use dashboard_refresh_service.get_available_date_range() instead!")
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
        DEPRECATED: Moved to dashboard_refresh_service.py
        Use dashboard_refresh_service.get_segment_performance() instead.
        
        Get segment performance data for conversion rate analysis
        
        Returns unique segments based on conversion rate cohort properties:
        - product_id, price_bucket, store, economic_tier, country, region
        - Shows user count, conversion rates, and accuracy level for each segment
        """
        try:
            logger.warning("DEPRECATED: analytics_query_service.get_segment_performance() called. Use dashboard_refresh_service.get_segment_performance() instead!")
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

    def get_overview_roas_chart_data(self, start_date: str, end_date: str, breakdown: str = 'all', entity_type: str = 'campaign') -> Dict[str, Any]:
        """
        ⚠️ DEPRECATED: This method has been moved to OverviewService.
        Use OverviewService.get_overview_roas_chart_data() instead.
        """
        logger.warning("⚠️ DEPRECATED: analytics_query_service.get_overview_roas_chart_data() called! Use OverviewService instead.")
        
        # Import and redirect to the correct service
        from .overview_service import OverviewService
        overview_service = OverviewService(
            meta_db_path=self.meta_db_path,
            mixpanel_db_path=self.mixpanel_db_path,
            mixpanel_analytics_db_path=self.mixpanel_analytics_db_path
        )
        return overview_service.get_overview_roas_chart_data(start_date, end_date, breakdown, entity_type)


    def execute_analytics_query_optimized(self, config: QueryConfig) -> Dict[str, Any]:
        """
        ⚠️ DEPRECATED: This method has been moved to DashboardRefreshService.
        Use DashboardRefreshService.execute_optimized_dashboard_refresh() instead.
        """
        logger.warning("⚠️ DEPRECATED: analytics_query_service.execute_analytics_query_optimized() called. Use DashboardRefreshService.execute_optimized_dashboard_refresh() instead!")
        
        # Import and redirect to the correct service
        from .dashboard_refresh_service import DashboardRefreshService
        refresh_service = DashboardRefreshService(
            meta_db_path=self.meta_db_path,
            mixpanel_db_path=self.mixpanel_db_path,
            mixpanel_analytics_db_path=self.mixpanel_analytics_db_path
        )
        return refresh_service.execute_optimized_dashboard_refresh(config)
