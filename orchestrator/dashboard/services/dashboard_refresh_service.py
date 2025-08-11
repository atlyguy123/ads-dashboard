"""
Dashboard Refresh Service

This service handles optimized dashboard data refresh requests from the frontend.
It provides fast data retrieval using pre-computed tables for dashboard refresh operations.

This is a refactored module that extracts optimized dashboard functionality from
analytics_query_service.py for better maintainability and separation of concerns.
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

# Import the QueryConfig from analytics_query_service
from .analytics_query_service import QueryConfig
from .overview_service import OverviewService

logger = logging.getLogger(__name__)


class DashboardRefreshService:
    """Service for optimized dashboard refresh operations"""
    
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
        
        # Table mapping based on breakdown parameter (moved from analytics_query_service)
        self.table_mapping = {
            'all': 'ad_performance_daily',
            'country': 'ad_performance_daily_country', 
            'region': 'ad_performance_daily_region',
            'device': 'ad_performance_daily_device'
        }


    def execute_optimized_dashboard_refresh(self, config: QueryConfig) -> Dict[str, Any]:
        """
        🚀 OPTIMIZED analytics query using ONLY pre-computed data
        
        No real-time calculations - just reads from daily_mixpanel_metrics and structures the data.
        Uses proper JSON parsing for user lists to fix tooltip/modal display.
        Handles both overall and country breakdowns using pre-computed tables.
        """
        try:
            import sqlite3
            import json
            from utils.database_utils import get_database_connection
            
            with get_database_connection('mixpanel_data') as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if config.breakdown == 'all':
                    # Use main daily_mixpanel_metrics table (overall data)
                    query = """
                    SELECT 
                        d.entity_id,
                        n.canonical_name as name,
                        d.entity_type,
                        d.date,
                        d.trial_users_count,
                        d.meta_trial_count,
                        d.purchase_users_count, 
                        d.meta_purchase_count,
                        d.trial_conversion_rate_actual,
                        d.trial_conversion_rate_estimated,
                        d.trial_refund_rate_estimated,
                        d.purchase_refund_rate_estimated,
                        d.meta_spend,
                        d.estimated_revenue_usd,
                        d.adjusted_estimated_revenue_usd,
                        d.actual_revenue_usd,
                        d.net_actual_revenue_usd,
                        d.profit_usd,
                        d.trial_user_ids,
                        d.converted_user_ids,
                        d.post_trial_user_ids,
                        d.trial_refund_user_ids,
                        d.purchase_refund_user_ids,
                        d.purchase_user_ids
                        
                    FROM daily_mixpanel_metrics d
                    LEFT JOIN id_name_mapping n ON d.entity_id = n.entity_id
                    WHERE d.date BETWEEN ? AND ?
                      AND d.entity_type = ?
                    ORDER BY d.entity_id, d.date
                    """
                    
                    cursor.execute(query, (config.start_date, config.end_date, config.group_by))
                    
                else:
                    # Use breakdown table for country/device/region breakdowns
                    query = """
                    SELECT 
                        d.entity_id,
                        n.canonical_name as name,
                        d.entity_type,
                        d.breakdown_value as breakdown_key,
                        d.date,
                        d.mixpanel_trial_count as trial_users_count,
                        d.meta_trial_count,
                        d.mixpanel_purchase_count as purchase_users_count, 
                        d.meta_purchase_count,
                        d.trial_conversion_rate_actual,
                        d.trial_conversion_rate_estimated,
                        d.trial_refund_rate_estimated,
                        d.purchase_refund_rate_estimated,
                        d.meta_spend,
                        d.estimated_revenue_usd,
                        d.adjusted_estimated_revenue_usd,
                        d.actual_revenue_usd,
                        d.net_actual_revenue_usd,
                        d.profit_usd,
                        d.trial_user_ids,
                        d.converted_user_ids,
                        d.post_trial_user_ids,
                        d.trial_refund_user_ids,
                        d.purchase_refund_user_ids,
                        d.purchase_user_ids
                        
                    FROM daily_mixpanel_metrics_breakdown d
                    LEFT JOIN id_name_mapping n ON d.entity_id = n.entity_id
                    WHERE d.date BETWEEN ? AND ?
                      AND d.entity_type = ?
                      AND d.breakdown_type = ?
                    ORDER BY d.entity_id, d.breakdown_value, d.date
                    """
                    
                    cursor.execute(query, (config.start_date, config.end_date, config.group_by, config.breakdown))
                
                rows = cursor.fetchall()
                
                # Helper function to parse user lists (handles both JSON arrays and comma-separated strings)
                def parse_user_list(user_data):
                    if not user_data:
                        return []
                    
                    # If it's a comma-separated string of JSON arrays, split and parse each
                    if ',' in user_data and user_data.strip().startswith('['):
                        # Split by comma, but handle embedded commas in JSON
                        parts = []
                        current_part = ""
                        bracket_count = 0
                        in_quotes = False
                        
                        for char in user_data:
                            if char == '"' and (not current_part or current_part[-1] != '\\'):
                                in_quotes = not in_quotes
                            elif char == '[' and not in_quotes:
                                bracket_count += 1
                            elif char == ']' and not in_quotes:
                                bracket_count -= 1
                            elif char == ',' and bracket_count == 0 and not in_quotes:
                                if current_part.strip():
                                    parts.append(current_part.strip())
                                current_part = ""
                                continue
                            
                            current_part += char
                        
                        if current_part.strip():
                            parts.append(current_part.strip())
                        
                        # Parse each JSON part
                        all_users = []
                        for part in parts:
                            try:
                                parsed = json.loads(part)
                                if isinstance(parsed, list):
                                    all_users.extend(parsed)
                                else:
                                    all_users.append(parsed)
                            except:
                                # Fallback to treating as string
                                all_users.append(part.strip('"'))
                        return all_users
                    
                    # Single JSON array
                    try:
                        parsed = json.loads(user_data)
                        return parsed if isinstance(parsed, list) else [parsed]
                    except:
                        # Fallback to comma-separated string
                        return [u.strip().strip('"') for u in user_data.split(',') if u.strip()]
                
                # Group rows by entity_id + breakdown_key for proper user list aggregation
                entity_data = {}
                for row in rows:
                    entity_id = row['entity_id']
                    breakdown_key = row['breakdown_key'] if config.breakdown != 'all' and 'breakdown_key' in row.keys() else None
                    # Create composite key for breakdowns
                    composite_key = f"{entity_id}_{breakdown_key}" if breakdown_key else entity_id
                    
                    if composite_key not in entity_data:
                        entity_data[composite_key] = {
                            'entity_id': entity_id,
                            'name': row['name'] or f"Unknown {config.group_by}",
                            'entity_type': row['entity_type'],
                            'daily_rows': [],
                            'breakdown_key': breakdown_key,
                            'breakdown_type': config.breakdown if config.breakdown != 'all' else None
                        }
                    entity_data[composite_key]['daily_rows'].append(row)
                
                # Structure the data for frontend - SIMPLIFIED FLAT FORMAT
                entities = []
                
                # Process all entities with optimized aggregation
                for composite_key, data in entity_data.items():
                    daily_rows = data['daily_rows']
                    entity_id = data['entity_id']
                    entity_type = data['entity_type']
                    name = data['name']
                    breakdown_key = data.get('breakdown_key')
                    
                    # Aggregate daily metrics using the same logic as the working optimized method
                    aggregated_metrics = self._aggregate_daily_metrics_optimized(daily_rows)
                    
                    # Build entity record with all frontend fields
                    entity = {
                        'id': f"{entity_type}_{entity_id}",
                        'entity_id': entity_id,
                        'entity_type': entity_type,
                        'name': name,
                        'children': self._get_entity_children_optimized(entity_type, entity_id, config),
                        
                        # Pre-computed metrics
                        **aggregated_metrics,
                        
                        # Sparkline data (always 14 days from end_date)
                        'sparkline_data': self._format_sparkline_data_14_days(entity_type, entity_id, config.end_date)
                    }
                    
                    # Add breakdown info if applicable
                    if breakdown_key:
                        entity['breakdown_key'] = breakdown_key
                        entity['breakdown_type'] = config.breakdown
                    
                    entities.append(entity)
                
                # Debug: Log sparkline data inclusion
                if entities:
                    first_entity = entities[0]
                    sparkline_count = len(first_entity.get('sparkline_data', []))
                    logger.info(f"📊 API RESPONSE DEBUG: First entity has {sparkline_count} sparkline points")
                    if sparkline_count > 0:
                        logger.info(f"📊 API RESPONSE DEBUG: First sparkline point: {first_entity['sparkline_data'][0]}")
                
                return {
                    'success': True,
                    'data': entities,
                    'total_records': len(entities),
                    'breakdown': config.breakdown,
                    'method': 'optimized_precomputed_flat'
                }
        
        except Exception as e:
            logger.error(f"Error executing optimized analytics query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'method': 'optimized_precomputed_error'
            }
    

    def _group_and_aggregate_entities_optimized(self, rows: List[Dict], config: QueryConfig) -> List[Dict[str, Any]]:
        """Extract and reuse the main entity aggregation logic"""
        import json
        
        # Group rows by entity_id + breakdown_key for proper user list aggregation
        entity_data = {}
        for row in rows:
            entity_id = row['entity_id']
            breakdown_key = row['breakdown_key'] if config.breakdown != 'all' and 'breakdown_key' in row.keys() else None
            # Create composite key for breakdowns
            composite_key = f"{entity_id}_{breakdown_key}" if breakdown_key else entity_id
            
            if composite_key not in entity_data:
                entity_data[composite_key] = {
                    'entity_id': entity_id,
                    'name': row['name'] or f"Unknown {config.group_by}",
                    'entity_type': row['entity_type'],
                    'daily_rows': [],
                    'breakdown_key': breakdown_key,
                    'breakdown_type': config.breakdown if config.breakdown != 'all' else None
                }
            entity_data[composite_key]['daily_rows'].append(row)
        
        # Structure the data for frontend - SIMPLIFIED FLAT FORMAT
        entities = []
        
        # Process all entities with optimized aggregation
        for composite_key, data in entity_data.items():
            daily_rows = data['daily_rows']
            entity_id = data['entity_id']
            entity_type = data['entity_type']
            name = data['name']
            breakdown_key = data['breakdown_key']
            
            # Aggregate daily metrics using optimized method
            aggregated_metrics = self._aggregate_daily_metrics_optimized(daily_rows)
            
            # Build entity with complete metrics
            entity = {
                'id': f"{entity_type}_{entity_id}",
                'entity_id': entity_id,
                'entity_type': entity_type,
                'name': name,
                'children': self._get_entity_children_optimized(entity_type, entity_id, config),
                
                # Pre-computed metrics
                **aggregated_metrics,
                
                # User data structure
                'user_data': {
                    'trial_user_ids': aggregated_metrics.get('trial_user_ids', []),
                    'purchase_user_ids': aggregated_metrics.get('purchase_user_ids', []),
                    'trial_users_count': aggregated_metrics.get('trial_users_count', 0),
                    'purchase_users_count': aggregated_metrics.get('purchase_users_count', 0),
                    'performance_impact_score': aggregated_metrics.get('performance_impact_score', 0.0)
                },
                
                # Sparkline data (always 14 days from end_date)
                'sparkline_data': self._format_sparkline_data_14_days(entity_type, entity_id, config.end_date)
            }
            
            # Add breakdown information if applicable
            if breakdown_key:
                entity['breakdown_key'] = breakdown_key
                entity['breakdown_type'] = data['breakdown_type']
                entity['id'] = f"{entity_type}_{entity_id}_{breakdown_key}"
            
            entities.append(entity)
        
        return entities


    def _execute_optimized_query_for_entities(self, entity_ids: List[str], config: QueryConfig) -> Dict[str, Any]:
        """Execute the main optimized query for specific entity IDs"""
        try:
            # Get daily metrics for these specific entities
            daily_rows = self._get_precomputed_daily_metrics(entity_ids, config.group_by, config.start_date, config.end_date)
            
            if not daily_rows:
                return {'success': True, 'data': [], 'method': 'optimized_precomputed_children'}
            
            # Use the same aggregation logic as main method
            grouped_data = self._group_and_aggregate_entities_optimized(daily_rows, config)
            
            return {
                'success': True,
                'data': grouped_data,
                'total_entities': len(grouped_data),
                'method': 'optimized_precomputed_children'
            }
            
        except Exception as e:
            logger.error(f"Error executing optimized query for children: {e}")
            return {'success': False, 'data': [], 'error': str(e)}


    def _get_entity_children_optimized(self, entity_type: str, entity_id: str, config: QueryConfig) -> List[Dict[str, Any]]:
        """
        Get children entities using the same optimized method recursively.
        This ensures children have identical data structure and calculation logic.
        """
        try:
            # Determine child entity type and get their IDs
            if entity_type == 'campaign':
                child_type = 'adset'
                child_ids = self._get_child_entity_ids(entity_id, 'campaign', 'adset')
            elif entity_type == 'adset':  
                child_type = 'ad'
                child_ids = self._get_child_entity_ids(entity_id, 'adset', 'ad')
            else:
                # Ads have no children
                return []
            
            if not child_ids:
                return []
                
            # Create new config for children with same parameters but different group_by
            # Note: hierarchy and enable_breakdown_mapping may not exist in QueryConfig but are passed through
            child_config = QueryConfig(
                start_date=config.start_date,
                end_date=config.end_date,
                group_by=child_type,  # Key difference: group by child type
                breakdown=config.breakdown,
                include_mixpanel=getattr(config, 'include_mixpanel', True),
                enable_breakdown_mapping=getattr(config, 'enable_breakdown_mapping', True)
            )
            
            # Use the SAME optimized method to get children data
            # This ensures 100% identical logic and data structure
            child_result = self._execute_optimized_query_for_entities(child_ids, child_config)
            
            return child_result.get('data', [])
                
        except Exception as e:
            logger.error(f"Error getting children for {entity_type} {entity_id}: {e}")
            return []


    def _format_sparkline_data_14_days(self, entity_type: str, entity_id: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Format sparkline data for exactly 14 days ending on end_date.
        Missing days are filled with zeros (will render as gray in frontend).
        """
        try:
            from datetime import datetime, timedelta
            
            # Calculate 14-day window (13 days back + end_date = 14 total)
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            start_dt = end_dt - timedelta(days=13)
            start_date_14 = start_dt.strftime('%Y-%m-%d')
            
            # Get actual data from database
            raw_data = self._get_precomputed_sparkline_data(entity_type, entity_id, start_date_14, end_date)
            
            # Create lookup dict for actual data
            data_by_date = {row['date']: row for row in raw_data}
            
            # Generate exactly 14 days, filling missing with zeros
            formatted_data = []
            current_dt = start_dt
            
            for day in range(14):
                date_str = current_dt.strftime('%Y-%m-%d')
                
                if date_str in data_by_date:
                    # Use actual data
                    day_data = data_by_date[date_str]
                    spend = float(day_data.get('meta_spend', 0) or 0)
                    revenue = float(day_data.get('adjusted_estimated_revenue_usd', 0) or 0)
                    roas = (revenue / spend) if spend > 0 else 0.0
                    
                    formatted_point = {
                        'date': date_str,
                        'rolling_1d_roas': round(roas, 2),
                        'rolling_1d_spend': spend,
                        'rolling_1d_revenue': revenue,
                        'rolling_1d_conversions': int(day_data.get('purchase_users_count', 0) or 0),
                        'rolling_1d_trials': int(day_data.get('trial_users_count', 0) or 0),
                        'rolling_1d_meta_trials': int(day_data.get('meta_trial_count', 0) or 0),
                        'rolling_window_days': 1,
                        'has_data': True  # Flag for frontend to render with color
                    }
                else:
                    # Fill missing day with zeros (will render gray)
                    formatted_point = {
                        'date': date_str,
                        'rolling_1d_roas': 0.0,
                        'rolling_1d_spend': 0.0,
                        'rolling_1d_revenue': 0.0,
                        'rolling_1d_conversions': 0,
                        'rolling_1d_trials': 0,
                        'rolling_1d_meta_trials': 0,
                        'rolling_window_days': 1,
                        'has_data': False  # Flag for frontend to render gray
                    }
                
                formatted_data.append(formatted_point)
                current_dt += timedelta(days=1)
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error formatting 14-day sparkline data for {entity_type} {entity_id}: {e}")
            return []

    def _format_sparkline_data_optimized(self, entity_type: str, entity_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Format pre-computed sparkline data for frontend consumption within the optimized method.
        Returns array of daily data points with rolling_1d_roas and other frontend-expected fields.
        """
        try:
            # Get raw sparkline data from the pre-computed table
            raw_data = self._get_precomputed_sparkline_data(entity_type, entity_id, start_date, end_date)
            
            # Format each day's data for frontend
            formatted_data = []
            for day_data in raw_data:
                # Calculate ROAS from pre-computed values
                spend = float(day_data.get('meta_spend', 0) or 0)
                revenue = float(day_data.get('adjusted_estimated_revenue_usd', 0) or 0)
                roas = (revenue / spend) if spend > 0 else 0.0
                
                # Format data point for frontend sparkline
                formatted_point = {
                    'date': day_data.get('date'),
                    'rolling_1d_roas': round(roas, 2),
                    'rolling_1d_spend': spend,
                    'rolling_1d_revenue': revenue,
                    'rolling_1d_conversions': int(day_data.get('purchase_users_count', 0) or 0),
                    'rolling_1d_trials': int(day_data.get('trial_users_count', 0) or 0),
                    'rolling_1d_meta_trials': int(day_data.get('meta_trial_count', 0) or 0),
                    'rolling_window_days': 1
                }
                formatted_data.append(formatted_point)
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error formatting sparkline data for {entity_type} {entity_id}: {e}")
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
                    estimated_revenue_usd,
                    meta_spend,
                    adjusted_estimated_revenue_usd,
                    meta_trial_count
                FROM daily_mixpanel_metrics
                WHERE entity_type = ? 
                  AND entity_id = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """
                
                cursor.execute(sparkline_query, [entity_type, entity_id, start_date, end_date])
                results = cursor.fetchall()
                
                logger.info(f"📊 SPARKLINE MIXPANEL: Retrieved {len(results)} days from daily_mixpanel_metrics for {entity_type} {entity_id}")
                
                # Debug: Log when no data found
                if not results:
                    logger.debug(f"No sparkline data found for {entity_type} {entity_id} between {start_date} and {end_date}")
                
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error getting pre-computed sparkline data for {entity_type} {entity_id}: {e}")
            return []


    def _aggregate_daily_metrics_optimized(self, daily_rows: List[Dict]) -> Dict[str, Any]:
        """Aggregate daily metrics using optimized JSON parsing for user lists"""
        import json
        
        def parse_user_list(user_data):
            """Helper to parse user lists from JSON strings"""
            if not user_data:
                return []
            
            try:
                parsed = json.loads(user_data)
                return parsed if isinstance(parsed, list) else [parsed]
            except:
                return [u.strip().strip('"') for u in user_data.split(',') if u.strip()]
        
        # Aggregate unique users and financial metrics
        all_trial_users = set()
        all_converted_users = set()
        all_purchase_users = set()
        all_post_trial_users = set()
        all_trial_refund_users = set()
        all_purchase_refund_users = set()
        total_meta_spend = 0
        total_meta_trials = 0
        total_meta_purchases = 0
        total_estimated_revenue = 0
        total_adjusted_revenue = 0
        total_actual_revenue = 0
        total_profit = 0
        estimated_rate_sum = 0
        valid_estimated_days = 0

        logger.info(f"🔍 BACKEND AGGREGATION DEBUG: Processing {len(daily_rows)} daily rows")
        for i, row in enumerate(daily_rows):
            if i < 3:  # Log first 3 rows for debugging
                logger.info(f"🔍 Sample row {i}: date={row['date']}, spend={row['meta_spend']}, revenue={row['adjusted_estimated_revenue_usd']}, profit={row['profit_usd']}")
            
            # Parse user lists and aggregate unique users (sqlite3.Row access)
            if 'trial_user_ids' in row.keys() and row['trial_user_ids']:
                all_trial_users.update(parse_user_list(row['trial_user_ids']))
                
            if 'converted_user_ids' in row.keys() and row['converted_user_ids']:
                all_converted_users.update(parse_user_list(row['converted_user_ids']))
                
            if 'post_trial_user_ids' in row.keys() and row['post_trial_user_ids']:
                all_post_trial_users.update(parse_user_list(row['post_trial_user_ids']))
                
            if 'purchase_user_ids' in row.keys() and row['purchase_user_ids']:
                all_purchase_users.update(parse_user_list(row['purchase_user_ids']))
                
            if 'trial_refund_user_ids' in row.keys() and row['trial_refund_user_ids']:
                all_trial_refund_users.update(parse_user_list(row['trial_refund_user_ids']))
                
            if 'purchase_refund_user_ids' in row.keys() and row['purchase_refund_user_ids']:
                all_purchase_refund_users.update(parse_user_list(row['purchase_refund_user_ids']))
            
            # Sum financial metrics
            total_meta_spend += float(row['meta_spend'] or 0)
            total_meta_trials += int(row['meta_trial_count'] or 0)
            total_meta_purchases += int(row['meta_purchase_count'] or 0)
            total_estimated_revenue += float(row['estimated_revenue_usd'] or 0)
            total_adjusted_revenue += float(row['adjusted_estimated_revenue_usd'] or 0)
            total_actual_revenue += float(row['actual_revenue_usd'] or 0)
            total_profit += float(row['profit_usd'] or 0)
            
            if 'trial_conversion_rate_estimated' in row.keys() and row['trial_conversion_rate_estimated'] is not None:
                estimated_rate_sum += float(row['trial_conversion_rate_estimated'])
                valid_estimated_days += 1
        
        logger.info(f"🔍 BACKEND TOTALS: spend={total_meta_spend}, revenue={total_adjusted_revenue}, profit={total_profit}")
        
        # Calculate final metrics
        unique_trial_count = len(all_trial_users)
        unique_converted_count = len(all_converted_users)
        unique_purchase_count = len(all_purchase_users)
        unique_post_trial_count = len(all_post_trial_users)
        
        return {
            'mixpanel_trials_started': unique_trial_count,
            'meta_trials_started': total_meta_trials,
            'mixpanel_purchases': unique_purchase_count,
            'meta_purchases': total_meta_purchases,
            'trial_accuracy_ratio': (unique_trial_count / total_meta_trials) if total_meta_trials > 0 else 0.0,
            'purchase_accuracy_ratio': (unique_purchase_count / total_meta_purchases) if total_meta_purchases > 0 else 0.0,
            'trial_conversion_rate': (unique_converted_count / unique_post_trial_count) if unique_post_trial_count > 0 else 0.0,
            'trial_conversion_rate_estimated': (estimated_rate_sum / valid_estimated_days) if valid_estimated_days > 0 else 0.0,
            # ACTUAL rates: calculated from real user counts (actual performance)
            'avg_trial_refund_rate': (len(all_trial_refund_users) / len(all_converted_users)) if len(all_converted_users) > 0 else 0.0,
            'purchase_refund_rate': (len(all_purchase_refund_users) / len(all_purchase_users)) if len(all_purchase_users) > 0 else 0.0,
            
            # ESTIMATED rates: averaged from pre-computed predictions (user-level estimates)
            'trial_refund_rate_estimated': sum(float(row['trial_refund_rate_estimated'] or 0) for row in daily_rows) / len(daily_rows) if daily_rows else 0.0,
            'purchase_refund_rate_estimated': sum(float(row['purchase_refund_rate_estimated'] or 0) for row in daily_rows) / len(daily_rows) if daily_rows else 0.0,
            'spend': total_meta_spend,
            'estimated_revenue_usd': total_estimated_revenue,
            'estimated_revenue_adjusted': total_adjusted_revenue,
            'actual_revenue_usd': total_actual_revenue,
            'profit': total_profit,
            'estimated_roas': (total_adjusted_revenue / total_meta_spend) if total_meta_spend > 0 else 0.0,
            'performance_impact_score': (total_adjusted_revenue / total_meta_spend) if total_meta_spend > 0 else 0.0,
            'trial_users_count': unique_trial_count,
            'post_trial_users_count': unique_post_trial_count,
            'converted_users_count': unique_converted_count,
            'purchase_users_count': unique_purchase_count,
            'user_details': {
                'trial_users': list(all_trial_users),
                'converted_users': list(all_converted_users),
                'trial_refund_user_ids': list(all_trial_refund_users),
                'purchase_refund_user_ids': list(all_purchase_refund_users)
            }
        }


    def _get_child_entity_ids(self, parent_id: str, parent_type: str, child_type: str) -> List[str]:
        """Get IDs of child entities from hierarchy mapping"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if parent_type == 'campaign' and child_type == 'adset':
                    query = "SELECT DISTINCT adset_id as child_id FROM id_hierarchy_mapping WHERE campaign_id = ?"
                elif parent_type == 'adset' and child_type == 'ad':
                    query = "SELECT DISTINCT ad_id as child_id FROM id_hierarchy_mapping WHERE adset_id = ?"
                else:
                    return []
                    
                cursor.execute(query, (parent_id,))
                rows = cursor.fetchall()
                return [str(row['child_id']) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting {child_type} children for {parent_type} {parent_id}: {e}")
            return []


    def _get_precomputed_daily_metrics(self, entity_ids: List[str], entity_type: str, start_date: str, end_date: str) -> List[Dict]:
        """Get pre-computed daily metrics for specific entity IDs"""
        try:
            import sqlite3
            from utils.database_utils import get_database_connection
            
            with get_database_connection('mixpanel_data') as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                placeholders = ','.join(['?' for _ in entity_ids])
                query = f"""
                SELECT 
                    d.entity_id,
                    n.canonical_name as name,
                    d.entity_type,
                    d.date,
                    d.trial_users_count,
                    d.meta_trial_count,
                    d.purchase_users_count, 
                    d.meta_purchase_count,
                    d.trial_conversion_rate_actual,
                    d.trial_conversion_rate_estimated,
                    d.trial_refund_rate_estimated,
                    d.purchase_refund_rate_estimated,
                    d.meta_spend,
                    d.estimated_revenue_usd,
                    d.adjusted_estimated_revenue_usd,
                    d.actual_revenue_usd,
                    d.net_actual_revenue_usd,
                    d.profit_usd,
                    d.trial_user_ids,
                    d.converted_user_ids,
                    d.post_trial_user_ids,
                    d.trial_refund_user_ids,
                    d.purchase_refund_user_ids,
                    d.purchase_user_ids
                    
                FROM daily_mixpanel_metrics d
                LEFT JOIN id_name_mapping n ON d.entity_id = n.entity_id
                WHERE d.entity_id IN ({placeholders})
                  AND d.entity_type = ?
                  AND d.date BETWEEN ? AND ?
                ORDER BY d.entity_id, d.date
                """
                
                params = [*entity_ids, entity_type, start_date, end_date]
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting precomputed daily metrics: {e}")
            return []

    def get_earliest_meta_date(self) -> str:
        """
        Get the earliest date available in meta analytics database with DYNAMIC fallbacks.
        NO hardcoded dates - uses actual data or calculates reasonable fallback.
        """
        try:
            # First try to get from Meta database
            if self.meta_db_path:
                with sqlite3.connect(self.meta_db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    # Query all performance tables to find the absolute earliest date
                    tables = ['ad_performance_daily', 'ad_performance_daily_country', 
                             'ad_performance_daily_region', 'ad_performance_daily_device']
                    earliest_dates = []
                    
                    for table in tables:
                        try:
                            cursor.execute(f"SELECT MIN(date) as min_date FROM {table}")
                            result = cursor.fetchone()
                            if result and result['min_date']:
                                earliest_dates.append(result['min_date'])
                        except sqlite3.OperationalError:
                            # Table might not exist, continue
                            continue
                    
                    if earliest_dates:
                        return min(earliest_dates)
            
            # Fallback: Try Mixpanel data
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("SELECT MIN(date) as min_date FROM daily_mixpanel_metrics")
                result = cursor.fetchone()
                if result and result['min_date']:
                    logger.info(f"Using earliest Mixpanel date as fallback: {result['min_date']}")
                    return result['min_date']
            
            # Final fallback: Dynamic calculation (90 days ago)
            fallback_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            logger.warning(f"No data found in any database, using dynamic fallback: {fallback_date}")
            return fallback_date
                
        except Exception as e:
            logger.error(f"Error getting earliest meta date: {e}", exc_info=True)
            # Dynamic fallback - 90 days ago from today
            fallback_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            logger.warning(f"Exception occurred, using dynamic fallback: {fallback_date}")
            return fallback_date

    def get_available_date_range(self) -> Dict[str, Any]:
        """
        Get available date range from analytics data with DYNAMIC fallbacks.
        NO hardcoded dates - uses actual data or calculates reasonable defaults.
        """
        try:
            logger.info("Getting available date range for dashboard data")
            earliest_date = self.get_earliest_meta_date()
            latest_date = now_in_timezone().strftime('%Y-%m-%d')
            
            return {
                'success': True,
                'data': {
                    'earliest_date': earliest_date,
                    'latest_date': latest_date
                },
                'timestamp': now_in_timezone().isoformat(),
                'source': 'dashboard_refresh_service'
            }
        except Exception as e:
            logger.error(f"Error getting available date range: {e}", exc_info=True)
            # Dynamic fallback calculation
            today = now_in_timezone().strftime('%Y-%m-%d')
            fallback_start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            
            return {
                'success': False,
                'error': str(e),
                'data': {
                    'earliest_date': fallback_start,
                    'latest_date': today
                },
                'fallback': True,
                'source': 'dashboard_refresh_service_fallback'
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

    def get_table_name(self, breakdown: str) -> str:
        """Get the appropriate table name based on breakdown parameter"""
        return self.table_mapping.get(breakdown, 'ad_performance_daily')

    def execute_analytics_query_optimized(self, config: QueryConfig) -> Dict[str, Any]:
        """
        DEPRECATED: This method has been replaced by execute_optimized_dashboard_refresh().
        Redirects to the new method to maintain compatibility.
        """
        logger.warning("DEPRECATED: execute_analytics_query_optimized() called. Use execute_optimized_dashboard_refresh() instead!")
        return self.execute_optimized_dashboard_refresh(config)

    def get_overview_roas_chart_data(self, start_date: str, end_date: str, breakdown: str = 'all', entity_type: str = 'campaign') -> Dict[str, Any]:
        """
        ⚠️ DEPRECATED: This method has been moved to OverviewService.
        Use OverviewService.get_overview_roas_chart_data() instead.
        """
        logger.warning("⚠️ DEPRECATED: get_overview_roas_chart_data() called from DashboardRefreshService. Use OverviewService instead.")
        
        # Redirect to overview service
        overview_service = OverviewService(
            meta_db_path=self.meta_db_path,
            mixpanel_db_path=self.mixpanel_db_path,
            mixpanel_analytics_db_path=self.mixpanel_analytics_db_path
        )
        return overview_service.get_overview_roas_chart_data(start_date, end_date, breakdown, entity_type)

    def get_dashboard_stats(self, start_date: str, end_date: str, breakdown: str = 'all', entity_type: str = 'campaign') -> Dict[str, Any]:
        """
        ⚠️ DEPRECATED: This method has been moved to OverviewService.
        Use OverviewService.get_dashboard_stats() instead.
        """
        logger.warning("⚠️ DEPRECATED: get_dashboard_stats() called from DashboardRefreshService. Use OverviewService instead.")
        
        # Redirect to overview service
        overview_service = OverviewService(
            meta_db_path=self.meta_db_path,
            mixpanel_db_path=self.mixpanel_db_path,
            mixpanel_analytics_db_path=self.mixpanel_analytics_db_path
        )
        return overview_service.get_dashboard_stats(start_date, end_date, breakdown, entity_type)


