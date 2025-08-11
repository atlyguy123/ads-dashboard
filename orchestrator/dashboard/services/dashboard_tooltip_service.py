"""
Dashboard Tooltip Service

This service handles user statistics and detailed information for dashboard tooltips and modals.
Separated from the main dashboard refresh service to maintain clean organization.
"""

import sqlite3
import logging
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

# Add the project root to the Python path for database utilities
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.database_utils import get_database_path, get_database_connection

# Import timezone utilities for consistent timezone handling
from ...utils.timezone_utils import now_in_timezone

logger = logging.getLogger(__name__)


class DashboardTooltipService:
    """Service for providing detailed user statistics for dashboard tooltips and modals"""
    
    def __init__(self):
        self.mixpanel_db_path = get_database_path('mixpanel_data')
        self.meta_db_path = get_database_path('meta_analytics')
        logger.info(f"🔧 Dashboard Tooltip Service initialized with databases: {self.mixpanel_db_path}, {self.meta_db_path}")


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
            if metric_type == 'trial_conversion_rate':
                user_list_column = 'trial_user_ids'
                is_trial_metric = True
                logger.info(f"📊 Using trial users for metric type: {metric_type}")
            elif metric_type == 'avg_trial_refund_rate':
                user_list_column = 'converted_user_ids'
                is_trial_metric = True
                logger.info(f"📊 Using converted users for trial refund rate metric: {metric_type}")
            elif metric_type == 'purchase_refund_rate':
                user_list_column = 'purchase_user_ids'
                is_trial_metric = False
                logger.info(f"📊 Using purchase users for metric type: {metric_type}")
            else:
                # Default to trial users for unknown metric types
                user_list_column = 'trial_user_ids'
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
                        'debug_info': {
                            'entity_type': entity_type,
                            'entity_id': actual_entity_id,
                            'date_range': f"{start_date} to {end_date}",
                            'user_list_column': user_list_column,
                            'daily_records_found': len(daily_records),
                            'is_trial_metric': is_trial_metric
                        }
                    }
                    return empty_result
                
                # STEP 3: Build query to get all user data
                logger.info(f"🔍 Fetching detailed user stats for {len(all_distinct_ids)} distinct IDs...")
                
                # Build placeholders for the IN clause
                placeholders = ','.join(['?' for _ in all_distinct_ids])
                
                # Query to get comprehensive user stats from user_product_metrics
                user_stats_query = f"""
                SELECT 
                    distinct_id,
                    
                    -- Personal info
                    country as user_country,
                    device as user_device,
                    region as user_region,
                    price_bucket,
                    
                    -- Product info
                    product_id,
                    credited_date,
                    current_status,
                    current_value,
                    value_status,
                    
                    -- Conversion rates
                    trial_conversion_rate,
                    trial_converted_to_refund_rate,
                    initial_purchase_to_refund_rate,
                    
                    -- Status indicators
                    CASE WHEN current_status LIKE '%trial%' THEN 1 ELSE 0 END as has_trial,
                    CASE WHEN current_status LIKE '%purchase%' OR current_value > 0 THEN 1 ELSE 0 END as has_purchase,
                    CASE WHEN current_status LIKE '%refund%' THEN 1 ELSE 0 END as is_trial_refund,
                    CASE WHEN value_status LIKE '%refund%' THEN 1 ELSE 0 END as is_purchase_refund,
                    
                    -- Metrics
                    accuracy_score,
                    assignment_type,
                    last_updated_ts
                    
                FROM user_product_metrics 
                WHERE distinct_id IN ({placeholders})
                """
                
                cursor.execute(user_stats_query, list(all_distinct_ids))
                raw_user_records = [dict(row) for row in cursor.fetchall()]
                
                # CRITICAL FIX: Deduplicate users (user_product_metrics can have multiple records per user)
                seen_users = {}
                user_records = []
                for record in raw_user_records:
                    user_id = record['distinct_id']
                    if user_id not in seen_users:
                        seen_users[user_id] = True
                        user_records.append(record)
                
                logger.info(f"📊 Retrieved {len(raw_user_records)} records, deduplicated to {len(user_records)} unique users")
                
                # STEP 4: Build unified response with DUAL modes (estimated vs actual)
                def calculate_summary_stats(users_list):
                    if users_list:
                        trial_conversion_rates = [r['trial_conversion_rate'] for r in users_list if r['trial_conversion_rate'] is not None]
                        trial_refund_rates = [r['trial_converted_to_refund_rate'] for r in users_list if r['trial_converted_to_refund_rate'] is not None]
                        purchase_refund_rates = [r['initial_purchase_to_refund_rate'] for r in users_list if r['initial_purchase_to_refund_rate'] is not None]
                        
                        return {
                            'total_users': len(users_list),
                            'avg_trial_conversion_rate': sum(trial_conversion_rates) / len(trial_conversion_rates) if trial_conversion_rates else 0,
                            'avg_trial_refund_rate': sum(trial_refund_rates) / len(trial_refund_rates) if trial_refund_rates else 0,
                            'avg_purchase_refund_rate': sum(purchase_refund_rates) / len(purchase_refund_rates) if purchase_refund_rates else 0,
                            'total_estimated_revenue': sum(float(r.get('current_value') or 0) for r in users_list),
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
                
                # ESTIMATED mode: All users from daily_mixpanel_metrics (current behavior)
                estimated_users = user_records  # All users from the list
                estimated_summary = calculate_summary_stats(estimated_users)
                
                # ACTUAL mode: Only users who have actually converted or have meaningful status
                if is_trial_metric:
                    # Trial metrics: filter for users who have trial status or current value
                    actual_users = []
                    converted_count = 0
                    total_eligible = len(user_records)  # All users are eligible for analysis
                    
                    for user in user_records:
                        # Include users who have trial activity or current value
                        if user.get('has_trial') or (user.get('current_value') and user['current_value'] > 0):
                            actual_users.append(user)
                            if user.get('current_value') and user['current_value'] > 0:
                                converted_count += 1
                    
                    # Calculate actual conversion rate based on eligible population
                    actual_conversion_rate = (converted_count / total_eligible * 100) if total_eligible > 0 else 0
                    
                    # Get average refund rate among users with refund data
                    users_with_refund_data = [u for u in actual_users if u.get('trial_converted_to_refund_rate') is not None]
                    avg_trial_refund = sum(u['trial_converted_to_refund_rate'] for u in users_with_refund_data) / len(users_with_refund_data) if users_with_refund_data else 0
                    
                    actual_summary = {
                        'total_users': total_eligible,  # Show all users
                        'converted_users': converted_count,  # Track how many actually converted  
                        'avg_trial_conversion_rate': actual_conversion_rate,
                        'avg_trial_refund_rate': avg_trial_refund * 100,  # Convert to percentage
                        'avg_purchase_refund_rate': 0,  # Not applicable for trial metrics
                        'total_estimated_revenue': sum(float(u.get('current_value') or 0) for u in actual_users),
                        'breakdown_applied': breakdown,
                        'breakdown_value': breakdown_value
                    }
                    
                else:
                    # Purchase metrics: filter for users who made purchases
                    actual_users = []
                    converted_count = 0
                    total_eligible = len(user_records)
                    
                    for user in user_records:
                        # Include users who have purchase status or current value
                        if user.get('has_purchase') or (user.get('current_value') and user['current_value'] > 0):
                            actual_users.append(user)
                            if user.get('has_purchase'):
                                converted_count += 1
                    
                    # Calculate actual purchase refund rate
                    users_with_refund_data = [u for u in actual_users if u.get('initial_purchase_to_refund_rate') is not None]
                    avg_purchase_refund = sum(u['initial_purchase_to_refund_rate'] for u in users_with_refund_data) / len(users_with_refund_data) if users_with_refund_data else 0
                    
                    actual_summary = {
                        'total_users': total_eligible,
                        'converted_users': converted_count,
                        'avg_trial_conversion_rate': 0,  # Not applicable for purchase metrics
                        'avg_trial_refund_rate': 0,  # Not applicable for purchase metrics
                        'avg_purchase_refund_rate': avg_purchase_refund * 100,  # Convert to percentage
                        'total_estimated_revenue': sum(float(u.get('current_value') or 0) for u in actual_users),
                        'breakdown_applied': breakdown,
                        'breakdown_value': breakdown_value
                    }
                
                # Apply breakdown filtering if specified
                if breakdown != 'all' and breakdown_value:
                    breakdown_field_map = {
                        'country': 'user_country',
                        'device': 'user_device',
                        'region': 'user_region'
                    }
                    
                    if breakdown in breakdown_field_map:
                        field_name = breakdown_field_map[breakdown]
                        logger.info(f"🔍 Applying {breakdown} breakdown filter: {breakdown_value}")
                        
                        # Filter both estimated and actual users
                        estimated_users = [u for u in estimated_users if u.get(field_name) == breakdown_value]
                        actual_users = [u for u in actual_users if u.get(field_name) == breakdown_value]
                        
                        # Recalculate summaries with filtered data
                        estimated_summary = calculate_summary_stats(estimated_users)
                        # Note: actual_summary percentages remain the same, just filter the user list
                        actual_summary['total_users'] = len(actual_users)
                        actual_summary['total_estimated_revenue'] = sum(u['current_value_usd'] or 0 for u in actual_users)
                        
                        logger.info(f"📊 After {breakdown} filtering: {len(estimated_users)} estimated, {len(actual_users)} actual users")
                
                result = {
                    'success': True,
                    'estimated': {
                        'summary': estimated_summary,
                        'users': estimated_users
                    },
                    'actual': {
                        'summary': actual_summary,
                        'users': actual_users
                    },
                    'debug_info': {
                        'entity_type': entity_type,
                        'entity_id': actual_entity_id,
                        'date_range': f"{start_date} to {end_date}",
                        'user_list_column': user_list_column,
                        'distinct_ids_found': len(all_distinct_ids),
                        'user_records_retrieved': len(user_records),
                        'is_trial_metric': is_trial_metric,
                        'breakdown_applied': breakdown != 'all',
                        'breakdown_value': breakdown_value,
                        'estimated_users_count': len(estimated_users),
                        'actual_users_count': len(actual_users)
                    }
                }
                
                logger.info(f"✅ Tooltip data ready: {estimated_summary['total_users']} estimated, {actual_summary['total_users']} actual users")
                return result
                
        except Exception as e:
            logger.error(f"Error getting user details for tooltip: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
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
                'debug_info': {
                    'entity_type': entity_type,
                    'entity_id': entity_id,
                    'error': str(e),
                    'generated_at': now_in_timezone().isoformat()
                }
            }
