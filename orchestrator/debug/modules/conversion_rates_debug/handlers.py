# Conversion Rates Debug Handlers
# This module contains handlers for debugging conversion rate calculations

import sqlite3
import os
import logging
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

logger = logging.getLogger(__name__)

# Constants from the conversion rates pipeline
MIN_COHORT_SIZE = 12
PROPERTY_HIERARCHY = ['product_id', 'price_bucket', 'store', 'economic_tier', 'country', 'region']
ACCURACY_LEVELS = ['very_high', 'high', 'medium', 'low', 'default']
DEFAULT_RATES = {
    'trial_conversion_rate': 0.25,
    'trial_converted_to_refund_rate': 0.20,
    'initial_purchase_to_refund_rate': 0.40
}



def get_database_connection():
    """Get database connection using the database_utils function"""
    try:
        db_path = get_database_path('mixpanel_data')
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def handle_get_overview_data(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get overview data for conversion rates debug including statistics and validation
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get all user-product records with their conversion rates - CORRECT SCHEMA ACCESS
        cursor.execute("""
            SELECT 
                upm.user_product_id,
                upm.distinct_id,
                upm.product_id,
                upm.credited_date,
                upm.price_bucket,
                upm.store,
                upm.trial_conversion_rate,
                upm.trial_converted_to_refund_rate,
                upm.initial_purchase_to_refund_rate,
                upm.accuracy_score,
                u.economic_tier,
                u.country,
                u.region
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE upm.valid_lifecycle = TRUE AND u.valid_user = TRUE
            ORDER BY upm.product_id, upm.price_bucket, upm.store, u.economic_tier, u.country, u.region
        """)
        
        user_data = cursor.fetchall()
        conn.close()
        
        if not user_data:
            return {
                'success': True,
                'data': {
                    'statistics': {
                        'total_users': 0,
                        'total_segments': 0,
                        'valid_segments': 0,
                        'invalid_segments': 0,
                        'default_rate_segments': 0,
                        'accuracy_breakdown': {level: 0 for level in ACCURACY_LEVELS}
                    },
                    'hierarchy_tree': [],
                    'validation_errors': []
                }
            }
        
        # Build hierarchical tree and validate consistency
        hierarchy_tree, statistics, validation_errors = build_hierarchical_tree(user_data)
        
        return {
            'success': True,
            'data': {
                'statistics': statistics,
                'hierarchy_tree': hierarchy_tree,
                'validation_errors': validation_errors
            }
        }
        
    except Exception as e:
        logger.error(f"Error in conversion rates overview: {e}")
        return {'success': False, 'error': str(e)}

def infer_store_from_product_id(product_id: str) -> str:
    """
    Infer store information from product ID patterns when store field is empty
    """
    if not product_id:
        return 'unknown_store'
    
    product_lower = product_id.lower()
    
    # Check for common patterns that might indicate store type
    # Check for colon format first (Google Play) before other patterns
    if ':' in product_id:
        return 'google_play'  # Colon format often indicates Google Play
    elif product_lower.startswith('prod_'):
        return 'app_store'  # RevenueCat iOS products often start with prod_
    elif 'gluten' in product_lower and ('yearly' in product_lower or 'monthly' in product_lower):
        return 'app_store'  # Gluten free apps likely iOS
    elif product_lower.startswith('atly'):
        return 'app_store'  # Atly products
    elif '.' in product_id and len(product_id.split('.')) >= 3:
        return 'app_store'  # Bundle ID format (com.company.app)
    else:
        return 'unknown_store'

def build_hierarchical_tree(user_data: List[sqlite3.Row]) -> Tuple[List[Dict], Dict, List[Dict]]:
    """
    Build a clean hierarchical tree structure by adding users one by one and checking for rate consistency
    """
    # Define cohort date windows (matching the conversion rate processor logic)
    cohort_start_date = (datetime.now() - timedelta(days=53)).strftime('%Y-%m-%d')
    cohort_end_date = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    
    # Initialize tree structure and statistics
    tree_root = {}
    statistics = {
        'total_users': len(user_data),
        'total_segments': 0,
        'valid_segments': 0,
        'invalid_segments': 0,
        'default_rate_segments': 0,
        'accuracy_breakdown': {level: 0 for level in ACCURACY_LEVELS},
        'cohort_window_info': {
            'start_date': cohort_start_date,
            'end_date': cohort_end_date,
            'total_cohort_users': 0
        }
    }
    validation_errors = []
    
    # Process each user individually and add to tree
    for user in user_data:
        # Handle both dict and sqlite3.Row objects
        if hasattr(user, 'keys'):  # sqlite3.Row
            user_dict = dict(user)
        else:  # already a dict
            user_dict = user
            
        # Count this user in the accuracy breakdown (count users, not segments)
        accuracy = user_dict.get('accuracy_score')
        if accuracy in statistics['accuracy_breakdown']:
            statistics['accuracy_breakdown'][accuracy] += 1
            
        # Check if this user is in the cohort window
        credited_date = user_dict.get('credited_date', '')
        is_in_cohort_window = False
        if credited_date:  # Make sure credited_date is not empty/None
            is_in_cohort_window = cohort_start_date <= credited_date <= cohort_end_date
        
        if is_in_cohort_window:
            statistics['cohort_window_info']['total_cohort_users'] += 1
            
        # Determine store value - use actual store field or infer from product_id
        store_value = user_dict.get('store')
        if not store_value:  # If store is None or empty
            store_value = infer_store_from_product_id(user_dict.get('product_id', ''))
            
        # Create the full property path for this user
        path = [
            user_dict['product_id'],
            f"${user_dict['price_bucket']:.2f}" if user_dict['price_bucket'] is not None and user_dict['price_bucket'] > 0 else "$0.00",
            store_value,
            user_dict['economic_tier'] or 'unknown_tier',
            user_dict['country'] or 'unknown_country',
            user_dict['region'] or 'unknown_region'
        ]
        
        # Get user's conversion rates
        user_rates = {
            'trial_conversion_rate': user_dict['trial_conversion_rate'],
            'trial_converted_to_refund_rate': user_dict['trial_converted_to_refund_rate'],
            'initial_purchase_to_refund_rate': user_dict['initial_purchase_to_refund_rate'],
            'accuracy_score': user_dict['accuracy_score']
        }
        
        # Navigate/create tree path for this user
        current_node = tree_root
        for level, value in enumerate(path):
            if value not in current_node:
                # Create new node
                current_node[value] = {
                    'children': {},
                    'level': level,
                    'value': value,
                    'user_count': 0,
                    'cohort_user_count': 0,  # NEW: Count users in cohort window
                    'rates': None,
                    'is_viable': False,
                    'uses_default': False,
                    'rates_consistent': True,
                    'users': [],
                    'rate_errors': []
                }
            
            # Increment user count for this node
            current_node[value]['user_count'] += 1
            
            # Increment cohort user count if this user is in the cohort window
            if is_in_cohort_window:
                current_node[value]['cohort_user_count'] += 1
            
            # If this is the leaf node (region level), handle rate consistency
            if level == len(path) - 1:
                leaf_node = current_node[value]
                
                # Check if this is the first user at this leaf
                if leaf_node['rates'] is None:
                    # First user - set the rates
                    leaf_node['rates'] = user_rates.copy()
                    leaf_node['uses_default'] = user_rates['accuracy_score'] == 'default'
                else:
                    # Check if rates match existing rates
                    existing_rates = leaf_node['rates']
                    if user_rates != existing_rates:
                        # Rate inconsistency detected!
                        leaf_node['rates_consistent'] = False
                        leaf_node['rate_errors'].append({
                            'user_id': user_dict['distinct_id'],
                            'expected_rates': existing_rates,
                            'actual_rates': user_rates
                        })
                        
                        # Log validation error
                        error_path = ' → '.join(path)
                        validation_errors.append({
                            'path': error_path,
                            'error_type': 'inconsistent_rates',
                            'message': f'User {user_dict["distinct_id"]} has different conversion rates than other users in this segment',
                            'user_count': leaf_node['user_count'],
                            'expected_rates': existing_rates,
                            'actual_rates': user_rates
                        })
                
                # Add user to the leaf node's user list
                leaf_node['users'].append(user_dict)
                
                # Update viability based on COHORT USER COUNT (this matches the rate calculation logic)
                leaf_node['is_viable'] = leaf_node['cohort_user_count'] >= MIN_COHORT_SIZE
                
                # Note: Segment statistics will be calculated after tree is complete
            
            current_node = current_node[value]['children']
    
    # Calculate segment statistics AFTER building the complete tree
    # Count actual leaf nodes and their validity + accuracy breakdown
    def count_segments(node_dict, level=0):
        total = 0
        valid = 0
        invalid = 0
        default_segments = 0
        segment_accuracy_breakdown = {level: 0 for level in ACCURACY_LEVELS}
        
        for value, node_data in node_dict.items():
            if len(node_data['children']) == 0:  # Leaf node = segment
                total += 1
                if node_data['rates_consistent']:
                    valid += 1
                else:
                    invalid += 1
                    
                if node_data.get('uses_default', False):
                    default_segments += 1
                
                # Determine segment accuracy from the users in this segment
                if node_data['users'] and len(node_data['users']) > 0:
                    # All users in a segment should have the same accuracy (since they have consistent rates)
                    segment_accuracy = node_data['users'][0].get('accuracy_score', 'default')
                    if segment_accuracy in segment_accuracy_breakdown:
                        segment_accuracy_breakdown[segment_accuracy] += 1
            
            # Recursively count children
            child_total, child_valid, child_invalid, child_default, child_accuracy = count_segments(node_data['children'], level + 1)
            total += child_total
            valid += child_valid
            invalid += child_invalid
            default_segments += child_default
            
            # Merge child accuracy breakdown
            for acc_level in ACCURACY_LEVELS:
                segment_accuracy_breakdown[acc_level] += child_accuracy[acc_level]
        
        return total, valid, invalid, default_segments, segment_accuracy_breakdown
    
    # Get the actual counts
    total_segments, valid_segments, invalid_segments, default_segments, segment_accuracy_breakdown = count_segments(tree_root)
    
    # Update statistics with correct values
    statistics['total_segments'] = total_segments
    statistics['valid_segments'] = valid_segments
    statistics['invalid_segments'] = invalid_segments
    statistics['default_rate_segments'] = default_segments
    statistics['segment_accuracy_breakdown'] = segment_accuracy_breakdown
    
    # Convert tree structure to list format for frontend
    hierarchy_tree = convert_tree_to_list(tree_root)
    
    return hierarchy_tree, statistics, validation_errors

def convert_tree_to_list(tree_node: Dict, level: int = 0, parent_path: str = "") -> List[Dict]:
    """
    Convert the tree structure to a flat list with hierarchy information
    """
    result = []
    
    for value, node_data in tree_node.items():
        current_path = f"{parent_path} → {value}" if parent_path else value
        
        # Create the node entry
        node_entry = {
            'level': level,
            'value': value,
            'path': current_path,
            'user_count': node_data['user_count'],
            'cohort_user_count': node_data.get('cohort_user_count', 0),  # NEW: Cohort window user count
            'rates': node_data['rates'],
            'is_viable': node_data.get('is_viable', False),
            'uses_default': node_data.get('uses_default', False),
            'rates_consistent': node_data.get('rates_consistent', True),
            'users': node_data.get('users', []),
            'has_children': len(node_data['children']) > 0,
            'is_leaf': len(node_data['children']) == 0,
            'rate_errors': node_data.get('rate_errors', [])
        }
        
        result.append(node_entry)
        
        # Add children recursively
        if node_data['children']:
            children = convert_tree_to_list(node_data['children'], level + 1, current_path)
            result.extend(children)
    
    return result

def validate_segment_rates(users: List[Dict]) -> Tuple[bool, Dict[str, Any], int]:
    """
    Validate that all users in a segment have consistent conversion rates
    """
    if not users:
        return True, {}, 0
    
    user_count = len(users)
    
    # Get the first user's rates as reference
    reference_rates = {
        'trial_conversion_rate': users[0]['trial_conversion_rate'],
        'trial_converted_to_refund_rate': users[0]['trial_converted_to_refund_rate'],
        'initial_purchase_to_refund_rate': users[0]['initial_purchase_to_refund_rate'],
        'accuracy_score': users[0]['accuracy_score']
    }
    
    # Check if all users have the same rates
    rates_consistent = True
    rate_variants = []
    
    for user in users:
        user_rates = {
            'trial_conversion_rate': user['trial_conversion_rate'],
            'trial_converted_to_refund_rate': user['trial_converted_to_refund_rate'],
            'initial_purchase_to_refund_rate': user['initial_purchase_to_refund_rate'],
            'accuracy_score': user['accuracy_score']
        }
        
        if user_rates != reference_rates:
            rates_consistent = False
            if user_rates not in rate_variants:
                rate_variants.append(user_rates)
    
    # If inconsistent, include all variants
    if not rates_consistent and reference_rates not in rate_variants:
        rate_variants.insert(0, reference_rates)
    
    return rates_consistent, reference_rates if rates_consistent else rate_variants, user_count

def handle_get_cohort_tree(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get detailed cohort tree data with filtering options
    """
    try:
        filters = request_data.get('filters', {})
        
        # Get the overview data first
        overview_result = handle_get_overview_data({})
        if not overview_result['success']:
            return overview_result
        
        hierarchy_tree = overview_result['data']['hierarchy_tree']
        
        # Apply filters if provided
        if filters:
            hierarchy_tree = apply_tree_filters(hierarchy_tree, filters)
        
        return {
            'success': True,
            'data': {
                'hierarchy_tree': hierarchy_tree,
                'filters_applied': filters
            }
        }
        
    except Exception as e:
        logger.error(f"Error in cohort tree: {e}")
        return {'success': False, 'error': str(e)}

def apply_tree_filters(hierarchy_tree: List[Dict], filters: Dict[str, Any]) -> List[Dict]:
    """
    Apply filters to the hierarchical tree data
    """
    filtered_tree = []
    
    for node in hierarchy_tree:
        # Apply filters
        if filters.get('product_id') and filters['product_id'] not in node['value']:
            continue
        
        if filters.get('min_users') and node['user_count'] < int(filters['min_users']):
            continue
            
        if filters.get('max_users') and node['user_count'] > int(filters['max_users']):
            continue
        
        if filters.get('show_errors_only') and node['rates_consistent']:
            continue
            
        if filters.get('show_viable_only') and not node['is_viable'] and node['is_leaf']:
            continue
        
        filtered_tree.append(node)
    
    return filtered_tree

def handle_validate_data(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate conversion rate data consistency and return detailed validation report
    """
    try:
        # Get overview data which includes validation
        overview_result = handle_get_overview_data({})
        if not overview_result['success']:
            return overview_result
        
        validation_errors = overview_result['data']['validation_errors']
        statistics = overview_result['data']['statistics']
        
        # Create detailed validation report
        validation_report = {
            'total_errors': len(validation_errors),
            'error_types': {},
            'consistency_score': 0.0,
            'errors': validation_errors
        }
        
        # Analyze errors
        for error in validation_errors:
            error_type = error['error_type']
            validation_report['error_types'][error_type] = validation_report['error_types'].get(error_type, 0) + 1
        
        # Calculate consistency score
        if statistics['total_segments'] > 0:
            validation_report['consistency_score'] = (statistics['valid_segments'] / statistics['total_segments']) * 100
        
        return {
            'success': True,
            'data': {
                'validation_report': validation_report,
                'statistics': statistics
            }
        }
        
    except Exception as e:
        logger.error(f"Error in data validation: {e}")
        return {'success': False, 'error': str(e)}