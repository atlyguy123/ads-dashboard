import sqlite3
import json
import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add utils directory to path for database utilities
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "utils"))
from database_utils import get_database_path

logger = logging.getLogger(__name__)

# Database path - use dynamic discovery
def get_db_path():
    return get_database_path('mixpanel_data')

def handle_load_overview(data: Dict[str, Any]) -> Dict[str, Any]:
    """Load overview statistics from the database"""
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        
        # Get basic stats
        overview_stats = _get_overview_stats(conn)
        
        # Get accuracy distribution  
        accuracy_distribution = _get_accuracy_distribution(conn)
        
        conn.close()
        
        return {
            "overview_stats": overview_stats,
            "accuracy_distribution": accuracy_distribution
        }
        
    except Exception as e:
        logger.error(f"Error loading overview: {str(e)}")
        raise Exception(f"Database query failed: {str(e)}")

def handle_load_cohort_tree(data: Dict[str, Any]) -> Dict[str, Any]:
    """Load hierarchical cohort tree from the database"""
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        
        # Build hierarchical tree structure
        cohort_tree = _build_cohort_hierarchy(conn)
        
        conn.close()
        
        return {
            "cohort_tree": cohort_tree
        }
        
    except Exception as e:
        logger.error(f"Error loading cohort tree: {str(e)}")
        raise Exception(f"Database query failed: {str(e)}")

def handle_expand_all_cohorts(data: Dict[str, Any]) -> Dict[str, Any]:
    """Client-side action, no server processing needed"""
    return {"message": "Expanding all cohorts"}

def handle_collapse_all_cohorts(data: Dict[str, Any]) -> Dict[str, Any]:
    """Client-side action, no server processing needed"""
    return {"message": "Collapsing all cohorts"}

def _get_overview_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Get high-level statistics from user_product_metrics"""
    cursor = conn.cursor()
    
    # Total records
    cursor.execute("""
        SELECT COUNT(*) as total_records
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        AND upm.valid_lifecycle = TRUE
    """)
    total_records = cursor.fetchone()['total_records']
    
    # Average conversion rates (excluding NULL values)
    cursor.execute("""
        SELECT 
            AVG(trial_conversion_rate) as avg_trial_conversion,
            AVG(trial_converted_to_refund_rate) as avg_trial_refund,
            AVG(initial_purchase_to_refund_rate) as avg_purchase_refund
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        AND upm.valid_lifecycle = TRUE
        AND trial_conversion_rate IS NOT NULL
        AND trial_converted_to_refund_rate IS NOT NULL
        AND initial_purchase_to_refund_rate IS NOT NULL
    """)
    rates = cursor.fetchone()
    
    # Count of default vs calculated rates
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN accuracy_score = 'default' THEN 1 ELSE 0 END) as default_accuracy_count,
            SUM(CASE WHEN accuracy_score != 'default' OR accuracy_score IS NULL THEN 1 ELSE 0 END) as calculated_rates_count
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        AND upm.valid_lifecycle = TRUE
    """)
    accuracy_counts = cursor.fetchone()
    
    return {
        "total_records": total_records,
        "avg_trial_conversion": rates['avg_trial_conversion'] or 0,
        "avg_trial_refund": rates['avg_trial_refund'] or 0,
        "avg_purchase_refund": rates['avg_purchase_refund'] or 0,
        "default_accuracy_count": accuracy_counts['default_accuracy_count'] or 0,
        "calculated_rates_count": accuracy_counts['calculated_rates_count'] or 0
    }

def _get_accuracy_distribution(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Get distribution of accuracy scores"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COALESCE(accuracy_score, 'unknown') as accuracy_score,
            COUNT(*) as count
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        AND upm.valid_lifecycle = TRUE
        GROUP BY accuracy_score
        ORDER BY count DESC
    """)
    
    return [dict(row) for row in cursor.fetchall()]

def _build_cohort_hierarchy(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Build hierarchical cohort tree following property removal order"""
    
    # Property hierarchy order (matches the logic in 02_assign_conversion_rates.py)
    # 1. product_id (always present)
    # 2. store  
    # 3. price_bucket
    # 4. economic_tier
    # 5. country
    # 6. region (removed first in property removal)
    
    hierarchy_levels = [
        ("product_id", "Product ID"),
        ("store", "Store"),
        ("price_bucket", "Price Bucket"), 
        ("economic_tier", "Economic Tier"),
        ("country", "Country"),
        ("region", "Region")
    ]
    
    # Get all cohort data
    cohort_data = _get_all_cohort_data(conn)
    
    # Build tree recursively
    tree = _build_tree_level(cohort_data, hierarchy_levels, 0, {})
    
    return tree

def _get_all_cohort_data(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Get all user-product records with their properties and rates"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            upm.product_id,
            upm.store,
            upm.price_bucket,
            u.economic_tier,
            upm.country,
            upm.region,
            upm.accuracy_score,
            upm.trial_conversion_rate,
            upm.trial_converted_to_refund_rate,
            upm.initial_purchase_to_refund_rate,
            COUNT(*) as record_count
        FROM user_product_metrics upm
        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
        WHERE u.valid_user = TRUE
        AND upm.valid_lifecycle = TRUE
        GROUP BY 
            upm.product_id,
            upm.store,
            upm.price_bucket,
            u.economic_tier,
            upm.country,
            upm.region,
            upm.accuracy_score,
            upm.trial_conversion_rate,
            upm.trial_converted_to_refund_rate,
            upm.initial_purchase_to_refund_rate
        ORDER BY upm.product_id, upm.store, upm.price_bucket
    """)
    
    return [dict(row) for row in cursor.fetchall()]

def _build_tree_level(data: List[Dict], hierarchy_levels: List[tuple], level_index: int, current_filter: Dict) -> List[Dict]:
    """Recursively build tree level"""
    if level_index >= len(hierarchy_levels):
        return []
    
    property_key, property_name = hierarchy_levels[level_index]
    
    # Group data by current property
    grouped_data = defaultdict(list)
    
    for record in data:
        # Check if record matches current filter
        if _matches_filter(record, current_filter):
            property_value = record.get(property_key)
            # Convert None to string for consistent grouping
            if property_value is None:
                property_value = "NULL"
            grouped_data[property_value].append(record)
    
    # Build nodes for this level
    nodes = []
    node_id = 0
    
    for property_value, group_records in grouped_data.items():
        node_id += 1
        
        # Calculate aggregate stats for this group
        cohort_size = sum(record['record_count'] for record in group_records)
        
        # Calculate average rates (weighted by record count)
        total_records = sum(record['record_count'] for record in group_records)
        avg_trial_conversion = _weighted_average(group_records, 'trial_conversion_rate', 'record_count') if total_records > 0 else 0
        avg_trial_refund = _weighted_average(group_records, 'trial_converted_to_refund_rate', 'record_count') if total_records > 0 else 0
        
        # Find most common accuracy
        accuracy_counts = defaultdict(int)
        for record in group_records:
            accuracy = record.get('accuracy_score') or 'unknown'
            accuracy_counts[accuracy] += record['record_count']
        most_common_accuracy = max(accuracy_counts.items(), key=lambda x: x[1])[0] if accuracy_counts else 'unknown'
        
        # Create node
        node = {
            "id": f"level_{level_index}_node_{node_id}",
            "property_name": property_name,
            "property_value": property_value,
            "cohort_size": cohort_size,
            "avg_trial_conversion": avg_trial_conversion,
            "avg_trial_refund": avg_trial_refund,
            "most_common_accuracy": most_common_accuracy,
            "children": []
        }
        
        # Build children for next level
        if level_index < len(hierarchy_levels) - 1:
            next_filter = current_filter.copy()
            next_filter[property_key] = property_value
            node["children"] = _build_tree_level(data, hierarchy_levels, level_index + 1, next_filter)
        
        nodes.append(node)
    
    # Sort nodes by cohort size (descending)
    nodes.sort(key=lambda x: x['cohort_size'], reverse=True)
    
    return nodes

def _matches_filter(record: Dict, filter_dict: Dict) -> bool:
    """Check if record matches the current filter"""
    for key, value in filter_dict.items():
        record_value = record.get(key)
        if record_value is None:
            record_value = "NULL"
        if record_value != value:
            return False
    return True

def _weighted_average(records: List[Dict], value_key: str, weight_key: str) -> float:
    """Calculate weighted average"""
    total_weighted_value = 0
    total_weight = 0
    
    for record in records:
        value = record.get(value_key)
        weight = record.get(weight_key, 0)
        
        if value is not None and weight > 0:
            total_weighted_value += value * weight
            total_weight += weight
    
    return total_weighted_value / total_weight if total_weight > 0 else 0

# Debug framework automatically finds functions named handle_{action_name} 