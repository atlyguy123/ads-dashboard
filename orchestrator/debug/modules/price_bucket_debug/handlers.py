# Price Bucket Debug Handlers
# This module will contain handlers for debugging price bucket assignments

import sqlite3
import logging
from pathlib import Path
import sys
from typing import Dict, Any, List, Optional

# Add utils directory to path for database utilities
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "utils"))
from database_utils import get_database_path

logger = logging.getLogger(__name__)

def get_db_path():
    """Get database path using dynamic discovery"""
    return get_database_path('mixpanel_data')

def handle_get_overview_data(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get price bucket overview data with statistics and table data"""
    try:
        db_path = get_db_path()
        if not db_path:
            return {
                'success': False,
                'error': 'Database not found'
            }
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Get statistics
        stats = _get_price_bucket_statistics(conn)
        
        # Get table data (aggregated by product_id, country, price_bucket)
        table_data = _get_price_bucket_table_data(conn)
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'statistics': stats,
                'table_data': table_data
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting price bucket overview: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def _get_price_bucket_statistics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Calculate price bucket assignment statistics using assignment_type field"""
    cursor = conn.cursor()
    
    # Total user-product pairs and valid lifecycle counts
    cursor.execute("""
        SELECT 
            COUNT(*) as total_user_product_pairs,
            COUNT(CASE WHEN upm.valid_lifecycle = 1 AND mu.valid_user = 1 THEN 1 END) as total_valid_users
        FROM user_product_metrics upm
        JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
    """)
    lifecycle_stats = cursor.fetchone()
    total_user_product_pairs = lifecycle_stats['total_user_product_pairs']
    total_users = lifecycle_stats['total_valid_users']
    
    # Assignment type breakdown using the actual assignment_type field
    cursor.execute("""
        SELECT 
            upm.assignment_type,
            COUNT(*) as count,
            COUNT(CASE WHEN upm.price_bucket > 0 THEN 1 END) as positive_bucket_count,
            COUNT(CASE WHEN upm.price_bucket = 0 OR upm.price_bucket IS NULL THEN 1 END) as zero_bucket_count
        FROM user_product_metrics upm
        JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1 AND mu.valid_user = 1
        GROUP BY upm.assignment_type
    """)
    assignment_stats = cursor.fetchall()
    
    # Calculate totals by assignment type
    properly_sorted = 0  # Direct conversions
    inherited = 0        # Inherited buckets (prior or closest)
    zero_bucket = 0      # No bucket assigned
    
    for row in assignment_stats:
        assignment_type = row['assignment_type']
        count = row['count']
        
        if assignment_type == 'conversion':
            properly_sorted += count
        elif assignment_type in ['inherited_prior', 'inherited_closest']:
            inherited += count
        elif assignment_type in ['no_event', 'conversion_no_bucket', 'no_conversions_ever', 'unassigned']:
            zero_bucket += count
    
    # Unique price buckets
    cursor.execute("""
        SELECT COUNT(DISTINCT upm.price_bucket) as unique_buckets
        FROM user_product_metrics upm
        JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1 AND mu.valid_user = 1
        AND upm.price_bucket > 0
    """)
    unique_buckets = cursor.fetchone()['unique_buckets'] or 0
    
    return {
        'total_user_product_pairs': total_user_product_pairs,
        'total_users': total_users,
        'properly_sorted': properly_sorted,
        'inherited': inherited,
        'zero_bucket': zero_bucket,
        'unique_buckets': unique_buckets
    }

def _get_price_bucket_table_data(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Get aggregated table data with correct event type and assignment type calculations"""
    cursor = conn.cursor()
    
    # Create comprehensive data with proper event type detection and assignment type usage
    cursor.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_comprehensive_data AS
        SELECT 
            upm.distinct_id,
            upm.product_id,
            COALESCE(mu.country, 'Unknown') as country,
            COALESCE(upm.price_bucket, 0) as price_bucket,
            COALESCE(upm.assignment_type, 'unassigned') as assignment_type,
            CASE 
                -- Event type based on what actually caused the bucket assignment
                WHEN upm.price_bucket > 0 AND upm.inherited_from_event_type IS NOT NULL THEN upm.inherited_from_event_type
                WHEN upm.assignment_type = 'conversion' THEN (
                    CASE 
                        WHEN EXISTS(
                            SELECT 1 FROM mixpanel_event me 
                            WHERE me.distinct_id = upm.distinct_id 
                            AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
                            AND me.event_name = 'RC Initial purchase'
                            AND me.revenue_usd > 0
                            LIMIT 1
                        ) THEN 'RC Initial purchase'
                        WHEN EXISTS(
                            SELECT 1 FROM mixpanel_event me 
                            WHERE me.distinct_id = upm.distinct_id 
                            AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
                            AND me.event_name = 'RC Trial converted'
                            AND me.revenue_usd > 0
                            LIMIT 1
                        ) THEN 'RC Trial converted'
                        ELSE 'No Conversion'
                    END
                )
                ELSE 'No Conversion'
            END as event_type
        FROM user_product_metrics upm
        JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1 AND mu.valid_user = 1
    """)
    
    # Aggregate the data properly
    aggregation_query = """
        SELECT 
            product_id,
            country,
            price_bucket,
            event_type,
            COUNT(*) as total_user_count,
            COUNT(CASE WHEN assignment_type = 'conversion' THEN 1 END) as properly_sorted_count,
            COUNT(CASE WHEN assignment_type IN ('inherited_prior', 'inherited_closest') THEN 1 END) as inherited_count
        FROM temp_comprehensive_data
        GROUP BY product_id, country, price_bucket, event_type
        ORDER BY product_id, country, price_bucket DESC
    """
    
    cursor.execute(aggregation_query)
    rows = cursor.fetchall()
    
    # Clean up temp table
    cursor.execute("DROP TABLE IF EXISTS temp_comprehensive_data")
    
    table_data = []
    for row in rows:
        table_data.append({
            'product_id': row['product_id'],
            'country': row['country'],
            'event_type': row['event_type'],
            'price_bucket': float(row['price_bucket']),
            'user_count': row['total_user_count'],
            'properly_sorted_count': row['properly_sorted_count'],
            'inherited_count': row['inherited_count']
        })
    
    return table_data

def handle_search_data(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Search and filter price bucket data"""
    try:
        # Get filter parameters
        product_filter = request_data.get('product_id', '').strip()
        country_filter = request_data.get('country', '').strip()
        min_bucket = request_data.get('min_bucket')
        max_bucket = request_data.get('max_bucket')
        min_users = request_data.get('min_users')
        max_users = request_data.get('max_users')
        
        db_path = get_db_path()
        if not db_path:
            return {
                'success': False,
                'error': 'Database not found'
            }
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Build dynamic query with filters
        conditions = ["upm.valid_lifecycle = 1", "mu.valid_user = 1"]
        params = []
        
        if product_filter:
            conditions.append("upm.product_id LIKE ?")
            params.append(f"%{product_filter}%")
        
        if country_filter:
            conditions.append("COALESCE(mu.country, 'Unknown') LIKE ?")
            params.append(f"%{country_filter}%")
        
        if min_bucket is not None:
            conditions.append("COALESCE(upm.price_bucket, 0) >= ?")
            params.append(float(min_bucket))
        
        if max_bucket is not None:
            conditions.append("COALESCE(upm.price_bucket, 0) <= ?")
            params.append(float(max_bucket))
        
        # Build the main query
        having_conditions = []
        if min_users is not None:
            having_conditions.append("COUNT(*) >= ?")
            params.append(int(min_users))
        
        if max_users is not None:
            having_conditions.append("COUNT(*) <= ?")
            params.append(int(max_users))
        
        where_clause = " AND ".join(conditions)
        having_clause = " AND ".join(having_conditions) if having_conditions else ""
        
        query = f"""
            SELECT 
                upm.product_id,
                COALESCE(mu.country, 'Unknown') as country,
                COALESCE(upm.price_bucket, 0) as price_bucket,
                CASE 
                    WHEN upm.price_bucket > 0 AND upm.inherited_from_event_type IS NOT NULL THEN upm.inherited_from_event_type
                    WHEN upm.assignment_type = 'conversion' THEN (
                        CASE 
                            WHEN EXISTS(
                                SELECT 1 FROM mixpanel_event me 
                                WHERE me.distinct_id = upm.distinct_id 
                                AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
                                AND me.event_name = 'RC Initial purchase'
                                AND me.revenue_usd > 0
                                LIMIT 1
                            ) THEN 'RC Initial purchase'
                            WHEN EXISTS(
                                SELECT 1 FROM mixpanel_event me 
                                WHERE me.distinct_id = upm.distinct_id 
                                AND JSON_EXTRACT(me.event_json, '$.properties.product_id') = upm.product_id
                                AND me.event_name = 'RC Trial converted'
                                AND me.revenue_usd > 0
                                LIMIT 1
                            ) THEN 'RC Trial converted'
                            ELSE 'No Conversion'
                        END
                    )
                    ELSE 'No Conversion'
                END as event_type,
                COUNT(*) as total_user_count,
                COUNT(CASE WHEN upm.assignment_type = 'conversion' THEN 1 END) as properly_sorted_count,
                COUNT(CASE WHEN upm.assignment_type IN ('inherited_prior', 'inherited_closest') THEN 1 END) as inherited_count
            FROM user_product_metrics upm
            JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
            WHERE {where_clause}
            GROUP BY upm.product_id, COALESCE(mu.country, 'Unknown'), COALESCE(upm.price_bucket, 0), event_type
            {f'HAVING {having_clause}' if having_clause else ''}
            ORDER BY upm.product_id, country, price_bucket DESC
            LIMIT 1000
        """
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        filtered_data = []
        for row in rows:
            filtered_data.append({
                'product_id': row['product_id'],
                'country': row['country'],
                'event_type': row['event_type'],
                'price_bucket': float(row['price_bucket']),
                'user_count': row['total_user_count'],
                'properly_sorted_count': row['properly_sorted_count'],
                'inherited_count': row['inherited_count']
            })
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'filtered_data': filtered_data,
                'total_results': len(filtered_data),
                'filters_applied': {
                    'product_id': product_filter,
                    'country': country_filter,
                    'min_bucket': min_bucket,
                    'max_bucket': max_bucket,
                    'min_users': min_users,
                    'max_users': max_users
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error searching price bucket data: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def placeholder():
    """Placeholder function - to be implemented"""
    pass 