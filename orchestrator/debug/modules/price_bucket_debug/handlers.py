"""
Backend handlers for Price Bucket Debug Module

This module provides debug tools for the price bucket assignment pipeline.
It helps verify bucket creation, assignment logic, and data integrity.
"""

import sqlite3
import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
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

def handle_get_bucket_overview(request_data):
    """Get overview of all price buckets created by the assignment pipeline"""
    try:
        if not get_db_path():
            return {
                'success': False,
                'error': f'Database not found'
            }
        
        conn = sqlite3.connect(get_db_path())
        
        # Get price bucket distribution
        query = """
        SELECT 
            upm.product_id,
            COALESCE(mu.country, 'Unknown') as country,
            upm.price_bucket,
            COUNT(*) as user_count,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_product_metrics WHERE valid_lifecycle = 1) as percentage
        FROM user_product_metrics upm
        LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1 
        AND upm.price_bucket IS NOT NULL
        GROUP BY upm.product_id, COALESCE(mu.country, 'Unknown'), upm.price_bucket
        ORDER BY upm.product_id, COALESCE(mu.country, 'Unknown'), upm.price_bucket
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Convert to list of dictionaries for JSON response
        buckets = []
        for _, row in df.iterrows():
            buckets.append({
                'product_id': row['product_id'],
                'country': row['country'],
                'price_bucket': round(float(row['price_bucket']), 2),
                'user_count': int(row['user_count']),
                'percentage': round(float(row['percentage']), 2)
            })
        
        # Get summary statistics
        total_query = """
        SELECT 
            COUNT(*) as total_users,
            COUNT(CASE WHEN price_bucket = 0 THEN 1 END) as zero_buckets,
            COUNT(CASE WHEN price_bucket > 0 THEN 1 END) as positive_buckets,
            MIN(price_bucket) as min_bucket,
            MAX(price_bucket) as max_bucket,
            AVG(CASE WHEN price_bucket > 0 THEN price_bucket END) as avg_positive_bucket
        FROM user_product_metrics 
        WHERE valid_lifecycle = 1
        """
        
        summary_df = pd.read_sql_query(total_query, conn)
        summary = summary_df.iloc[0].to_dict()
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'buckets': buckets,
                'summary': {
                    'total_users': int(summary['total_users']),
                    'zero_buckets': int(summary['zero_buckets']),
                    'positive_buckets': int(summary['positive_buckets']),
                    'min_bucket': round(float(summary['min_bucket']) if summary['min_bucket'] else 0, 2),
                    'max_bucket': round(float(summary['max_bucket']) if summary['max_bucket'] else 0, 2),
                    'avg_positive_bucket': round(float(summary['avg_positive_bucket']) if summary['avg_positive_bucket'] else 0, 2)
                }
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_get_assignment_summary(request_data):
    """Get summary of how price buckets were assigned (conversion types)"""
    try:
        conn = sqlite3.connect(get_db_path())
        
        # Since assignment_type is not stored, we need to infer it from the data
        # We'll analyze the assignment patterns to understand the distribution
        
        # Get conversions data
        conversions_query = """
        SELECT 
            me.distinct_id,
            JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id,
            me.event_name,
            COUNT(*) as conversion_count
        FROM mixpanel_event me 
        WHERE me.revenue_usd > 0 
        AND me.event_name IN ('RC Initial purchase', 'RC Trial converted')
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
        GROUP BY me.distinct_id, product_id, me.event_name
        """
        
        conversions_df = pd.read_sql_query(conversions_query, conn)
        
        # Get all user assignments
        assignments_query = """
        SELECT 
            upm.distinct_id,
            upm.product_id,
            upm.price_bucket,
            COALESCE(mu.country, 'Unknown') as country
        FROM user_product_metrics upm
        LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1
        """
        
        assignments_df = pd.read_sql_query(assignments_query, conn)
        
        # Merge to determine assignment types
        merged = assignments_df.merge(
            conversions_df, 
            on=['distinct_id', 'product_id'], 
            how='left'
        )
        
        # Categorize assignments
        def categorize_assignment(row):
            if pd.notna(row['conversion_count']):
                return 'Own Conversions'
            elif row['price_bucket'] == 0:
                return 'Zero Bucket (No Events/Conversions)'
            elif row['price_bucket'] > 0:
                return 'Inherited from Others'
            else:
                return 'Unknown'
        
        merged['assignment_type'] = merged.apply(categorize_assignment, axis=1)
        
        # Calculate summary
        summary = merged.groupby('assignment_type').agg({
            'distinct_id': 'count',
            'price_bucket': ['mean', 'min', 'max']
        }).round(2)
        
        summary.columns = ['user_count', 'avg_bucket', 'min_bucket', 'max_bucket']
        summary['percentage'] = (summary['user_count'] / len(merged) * 100).round(2)
        
        # Convert to list format
        assignment_summary = []
        for assignment_type, row in summary.iterrows():
            assignment_summary.append({
                'assignment_type': assignment_type,
                'user_count': int(row['user_count']),
                'percentage': float(row['percentage']),
                'avg_bucket': float(row['avg_bucket']),
                'min_bucket': float(row['min_bucket']),
                'max_bucket': float(row['max_bucket'])
            })
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'assignment_summary': assignment_summary,
                'total_users': len(merged)
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_search_assignments(request_data):
    """Search and filter user assignments"""
    try:
        # Get search parameters
        product_id = request_data.get('product_id', '')
        country = request_data.get('country', '')
        min_bucket = request_data.get('min_bucket', '')
        max_bucket = request_data.get('max_bucket', '')
        limit = min(int(request_data.get('limit', 100)), 500)  # Max 500 results
        
        conn = sqlite3.connect(get_db_path())
        
        # Build dynamic query
        conditions = ["upm.valid_lifecycle = 1"]
        params = []
        
        if product_id:
            conditions.append("upm.product_id LIKE ?")
            params.append(f"%{product_id}%")
        
        if country:
            conditions.append("COALESCE(mu.country, 'Unknown') LIKE ?")
            params.append(f"%{country}%")
        
        if min_bucket:
            conditions.append("upm.price_bucket >= ?")
            params.append(float(min_bucket))
        
        if max_bucket:
            conditions.append("upm.price_bucket <= ?")
            params.append(float(max_bucket))
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        SELECT 
            upm.distinct_id,
            upm.product_id,
            COALESCE(mu.country, 'Unknown') as country,
            upm.price_bucket,
            upm.current_status,
            upm.credited_date,
            CASE 
                WHEN conv.conversion_count > 0 THEN 'Own Conversions'
                WHEN upm.price_bucket = 0 THEN 'Zero Bucket'
                WHEN upm.price_bucket > 0 THEN 'Inherited'
                ELSE 'Unknown'
            END as assignment_type
        FROM user_product_metrics upm
        LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        LEFT JOIN (
            SELECT 
                distinct_id,
                JSON_EXTRACT(event_json, '$.properties.product_id') as product_id,
                COUNT(*) as conversion_count
            FROM mixpanel_event 
            WHERE revenue_usd > 0 
            AND event_name IN ('RC Initial purchase', 'RC Trial converted')
            GROUP BY distinct_id, product_id
        ) conv ON upm.distinct_id = conv.distinct_id AND upm.product_id = conv.product_id
        WHERE {where_clause}
        ORDER BY upm.price_bucket DESC, upm.distinct_id
        LIMIT ?
        """
        
        params.append(limit)
        df = pd.read_sql_query(query, conn, params=params)
        
        # Convert to list of dictionaries
        results = []
        for _, row in df.iterrows():
            results.append({
                'distinct_id': row['distinct_id'][:12] + '...',  # Truncate for privacy
                'product_id': row['product_id'],
                'country': row['country'],
                'price_bucket': round(float(row['price_bucket']), 2),
                'current_status': row['current_status'],
                'credited_date': row['credited_date'],
                'assignment_type': row['assignment_type']
            })
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'results': results,
                'count': len(results),
                'filters_applied': {
                    'product_id': product_id,
                    'country': country,
                    'min_bucket': min_bucket,
                    'max_bucket': max_bucket
                }
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_validate_data(request_data):
    """Validate data integrity and identify potential issues"""
    try:
        conn = sqlite3.connect(get_db_path())
        
        issues = []
        
        # Check 1: Users without price bucket assignments
        missing_query = """
        SELECT COUNT(*) as count
        FROM user_product_metrics 
        WHERE valid_lifecycle = 1 
        AND price_bucket IS NULL
        """
        missing_count = conn.execute(missing_query).fetchone()[0]
        if missing_count > 0:
            issues.append(f"⚠️ {missing_count:,} valid lifecycle users missing price bucket assignments")
        
        # Check 2: Negative price buckets (invalid)
        negative_query = """
        SELECT COUNT(*) as count
        FROM user_product_metrics 
        WHERE valid_lifecycle = 1 
        AND price_bucket < 0
        """
        negative_count = conn.execute(negative_query).fetchone()[0]
        if negative_count > 0:
            issues.append(f"❌ {negative_count:,} users have negative price buckets (invalid)")
        
        # Check 3: Extremely high price buckets (potential issues)
        high_query = """
        SELECT COUNT(*) as count, MAX(price_bucket) as max_bucket
        FROM user_product_metrics 
        WHERE valid_lifecycle = 1 
        AND price_bucket > 1000
        """
        high_result = conn.execute(high_query).fetchone()
        if high_result[0] > 0:
            issues.append(f"⚠️ {high_result[0]:,} users have very high price buckets (max: ${high_result[1]:.2f})")
        
        # Check 4: Countries with only zero buckets
        zero_countries_query = """
        SELECT 
            COALESCE(mu.country, 'Unknown') as country,
            COUNT(*) as total_users,
            COUNT(CASE WHEN upm.price_bucket = 0 THEN 1 END) as zero_users
        FROM user_product_metrics upm
        LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
        WHERE upm.valid_lifecycle = 1
        GROUP BY COALESCE(mu.country, 'Unknown')
        HAVING zero_users = total_users AND total_users > 10
        """
        zero_countries_df = pd.read_sql_query(zero_countries_query, conn)
        if len(zero_countries_df) > 0:
            for _, row in zero_countries_df.iterrows():
                issues.append(f"⚠️ Country '{row['country']}' has {row['total_users']} users, all with zero buckets")
        
        # Check 5: Products with no conversion buckets
        no_conversion_query = """
        SELECT 
            upm.product_id,
            COUNT(*) as total_users,
            COUNT(CASE WHEN upm.price_bucket = 0 THEN 1 END) as zero_users
        FROM user_product_metrics upm
        WHERE upm.valid_lifecycle = 1
        GROUP BY upm.product_id
        HAVING zero_users = total_users AND total_users > 10
        """
        no_conversion_df = pd.read_sql_query(no_conversion_query, conn)
        if len(no_conversion_df) > 0:
            for _, row in no_conversion_df.iterrows():
                issues.append(f"⚠️ Product '{row['product_id']}' has {row['total_users']} users, all with zero buckets")
        
        # Get overall statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_valid_users,
            COUNT(CASE WHEN price_bucket IS NOT NULL THEN 1 END) as assigned_users,
            COUNT(CASE WHEN price_bucket = 0 THEN 1 END) as zero_bucket_users,
            COUNT(CASE WHEN price_bucket > 0 THEN 1 END) as positive_bucket_users
        FROM user_product_metrics 
        WHERE valid_lifecycle = 1
        """
        stats = conn.execute(stats_query).fetchone()
        
        conn.close()
        
        status = "✅ All validations passed" if not issues else f"⚠️ Found {len(issues)} potential issues"
        
        return {
            'success': True,
            'data': {
                'status': status,
                'issues': issues,
                'statistics': {
                    'total_valid_users': stats[0],
                    'assigned_users': stats[1],
                    'zero_bucket_users': stats[2],
                    'positive_bucket_users': stats[3],
                    'assignment_rate': round((stats[1] / stats[0] * 100) if stats[0] > 0 else 0, 2)
                }
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_analyze_conversions(request_data):
    """Analyze conversion events that create the price buckets"""
    try:
        conn = sqlite3.connect(get_db_path())
        
        # Get conversion events analysis
        query = """
        SELECT 
            JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id,
            COALESCE(mu.country, 'Unknown') as country,
            me.event_name,
            COUNT(*) as conversion_count,
            MIN(me.revenue_usd) as min_price,
            MAX(me.revenue_usd) as max_price,
            AVG(me.revenue_usd) as avg_price,
            MIN(me.event_time) as first_conversion,
            MAX(me.event_time) as last_conversion
        FROM mixpanel_event me 
        LEFT JOIN mixpanel_user mu ON me.distinct_id = mu.distinct_id
        WHERE me.revenue_usd > 0 
        AND me.event_name IN ('RC Initial purchase', 'RC Trial converted')
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
        GROUP BY JSON_EXTRACT(me.event_json, '$.properties.product_id'), COALESCE(mu.country, 'Unknown'), me.event_name
        ORDER BY JSON_EXTRACT(me.event_json, '$.properties.product_id'), COALESCE(mu.country, 'Unknown'), me.event_name
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Convert to list of dictionaries
        conversions = []
        for _, row in df.iterrows():
            conversions.append({
                'product_id': row['product_id'],
                'country': row['country'],
                'event_name': row['event_name'],
                'conversion_count': int(row['conversion_count']),
                'min_price': round(float(row['min_price']), 2),
                'max_price': round(float(row['max_price']), 2),
                'avg_price': round(float(row['avg_price']), 2),
                'price_range': round(float(row['max_price']) - float(row['min_price']), 2),
                'first_conversion': row['first_conversion'][:10],  # Just date part
                'last_conversion': row['last_conversion'][:10]     # Just date part
            })
        
        # Get summary statistics
        total_conversions = sum(c['conversion_count'] for c in conversions)
        unique_products = len(set(c['product_id'] for c in conversions))
        unique_countries = len(set(c['country'] for c in conversions))
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'conversions': conversions,
                'summary': {
                    'total_conversions': total_conversions,
                    'unique_products': unique_products,
                    'unique_countries': unique_countries,
                    'conversion_groups': len(conversions)
                }
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_get_sample_data(request_data):
    """Get sample data showing how buckets were assigned"""
    try:
        conn = sqlite3.connect(get_db_path())
        
        # Get sample of each assignment type
        query = """
        WITH conversion_users AS (
            SELECT DISTINCT 
                distinct_id,
                JSON_EXTRACT(event_json, '$.properties.product_id') as product_id
            FROM mixpanel_event 
            WHERE revenue_usd > 0 
            AND event_name IN ('RC Initial purchase', 'RC Trial converted')
        ),
        sample_data AS (
            SELECT 
                upm.distinct_id,
                upm.product_id,
                COALESCE(mu.country, 'Unknown') as country,
                upm.price_bucket,
                upm.current_status,
                CASE 
                    WHEN cu.distinct_id IS NOT NULL THEN 'Own Conversions'
                    WHEN upm.price_bucket = 0 THEN 'Zero Bucket'
                    WHEN upm.price_bucket > 0 THEN 'Inherited'
                    ELSE 'Unknown'
                END as assignment_type,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE 
                        WHEN cu.distinct_id IS NOT NULL THEN 'Own Conversions'
                        WHEN upm.price_bucket = 0 THEN 'Zero Bucket'
                        WHEN upm.price_bucket > 0 THEN 'Inherited'
                        ELSE 'Unknown'
                    END 
                    ORDER BY RANDOM()
                ) as rn
            FROM user_product_metrics upm
            LEFT JOIN mixpanel_user mu ON upm.distinct_id = mu.distinct_id
            LEFT JOIN conversion_users cu ON upm.distinct_id = cu.distinct_id AND upm.product_id = cu.product_id
            WHERE upm.valid_lifecycle = 1
        )
        SELECT *
        FROM sample_data
        WHERE rn <= 5
        ORDER BY assignment_type, price_bucket DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Convert to list of dictionaries
        samples = []
        for _, row in df.iterrows():
            samples.append({
                'distinct_id': row['distinct_id'][:12] + '...',
                'product_id': row['product_id'],
                'country': row['country'],
                'price_bucket': round(float(row['price_bucket']), 2),
                'current_status': row['current_status'],
                'assignment_type': row['assignment_type']
            })
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'samples': samples,
                'note': 'Showing up to 5 examples of each assignment type'
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        } 