"""
Backend handlers for Value Estimation debug actions
"""

import sqlite3
import os
from datetime import datetime, timedelta
from decimal import Decimal
import json
import logging
from pathlib import Path
import sys

# Add utils directory to path for database utilities
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "utils"))
from database_utils import get_database_path

logger = logging.getLogger(__name__)

def get_db_path():
    """Get database path using dynamic discovery"""
    return get_database_path('mixpanel_data')


def handle_load_timeline_data(request_data):
    """Load timeline data showing credited dates and current values"""
    try:
        date_range = request_data.get('date_range', '30')
        
        # Calculate date filter based on range
        if date_range == 'all':
            date_filter = ""
            date_params = []
        else:
            days = int(date_range)
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            date_filter = "WHERE credited_date >= ?"
            date_params = [cutoff_date]
        
        db_path = get_db_path()
        
        if not os.path.exists(db_path):
            return {
                'success': False,
                'error': f'Database not found at {db_path}'
            }
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query for timeline data: count of trials and sum of current_value by credited_date
        query = f"""
            SELECT 
                credited_date,
                COUNT(*) as trials_count,
                SUM(current_value) as value_sum,
                AVG(current_value) as avg_value
            FROM user_product_metrics 
            {date_filter}
            GROUP BY credited_date 
            ORDER BY credited_date
        """
        
        cursor.execute(query, date_params)
        rows = cursor.fetchall()
        
        # Process the data
        timeline_data = []
        total_trials = 0
        total_value = 0
        
        for row in rows:
            credited_date, trials_count, value_sum, avg_value = row
            
            # Handle potential None values
            value_sum = float(value_sum) if value_sum is not None else 0.0
            avg_value = float(avg_value) if avg_value is not None else 0.0
            
            timeline_data.append({
                'date': credited_date,
                'trials_count': trials_count,
                'value_sum': value_sum,
                'avg_value': avg_value
            })
            
            total_trials += trials_count
            total_value += value_sum
        
        # Calculate summary statistics
        summary = {
            'total_trials': total_trials,
            'total_value': total_value,
            'avg_value': total_value / total_trials if total_trials > 0 else 0.0,
            'date_range': f"{len(timeline_data)} days" if date_range != 'all' else "All time"
        }
        
        # Add date range info if we have data
        if timeline_data:
            first_date = timeline_data[0]['date']
            last_date = timeline_data[-1]['date']
            summary['date_range'] = f"{first_date} to {last_date}"
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'timeline': timeline_data,
                'summary': summary
            },
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_refresh_data(request_data):
    """Refresh data by running a quick database query to verify connectivity"""
    try:
        db_path = get_db_path()
        
        if not os.path.exists(db_path):
            return {
                'success': False,
                'error': f'Database not found at {db_path}'
            }
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Quick connectivity test and basic stats
        cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT credited_date) FROM user_product_metrics")
        distinct_dates = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(credited_date), MAX(credited_date) FROM user_product_metrics")
        date_range = cursor.fetchone()
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'total_records': total_records,
                'distinct_dates': distinct_dates,
                'date_range': date_range,
                'message': f'Database connection successful. Found {total_records} records across {distinct_dates} dates.'
            },
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def handle_get_database_info(request_data):
    """Get general database information for debugging"""
    try:
        db_path = get_db_path()
        
        if not os.path.exists(db_path):
            return {
                'success': False,
                'error': f'Database not found at {db_path}'
            }
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table schema info
        cursor.execute("PRAGMA table_info(user_product_metrics)")
        columns = cursor.fetchall()
        
        # Get sample data
        cursor.execute("SELECT * FROM user_product_metrics LIMIT 5")
        sample_rows = cursor.fetchall()
        
        # Get basic stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT distinct_id) as unique_users,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT credited_date) as unique_dates,
                MIN(credited_date) as earliest_date,
                MAX(credited_date) as latest_date,
                SUM(current_value) as total_current_value
            FROM user_product_metrics
        """)
        stats = cursor.fetchone()
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'database_path': db_path,
                'columns': [{'name': col[1], 'type': col[2]} for col in columns],
                'sample_data': sample_rows,
                'statistics': {
                    'total_records': stats[0],
                    'unique_users': stats[1],
                    'unique_products': stats[2],
                    'unique_dates': stats[3],
                    'earliest_date': stats[4],
                    'latest_date': stats[5],
                    'total_current_value': float(stats[6]) if stats[6] else 0.0
                }
            },
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        } 