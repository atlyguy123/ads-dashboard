"""
Dashboard Overview Service

This service handles all overview calculations for the dashboard including:
- Overview ROAS sparklines
- Total spend, revenue, profit calculations
- Overview summary statistics
- Dashboard summary cards data

This is a dedicated service that extracts overview functionality from
dashboard_refresh_service.py for better organization and separation of concerns.
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

logger = logging.getLogger(__name__)


class OverviewService:
    """Service for dashboard overview calculations and data"""
    
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

    def get_overview_roas_chart_data(self, start_date: str, end_date: str, breakdown: str = 'all', entity_type: str = 'campaign') -> Dict[str, Any]:
        """
        Get overview ROAS sparkline data for dashboard summary
        
        This aggregates data across all campaigns to show overall performance trends.
        ✅ FIXED: Uses pre-computed adjusted_estimated_revenue_usd to prevent double inflation.
        """
        try:
            logger.info(f"Getting overview ROAS chart data: {start_date} to {end_date}, breakdown={breakdown}")
            
            # Calculate date range for chart period
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Calculate how many days we need
            date_diff = (end_dt - start_dt).days + 1
            
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
            table_name = self.get_table_name(breakdown)
            
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
            
            try:
                with sqlite3.connect(self.meta_db_path) as meta_conn:
                    meta_conn.row_factory = sqlite3.Row
                    meta_cursor = meta_conn.cursor()
                    
                    meta_cursor.execute(meta_query, (start_date, end_date))
                    meta_results = meta_cursor.fetchall()
                    
                    # Process Meta results
                    for row in meta_results:
                        date_str = row['date']
                        if date_str in daily_data:
                            daily_data[date_str].update({
                                'daily_spend': float(row['daily_spend'] or 0),
                                'daily_impressions': int(row['daily_impressions'] or 0),
                                'daily_clicks': int(row['daily_clicks'] or 0),
                                'daily_meta_trials': int(row['daily_meta_trials'] or 0),
                                'daily_meta_purchases': int(row['daily_meta_purchases'] or 0),
                                'is_inactive': False
                            })
                    
                    logger.info(f"✅ Retrieved Meta data for {len(meta_results)} days")
            except Exception as e:
                logger.warning(f"⚠️ Meta database error (overview chart): {e}")
                # Continue with Mixpanel data only
            
            # ✅ STEP 2: Get aggregated Mixpanel data from pre-computed daily metrics  
            # ✅ FIXED: Use adjusted_estimated_revenue_usd (pre-computed adjusted values)
            # ✅ FIXED: Only aggregate selected entity_type to prevent triple-counting
            mixpanel_query = """
                SELECT 
                    date,
                    SUM(trial_users_count) as daily_mixpanel_trials,
                    SUM(purchase_users_count) as daily_mixpanel_purchases,
                    SUM(adjusted_estimated_revenue_usd) as daily_estimated_revenue,
                    SUM(actual_revenue_usd) as daily_mixpanel_revenue,
                    COUNT(DISTINCT entity_id) as daily_attributed_users
                FROM daily_mixpanel_metrics
                WHERE date BETWEEN ? AND ?
                  AND entity_type = ?
                GROUP BY date
                ORDER BY date
            """
            
            try:
                with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                    mixpanel_conn.row_factory = sqlite3.Row
                    mixpanel_cursor = mixpanel_conn.cursor()
                    
                    mixpanel_cursor.execute(mixpanel_query, (start_date, end_date, entity_type))
                    mixpanel_results = mixpanel_cursor.fetchall()
                    
                    # Process Mixpanel results
                    for row in mixpanel_results:
                        date_str = row['date']
                        if date_str in daily_data:
                            daily_data[date_str].update({
                                'daily_mixpanel_trials': int(row['daily_mixpanel_trials'] or 0),
                                'daily_mixpanel_purchases': int(row['daily_mixpanel_purchases'] or 0),
                                'daily_mixpanel_conversions': int(row['daily_mixpanel_purchases'] or 0),  # Alias
                                'daily_estimated_revenue': float(row['daily_estimated_revenue'] or 0),
                                'daily_mixpanel_revenue': float(row['daily_mixpanel_revenue'] or 0),
                                'daily_attributed_users': int(row['daily_attributed_users'] or 0),
                                'is_inactive': False
                            })
                    
                    logger.info(f"✅ Retrieved Mixpanel data for {len(mixpanel_results)} days")
            except Exception as e:
                logger.error(f"❌ Mixpanel database error (overview chart): {e}")
                # Continue with Meta data only
            
            # ✅ STEP 3: Process daily data into chart format
            chart_data = []
            total_spend = 0.0
            total_revenue = 0.0
            total_trials = 0
            total_purchases = 0
            
            for date_str in sorted(daily_data.keys()):
                day_data = daily_data[date_str]
                
                # Calculate daily ROAS using ADJUSTED revenue (no double inflation)
                daily_spend = day_data['daily_spend']
                daily_revenue = day_data['daily_estimated_revenue']  # ✅ FIXED: Pre-adjusted revenue
                daily_roas = (daily_revenue / daily_spend) if daily_spend > 0 else 0.0
                
                # Update totals
                total_spend += daily_spend
                total_revenue += daily_revenue
                total_trials += day_data['daily_mixpanel_trials']
                total_purchases += day_data['daily_mixpanel_purchases']
                
                chart_data.append({
                    'date': date_str,
                    'spend': daily_spend,
                    'revenue': daily_revenue,
                    'roas': daily_roas,
                    'rolling_1d_roas': daily_roas,  # Frontend expects this field name
                    'rolling_1d_spend': daily_spend,  # For consistency with other sparklines
                    'rolling_1d_revenue': daily_revenue,  # For consistency with other sparklines
                    'trials': day_data['daily_mixpanel_trials'],
                    'purchases': day_data['daily_mixpanel_purchases'],
                    'impressions': day_data['daily_impressions'],
                    'clicks': day_data['daily_clicks'],
                    'is_inactive': day_data['is_inactive']
                })
            
            # Calculate overall metrics
            overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0.0
            conversion_rate = (total_purchases / total_trials) if total_trials > 0 else 0.0
            
            logger.info(f"✅ OVERVIEW TOTALS: spend={total_spend}, revenue={total_revenue}, ROAS={overall_roas}")
            
            return {
                'success': True,
                'chart_data': chart_data,
                'summary': {
                    'total_spend': total_spend,
                    'total_revenue': total_revenue,
                    'overall_roas': overall_roas,
                    'total_trials': total_trials,
                    'total_purchases': total_purchases,
                    'conversion_rate': conversion_rate,
                    'date_range': {
                        'start': start_date,
                        'end': end_date,
                        'days': date_diff
                    }
                },
                'metadata': {
                    'breakdown': breakdown,
                    'table_used': table_name,
                    'data_points': len(chart_data),
                    'generated_at': now_in_timezone().isoformat(),
                    'service': 'overview_service',
                    'fixed_double_inflation': True  # Flag indicating the fix is applied
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting overview ROAS chart data: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'chart_data': [],
                'summary': {},
                'service': 'overview_service'
            }

    def get_dashboard_stats(self, start_date: str, end_date: str, breakdown: str = 'all', entity_type: str = 'campaign') -> Dict[str, Any]:
        """
        Get dashboard overview statistics (total spend, revenue, profit, ROAS)
        
        This provides the summary statistics shown in the dashboard overview cards.
        ✅ FIXED: Uses pre-computed adjusted values to prevent double inflation.
        """
        try:
            logger.info(f"Getting dashboard stats: {start_date} to {end_date}, breakdown={breakdown}")
            
            # Get the overview chart data which contains all the aggregated metrics
            overview_data = self.get_overview_roas_chart_data(start_date, end_date, breakdown, entity_type)
            
            if not overview_data.get('success'):
                return {
                    'success': False,
                    'error': 'Failed to get overview data for stats calculation',
                    'stats': {}
                }
            
            summary = overview_data.get('summary', {})
            chart_data = overview_data.get('chart_data', [])
            
            # Calculate profit (revenue - spend)
            total_spend = summary.get('total_spend', 0)
            total_revenue = summary.get('total_revenue', 0)
            total_profit = total_revenue - total_spend
            
            # Build stats response
            stats = {
                'totalSpend': total_spend,
                'totalRevenue': total_revenue,
                'totalProfit': total_profit,
                'totalROAS': summary.get('overall_roas', 0),
                'sparklines': {
                    'spend': [{'date': day['date'], 'value': day['spend']} for day in chart_data],
                    'revenue': [{'date': day['date'], 'value': day['revenue']} for day in chart_data],
                    'profit': [{'date': day['date'], 'value': day['revenue'] - day['spend']} for day in chart_data],
                    'roas': [{'date': day['date'], 'value': day['roas']} for day in chart_data]
                }
            }
            
            logger.info(f"✅ DASHBOARD STATS: spend={total_spend}, revenue={total_revenue}, profit={total_profit}, ROAS={summary.get('overall_roas', 0)}")
            
            return {
                'success': True,
                'stats': stats,
                'metadata': {
                    'date_range': summary.get('date_range', {}),
                    'breakdown': breakdown,
                    'generated_at': now_in_timezone().isoformat(),
                    'service': 'overview_service',
                    'fixed_double_inflation': True
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'stats': {},
                'service': 'overview_service'
            }
