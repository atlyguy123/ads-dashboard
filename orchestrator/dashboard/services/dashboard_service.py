# Dashboard Service - Simplified
# 
# Delegates to the working analytics service

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from .analytics_query_service import AnalyticsQueryService, QueryConfig

logger = logging.getLogger(__name__)

class DashboardService:
    """Simplified dashboard service that delegates to analytics service"""
    
    def __init__(self):
        self.analytics_service = AnalyticsQueryService()
    
    def get_available_configurations(self) -> Dict[str, Any]:
        """Get available analytics configurations"""
        return {
            'analytics_all': {
                'name': 'All Ad Data',
                'description': 'Complete ad performance data with Mixpanel attribution',
                'fields': ['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name', 'impressions', 'clicks', 'spend'],
                'breakdowns': [],
                'breakdown': 'all',
                'group_by': 'ad',
                'is_default': True
            },
            'analytics_campaign': {
                'name': 'Campaign Level Data',
                'description': 'Campaign-level performance with hierarchy',
                'fields': ['campaign_id', 'campaign_name', 'impressions', 'clicks', 'spend'],
                'breakdowns': [],
                'breakdown': 'all',
                'group_by': 'campaign',
                'is_default': False
            }
        }

    def get_config_by_hash(self, config_hash: str) -> Optional[Dict[str, Any]]:
        """Get configuration details by hash"""
        configs = self.get_available_configurations()
        return configs.get(config_hash)

    def get_dashboard_data(self, start_date: str, end_date: str, config_key: str) -> Dict[str, Any]:
        """Get dashboard data using analytics service"""
        try:
            config = self.get_config_by_hash(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found")
            
            # Create QueryConfig for analytics service
            query_config = QueryConfig(
                breakdown=config.get('breakdown', 'all'),
                start_date=start_date,
                end_date=end_date,
                group_by=config.get('group_by', 'ad'),
                include_mixpanel=True
            )
            
            # Use analytics service
            result = self.analytics_service.execute_analytics_query(query_config)
            
            if result.get('success'):
                return {
                    'data': result['data'],
                    'metadata': result['metadata']
                }
            else:
                raise Exception(result.get('error', 'Unknown error from analytics service'))
                
        except Exception as e:
            logger.error(f"Error in get_dashboard_data: {str(e)}")
            raise

    def get_chart_data(self, start_date: str, end_date: str, config_key: str, 
                      entity_type: str, entity_id: str, entity_name: str = "Unknown") -> Dict[str, Any]:
        """Get chart data using analytics service"""
        try:
            config = self.get_config_by_hash(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found")
            
            # Create QueryConfig for analytics service
            query_config = QueryConfig(
                breakdown=config.get('breakdown', 'all'),
                start_date=start_date,
                end_date=end_date,
                include_mixpanel=False
            )
            
            # Use analytics service
            result = self.analytics_service.get_chart_data(query_config, entity_type, entity_id)
            
            if result.get('success'):
                return result
            else:
                raise Exception(result.get('error', 'Unknown error from analytics service'))
                
        except Exception as e:
            logger.error(f"Error in get_chart_data: {str(e)}")
            raise

    # Stub methods for compatibility
    def get_collection_job_status(self, job_id: str) -> Dict[str, Any]:
        return {'success': False, 'error': 'Collection jobs not supported'}

    def get_data_coverage_summary(self, config_key: str = 'analytics_all') -> Dict[str, Any]:
        return {'success': True, 'coverage': 'Available via analytics service'}

    def trigger_manual_collection(self, start_date: str, end_date: str, config_key: str = 'analytics_all') -> Dict[str, Any]:
        return {'success': False, 'error': 'Manual collection not supported'} 