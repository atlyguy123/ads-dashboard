# Dashboard Service
# 
# Main service for dashboard functionality that integrates with the historical 
# data system to provide campaign performance data.

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json

from .meta_service import meta_historical_service
from .config_manager import ConfigManager
from .data_transformer import DataTransformer

logger = logging.getLogger(__name__)

class DashboardService:
    """Main service for dashboard data operations"""
    
    def __init__(self):
        self.historical_service = meta_historical_service
        self.config_manager = ConfigManager()
        self.data_transformer = DataTransformer()
    
    def get_available_configurations(self) -> Dict[str, Any]:
        """Get only configurations that actually exist in the database with data"""
        try:
            # Get all configurations from the historical service database
            db_configurations = self.historical_service.get_configurations()
            
            result = {}
            for db_config in db_configurations:
                # Only include configurations that have actual data (day_count > 0)
                if db_config.get('day_count', 0) > 0:
                    # Parse the fields to create a meaningful name
                    fields = db_config.get('fields', '')
                    breakdowns = db_config.get('breakdowns', '')
                    
                    # Create a descriptive name based on fields
                    if 'impressions' in fields and 'clicks' in fields and 'spend' in fields:
                        if breakdowns:
                            name = f"Ad Data with {breakdowns.title()}"
                            description = f"Basic ad data broken down by {breakdowns}"
                        else:
                            name = "Basic Ad Data"
                            description = "Core Meta fields: Ad ID, Ad Name, Adset ID, Adset Name, Campaign ID, Campaign Name, Impressions, Clicks, Spend"
                    else:
                        name = f"Custom Data ({len(fields.split(','))} fields)"
                        description = f"Custom configuration with fields: {fields}"
                    
                    # Use config hash as the key
                    config_key = db_config['config_hash']
                    
                    result[config_key] = {
                        'name': name,
                        'description': description,
                        'fields': fields.split(','),
                        'breakdowns': breakdowns.split(',') if breakdowns else [],
                        'day_count': db_config['day_count'],
                        'date_range': f"{db_config['earliest_date']} to {db_config['latest_date']}",
                        'is_default': False  # No default from DB configurations
                    }
            
            # If we found configurations, make the first one with the most data the default
            if result:
                # Find config with most days and no breakdowns for default
                best_config = max(
                    result.items(), 
                    key=lambda x: (x[1]['day_count'], len(x[1]['breakdowns']) == 0)
                )
                result[best_config[0]]['is_default'] = True
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting available configurations: {str(e)}")
            # Fallback to empty dict if database is not available
            return {}

    def get_config_by_hash(self, config_hash: str) -> Optional[Dict[str, Any]]:
        """Get configuration details by hash from available configurations"""
        configs = self.get_available_configurations()
        return configs.get(config_hash)

    def get_dashboard_data(self, start_date: str, end_date: str, config_key: str) -> Dict[str, Any]:
        """Get dashboard data for the specified parameters"""
        try:
            # Get configuration by hash (config_key is now a hash)
            config = self.get_config_by_hash(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found or has no data")
            
            # Get raw data from historical service using the string-based API (without entity filtering for basic data)
            raw_data = self.historical_service.export_data_for_config(
                start_date=start_date,
                end_date=end_date,
                fields=','.join(config['fields']),
                breakdowns=','.join(config.get('breakdowns', []))
            )
            
            # Extract the actual data records from the export result
            extracted_data = []
            
            if raw_data and 'data' in raw_data:
                for day_data in raw_data['data']:
                    if 'data' in day_data and 'data' in day_data['data']:
                        # Check the structure of day_data['data']['data']
                        data_container = day_data['data']['data']
                        
                        if isinstance(data_container, dict) and 'data' in data_container:
                            # Structure: day_data['data']['data']['data'] contains the actual records
                            records = data_container['data']
                        elif isinstance(data_container, list):
                            # Structure: day_data['data']['data'] directly contains the records
                            records = data_container
                        else:
                            logger.warning(f"Unexpected data container structure: {type(data_container)}")
                            continue
                            
                        # Process the actual records and add date information
                        for record in records:
                            if isinstance(record, dict):
                                # Add the date from the day_data level to each record
                                record['date_start'] = day_data['date']
                                extracted_data.append(record)
                            else:
                                logger.warning(f"Unexpected record type: {type(record)}")
            
            # Collect unique entity IDs for business metrics requests
            entity_ids = {
                'campaigns': set(),
                'adsets': set(), 
                'ads': set()
            }
            
            for record in extracted_data:
                if record.get('campaign_id'):
                    entity_ids['campaigns'].add(record['campaign_id'])
                if record.get('adset_id'):
                    entity_ids['adsets'].add(record['adset_id'])
                if record.get('ad_id'):
                    entity_ids['ads'].add(record['ad_id'])
            
            # Get entity-specific business metrics for each entity
            entity_business_metrics = {}
            
            # Get business metrics for campaigns
            for campaign_id in entity_ids['campaigns']:
                entity_data = self.historical_service.export_data_for_config(
                    start_date=start_date,
                    end_date=end_date,
                    fields=','.join(config['fields']),
                    breakdowns=','.join(config.get('breakdowns', [])),
                    entity_type='campaign',
                    entity_id=campaign_id
                )
                
                # Extract business metrics by date for this campaign
                for day_data in entity_data.get('data', []):
                    if 'business_metrics' in day_data and day_data['business_metrics']:
                        key = f"campaign:{campaign_id}"
                        if key not in entity_business_metrics:
                            entity_business_metrics[key] = {}
                        entity_business_metrics[key][day_data['date']] = day_data['business_metrics']
            
            # Get business metrics for adsets  
            for adset_id in entity_ids['adsets']:
                entity_data = self.historical_service.export_data_for_config(
                    start_date=start_date,
                    end_date=end_date,
                    fields=','.join(config['fields']),
                    breakdowns=','.join(config.get('breakdowns', [])),
                    entity_type='adset',
                    entity_id=adset_id
                )
                
                # Extract business metrics by date for this adset
                for day_data in entity_data.get('data', []):
                    if 'business_metrics' in day_data and day_data['business_metrics']:
                        key = f"adset:{adset_id}"
                        if key not in entity_business_metrics:
                            entity_business_metrics[key] = {}
                        entity_business_metrics[key][day_data['date']] = day_data['business_metrics']
            
            # Get business metrics for ads
            for ad_id in entity_ids['ads']:
                entity_data = self.historical_service.export_data_for_config(
                    start_date=start_date,
                    end_date=end_date,
                    fields=','.join(config['fields']),
                    breakdowns=','.join(config.get('breakdowns', [])),
                    entity_type='ad',
                    entity_id=ad_id
                )
                
                # Extract business metrics by date for this ad
                for day_data in entity_data.get('data', []):
                    if 'business_metrics' in day_data and day_data['business_metrics']:
                        key = f"ad:{ad_id}"
                        if key not in entity_business_metrics:
                            entity_business_metrics[key] = {}
                        entity_business_metrics[key][day_data['date']] = day_data['business_metrics']
            
            logger.info(f"Extracted {len(extracted_data)} records and entity-specific business metrics for {len(entity_business_metrics)} entities")
            
            # Transform data into hierarchical structure with entity-specific business metrics
            transformed_data = self.data_transformer.transform_to_hierarchy(extracted_data, entity_business_metrics)
            
            return {
                'data': transformed_data,
                'metadata': {
                    'config': config,
                    'field_availability': self.data_transformer.get_field_availability(),
                    'entity_business_metrics_count': len(entity_business_metrics),
                    'business_concepts': list(next(iter(next(iter(entity_business_metrics.values()), {}).values()), {}).keys()) if entity_business_metrics else []
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard data: {str(e)}")
            raise
    
    def get_chart_data(self, start_date: str, end_date: str, config_key: str, 
                      entity_type: str, entity_id: str, entity_name: str = "Unknown") -> Dict[str, Any]:
        """Get time series chart data for a specific entity"""
        try:
            # Get configuration by hash
            config = self.get_config_by_hash(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found or has no data")
            
            # Get daily data for the entity
            daily_data = self._get_daily_entity_data(start_date, end_date, entity_type, entity_id, config)
            
            # Transform to chart format
            chart_data = self._transform_to_chart_format(daily_data, config)
            
            return {
                'entity_name': entity_name,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date
                },
                'trend_data': chart_data['trend_data'],
                'summary_stats': chart_data['summary_stats'],
                'field_availability': self.data_transformer.get_field_availability()
            }
            
        except Exception as e:
            logger.error(f"Error getting chart data: {str(e)}")
            raise

    def _get_daily_entity_data(self, start_date: str, end_date: str, entity_type: str, 
                              entity_id: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get daily data for a specific entity"""
        try:
            # Get raw daily data using the string-based API
            raw_data = self.historical_service.export_data_for_config(
                start_date=start_date,
                end_date=end_date,
                fields=','.join(config['fields']),
                breakdowns=''  # No breakdowns for chart data
            )
            
            # Extract the actual data records and filter for the specific entity
            entity_data = []
            entity_field_map = {
                'campaign': 'campaign_id',
                'adset': 'adset_id', 
                'ad': 'ad_id'
            }
            
            entity_field = entity_field_map.get(entity_type)
            if not entity_field:
                raise ValueError(f"Invalid entity type: {entity_type}")
            
            if raw_data and 'data' in raw_data:
                for day_data in raw_data['data']:
                    if 'data' in day_data and 'data' in day_data['data']:
                        # Check the structure of day_data['data']['data']
                        data_container = day_data['data']['data']
                        
                        if isinstance(data_container, dict) and 'data' in data_container:
                            # Structure: day_data['data']['data']['data'] contains the actual records
                            records = data_container['data']
                        elif isinstance(data_container, list):
                            # Structure: day_data['data']['data'] directly contains the records
                            records = data_container
                        else:
                            logger.warning(f"Unexpected data container structure: {type(data_container)}")
                            continue
                            
                        # Process the actual records
                        for record in records:
                            if isinstance(record, dict):
                                row = record
                                if str(row.get(entity_field)) == str(entity_id):
                                    # Add the date from the day_data level
                                    row['date_start'] = day_data['date']
                                    entity_data.append(row)
                            else:
                                logger.warning(f"Unexpected record type: {type(record)}")
            
            return entity_data
            
        except Exception as e:
            logger.error(f"Error getting daily entity data: {str(e)}")
            raise

    def _transform_to_chart_format(self, daily_data: List[Dict[str, Any]], 
                                  config: Dict[str, Any]) -> Dict[str, Any]:
        """Transform daily data to chart format"""
        try:
            # Group by date and aggregate
            date_aggregates = {}
            
            for row in daily_data:
                date_str = row.get('date_start', '')
                if not date_str:
                    continue
                    
                if date_str not in date_aggregates:
                    date_aggregates[date_str] = {
                        'date': date_str,
                        'spend': 0,
                        'impressions': 0,
                        'clicks': 0,
                        'total_trials_started': 0,
                        'total_conversions': 0,
                        'revenue_usd': 0,
                        'total_refunds_usd': 0
                    }
                
                # Aggregate numeric fields
                agg = date_aggregates[date_str]
                agg['spend'] += float(row.get('spend', 0))
                agg['impressions'] += int(row.get('impressions', 0))
                agg['clicks'] += int(row.get('clicks', 0))
                agg['total_trials_started'] += int(row.get('total_trials_started', 0))
                agg['total_conversions'] += int(row.get('total_conversions', 0))
                agg['revenue_usd'] += float(row.get('revenue_usd', 0))
                agg['total_refunds_usd'] += float(row.get('total_refunds_usd', 0))
            
            # Convert to sorted list and calculate derived metrics
            trend_data = []
            for date_str in sorted(date_aggregates.keys()):
                data_point = date_aggregates[date_str]
                
                # Calculate derived metrics
                data_point['roas'] = data_point['revenue_usd'] / data_point['spend'] if data_point['spend'] > 0 else 0
                data_point['ctr'] = data_point['clicks'] / data_point['impressions'] if data_point['impressions'] > 0 else 0
                data_point['cpc'] = data_point['spend'] / data_point['clicks'] if data_point['clicks'] > 0 else 0
                data_point['cpm'] = (data_point['spend'] / data_point['impressions']) * 1000 if data_point['impressions'] > 0 else 0
                
                trend_data.append(data_point)
            
            # Calculate summary statistics
            if trend_data:
                total_spend = sum(d['spend'] for d in trend_data)
                total_revenue = sum(d['revenue_usd'] for d in trend_data)
                total_impressions = sum(d['impressions'] for d in trend_data)
                total_clicks = sum(d['clicks'] for d in trend_data)
                total_trials = sum(d['total_trials_started'] for d in trend_data)
                total_conversions = sum(d['total_conversions'] for d in trend_data)
                
                summary_stats = {
                    'total_spend': total_spend,
                    'total_revenue': total_revenue,
                    'total_impressions': total_impressions,
                    'total_clicks': total_clicks,
                    'total_trials': total_trials,
                    'total_conversions': total_conversions,
                    'avg_roas': total_revenue / total_spend if total_spend > 0 else 0,
                    'avg_ctr': total_clicks / total_impressions if total_impressions > 0 else 0,
                    'avg_cpc': total_spend / total_clicks if total_clicks > 0 else 0,
                    'avg_cpm': (total_spend / total_impressions) * 1000 if total_impressions > 0 else 0
                }
            else:
                summary_stats = {}
            
            return {
                'trend_data': trend_data,
                'summary_stats': summary_stats
            }
            
        except Exception as e:
            logger.error(f"Error transforming to chart format: {str(e)}")
            raise

    def _check_data_coverage(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Check data coverage and trigger collection for missing dates if needed
        
        Returns:
            Dictionary with coverage information and collection status
        """
        try:
            # Get current data coverage using the correct API
            coverage = self.historical_service.get_data_coverage(
                fields=','.join(self.data_transformer.get_field_availability()),
                breakdowns=None,
                start_date=start_date,
                end_date=end_date
            )
            
            # Check if we have missing dates using the correct API
            missing_dates = self.historical_service.get_missing_dates_for_config(
                start_date=start_date,
                end_date=end_date,
                fields=','.join(self.data_transformer.get_field_availability()),
                breakdowns=None
            )
            
            # Calculate coverage percentage
            total_days = len(self.historical_service.get_date_range_list(start_date, end_date))
            days_with_data = total_days - len(missing_dates)
            coverage_percentage = (days_with_data / total_days * 100) if total_days > 0 else 0
            
            coverage_info = {
                'total_days_requested': total_days,
                'days_with_data': days_with_data,
                'missing_days': len(missing_dates),
                'missing_dates': missing_dates[:10] if missing_dates else [],  # Limit to first 10 for display
                'coverage_percentage': coverage_percentage,
                'collection_triggered': False,
                'collection_job_id': None
            }
            
            # If we have missing dates, trigger data collection
            if missing_dates:
                logger.info(f"Found {len(missing_dates)} missing dates, triggering collection...")
                
                try:
                    job_id = self.historical_service.start_collection_job(
                        start_date=start_date,
                        end_date=end_date,
                        fields=','.join(self.data_transformer.get_field_availability()),
                        breakdowns=None
                    )
                    
                    coverage_info['collection_triggered'] = True
                    coverage_info['collection_job_id'] = job_id
                    logger.info(f"Started collection job: {job_id}")
                except Exception as job_error:
                    logger.warning(f"Failed to start collection job: {job_error}")
            
            return coverage_info
            
        except Exception as e:
            logger.error(f"Error checking data coverage: {str(e)}", exc_info=True)
            return {
                'total_days_requested': 0,
                'days_with_data': 0,
                'missing_days': 0,
                'coverage_percentage': 0,
                'collection_triggered': False,
                'error': str(e)
            }
    
    def get_collection_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a data collection job"""
        try:
            status = self.historical_service.get_job_status(job_id)
            return {
                'success': True,
                'job_status': status
            }
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_data_coverage_summary(self, config_key: str = 'basic_ad_data') -> Dict[str, Any]:
        """Get a summary of data coverage for a configuration"""
        try:
            config = self.config_manager.get_config(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found")
            
            fields_string = ','.join(config['fields'])
            breakdowns_string = ','.join(config['breakdowns']) if config['breakdowns'] else None
            
            # Get summary statistics from the historical service
            coverage = self.historical_service.get_data_coverage(
                fields=fields_string,
                breakdowns=breakdowns_string
            )
            
            return {
                'success': True,
                'config_key': config_key,
                'config_name': config['name'],
                'summary': {
                    'total_days_available': coverage.get('total_days', 0),
                    'date_range': {
                        'earliest_date': coverage.get('earliest_date'),
                        'latest_date': coverage.get('latest_date')
                    },
                    'total_records': coverage.get('total_records', 0)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting coverage summary: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def trigger_manual_collection(self, start_date: str, end_date: str, config_key: str = 'basic_ad_data') -> Dict[str, Any]:
        """Manually trigger data collection for a date range"""
        try:
            config = self.config_manager.get_config(config_key)
            if not config:
                raise ValueError(f"Configuration '{config_key}' not found")
            
            fields_string = ','.join(config['fields'])
            breakdowns_string = ','.join(config['breakdowns']) if config['breakdowns'] else None
            
            job_id = self.historical_service.start_collection_job(
                start_date=start_date,
                end_date=end_date,
                fields=fields_string,
                breakdowns=breakdowns_string
            )
            
            return {
                'success': True,
                'message': f'Collection job started for {start_date} to {end_date}',
                'job_id': job_id,
                'config_name': config['name']
            }
                
        except Exception as e:
            logger.error(f"Error triggering manual collection: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            } 