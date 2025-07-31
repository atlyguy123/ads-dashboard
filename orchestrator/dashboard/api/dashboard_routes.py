# Dashboard API Routes
# 
# Provides RESTful API endpoints for dashboard functionality

from flask import Blueprint, jsonify, request
import logging
from datetime import datetime

from ..services.dashboard_service import DashboardService
from ..services.analytics_query_service import AnalyticsQueryService, QueryConfig

# Import timezone utilities for consistent timezone handling
from ...utils.timezone_utils import now_in_timezone

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

# Initialize the dashboard service
dashboard_service = DashboardService()

# Initialize the analytics query service with thread safety
import threading
analytics_service = AnalyticsQueryService()
analytics_lock = threading.Lock()

@dashboard_bp.route('/configurations', methods=['GET'])
def get_configurations():
    """Get all available data configurations for the dropdown"""
    try:
        configurations = dashboard_service.get_available_configurations()
        return jsonify({
            'success': True,
            'configurations': configurations
        })
    except Exception as e:
        logger.error(f"Error getting configurations: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/data', methods=['POST'])
def get_dashboard_data():
    """
    Get dashboard data for specified parameters
    
    Expected JSON payload:
    {
        "start_date": "2025-05-01",
        "end_date": "2025-05-31", 
        "config_key": "basic_ad_data"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided in request'
            }), 400
        
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        config_key = data.get('config_key', 'basic_ad_data')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start_date and end_date are required'
            }), 400
        
        # Get dashboard data
        result = dashboard_service.get_dashboard_data(
            start_date=start_date,
            end_date=end_date,
            config_key=config_key
        )
        
        return jsonify({
            'success': True,
            'data': result['data'],
            'metadata': result['metadata']
        })
            
    except Exception as e:
        logger.error(f"Error in get_dashboard_data: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/collection/trigger', methods=['POST'])
def trigger_collection():
    """
    Manually trigger data collection for a date range
    
    Expected JSON payload:
    {
        "start_date": "2025-05-01",
        "end_date": "2025-05-31",
        "config_key": "basic_ad_data"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided in request'
            }), 400
        
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        config_key = data.get('config_key', 'basic_ad_data')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start_date and end_date are required'
            }), 400
        
        result = dashboard_service.trigger_manual_collection(
            start_date=start_date,
            end_date=end_date,
            config_key=config_key
        )
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error triggering collection: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/collection/status/<job_id>', methods=['GET'])
def get_collection_status(job_id):
    """Get the status of a data collection job"""
    try:
        result = dashboard_service.get_collection_job_status(job_id)
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting collection status: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/coverage/<config_key>', methods=['GET'])
def get_coverage_summary(config_key):
    """Get data coverage summary for a configuration"""
    try:
        result = dashboard_service.get_data_coverage_summary(config_key)
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting coverage summary: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'dashboard',
        'timestamp': now_in_timezone().isoformat()
    })



@dashboard_bp.route('/chart-data', methods=['POST'])
def get_chart_data():
    """Get chart data for a specific campaign/adset/ad"""
    print("🚨🚨🚨 OLD CHART DATA ENDPOINT CALLED! 🚨🚨🚨")
    print(f"🚨 REQUEST DATA: {request.get_json()}")
    try:
        data = request.get_json()
        
        # Validate required parameters
        required_params = ['start_date', 'end_date', 'config_key', 'entity_type', 'entity_id']
        for param in required_params:
            if param not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required parameter: {param}'
                }), 400
        
        # Get chart data
        result = dashboard_service.get_chart_data(
            start_date=data['start_date'],
            end_date=data['end_date'],
            config_key=data['config_key'],
            entity_type=data['entity_type'],
            entity_id=data['entity_id'],
            entity_name=data.get('entity_name', 'Unknown')
        )
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Error getting chart data: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/analytics/data', methods=['POST'])
def get_analytics_data():
    """
    Fast dashboard data retrieval with JOIN queries between meta_analytics.db and mixpanel_analytics.db
    
    Expected JSON payload:
    {
        "start_date": "2025-05-01",
        "end_date": "2025-05-31",
        "breakdown": "all",  // 'all', 'country', 'region', 'device'
        "group_by": "ad",    // 'campaign', 'adset', 'ad'
        "include_mixpanel": true
    }
    """
    try:
        # Use silent=True to prevent JSON decode errors from crashing the endpoint
        data = request.get_json(force=True, silent=True)
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided in request'
            }), 400
        
        # Validate required parameters
        required_params = ['start_date', 'end_date']
        for param in required_params:
            if param not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required parameter: {param}'
                }), 400
        
        # Extract parameters with defaults
        start_date = data['start_date']
        end_date = data['end_date']
        breakdown = data.get('breakdown', 'all')
        group_by = data.get('group_by', 'ad')
        include_mixpanel = data.get('include_mixpanel', True)
        
        # Validate breakdown parameter
        valid_breakdowns = ['all', 'country', 'region', 'device']
        if breakdown not in valid_breakdowns:
            return jsonify({
                'success': False,
                'error': f'Invalid breakdown parameter. Must be one of: {valid_breakdowns}'
            }), 400
        
        # Validate group_by parameter
        valid_group_by = ['campaign', 'adset', 'ad']
        if group_by not in valid_group_by:
            return jsonify({
                'success': False,
                'error': f'Invalid group_by parameter. Must be one of: {valid_group_by}'
            }), 400
        
        # Create query configuration
        config = QueryConfig(
            breakdown=breakdown,
            start_date=start_date,
            end_date=end_date,
            group_by=group_by,
            include_mixpanel=include_mixpanel
        )
        
        # Execute analytics query with thread safety
        with analytics_lock:
            result = analytics_service.execute_analytics_query(config)
        
        # 🔍 CRITICAL DEBUG - Log the exact response being sent to frontend
        logger.info("🔍 ANALYTICS ENDPOINT - CRITICAL RESPONSE DEBUG:")
        if result.get('success') and result.get('data'):
            data_records = result['data']
            logger.info(f"🔍 Total records being sent: {len(data_records)}")
            
            if data_records:
                first_record = data_records[0]
                logger.info(f"🔍 First record keys: {list(first_record.keys())}")
                logger.info(f"🔍 Has estimated_revenue_adjusted: {'estimated_revenue_adjusted' in first_record}")
                logger.info(f"🔍 estimated_revenue_adjusted value: {first_record.get('estimated_revenue_adjusted', 'MISSING')}")
                logger.info(f"🔍 estimated_revenue_usd value: {first_record.get('estimated_revenue_usd', 'MISSING')}")
                logger.info(f"🔍 spend value: {first_record.get('spend', 'MISSING')}")
                logger.info(f"🔍 campaign_name: {first_record.get('campaign_name', 'MISSING')}")
        else:
            logger.info(f"🔍 No data in result or query failed: {result}")
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error in get_analytics_data: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/analytics/chart-data', methods=['GET'])
def get_analytics_chart_data():
    """
    Detailed daily metrics for sparkline charts
    
    Query parameters:
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - breakdown: Breakdown type ('all', 'country', 'region', 'device')
    - entity_type: Entity type ('campaign', 'adset', 'ad')
    - entity_id: Entity ID
    """

    try:
        # Handle the case where frontend sends Content-Type: application/json on GET requests
        # This is a workaround for a frontend bug where makeRequest always adds this header
        if request.content_type == 'application/json' and request.method == 'GET':
            # Ignore the content-type for GET requests - they should use query parameters
            logger.debug("GET request with application/json content-type header detected - using query parameters")
            
            # Bypass Flask's automatic JSON parsing by manually processing query parameters
            # This prevents the "Failed to decode JSON object" error
            pass
        
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        breakdown = request.args.get('breakdown', 'all')
        entity_type = request.args.get('entity_type')
        entity_id = request.args.get('entity_id')
        
        # Debug logging
        logger.info(f"Chart data request: entity_type={entity_type}, entity_id={entity_id}, start_date={start_date}, end_date={end_date}, breakdown={breakdown}")
        
        # Validate required parameters
        required_params = {
            'start_date': start_date,
            'end_date': end_date,
            'entity_type': entity_type,
            'entity_id': entity_id
        }
        
        for param_name, param_value in required_params.items():
            if not param_value:
                error_msg = f'Missing required parameter: {param_name}'
                logger.error(f"Chart data validation error: {error_msg}")
                return jsonify({
                    'success': False,
                    'error': error_msg
                }), 400
        
        # Validate breakdown parameter
        valid_breakdowns = ['all', 'country', 'region', 'device']
        if breakdown not in valid_breakdowns:
            error_msg = f'Invalid breakdown parameter. Must be one of: {valid_breakdowns}'
            logger.error(f"Chart data validation error: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Validate entity_type parameter
        valid_entity_types = ['campaign', 'adset', 'ad']
        if entity_type not in valid_entity_types:
            error_msg = f'Invalid entity_type parameter. Must be one of: {valid_entity_types}'
            logger.error(f"Chart data validation error: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Get chart data with comprehensive error handling and thread safety
        try:
            # Create query configuration
            config = QueryConfig(
                breakdown=breakdown,
                start_date=start_date,
                end_date=end_date,
                include_mixpanel=True
            )
            
            # Use analytics service to get real chart data
            with analytics_lock:
                result = analytics_service.get_chart_data(config, entity_type, entity_id)
            
            if result.get('success'):
                return jsonify(result)
            else:
                # If analytics service failed, return the error
                error_msg = result.get('error', 'Unknown analytics service error')
                return jsonify(result), 500
            
        except Exception as analytics_error:
            error_msg = f"Analytics service exception: {str(analytics_error)}"
            logger.error(f"Chart data analytics error for {entity_type} {entity_id}: {error_msg}", exc_info=True)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        error_msg = f"Chart data endpoint exception: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500


@dashboard_bp.route('/analytics/user-details', methods=['GET'])
def get_user_details_for_tooltip():
    """
    Get individual user details for tooltip display on conversion rates
    
    Query parameters:
    - entity_type: Entity type ('campaign', 'adset', 'ad')
    - entity_id: Entity ID
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - breakdown: Breakdown type ('all', 'country', 'device', etc.)
    - breakdown_value: Specific breakdown value if applicable (e.g., 'US', 'mobile')
    - metric_type: Type of metric ('trial_conversion_rate', 'avg_trial_refund_rate', 'purchase_refund_rate')
    """
    try:
        # Get query parameters
        entity_type = request.args.get('entity_type')
        entity_id = request.args.get('entity_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        breakdown = request.args.get('breakdown', 'all')
        breakdown_value = request.args.get('breakdown_value')
        metric_type = request.args.get('metric_type', 'trial_conversion_rate')
        
        # Debug logging
        logger.info(f"User details request: entity_type={entity_type}, entity_id={entity_id}, start_date={start_date}, end_date={end_date}, breakdown={breakdown}, breakdown_value={breakdown_value}, metric_type={metric_type}")
        
        # Validate required parameters
        required_params = {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'start_date': start_date,
            'end_date': end_date
        }
        
        for param_name, param_value in required_params.items():
            if not param_value:
                error_msg = f'Missing required parameter: {param_name}'
                logger.error(f"User details validation error: {error_msg}")
                return jsonify({
                    'success': False,
                    'error': error_msg
                }), 400
        
        # Validate entity_type parameter
        valid_entity_types = ['campaign', 'adset', 'ad']
        if entity_type not in valid_entity_types:
            error_msg = f'Invalid entity_type parameter. Must be one of: {valid_entity_types}'
            logger.error(f"User details validation error: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Validate breakdown parameter
        valid_breakdowns = ['all', 'country', 'region', 'device']
        if breakdown not in valid_breakdowns:
            error_msg = f'Invalid breakdown parameter. Must be one of: {valid_breakdowns}'
            logger.error(f"User details validation error: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Get user details with thread safety
        try:
            with analytics_lock:
                result = analytics_service.get_user_details_for_tooltip(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    start_date=start_date,
                    end_date=end_date,
                    breakdown=breakdown,
                    breakdown_value=breakdown_value,
                    metric_type=metric_type
                )
            
            if result.get('success'):
                return jsonify(result)
            else:
                error_msg = result.get('error', 'Unknown analytics service error')
                logger.error(f"User details analytics error: {error_msg}")
                return jsonify(result), 500
            
        except Exception as analytics_error:
            error_msg = f"Analytics service exception: {str(analytics_error)}"
            logger.error(f"User details analytics error for {entity_type} {entity_id}: {error_msg}", exc_info=True)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        error_msg = f"User details endpoint exception: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500

@dashboard_bp.route('/analytics/date-range', methods=['GET'])
def get_available_date_range():
    """Get the available date range from the analytics data"""
    try:
        with analytics_lock:
            logger.info("Getting available date range for analytics data")
            result = analytics_service.get_available_date_range()
            
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting date range: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@dashboard_bp.route('/analytics/segments', methods=['POST'])
def get_segment_performance():
    """
    Get segment performance data for conversion rate analysis
    
    Expected JSON payload:
    {
        "product_id": "optional_product_filter",
        "store": "optional_store_filter", 
        "economic_tier": "optional_tier_filter",
        "country": "optional_country_filter",
        "region": "optional_region_filter",
        "accuracy_score": "optional_accuracy_filter",
        "min_user_count": 0,
        "sort_column": "trial_conversion_rate",
        "sort_direction": "desc"
    }
    """
    try:
        data = request.get_json() or {}
        
        logger.info(f"Getting segment performance data with filters: {data}")
        
        # Extract filters
        filters = {
            'product_id': data.get('product_id', ''),
            'store': data.get('store', ''),
            'economic_tier': data.get('economic_tier', ''),
            'country': data.get('country', ''),
            'region': data.get('region', ''),
            'accuracy_score': data.get('accuracy_score', ''),
            'min_user_count': data.get('min_user_count', 0)
        }
        
        # Extract sorting
        sort_column = data.get('sort_column', 'trial_conversion_rate')
        sort_direction = data.get('sort_direction', 'desc')
        
        # Validate sort column
        valid_sort_columns = [
            'product_id', 'store', 'economic_tier', 'country', 'region',
            'user_count', 'trial_conversion_rate', 'trial_converted_to_refund_rate',
            'initial_purchase_to_refund_rate', 'accuracy_score'
        ]
        
        if sort_column not in valid_sort_columns:
            sort_column = 'trial_conversion_rate'
            
        if sort_direction not in ['asc', 'desc']:
            sort_direction = 'desc'
        
        with analytics_lock:
            result = analytics_service.get_segment_performance(
                filters=filters,
                sort_column=sort_column,
                sort_direction=sort_direction
            )
            
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting segment performance: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/analytics/overview-roas-chart', methods=['GET'])
def get_overview_roas_chart():
    """
    Get overview ROAS sparkline data for dashboard summary
    
    Query parameters:
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD) 
    - breakdown: Breakdown type ('all', 'country', 'device', etc.)
    """
    try:
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        breakdown = request.args.get('breakdown', 'all')
        
        logger.info(f"Getting overview ROAS chart data: start_date={start_date}, end_date={end_date}, breakdown={breakdown}")
        
        # Validate required parameters
        if not start_date or not end_date:
            error_msg = 'Missing required parameters: start_date and end_date'
            logger.error(f"Overview ROAS chart validation error: {error_msg}")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Get overview ROAS chart data with thread safety
        try:
            with analytics_lock:
                result = analytics_service.get_overview_roas_chart_data(
                    start_date=start_date,
                    end_date=end_date,
                    breakdown=breakdown
                )
            
            if result.get('success'):
                return jsonify(result)
            else:
                error_msg = result.get('error', 'Unknown analytics service error')
                logger.error(f"Overview ROAS chart analytics error: {error_msg}")
                return jsonify(result), 500
            
        except Exception as analytics_error:
            error_msg = f"Analytics service exception: {str(analytics_error)}"
            logger.error(f"Overview ROAS chart analytics error: {error_msg}", exc_info=True)
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        error_msg = f"Overview ROAS chart endpoint exception: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500