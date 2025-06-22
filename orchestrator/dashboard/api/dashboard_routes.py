# Dashboard API Routes
# 
# Provides RESTful API endpoints for dashboard functionality

from flask import Blueprint, jsonify, request
import logging
from datetime import datetime

from ..services.dashboard_service import DashboardService
from ..services.analytics_query_service import AnalyticsQueryService, QueryConfig

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
        'timestamp': datetime.now().isoformat()
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
    print("🔥🔥🔥 CHART DATA ENDPOINT CALLED! 🔥🔥🔥")
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
        
        print(f"🔥 RAW REQUEST ARGS: {dict(request.args)}")
        
        # Debug logging
        logger.info(f"Chart data request: entity_type={entity_type}, entity_id={entity_id}, start_date={start_date}, end_date={end_date}, breakdown={breakdown}")
        
        # CRITICAL DEBUG: Log exactly what we're receiving
        print(f"🔍 SPARKLINE DEBUG - Received parameters:")
        print(f"   entity_type: {entity_type}")
        print(f"   entity_id: {entity_id}")
        print(f"   start_date: {start_date}")
        print(f"   end_date: {end_date}")
        print(f"   breakdown: {breakdown}")
        
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
            
            print(f"🔍 ANALYTICS SERVICE RESULT: {result.get('success', False)}")
            if result.get('success'):
                chart_data = result.get('chart_data', [])
                print(f"🔍 RETURNING REAL DATA: {len(chart_data)} days")
                if chart_data:
                    print(f"🔍 FIRST RECORD BEING SENT: {chart_data[0]}")
                    print(f"🔍 ALL DAILY_ROAS BEING SENT: {[r.get('daily_roas', 0) for r in chart_data]}")
                return jsonify(result)
            else:
                # If analytics service failed, return the error
                error_msg = result.get('error', 'Unknown analytics service error')
                print(f"🔍 ANALYTICS SERVICE ERROR: {error_msg}")
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