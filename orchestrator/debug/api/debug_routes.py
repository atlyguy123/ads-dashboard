"""
Debug API Routes

Clean, modular debug routes for the pipeline debug tools.
"""

from flask import Blueprint, request, jsonify, render_template
import logging

logger = logging.getLogger(__name__)

# Create the debug blueprint
debug_bp = Blueprint('debug', __name__)

# Import debug modules
try:
    from ..modules.conversion_rates_debug.handlers import *
    from ..modules.price_bucket_debug.handlers import *
    from ..modules.value_estimation_debug.handlers import *
except ImportError as e:
    logger.warning(f"Could not import debug handlers: {e}")

@debug_bp.route('/debug')
def debug_page():
    """Render the debug page template"""
    return render_template('debug.html')

# Conversion Rates Debug Routes
@debug_bp.route('/api/debug/conversion-rates/overview', methods=['POST'])
def conversion_rates_overview():
    """Get conversion rates overview data with statistics and validation"""
    try:
        from ..modules.conversion_rates_debug.handlers import handle_get_overview_data
        data = request.get_json() or {}
        result = handle_get_overview_data(data)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in conversion rates overview: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/conversion-rates/cohort-tree', methods=['POST'])
def conversion_rates_cohort_tree():
    """Get conversion rates cohort tree data with filtering"""
    try:
        from ..modules.conversion_rates_debug.handlers import handle_get_cohort_tree
        data = request.get_json() or {}
        result = handle_get_cohort_tree(data)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in conversion rates cohort tree: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/conversion-rates/validate', methods=['POST'])
def conversion_rates_validate():
    """Validate conversion rate data consistency"""
    try:
        from ..modules.conversion_rates_debug.handlers import handle_validate_data
        data = request.get_json() or {}
        result = handle_validate_data(data)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in conversion rates validation: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Price Bucket Debug Routes
@debug_bp.route('/api/debug/price-bucket/overview', methods=['POST'])
def price_bucket_overview():
    """Get price bucket overview data with statistics and table data"""
    try:
        from ..modules.price_bucket_debug.handlers import handle_get_overview_data
        data = request.get_json() or {}
        result = handle_get_overview_data(data)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in price bucket overview: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/price-bucket/search', methods=['POST'])
def price_bucket_search():
    """Search and filter price bucket data"""
    try:
        from ..modules.price_bucket_debug.handlers import handle_search_data
        data = request.get_json() or {}
        result = handle_search_data(data)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in price bucket search: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Value Estimation Debug Routes
@debug_bp.route('/api/debug/value-estimation/overview', methods=['POST'])
def value_estimation_overview():
    """Get value estimation overview data with statistics"""
    try:
        from ..modules.value_estimation_debug.handlers import load_overview
        result = load_overview()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in value estimation overview: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/value-estimation/examples', methods=['POST'])
def value_estimation_examples():
    """Load examples for all current_status and value_status"""
    try:
        from ..modules.value_estimation_debug.handlers import load_status_examples
        result = load_status_examples()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in value estimation examples: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/value-estimation/examples/refresh', methods=['POST'])
def value_estimation_refresh_status():
    """Refresh examples for a specific status"""
    try:
        data = request.get_json()
        status_type = data.get('status_type')  # 'current_status' or 'value_status'
        status_value = data.get('status_value')
        
        if not status_type or not status_value:
            return jsonify({'success': False, 'error': 'status_type and status_value are required'})
        
        from ..modules.value_estimation_debug.handlers import load_single_status_examples
        result = load_single_status_examples(status_type, status_value)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error refreshing status examples: {e}")
        return jsonify({'success': False, 'error': str(e)})

@debug_bp.route('/api/debug/value-estimation/validate', methods=['POST'])
def value_estimation_validate():
    """Validate value estimation calculations"""
    try:
        from ..modules.value_estimation_debug.handlers import validate_calculations
        result = validate_calculations()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in value estimation validation: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Generic module action handler (for backward compatibility)
@debug_bp.route('/api/debug/modules/<module_name>/actions/<action_name>', methods=['POST'])
def handle_module_action(module_name, action_name):
    """Handle generic module actions"""
    try:
        data = request.get_json() or {}
        
        # This will call the appropriate handler function when implemented
        # For now, return placeholder data
        return jsonify({
            'success': True,
            'data': {
                'module': module_name,
                'action': action_name,
                'message': f'Executed {action_name} on {module_name}',
                'placeholder': True
            }
        })
    except Exception as e:
        logger.error(f"Error in module action {module_name}.{action_name}: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Debug modules info
@debug_bp.route('/api/debug/modules', methods=['GET'])
def get_debug_modules():
    """Get available debug modules"""
    try:
        modules = [
            {
                'name': 'conversion_rates_debug',
                'display_name': 'Conversion Rates Debug',
                'description': 'Debug conversion rate calculations',
                'status': 'ready'
            },
            {
                'name': 'price_bucket_debug',
                'display_name': 'Price Bucket Debug',
                'description': 'Debug price bucket assignments',
                'status': 'ready'
            },
            {
                'name': 'value_estimation_debug',
                'display_name': 'Value Estimation Debug',
                'description': 'Debug value estimation calculations',
                'status': 'ready'
            }
        ]
        
        return jsonify({
            'success': True,
            'data': {
                'modules': modules,
                'count': len(modules)
            }
        })
    except Exception as e:
        logger.error(f"Error getting debug modules: {e}")
        return jsonify({'success': False, 'error': str(e)}) 