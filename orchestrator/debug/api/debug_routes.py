# Debug API Routes
# 
# Provides RESTful API endpoints for debug functionality

from flask import Blueprint, jsonify, request, render_template
import logging
from datetime import datetime
import importlib.util
import sys
import os

from ..registry import DebugModuleRegistry

logger = logging.getLogger(__name__)

debug_bp = Blueprint('debug', __name__, url_prefix='/api/debug')

# Initialize the debug module registry
debug_registry = DebugModuleRegistry()


@debug_bp.route('/modules', methods=['GET'])
def get_debug_modules():
    """Get all available debug modules"""
    try:
        modules = debug_registry.get_available_modules()
        return jsonify({
            'success': True,
            'modules': modules,
            'count': len(modules)
        })
    except Exception as e:
        logger.error(f"Error getting debug modules: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@debug_bp.route('/modules/<module_name>/interface', methods=['GET'])
def get_module_interface(module_name):
    """Get the HTML interface for a specific debug module"""
    try:
        if not debug_registry.module_exists(module_name):
            return jsonify({
                'success': False,
                'error': f'Debug module "{module_name}" not found'
            }), 404
        
        interface_path = debug_registry.get_module_interface_path(module_name)
        if not interface_path or not os.path.exists(interface_path):
            return jsonify({
                'success': False,
                'error': f'Interface file not found for module "{module_name}"'
            }), 404
        
        # Read the interface HTML file
        with open(interface_path, 'r', encoding='utf-8') as f:
            interface_html = f.read()
        
        return jsonify({
            'success': True,
            'html': interface_html,
            'module_name': module_name
        })
        
    except Exception as e:
        logger.error(f"Error getting interface for module {module_name}: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@debug_bp.route('/modules/<module_name>/actions/<action_name>', methods=['POST'])
def execute_module_action(module_name, action_name):
    """Execute a specific action for a debug module"""
    try:
        if not debug_registry.module_exists(module_name):
            return jsonify({
                'success': False,
                'error': f'Debug module "{module_name}" not found'
            }), 404
        
        # Get the handlers module path
        handlers_path = debug_registry.get_module_handlers_path(module_name)
        if not handlers_path or not os.path.exists(handlers_path):
            return jsonify({
                'success': False,
                'error': f'Handlers file not found for module "{module_name}"'
            }), 404
        
        # Dynamically import the handlers module
        spec = importlib.util.spec_from_file_location(f"{module_name}_handlers", handlers_path)
        handlers_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handlers_module)
        
        # Look for the action function in the handlers module
        action_function_name = f"handle_{action_name}"
        if not hasattr(handlers_module, action_function_name):
            return jsonify({
                'success': False,
                'error': f'Action "{action_name}" not found in module "{module_name}"'
            }), 404
        
        # Execute the action function
        action_function = getattr(handlers_module, action_function_name)
        request_data = request.get_json() or {}
        
        result = action_function(request_data)
        
        # Ensure result is properly formatted
        if not isinstance(result, dict):
            result = {'success': True, 'data': result}
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error executing action {action_name} for module {module_name}: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@debug_bp.route('/refresh', methods=['POST'])
def refresh_debug_modules():
    """Refresh the debug modules registry"""
    try:
        debug_registry.refresh_modules()
        modules = debug_registry.get_available_modules()
        
        return jsonify({
            'success': True,
            'message': 'Debug modules refreshed successfully',
            'modules': modules,
            'count': len(modules)
        })
        
    except Exception as e:
        logger.error(f"Error refreshing debug modules: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@debug_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for debug system"""
    try:
        module_count = debug_registry.get_module_count()
        return jsonify({
            'status': 'healthy',
            'service': 'debug',
            'timestamp': datetime.now().isoformat(),
            'modules_loaded': module_count
        })
    except Exception as e:
        logger.error(f"Error in debug health check: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'unhealthy',
            'service': 'debug',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500 