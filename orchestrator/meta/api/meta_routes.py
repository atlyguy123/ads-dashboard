"""
Meta API Routes

This module contains all Flask routes related to Meta API functionality.
Includes both live API testing and historical data collection endpoints.
"""

from flask import Blueprint, request, jsonify
from ..services.meta_service import fetch_meta_data, check_async_job_status, get_async_job_results
from ..services.meta_historical_service import meta_historical_service

# Create Blueprint for Meta routes
meta_bp = Blueprint('meta', __name__, url_prefix='/api/meta')

@meta_bp.route('/fetch', methods=['POST'])
def fetch_meta_data_endpoint():
    """Fetch data from Meta API (live testing) - supports both sync and async modes"""
    try:
        data = request.get_json()
        
        # Extract parameters
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        time_increment = data.get('time_increment', 1)
        fields = data.get('fields')
        breakdowns = data.get('breakdowns')
        action_breakdowns = data.get('action_breakdowns')
        filtering = data.get('filtering')
        use_async = data.get('use_async')  # Allow forcing async mode
        
        # Validate required parameters
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        if not fields:
            return jsonify({'error': 'fields parameter is required'}), 400
        
        # Call the meta service (now handles async automatically)
        result, error = fetch_meta_data(
            start_date=start_date,
            end_date=end_date,
            time_increment=time_increment,
            fields=fields,
            breakdowns=breakdowns,
            action_breakdowns=action_breakdowns,
            use_async=use_async
        )
        
        if error:
            return jsonify({'error': error}), 400
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/job/<report_run_id>/status', methods=['GET'])
def check_job_status(report_run_id):
    """Check the status of an async Meta API job"""
    try:
        status_info, error = check_async_job_status(report_run_id)
        
        if error:
            return jsonify({'error': error}), 400
        
        return jsonify(status_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/job/<report_run_id>/results', methods=['GET'])
def get_job_results(report_run_id):
    """Get results from a completed async Meta API job"""
    try:
        use_file_url = request.args.get('use_file_url', 'false').lower() == 'true'
        
        results, error = get_async_job_results(report_run_id, use_file_url)
        
        if error:
            return jsonify({'error': error}), 400
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/start', methods=['POST'])
def start_historical_collection():
    """Start historical data collection job"""
    try:
        data = request.get_json()
        
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        fields = data.get('fields')
        breakdowns = data.get('breakdowns')
        
        if not all([start_date, end_date, fields]):
            return jsonify({'error': 'start_date, end_date, and fields are required'}), 400
        
        job_id = meta_historical_service.start_collection_job(
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            breakdowns=breakdowns
        )
        
        return jsonify({'job_id': job_id, 'status': 'started'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/jobs/<job_id>/status', methods=['GET'])
def get_job_status(job_id):
    """Get status of a historical collection job"""
    try:
        status = meta_historical_service.get_job_status(job_id)
        if status is None:
            return jsonify({'error': 'Job not found'}), 404
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/jobs/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running historical collection job"""
    try:
        success = meta_historical_service.cancel_job(job_id)
        if not success:
            return jsonify({'error': 'Job not found or cannot be cancelled'}), 404
        return jsonify({'status': 'cancelled'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/jobs', methods=['GET'])
def list_jobs():
    """List all historical collection jobs"""
    try:
        jobs = meta_historical_service.list_jobs()
        return jsonify({'jobs': jobs})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/configurations', methods=['GET'])
def get_configurations():
    """Get all stored field/breakdown configurations"""
    try:
        configs = meta_historical_service.get_configurations()
        return jsonify(configs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/coverage', methods=['GET'])
def get_data_coverage():
    """Get data coverage information"""
    try:
        fields = request.args.get('fields')
        breakdowns = request.args.get('breakdowns')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not fields:
            return jsonify({'error': 'fields parameter is required'}), 400
        
        coverage = meta_historical_service.get_data_coverage(
            fields=fields,
            breakdowns=breakdowns,
            start_date=start_date,
            end_date=end_date
        )
        
        return jsonify(coverage)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/missing-dates', methods=['GET'])
def get_missing_dates():
    """Get missing dates for a configuration"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        fields = request.args.get('fields')
        breakdowns = request.args.get('breakdowns')
        
        if not all([start_date, end_date, fields]):
            return jsonify({'error': 'start_date, end_date, and fields are required'}), 400
        
        missing_dates = meta_historical_service.get_missing_dates_for_config(
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            breakdowns=breakdowns
        )
        
        return jsonify({'missing_dates': missing_dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/export', methods=['GET'])
def export_historical_data():
    """Export historical data"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        fields = request.args.get('fields')
        breakdowns = request.args.get('breakdowns')
        entity_type = request.args.get('entity_type')  # 'ad', 'adset', 'campaign'
        entity_id = request.args.get('entity_id')      # specific ID
        
        if not all([start_date, end_date, fields]):
            return jsonify({'error': 'start_date, end_date, and fields are required'}), 400
        
        data = meta_historical_service.export_data_for_config(
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            breakdowns=breakdowns,
            entity_type=entity_type,
            entity_id=entity_id
        )
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/historical/configurations/<config_hash>', methods=['DELETE'])
def delete_historical_configuration(config_hash):
    """Delete all historical data for a specific configuration"""
    try:
        success = meta_historical_service.delete_configuration_data(config_hash)
        if not success:
            return jsonify({'error': 'Configuration not found or could not be deleted'}), 404
        return jsonify({'status': 'deleted', 'config_hash': config_hash})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/action-mappings', methods=['GET'])
def get_action_mappings():
    """Get current action mappings"""
    try:
        mappings = meta_historical_service.get_action_mappings()
        return jsonify({'mappings': mappings})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@meta_bp.route('/action-mappings', methods=['POST'])
def save_action_mappings():
    """Save action mappings"""
    try:
        data = request.get_json()
        mappings = data.get('mappings', {})
        
        if not isinstance(mappings, dict):
            return jsonify({'error': 'Mappings must be a dictionary'}), 400
        
        success = meta_historical_service.save_action_mappings(mappings)
        if not success:
            return jsonify({'error': 'Failed to save action mappings'}), 500
            
        return jsonify({'status': 'saved', 'mappings': mappings})
    except Exception as e:
        return jsonify({'error': str(e)}), 500 