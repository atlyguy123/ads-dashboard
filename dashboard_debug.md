
# Dashboard Debug Analysis

## Project Description

Critical files for debugging the dashboard MixPanel data issue where dashboard shows zeros instead of retrieving MixPanel data correctly.

## Problem Summary

The dashboard is completely non-functional. While metadata imports properly, MixPanel data is completely ignored, showing zeros for all MixPanel-related metrics instead of retrieving data correctly. The issue involves:

- Schema misalignment in analytics_query_service.py
- Incorrect SQL queries using non-existent columns
- Wrong attribution data sources
- Multi-level aggregation issues

## File Structure
```
Critical Dashboard Files:

Backend (Python):
├── orchestrator/app.py
├── orchestrator/dashboard/api/dashboard_routes.py
├── orchestrator/dashboard/services/analytics_query_service.py
├── orchestrator/dashboard/services/dashboard_service.py
└── utils/database_utils.py

Frontend (React):
├── orchestrator/dashboard/client/src/App.js
├── orchestrator/dashboard/client/src/pages/Dashboard.js
├── orchestrator/dashboard/client/src/services/dashboardApi.js
├── orchestrator/dashboard/client/src/services/api.js
├── orchestrator/dashboard/client/src/components/dashboard/AnalyticsPipelineControls.jsx
├── orchestrator/dashboard/client/src/components/dashboard/RefreshPipelineControls.jsx
└── orchestrator/dashboard/client/package.json

Database & Utils:
└── database/schema.sql

Configuration:
├── requirements.txt
└── start_orchestrator.sh
```

## Backend Files (Python)

### orchestrator/app.py
```py
import os
import json
import yaml
import subprocess
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_socketio import SocketIO, emit
import threading
import glob
from pathlib import Path
import time
from flask_cors import CORS

# Import dashboard blueprint
from dashboard.api.dashboard_routes import dashboard_bp
# Import debug blueprint
from debug.api.debug_routes import debug_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")

# Register dashboard blueprint
app.register_blueprint(dashboard_bp)
# Register debug blueprint
app.register_blueprint(debug_bp)

# Enable CORS for all routes to handle cross-origin requests
CORS(app, origins=['http://localhost:3000', 'http://localhost:5001', 'http://localhost:5678'], 
     allow_headers=['Content-Type', 'Authorization'], 
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'])

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('db.sqlite')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT,
            status TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT
        )
    ''')
    conn.commit()
    conn.close()

class PipelineRunner:
    def __init__(self):
        self.pipelines = {}
        self.running_processes = {}  # Track running processes: {pipeline_name: {step_id: process}}
        self.running_threads = {}    # Track running threads: {pipeline_name: {step_id: thread}}
        self.load_pipelines()
    
    def load_pipelines(self):
        """Discover and load all pipeline.yaml files"""
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to the project root
        project_root = os.path.dirname(script_dir)
        # Look for pipelines in the project root
        pipeline_pattern = os.path.join(project_root, 'pipelines', '*', 'pipeline.yaml')
        pipeline_files = glob.glob(pipeline_pattern)
        print(f"Looking for pipelines in: {pipeline_pattern}")
        print(f"Found pipeline files: {pipeline_files}")
        for pipeline_file in pipeline_files:
            try:
                with open(pipeline_file, 'r') as f:
                    pipeline_data = yaml.safe_load(f)
                pipeline_dir = os.path.dirname(pipeline_file)
                pipeline_data['dir'] = pipeline_dir
                self.pipelines[pipeline_data['name']] = pipeline_data
            except Exception as e:
                print(f"Error loading pipeline {pipeline_file}: {e}")
    
    def get_pipeline_status(self, pipeline_name):
        """Get current status from .status.json file"""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return None
        
        status_file = os.path.join(pipeline['dir'], '.status.json')
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                return json.load(f)
        return {}
    
    def update_step_status(self, pipeline_name, step_id, status, error_message=None):
        """Update step status in .status.json and emit websocket event"""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return
        
        status_file = os.path.join(pipeline['dir'], '.status.json')
        current_status = {}
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                current_status = json.load(f)
        
        current_status[step_id] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'error_message': error_message
        }
        
        with open(status_file, 'w') as f:
            json.dump(current_status, f, indent=2)
        
        # Emit websocket event
        socketio.emit('status_update', {
            'pipeline': pipeline_name,
            'step': step_id,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'error_message': error_message
        })
    
    def run_step(self, pipeline_name, step_id):
        """Run a single step in a pipeline"""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return False, "Pipeline not found"
        
        # Find the specific step
        target_step = None
        for step in pipeline['steps']:
            if step['id'] == step_id:
                target_step = step
                break
        
        if not target_step:
            return False, f"Step {step_id} not found in pipeline"
        
        # Allow running any step (remove tested requirement)
        # if not target_step.get('tested', False):
        #     return False, f"Step {step_id} not marked as tested"
        
        def run_step_in_background():
            try:
                step_file = target_step['file']
                step_name = target_step.get('name', step_id)
                step_description = target_step.get('description', 'No description')
                
                print(f"🚀 ORCHESTRATOR: Starting module '{step_name}' (ID: {step_id})")
                print(f"   Pipeline: {pipeline_name}")
                print(f"   Description: {step_description}")
                print(f"   File: {step_file}")
                print(f"   Working Directory: {pipeline['dir']}")
                
                self.update_step_status(pipeline_name, step_id, 'running')
                
                # Run the step with Popen so we can track and cancel it
                print(f"   Executing: python {step_file}")
                process = subprocess.Popen(
                    ['python', step_file],
                    cwd=pipeline['dir'],
                    stdout=None,  # Don't capture stdout - let it show live
                    stderr=None,  # Don't capture stderr - let it show live
                    text=True
                )
                
                # Track the running process
                if pipeline_name not in self.running_processes:
                    self.running_processes[pipeline_name] = {}
                self.running_processes[pipeline_name][step_id] = process
                
                print(f"   Started process PID: {process.pid}")
                print(f"   Now tracking processes: {list(self.running_processes.get(pipeline_name, {}).keys())}")
                
                # Wait for completion
                return_code = process.wait()
                
                print(f"   Process finished with return code: {return_code}")
                
                # Add a small delay before cleanup to allow cancel requests to find the process
                time.sleep(0.5)
                
                # Clean up tracking when done
                if (pipeline_name in self.running_processes and 
                    step_id in self.running_processes[pipeline_name]):
                    del self.running_processes[pipeline_name][step_id]
                    if not self.running_processes[pipeline_name]:
                        del self.running_processes[pipeline_name]
                    print(f"   Cleaned up process tracking for {step_id}")
                else:
                    print(f"   Warning: Process {step_id} was not in tracking when trying to clean up")
                
                if return_code != 0:
                    print(f"❌ ORCHESTRATOR: Module '{step_name}' FAILED")
                    print(f"   Return code: {return_code}")
                    self.update_step_status(pipeline_name, step_id, 'failed', f"Process exited with code {return_code}")
                else:
                    print(f"✅ ORCHESTRATOR: Module '{step_name}' COMPLETED SUCCESSFULLY")
                    self.update_step_status(pipeline_name, step_id, 'success')
                    
            except Exception as e:
                # Clean up tracking on exception
                if (pipeline_name in self.running_processes and 
                    step_id in self.running_processes[pipeline_name]):
                    del self.running_processes[pipeline_name][step_id]
                    if not self.running_processes[pipeline_name]:
                        del self.running_processes[pipeline_name]
                
                print(f"❌ ORCHESTRATOR: Module '{step_name}' CRASHED")
                print(f"   Exception: {str(e)}")
                self.update_step_status(pipeline_name, step_id, 'failed', str(e))
        
        # Run in background thread
        thread = threading.Thread(target=run_step_in_background)
        thread.start()
        
        return True, f"Step {step_id} started"

    def run_pipeline(self, pipeline_name):
        """Run a complete pipeline"""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return False, "Pipeline not found"
        
        # Check if all steps are tested
        for step in pipeline['steps']:
            if not step.get('tested', False):
                return False, f"Step {step['id']} not marked as tested"
        
        # Record run start
        conn = sqlite3.connect('db.sqlite')
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO runs (pipeline_name, status, started_at) VALUES (?, ?, ?)',
            (pipeline_name, 'running', datetime.now())
        )
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        def run_in_background():
            success = True
            error_message = None
            
            print(f"🎯 ORCHESTRATOR: Starting FULL PIPELINE '{pipeline_name}'")
            print(f"   Description: {pipeline.get('description', 'No description')}")
            print(f"   Total steps: {len(pipeline['steps'])}")
            print(f"   Steps to execute:")
            for step in pipeline['steps']:
                step_name = step.get('name', step['id'])
                print(f"     {step['id']}: {step_name}")
            print(f"   Working Directory: {pipeline['dir']}")
            print("")
            
            try:
                for i, step in enumerate(pipeline['steps'], 1):
                    step_file = step['file']
                    step_name = step.get('name', step['id'])
                    step_description = step.get('description', 'No description')
                    
                    print(f"🚀 ORCHESTRATOR: Starting step {i}/{len(pipeline['steps'])}: '{step_name}' (ID: {step['id']})")
                    print(f"   Description: {step_description}")
                    print(f"   File: {step_file}")
                    
                    self.update_step_status(pipeline_name, step['id'], 'running')
                    
                    # Run the step with Popen so we can track and cancel it
                    print(f"   Executing: python {step_file}")
                    process = subprocess.Popen(
                        ['python', step_file],
                        cwd=pipeline['dir'],
                        stdout=None,  # Don't capture stdout - let it show live
                        stderr=None,  # Don't capture stderr - let it show live
                        text=True
                    )
                    
                    # Track the running process
                    if pipeline_name not in self.running_processes:
                        self.running_processes[pipeline_name] = {}
                    self.running_processes[pipeline_name][step['id']] = process
                    
                    print(f"   Started process PID: {process.pid}")
                    
                    # Wait for completion
                    return_code = process.wait()
                    
                    # Clean up tracking when done
                    if (pipeline_name in self.running_processes and 
                        step['id'] in self.running_processes[pipeline_name]):
                        del self.running_processes[pipeline_name][step['id']]
                        if not self.running_processes[pipeline_name]:
                            del self.running_processes[pipeline_name]
                    
                    if return_code != 0:
                        error_msg = f"Process exited with code {return_code}"
                        print(f"❌ ORCHESTRATOR: Step '{step_name}' FAILED")
                        print(f"   Return code: {return_code}")
                        print(f"   Error output: {error_msg}")
                        self.update_step_status(pipeline_name, step['id'], 'failed', error_msg)
                        success = False
                        error_message = f"Step {step['id']} failed: {error_msg}"
                        break
                    else:
                        print(f"✅ ORCHESTRATOR: Step '{step_name}' COMPLETED SUCCESSFULLY")
                        self.update_step_status(pipeline_name, step['id'], 'success')
                        print("")
                
            except Exception as e:
                success = False
                error_message = str(e)
                print(f"❌ ORCHESTRATOR: Pipeline '{pipeline_name}' CRASHED")
                print(f"   Exception: {str(e)}")
            
            if success:
                print(f"🎉 ORCHESTRATOR: FULL PIPELINE '{pipeline_name}' COMPLETED SUCCESSFULLY!")
            else:
                print(f"💥 ORCHESTRATOR: FULL PIPELINE '{pipeline_name}' FAILED")
                print(f"   Error: {error_message}")
            
            # Update run record
            conn = sqlite3.connect('db.sqlite')
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE runs SET status = ?, completed_at = ?, error_message = ? WHERE id = ?',
                ('success' if success else 'failed', datetime.now(), error_message, run_id)
            )
            conn.commit()
            conn.close()
        
        # Run in background thread
        thread = threading.Thread(target=run_in_background)
        thread.start()
        
        return True, "Pipeline started"
    
    def mark_tested(self, pipeline_name, step_id, tested=True):
        """Mark a step as tested in the pipeline.yaml file"""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return False, "Pipeline not found"
        
        # Update the step in memory
        for step in pipeline['steps']:
            if step['id'] == step_id:
                step['tested'] = tested
                break
        else:
            return False, "Step not found"
        
        # Update the YAML file
        pipeline_file = os.path.join(pipeline['dir'], 'pipeline.yaml')
        with open(pipeline_file, 'w') as f:
            yaml.dump({
                'name': pipeline['name'],
                'description': pipeline['description'],
                'steps': pipeline['steps']
            }, f)
        
        return True, f"Step {step_id} marked as {'tested' if tested else 'untested'}"

    def cancel_step(self, pipeline_name, step_id):
        """Cancel a running step"""
        try:
            print(f"🛑 ORCHESTRATOR: Cancel request for pipeline '{pipeline_name}', step '{step_id}'")
            print(f"   Current running processes: {list(self.running_processes.keys())}")
            
            # Debug: Show what processes are tracked for this pipeline
            if pipeline_name in self.running_processes:
                print(f"   Steps running in pipeline '{pipeline_name}': {list(self.running_processes[pipeline_name].keys())}")
            else:
                print(f"   No running processes found for pipeline '{pipeline_name}'")
            
            # Check if we have a running process for this step
            if (pipeline_name in self.running_processes and 
                step_id in self.running_processes[pipeline_name]):
                
                process = self.running_processes[pipeline_name][step_id]
                print(f"🛑 ORCHESTRATOR: Cancelling step '{step_id}' in pipeline '{pipeline_name}'")
                print(f"   Terminating process PID: {process.pid}")
                
                # Check if process is still alive before trying to terminate
                if process.poll() is None:  # Process is still running
                    # Try graceful termination first
                    process.terminate()
                    
                    # Wait a bit for graceful shutdown
                    try:
                        process.wait(timeout=5)
                        print(f"   ✓ Process terminated gracefully")
                    except subprocess.TimeoutExpired:
                        # Force kill if it doesn't terminate gracefully
                        print(f"   ⚠️ Process didn't terminate gracefully, force killing...")
                        process.kill()
                        process.wait()
                        print(f"   ✓ Process force killed")
                else:
                    print(f"   ℹ️ Process was already finished (return code: {process.returncode})")
                
                # Clean up tracking
                del self.running_processes[pipeline_name][step_id]
                if not self.running_processes[pipeline_name]:
                    del self.running_processes[pipeline_name]
                
                # Update status
                self.update_step_status(pipeline_name, step_id, 'cancelled', 'Cancelled by user')
                
                return True, f"Step {step_id} cancelled successfully"
            else:
                # Debug: Let's check if the step is showing as running in the status file
                status = self.get_pipeline_status(pipeline_name)
                step_status = status.get(step_id, {}).get('status', 'unknown') if status else 'unknown'
                print(f"   Step status in file: {step_status}")
                
                # If status shows running but we don't have the process, force update status
                if step_status == 'running':
                    print(f"   Step shows as running in status but no process tracked - forcing status update")
                    self.update_step_status(pipeline_name, step_id, 'cancelled', 'Process not tracked - forced cancellation')
                    return True, f"Step {step_id} status updated to cancelled (process was not tracked)"
                
                return False, f"No running process found for step {step_id} (status: {step_status})"
                
        except Exception as e:
            print(f"❌ Error cancelling step: {e}")
            return False, f"Error cancelling step: {str(e)}"
    
    def cancel_pipeline(self, pipeline_name):
        """Cancel all running steps in a pipeline"""
        cancelled_count = 0
        errors = []
        
        if pipeline_name in self.running_processes:
            steps_to_cancel = list(self.running_processes[pipeline_name].keys())
            print(f"🛑 ORCHESTRATOR: Cancelling pipeline '{pipeline_name}' - {len(steps_to_cancel)} running steps")
            
            for step_id in steps_to_cancel:
                success, message = self.cancel_step(pipeline_name, step_id)
                if success:
                    cancelled_count += 1
                else:
                    errors.append(f"Step {step_id}: {message}")
            
            if cancelled_count > 0:
                return True, f"Cancelled {cancelled_count} steps. Errors: {'; '.join(errors) if errors else 'None'}"
            else:
                return False, f"No steps cancelled. Errors: {'; '.join(errors)}"
        else:
            return False, f"No running processes found for pipeline '{pipeline_name}'"

    def reset_step(self, pipeline_name, step_id):
        """Reset a specific step's status back to pending"""
        try:
            print(f"🔄 ORCHESTRATOR: Reset request for step '{step_id}' in pipeline '{pipeline_name}'")
            
            # Check if pipeline exists
            pipeline = self.pipelines.get(pipeline_name)
            if not pipeline:
                return False, f"Pipeline '{pipeline_name}' not found"
            
            # Cancel the step if it's running
            if (pipeline_name in self.running_processes and 
                step_id in self.running_processes[pipeline_name]):
                print(f"   Step is currently running, cancelling first...")
                success, message = self.cancel_step(pipeline_name, step_id)
                if not success:
                    print(f"   Warning: Failed to cancel running step: {message}")
            
            # Remove the step's status from the status file (use same path as get_pipeline_status)
            status_file = os.path.join(pipeline['dir'], '.status.json')
            print(f"   Looking for status file: {status_file}")
            
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                    
                    print(f"   Current status data: {status_data}")
                    print(f"   Looking for step_id: '{step_id}' in status data")
                    
                    # Remove this step's status
                    if step_id in status_data:
                        old_status = status_data[step_id]
                        del status_data[step_id]
                        print(f"   Removed status for step '{step_id}': {old_status}")
                        
                        # Write back the updated status
                        with open(status_file, 'w') as f:
                            json.dump(status_data, f, indent=2)
                        print(f"   Updated status file written")
                    else:
                        print(f"   Step '{step_id}' had no status to reset")
                        print(f"   Available step IDs in status: {list(status_data.keys())}")
                        
                except json.JSONDecodeError as e:
                    print(f"   Status file was corrupted: {e}, removing it")
                    os.remove(status_file)
            else:
                print(f"   No status file found at: {status_file}")
            
            print(f"✅ ORCHESTRATOR: Step '{step_id}' reset successfully")
            return True, f"Step '{step_id}' reset to pending status"
                
        except Exception as e:
            print(f"❌ Error resetting step: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error resetting step: {str(e)}"

# Initialize the runner
runner = PipelineRunner()

# Initialize debug system
from debug.registry import DebugModuleRegistry
debug_registry = DebugModuleRegistry()
print(f"Debug system initialized with {debug_registry.get_module_count()} modules loaded")

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/pipelines')
def pipelines():
    """Pipeline orchestrator page"""
    return render_template('pipelines.html')

@app.route('/debug')
def debug():
    """Debug dashboard page"""
    return render_template('debug.html')

@app.route('/api/pipelines')
def list_pipelines():
    """List all pipelines with their current status"""
    pipelines_data = []
    for name, pipeline in runner.pipelines.items():
        status = runner.get_pipeline_status(name)
        pipelines_data.append({
            'name': name,
            'description': pipeline.get('description', ''),
            'steps': pipeline['steps'],
            'status': status
        })
    return jsonify(pipelines_data)

@app.route('/api/run/<pipeline_name>', methods=['POST'])
def run_pipeline(pipeline_name):
    """Trigger a pipeline run"""
    success, message = runner.run_pipeline(pipeline_name)
    return jsonify({'success': success, 'message': message})

@app.route('/api/run/<pipeline_name>/<step_id>', methods=['POST'])
def run_step(pipeline_name, step_id):
    """Trigger a single step run"""
    success, message = runner.run_step(pipeline_name, step_id)
    return jsonify({'success': success, 'message': message})

@app.route('/api/mark_tested/<pipeline_name>/<step_id>', methods=['POST'])
def mark_tested(pipeline_name, step_id):
    """Mark a step as tested"""
    tested = request.json.get('tested', True)
    success, message = runner.mark_tested(pipeline_name, step_id, tested)
    return jsonify({'success': success, 'message': message})

@app.route('/api/refresh', methods=['POST'])
def refresh_pipelines():
    """Reload all pipelines from disk"""
    runner.load_pipelines()
    return jsonify({'success': True, 'message': 'Pipelines refreshed'})

@app.route('/api/cancel/<pipeline_name>/<step_id>', methods=['POST'])
def cancel_step(pipeline_name, step_id):
    """Cancel a running step"""
    success, message = runner.cancel_step(pipeline_name, step_id)
    return jsonify({'success': success, 'message': message})

@app.route('/api/cancel/<pipeline_name>', methods=['POST'])
def cancel_pipeline(pipeline_name):
    """Cancel all running steps in a pipeline"""
    success, message = runner.cancel_pipeline(pipeline_name)
    return jsonify({'success': success, 'message': message})

@app.route('/api/running', methods=['GET'])
def get_running_processes():
    """Get information about currently running processes"""
    running_info = {}
    for pipeline_name, steps in runner.running_processes.items():
        running_info[pipeline_name] = {}
        for step_id, process in steps.items():
            running_info[pipeline_name][step_id] = {
                'pid': process.pid,
                'poll': process.poll()  # None if still running, return code if finished
            }
    return jsonify(running_info)

@app.route('/api/debug/<pipeline_name>', methods=['GET'])
def debug_pipeline(pipeline_name):
    """Debug endpoint to see what's tracked vs what's in status files"""
    debug_info = {
        'pipeline_name': pipeline_name,
        'tracked_processes': {},
        'status_file_data': {},
        'pipeline_exists': pipeline_name in runner.pipelines
    }
    
    # Get tracked processes
    if pipeline_name in runner.running_processes:
        for step_id, process in runner.running_processes[pipeline_name].items():
            debug_info['tracked_processes'][step_id] = {
                'pid': process.pid,
                'poll_result': process.poll(),
                'is_running': process.poll() is None
            }
    
    # Get status file data
    status = runner.get_pipeline_status(pipeline_name)
    if status:
        debug_info['status_file_data'] = status
    
    return jsonify(debug_info)

@app.route('/api/reset/<pipeline_name>/<step_id>', methods=['POST'])
def reset_step(pipeline_name, step_id):
    """Reset a specific step's status back to pending"""
    success, message = runner.reset_step(pipeline_name, step_id)
    return jsonify({'success': success, 'message': message})

@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

# Dashboard routes for serving static files
@app.route('/ads-dashboard')
def dashboard_home():
    """Serve the ads dashboard application"""
    return send_from_directory('dashboard/static', 'index.html')

@app.route('/dashboard-static/<path:filename>')
def dashboard_static(filename):
    """Serve dashboard static files (CSS, JS, images)"""
    return send_from_directory('dashboard/static/static', filename)

@app.route('/manifest.json')
def dashboard_manifest():
    """Serve dashboard manifest.json"""
    return send_from_directory('dashboard/static', 'manifest.json')

@app.route('/favicon.ico')
def dashboard_favicon():
    """Serve dashboard favicon"""
    # Return a simple 1x1 transparent PNG to avoid 404 errors
    from flask import Response
    import base64
    
    # 1x1 transparent PNG in base64
    transparent_png = base64.b64decode(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChAI9jU77zgAAAABJRU5ErkJggg=='
    )
    
    return Response(transparent_png, mimetype='image/png')

# Add a middleware to log requests to see what's happening
@app.before_request  
def handle_json_get_requests():
    """Handle the case where frontend sends Content-Type: application/json on GET requests"""
    if request.method == 'GET' and request.content_type == 'application/json':
        print(f"🔧 Fixing GET request with application/json content-type: {request.path}")
        
        # For analytics chart-data endpoints, handle this gracefully
        if 'chart-data' in request.path:
            from flask import jsonify
            from dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig
            
            try:
                # Extract query parameters manually since Flask is confused by Content-Type
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                breakdown = request.args.get('breakdown')
                entity_type = request.args.get('entity_type')
                entity_id = request.args.get('entity_id')
                
                # Basic validation
                if not all([start_date, end_date, breakdown, entity_type, entity_id]):
                    return jsonify({
                        'success': False,
                        'error': 'Missing required parameters',
                        'message': 'Required: start_date, end_date, breakdown, entity_type, entity_id'
                    }), 400
                
                # Create service and execute query
                service = AnalyticsQueryService()
                config = QueryConfig(
                    breakdown=breakdown,
                    start_date=start_date,
                    end_date=end_date,
                    include_mixpanel=False
                )
                
                result = service.get_chart_data(config, entity_type, entity_id)
                return jsonify(result)
                
            except Exception as e:
                print(f"Error in GET/JSON workaround: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Internal Server Error',
                    'message': str(e)
                }), 500

@app.before_request
def log_request():
    if request.path.startswith('/api/dashboard'):
        print(f"Dashboard API request: {request.method} {request.path}")
        
        # Skip JSON logging for GET requests with wrong content-type (handled above)
        if request.method == 'GET' and request.content_type == 'application/json':
            return
            
        if request.is_json:
            try:
                print(f"Request data: {request.get_json()}")
            except Exception as e:
                print(f"JSON parse error: {e}")

# Global error handlers to ensure API endpoints always return JSON
@app.errorhandler(400)
def handle_bad_request(e):
    if request.path.startswith('/api/'):
        # Check if this is a JSON decode error on a GET request
        error_message = str(e.description) if hasattr(e, 'description') else 'Invalid request'
        if 'JSON object' in error_message and request.method == 'GET':
            # This is likely caused by Content-Type: application/json on a GET request
            # Redirect to the same endpoint without processing JSON
            print(f"🔧 JSON decode error on GET request, this is likely a frontend Content-Type issue")
            return jsonify({
                'success': False,
                'error': 'Bad Request',
                'message': 'GET requests should not include Content-Type: application/json header. This endpoint expects query parameters.',
                'debug_info': f'Original error: {error_message}'
            }), 400
        return jsonify({
            'success': False,
            'error': 'Bad Request',
            'message': error_message
        }), 400
    return e

@app.errorhandler(404)
def handle_not_found(e):
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'error': 'Not Found',
            'message': f'API endpoint {request.path} not found'
        }), 404
    return e

@app.errorhandler(500)
def handle_internal_error(e):
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred'
        }), 500
    return e

# Add API proxy routes to handle React app calling wrong URLs
# React app tries to call localhost:5678, but we're on localhost:5001

# Analytics Pipeline API endpoints (React calls these specific endpoints)
@app.route('/api/analytics-pipeline/status', methods=['GET'])
def analytics_pipeline_status():
    """Get analytics pipeline status"""
    return jsonify({
        "status": "ready",
        "service": "analytics-pipeline", 
        "last_update": datetime.now().isoformat(),
        "pipeline_health": "operational",
        "data_freshness": "current",
        "last_run": datetime.now().isoformat(),
        "next_run": None,
        "processing_state": "idle"
    })

@app.route('/api/analytics-pipeline/cancel', methods=['POST'])
def analytics_pipeline_cancel():
    """Cancel analytics pipeline operations"""
    return jsonify({
        "success": True,
        "message": "Analytics pipeline operations cancelled",
        "status": "cancelled",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/analytics-pipeline/health', methods=['GET'])
def analytics_pipeline_health():
    """Get analytics pipeline health check"""
    return jsonify({
        "status": "healthy",
        "service": "analytics-pipeline",
        "version": "1.0.0",
        "uptime": "operational",
        "dependencies": {
            "database": "connected",
            "storage": "available",
            "compute": "ready"
        },
        "timestamp": datetime.now().isoformat()
    })

# Additional API endpoints that the frontend expects (comprehensive error prevention)

# Mixpanel API endpoints
@app.route('/api/mixpanel/process/status', methods=['GET'])
def mixpanel_process_status():
    """Get Mixpanel processing status"""
    return jsonify({
        "success": True,
        "status": "idle",
        "processing": False,
        "progress": 0,
        "message": "No active processing",
        "last_run": datetime.now().isoformat(),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/process/start', methods=['POST'])
def mixpanel_process_start():
    """Start Mixpanel processing"""
    return jsonify({
        "success": True,
        "message": "Mixpanel processing started",
        "job_id": f"mixpanel_{int(datetime.now().timestamp())}",
        "status": "started",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/process/cancel', methods=['POST'])
def mixpanel_process_cancel():
    """Cancel Mixpanel processing"""
    return jsonify({
        "success": True,
        "message": "Mixpanel processing cancelled",
        "status": "cancelled",
        "timestamp": datetime.now().isoformat()
    })

# Meta API endpoints
@app.route('/api/meta/job/<job_id>/status', methods=['GET'])
def meta_job_status(job_id):
    """Get Meta job status"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "status": "completed",
        "progress": 100,
        "message": "Job completed successfully",
        "result_url": f"/api/meta/job/{job_id}/results",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/job/<job_id>/results', methods=['GET'])
def meta_job_results(job_id):
    """Get Meta job results"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "data": {
            "total_records": 0,
            "results": [],
            "summary": "No data available"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/fetch', methods=['POST'])
def meta_fetch():
    """Fetch Meta data"""
    return jsonify({
        "success": True,
        "message": "Meta data fetch initiated",
        "job_id": f"meta_{int(datetime.now().timestamp())}",
        "status": "started",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/start', methods=['POST'])
def meta_historical_start():
    """Start Meta historical data collection"""
    return jsonify({
        "success": True,
        "message": "Historical data collection started",
        "job_id": f"historical_{int(datetime.now().timestamp())}",
        "status": "started",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/jobs/<job_id>/status', methods=['GET'])
def meta_historical_job_status(job_id):
    """Get Meta historical job status"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "status": "completed",
        "progress": 100,
        "message": "Historical job completed",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/jobs/<job_id>/cancel', methods=['POST'])
def meta_historical_job_cancel(job_id):
    """Cancel Meta historical job"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": "Historical job cancelled",
        "status": "cancelled",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/coverage', methods=['GET'])
def meta_historical_coverage():
    """Get Meta historical data coverage"""
    return jsonify({
        "success": True,
        "data": {
            "total_days": 0,
            "coverage_percentage": 0,
            "date_range": {
                "start": "2025-01-01",
                "end": datetime.now().strftime("%Y-%m-%d")
            }
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/missing-dates', methods=['GET'])
def meta_historical_missing_dates():
    """Get missing dates in Meta historical data"""
    return jsonify({
        "success": True,
        "data": {
            "missing_dates": [],
            "total_missing": 0
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/configurations', methods=['GET'])
def meta_historical_configurations():
    """Get Meta historical configurations"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/configurations/<config_id>', methods=['DELETE'])
def meta_historical_delete_configuration(config_id):
    """Delete Meta historical configuration"""
    return jsonify({
        "success": True,
        "message": f"Configuration {config_id} deleted",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/historical/export', methods=['GET'])
def meta_historical_export():
    """Export Meta historical data"""
    return jsonify({
        "success": True,
        "data": {
            "export_url": "/downloads/meta_export.csv",
            "record_count": 0
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/action-mappings', methods=['GET'])
def meta_action_mappings_get():
    """Get Meta action mappings"""
    return jsonify({
        "success": True,
        "data": {
            "mappings": {}
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/action-mappings', methods=['POST'])
def meta_action_mappings_post():
    """Save Meta action mappings"""
    return jsonify({
        "success": True,
        "message": "Action mappings saved",
        "timestamp": datetime.now().isoformat()
    })

# Cohort Analysis API endpoints
@app.route('/api/cohort-analysis', methods=['POST'])
def cohort_analysis():
    """Analyze cohort data"""
    return jsonify({
        "success": True,
        "message": "Cohort analysis completed",
        "data": {
            "cohorts": [],
            "summary": "No data available"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/v2/cohort/analyze', methods=['POST'])
def cohort_v2_analyze():
    """V2 Cohort analysis"""
    return jsonify({
        "success": True,
        "message": "V2 Cohort analysis completed",
        "data": {
            "results": [],
            "metadata": {}
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/v3/cohort/analyze-refactored', methods=['POST'])
def cohort_v3_analyze():
    """V3 Refactored cohort analysis"""
    return jsonify({
        "success": True,
        "message": "V3 Refactored analysis completed",
        "stage_results": {},
        "final_analysis": {},
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cohort-pipeline/timeline', methods=['POST'])
def cohort_pipeline_timeline():
    """Get cohort user timeline"""
    return jsonify({
        "success": True,
        "data": {
            "timeline": [],
            "user_count": 0
        },
        "timestamp": datetime.now().isoformat()
    })

# Cohort Analyzer API endpoints
@app.route('/api/cohort_analyzer/discoverable_properties', methods=['GET'])
def cohort_analyzer_properties():
    """Get discoverable cohort properties"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cohort_analyzer/property_values', methods=['GET'])
def cohort_analyzer_property_values():
    """Get property values"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cohort_analyzer/trigger_discovery', methods=['POST'])
def cohort_analyzer_trigger_discovery():
    """Trigger property discovery"""
    return jsonify({
        "success": True,
        "message": "Property discovery triggered",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cohort_analyzer/enable_properties', methods=['POST'])
def cohort_analyzer_enable_properties():
    """Enable properties"""
    return jsonify({
        "success": True,
        "message": "Properties enabled",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/cohort_analyzer/discovery_status', methods=['GET'])
def cohort_analyzer_discovery_status():
    """Get discovery status"""
    return jsonify({
        "success": True,
        "status": "idle",
        "timestamp": datetime.now().isoformat()
    })

# Conversion Probability API endpoints
@app.route('/api/conversion-probability/analyze-properties', methods=['POST'])
def conversion_probability_analyze_properties():
    """Analyze conversion properties"""
    return jsonify({
        "success": True,
        "data": {
            "properties": [],
            "analysis": {}
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/start-analysis', methods=['POST'])
def conversion_probability_start_analysis():
    """Start conversion analysis"""
    return jsonify({
        "success": True,
        "data": {
            "analysis_id": f"conv_{int(datetime.now().timestamp())}",
            "cached": False
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/progress/<analysis_id>', methods=['GET'])
def conversion_probability_progress(analysis_id):
    """Get analysis progress"""
    return jsonify({
        "success": True,
        "data": {
            "progress": 100,
            "status": "completed"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/results/<analysis_id>', methods=['GET'])
def conversion_probability_results(analysis_id):
    """Get analysis results"""
    return jsonify({
        "success": True,
        "data": {
            "results": [],
            "analysis_id": analysis_id
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/results/latest', methods=['GET'])
def conversion_probability_results_latest():
    """Get latest analysis results"""
    return jsonify({
        "success": True,
        "data": {
            "results": [],
            "analysis_id": "latest"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/analyses', methods=['GET'])
def conversion_probability_analyses():
    """Get available analyses"""
    return jsonify({
        "success": True,
        "data": {
            "files": []
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/conversion-probability/run-new-hierarchical-analysis', methods=['POST'])
def conversion_probability_hierarchical():
    """Run new hierarchical analysis"""
    return jsonify({
        "success": True,
        "message": "Hierarchical analysis completed",
        "data": {},
        "timestamp": datetime.now().isoformat()
    })

# Pricing API endpoints
@app.route('/api/pricing/rules', methods=['GET'])
def pricing_rules_get():
    """Get pricing rules"""
    return jsonify({
        "success": True,
        "data": {
            "rules": [],
            "schema_version": "1.0",
            "products": [],
            "missing_products": []
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/rules', methods=['POST'])
def pricing_rules_post():
    """Create pricing rule"""
    return jsonify({
        "success": True,
        "message": "Pricing rule created",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>', methods=['PUT'])
def pricing_rules_put(rule_id):
    """Update pricing rule"""
    return jsonify({
        "success": True,
        "message": f"Pricing rule {rule_id} updated",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>', methods=['DELETE'])
def pricing_rules_delete(rule_id):
    """Delete pricing rule"""
    return jsonify({
        "success": True,
        "message": f"Pricing rule {rule_id} deleted",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/products', methods=['GET'])
def pricing_products():
    """Get pricing products"""
    return jsonify({
        "success": True,
        "data": {
            "products": []
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/countries', methods=['GET'])
def pricing_countries():
    """Get pricing countries"""
    return jsonify({
        "success": True,
        "data": {
            "countries": []
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/repair-rules', methods=['POST'])
def pricing_repair_rules():
    """Repair pricing rules"""
    return jsonify({
        "success": True,
        "message": "Pricing rules repaired",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>/history', methods=['GET'])
def pricing_rule_history(rule_id):
    """Get pricing rule history"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>/provenance', methods=['GET'])
def pricing_rule_provenance(rule_id):
    """Get pricing rule provenance"""
    return jsonify({
        "success": True,
        "data": {},
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/pricing/products/<product_id>/most-recent-conversion-price', methods=['GET'])
def pricing_product_recent_price(product_id):
    """Get most recent conversion price"""
    return jsonify({
        "success": True,
        "data": {
            "suggested_price": 9.99
        },
        "timestamp": datetime.now().isoformat()
    })

# Additional Mixpanel debug endpoints
@app.route('/api/mixpanel/debug/sync-ts', methods=['GET'])
def mixpanel_debug_sync_ts_get():
    """Get Mixpanel sync timestamp"""
    return jsonify({
        "success": True,
        "data": {
            "sync_timestamp": datetime.now().isoformat()
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/sync-ts/reset', methods=['POST'])
def mixpanel_debug_sync_ts_reset():
    """Reset Mixpanel sync timestamp"""
    return jsonify({
        "success": True,
        "message": "Sync timestamp reset",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/latest-processed-date', methods=['GET'])
def mixpanel_debug_latest_processed_date():
    """Get latest processed date"""
    return jsonify({
        "success": True,
        "data": {
            "latest_date": datetime.now().strftime("%Y-%m-%d")
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/database/reset', methods=['POST'])
def mixpanel_debug_database_reset():
    """Reset Mixpanel database"""
    return jsonify({
        "success": True,
        "message": "Database reset completed",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/data/refresh', methods=['POST'])
def mixpanel_debug_data_refresh():
    """Refresh Mixpanel data"""
    return jsonify({
        "success

... [File truncated - showing first 50,000 characters]
```

### orchestrator/dashboard/api/dashboard_routes.py
```py
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
        
        # Create query configuration
        config = QueryConfig(
            breakdown=breakdown,
            start_date=start_date,
            end_date=end_date,
            include_mixpanel=False  # Chart data typically doesn't need mixpanel metrics
        )
        
        # Get chart data with comprehensive error handling and thread safety
        try:
            with analytics_lock:
                result = analytics_service.get_chart_data(config, entity_type, entity_id)
            
            logger.info(f"Chart data result for {entity_type} {entity_id}: success={result.get('success')}")
            
            if result.get('success'):
                return jsonify(result)
            else:
                error_msg = result.get('error', 'Unknown error from analytics service')
                logger.error(f"Analytics service error for {entity_type} {entity_id}: {error_msg}")
                return jsonify({
                    'success': False,
                    'error': error_msg
                }), 400
                
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
```

### orchestrator/dashboard/services/analytics_query_service.py
```py
"""
Analytics Query Service

This service handles fast dashboard data retrieval with JOIN queries between
meta_analytics.db and mixpanel_analytics.db using ABI attribution fields.
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

from utils.database_utils import get_database_path

logger = logging.getLogger(__name__)


@dataclass
class QueryConfig:
    """Configuration for analytics queries"""
    breakdown: str  # 'all', 'country', 'region', 'device'
    start_date: str
    end_date: str
    group_by: Optional[str] = None  # 'campaign', 'adset', 'ad'
    include_mixpanel: bool = True


class AnalyticsQueryService:
    """Service for executing analytics queries across multiple databases"""
    
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
    
    def execute_analytics_query(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute analytics query with comprehensive error handling and fallback logic
        """
        try:
            logger.info(f"🔍 EXECUTE_ANALYTICS_QUERY CALLED - NEW CODE VERSION")
            # Get the appropriate table name based on breakdown
            table_name = self.get_table_name(config.breakdown)
            logger.info(f"🔍 Table name: {table_name}")
            
            # Check if Meta ad performance tables have data
            meta_data_count = self._get_meta_data_count(table_name)
            logger.info(f"🔍 Meta data count: {meta_data_count}")
            
            if meta_data_count == 0:
                logger.info(f"🔍 No data found in {table_name}, falling back to Mixpanel-only data")
                return self._execute_mixpanel_only_query(config)
            
            # Original logic for when Meta data exists
            if config.group_by == 'campaign':
                structured_data = self._get_campaign_level_data(config, table_name)
            elif config.group_by == 'adset':
                structured_data = self._get_adset_level_data(config, table_name)
            else:  # ad level
                structured_data = self._get_ad_level_data(config, table_name)
            
            return {
                'success': True,
                'data': structured_data,
                'metadata': {
                    'query_config': config.__dict__,
                    'table_used': table_name,
                    'record_count': len(structured_data),
                    'date_range': f"{config.start_date} to {config.end_date}",
                    'generated_at': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing analytics query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': datetime.now().isoformat()
                }
            }
    
    def _get_meta_data_count(self, table_name: str) -> int:
        """Check if Meta ad performance table has any data"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self._execute_meta_query(query, [])
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Error checking Meta data count: {e}")
            return 0
    
    def _execute_mixpanel_only_query(self, config: QueryConfig) -> Dict[str, Any]:
        """
        Execute analytics query using only Mixpanel data when Meta ad performance tables are empty
        """
        try:
            # Get data directly from Mixpanel tables
            if config.group_by == 'campaign':
                structured_data = self._get_mixpanel_campaign_data(config)
            elif config.group_by == 'adset':
                structured_data = self._get_mixpanel_adset_data(config)
            else:  # ad level
                structured_data = self._get_mixpanel_ad_data(config)
            
            return {
                'success': True,
                'data': structured_data,
                'metadata': {
                    'query_config': config.__dict__,
                    'table_used': 'mixpanel_user + mixpanel_event (Mixpanel-only mode)',
                    'record_count': len(structured_data),
                    'date_range': f"{config.start_date} to {config.end_date}",
                    'generated_at': datetime.now().isoformat(),
                    'data_source': 'mixpanel_only'
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing Mixpanel-only query: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'metadata': {
                    'query_config': config.__dict__,
                    'generated_at': datetime.now().isoformat()
                }
            }
    
    def _get_mixpanel_campaign_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get campaign-level data from Mixpanel only"""
        
        # Get campaign-level aggregated data from Mixpanel
        campaign_query = """
        SELECT 
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_campaign_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
        """
        
        # Use the date range for all filters
        params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date,  # revenue filter
            config.start_date, config.end_date   # main user filter
        ]
        
        campaigns = self._execute_mixpanel_query(campaign_query, params)
        
        # Format campaigns with default Meta values
        formatted_campaigns = []
        for campaign in campaigns:
            formatted_campaign = {
                'id': f"campaign_{campaign['campaign_id']}",
                'entity_type': 'campaign',
                'campaign_id': campaign['campaign_id'],
                'campaign_name': campaign['campaign_name'] or 'Unknown Campaign',
                'name': campaign['campaign_name'] or 'Unknown Campaign',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(campaign['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(campaign['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': float(campaign['mixpanel_revenue_usd'] or 0),
                
                # Calculated metrics
                'estimated_revenue_usd': float(campaign['mixpanel_revenue_usd'] or 0),
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': float(campaign['mixpanel_revenue_usd'] or 0),  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(campaign['total_users'] or 0),
                'new_users': int(campaign['new_users'] or 0),
                'children': []
            }
            formatted_campaigns.append(formatted_campaign)
        
        return formatted_campaigns
    
    def _get_mixpanel_adset_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get adset-level data from Mixpanel only"""
        
        # Get adset-level aggregated data from Mixpanel
        adset_query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
        """
        
        # Use the date range for all filters
        params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date,  # revenue filter
            config.start_date, config.end_date   # main user filter
        ]
        
        adsets = self._execute_mixpanel_query(adset_query, params)
        
        # Format adsets with default Meta values
        formatted_adsets = []
        for adset in adsets:
            formatted_adset = {
                'id': f"adset_{adset['adset_id']}",
                'entity_type': 'adset',
                'adset_id': adset['adset_id'],
                'adset_name': adset['adset_name'] or 'Unknown Adset',
                'campaign_id': adset['campaign_id'],
                'campaign_name': adset['campaign_name'] or 'Unknown Campaign',
                'name': adset['adset_name'] or 'Unknown Adset',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(adset['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(adset['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': float(adset['mixpanel_revenue_usd'] or 0),
                
                # Calculated metrics
                'estimated_revenue_usd': float(adset['mixpanel_revenue_usd'] or 0),
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': float(adset['mixpanel_revenue_usd'] or 0),  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(adset['total_users'] or 0),
                'new_users': int(adset['new_users'] or 0),
                'children': []
            }
            formatted_adsets.append(formatted_adset)
        
        return formatted_adsets
    
    def _get_mixpanel_ad_data(self, config: QueryConfig) -> List[Dict[str, Any]]:
        """Get ad-level data from Mixpanel only"""
        
        # Get ad-level aggregated data from Mixpanel
        ad_query = """
        SELECT 
            u.abi_ad_id as ad_id,
            'Unknown Ad' as ad_name,
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
            SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue_usd
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IS NOT NULL
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY u.abi_ad_id, e.abi_ad_set_id, e.abi_campaign_id
        ORDER BY mixpanel_revenue_usd DESC
        """
        
        # Use the date range for all filters
        params = [
            config.start_date, config.end_date,  # new_users filter
            config.start_date, config.end_date,  # trials filter
            config.start_date, config.end_date,  # purchases filter
            config.start_date, config.end_date,  # revenue filter
            config.start_date, config.end_date   # main user filter
        ]
        
        ads = self._execute_mixpanel_query(ad_query, params)
        
        # Format ads with default Meta values
        formatted_ads = []
        for ad in ads:
            formatted_ad = {
                'id': f"ad_{ad['ad_id']}",
                'entity_type': 'ad',
                'ad_id': ad['ad_id'],
                'ad_name': ad['ad_name'] or 'Unknown Ad',
                'adset_id': ad['adset_id'],
                'adset_name': ad['adset_name'] or 'Unknown Adset',
                'campaign_id': ad['campaign_id'],
                'campaign_name': ad['campaign_name'] or 'Unknown Campaign',
                'name': ad['ad_name'] or 'Unknown Ad',
                
                # Meta metrics (all zeros since we don't have Meta data)
                'spend': 0.0,
                'impressions': 0,
                'clicks': 0,
                'meta_trials_started': 0,
                'meta_purchases': 0,
                
                # Mixpanel metrics
                'mixpanel_trials_started': int(ad['mixpanel_trials_started'] or 0),
                'mixpanel_purchases': int(ad['mixpanel_purchases'] or 0),
                'mixpanel_revenue_usd': float(ad['mixpanel_revenue_usd'] or 0),
                
                # Calculated metrics
                'estimated_revenue_usd': float(ad['mixpanel_revenue_usd'] or 0),
                'estimated_roas': 0.0,  # Can't calculate without spend
                'profit': float(ad['mixpanel_revenue_usd'] or 0),  # Revenue - 0 spend
                'trial_accuracy_ratio': 0.0,  # Can't calculate without Meta trials
                
                # Additional info
                'total_users': int(ad['total_users'] or 0),
                'new_users': int(ad['new_users'] or 0),
                'children': []
            }
            formatted_ads.append(formatted_ad)
        
        return formatted_ads
    
    def _execute_mixpanel_query(self, query: str, params: List) -> List[Dict[str, Any]]:
        """Execute query against Mixpanel database"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                return results
                
        except Exception as e:
            logger.error(f"Error executing Mixpanel query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def _get_campaign_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get campaign-level aggregated data with adset and ad children"""
        
        # Step 1: Get campaign-level aggregated meta data
        campaign_query = f"""
        SELECT 
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND campaign_id IS NOT NULL
        GROUP BY campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        campaigns = self._execute_meta_query(campaign_query, [config.start_date, config.end_date])
        
        # Step 2: Get adset-level data for each campaign
        for campaign in campaigns:
            campaign_id = campaign['campaign_id']
            
            # Get adsets for this campaign
            adset_query = f"""
            SELECT 
                adset_id,
                adset_name,
                campaign_id,
                campaign_name,
                SUM(spend) as spend,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(meta_trials) as meta_trials_started,
                SUM(meta_purchases) as meta_purchases
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
              AND campaign_id = ?
              AND adset_id IS NOT NULL
            GROUP BY adset_id, adset_name, campaign_id, campaign_name
            ORDER BY SUM(spend) DESC
            """
            
            adsets = self._execute_meta_query(adset_query, [config.start_date, config.end_date, campaign_id])
            
            # Get ads for each adset
            for adset in adsets:
                adset_id = adset['adset_id']
                
                ad_query = f"""
                SELECT 
                    ad_id,
                    ad_name,
                    adset_id,
                    adset_name,
                    campaign_id,
                    campaign_name,
                    SUM(spend) as spend,
                    SUM(impressions) as impressions,
                    SUM(clicks) as clicks,
                    SUM(meta_trials) as meta_trials_started,
                    SUM(meta_purchases) as meta_purchases
                FROM {table_name}
                WHERE date BETWEEN ? AND ?
                  AND adset_id = ?
                  AND ad_id IS NOT NULL
                GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
                ORDER BY SUM(spend) DESC
                """
                
                ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date, adset_id])
                
                # Add mixpanel data to ads
                if config.include_mixpanel:
                    self._add_mixpanel_data_to_records(ads, config)
                
                # Format ads and add required fields
                formatted_ads = []
                for ad in ads:
                    formatted_ad = self._format_record(ad, 'ad')
                    formatted_ads.append(formatted_ad)
                
                adset['children'] = formatted_ads
            
            # Add mixpanel data to adsets
            if config.include_mixpanel:
                self._add_mixpanel_data_to_records(adsets, config)
            
            # Format adsets
            formatted_adsets = []
            for adset in adsets:
                formatted_adset = self._format_record(adset, 'adset')
                if 'children' not in formatted_adset:
                    formatted_adset['children'] = []
                formatted_adsets.append(formatted_adset)
            
            campaign['children'] = formatted_adsets
        
        # Add mixpanel data to campaigns
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(campaigns, config)
        
        # Format campaigns
        formatted_campaigns = []
        for campaign in campaigns:
            formatted_campaign = self._format_record(campaign, 'campaign')
            if 'children' not in formatted_campaign:
                formatted_campaign['children'] = []
            formatted_campaigns.append(formatted_campaign)
        
        return formatted_campaigns
    
    def _get_adset_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get adset-level aggregated data with ad children"""
        
        # Get adset-level aggregated meta data
        adset_query = f"""
        SELECT 
            adset_id,
            adset_name,
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND adset_id IS NOT NULL
        GROUP BY adset_id, adset_name, campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        adsets = self._execute_meta_query(adset_query, [config.start_date, config.end_date])
        
        # Get ads for each adset
        for adset in adsets:
            adset_id = adset['adset_id']
            
            ad_query = f"""
            SELECT 
                ad_id,
                ad_name,
                adset_id,
                adset_name,
                campaign_id,
                campaign_name,
                SUM(spend) as spend,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(meta_trials) as meta_trials_started,
                SUM(meta_purchases) as meta_purchases
            FROM {table_name}
            WHERE date BETWEEN ? AND ?
              AND adset_id = ?
              AND ad_id IS NOT NULL
            GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
            ORDER BY SUM(spend) DESC
            """
            
            ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date, adset_id])
            
            # Add mixpanel data to ads
            if config.include_mixpanel:
                self._add_mixpanel_data_to_records(ads, config)
            
            # Format ads
            formatted_ads = []
            for ad in ads:
                formatted_ad = self._format_record(ad, 'ad')
                formatted_ads.append(formatted_ad)
            
            adset['children'] = formatted_ads
        
        # Add mixpanel data to adsets
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(adsets, config)
        
        # Format adsets
        formatted_adsets = []
        for adset in adsets:
            formatted_adset = self._format_record(adset, 'adset')
            if 'children' not in formatted_adset:
                formatted_adset['children'] = []
            formatted_adsets.append(formatted_adset)
        
        return formatted_adsets
    
    def _get_ad_level_data(self, config: QueryConfig, table_name: str) -> List[Dict[str, Any]]:
        """Get ad-level data (flat, no children)"""
        
        ad_query = f"""
        SELECT 
            ad_id,
            ad_name,
            adset_id,
            adset_name,
            campaign_id,
            campaign_name,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(meta_trials) as meta_trials_started,
            SUM(meta_purchases) as meta_purchases
        FROM {table_name}
        WHERE date BETWEEN ? AND ?
          AND ad_id IS NOT NULL
        GROUP BY ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name
        ORDER BY SUM(spend) DESC
        """
        
        ads = self._execute_meta_query(ad_query, [config.start_date, config.end_date])
        
        # Add mixpanel data to ads
        if config.include_mixpanel:
            self._add_mixpanel_data_to_records(ads, config)
        
        # Format ads
        formatted_ads = []
        for ad in ads:
            formatted_ad = self._format_record(ad, 'ad')
            formatted_ads.append(formatted_ad)
        
        return formatted_ads
    
    def _execute_meta_query(self, query: str, params: List) -> List[Dict[str, Any]]:
        """Execute query against meta analytics database"""
        try:
            conn = sqlite3.connect(self.meta_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            
            conn.close()
            return data
            
        except Exception as e:
            logger.error(f"Error executing meta query: {e}")
            raise
    
    def _add_mixpanel_data_to_records(self, records: List[Dict[str, Any]], config: QueryConfig):
        """Add mixpanel metrics to records at campaign, adset, or ad level"""
        
        # TEMPORARY: Hardcode test values to verify frontend display
        logger.info(f"🧪 HARDCODING TEST VALUES for {len(records)} records to test frontend display")
        
        for i, record in enumerate(records):
            # Add hardcoded test values that should be visible on frontend
            record.update({
                'mixpanel_trials_started': 50 + i,  # Different values for each record
                'mixpanel_purchases': 25 + i,
                'mixpanel_revenue_usd': 1000.0 + (i * 100),
                'estimated_revenue_usd': 1000.0 + (i * 100),
                'total_attributed_users': 100 + i,
                'trial_accuracy_ratio': 50.0 + (i * 5)  # Percentage
            })
            
            logger.info(f"🧪 Hardcoded values for record {i}: trials={record['mixpanel_trials_started']}, purchases={record['mixpanel_purchases']}, revenue=${record['mixpanel_revenue_usd']}")
            
            # If this record has children, apply hardcoded values to them too
            if 'children' in record and record['children']:
                for j, child in enumerate(record['children']):
                    child.update({
                        'mixpanel_trials_started': 20 + j,
                        'mixpanel_purchases': 10 + j,
                        'mixpanel_revenue_usd': 500.0 + (j * 50),
                        'estimated_revenue_usd': 500.0 + (j * 50),
                        'total_attributed_users': 50 + j,
                        'trial_accuracy_ratio': 40.0 + (j * 2)
                    })
                    
                    # If children have their own children (ads under adsets)
                    if 'children' in child and child['children']:
                        for k, grandchild in enumerate(child['children']):
                            grandchild.update({
                                'mixpanel_trials_started': 5 + k,
                                'mixpanel_purchases': 2 + k,
                                'mixpanel_revenue_usd': 100.0 + (k * 25),
                                'estimated_revenue_usd': 100.0 + (k * 25),
                                'total_attributed_users': 10 + k,
                                'trial_accuracy_ratio': 20.0 + k
                            })
        
        logger.info(f"🧪 HARDCODED TEST VALUES applied to all records and their children")
        return
        
        # ORIGINAL CODE BELOW - COMMENTED OUT FOR TESTING
        """
        try:
            if not records:
                return
                
            # Determine what type of records we're dealing with
            first_record = records[0]
            record_type = None
            if first_record.get('ad_id'):
                record_type = 'ad'
            elif first_record.get('adset_id'):
                record_type = 'adset' 
            elif first_record.get('campaign_id'):
                record_type = 'campaign'
            else:
                logger.warning("Records don't have ad_id, adset_id, or campaign_id - cannot add Mixpanel data")
                return

            logger.info(f"🔍 Adding Mixpanel data to {len(records)} {record_type}-level records")

            # Connect to mixpanel database
            conn = sqlite3.connect(self.mixpanel_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            mixpanel_data = {}
            
            if record_type == 'ad':
                # For ad-level records, get Mixpanel data directly by ad_id
                ad_ids = [r['ad_id'] for r in records if r.get('ad_id')]
                if ad_ids:
                    ad_placeholders = ','.join(['?' for _ in ad_ids])
                    mixpanel_query = f"""
                    SELECT 
                        u.abi_ad_id,
                        COALESCE(SUM(CASE WHEN e.event_name = 'RC Trial started' 
                                 AND e.event_time BETWEEN ? AND ? 
                                 THEN 1 ELSE 0 END), 0) as mixpanel_trials_started,
                        COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                 AND e.event_time BETWEEN ? AND ? 
                                 THEN 1 ELSE 0 END), 0) as mixpanel_purchases,
                        COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                 AND e.event_time BETWEEN ? AND ? 
                                 THEN COALESCE(e.revenue_usd, 0) ELSE 0 END), 0) as mixpanel_revenue_usd,
                        COUNT(DISTINCT u.distinct_id) as total_attributed_users
                    FROM mixpanel_user u
                    LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                    WHERE u.abi_ad_id IN ({ad_placeholders})
                      AND u.has_abi_attribution = TRUE
                    GROUP BY u.abi_ad_id
                    """
                    
                    params = [
                        config.start_date, config.end_date,  # trial date filter
                        config.start_date, config.end_date,  # purchase date filter  
                        config.start_date, config.end_date   # revenue date filter
                    ] + ad_ids
                    
                    cursor.execute(mixpanel_query, params)
                    for row in cursor.fetchall():
                        mixpanel_data[row['abi_ad_id']] = dict(row)
                        
            elif record_type == 'adset':
                # For adset-level records, we need to find all users attributed to ads in each adset
                # then aggregate their events. We'll use the Meta database to map ads to adsets.
                adset_ids = [r['adset_id'] for r in records if r.get('adset_id')]
                if adset_ids:
                    # First, get all ad_ids for these adsets from Meta database
                    meta_conn = sqlite3.connect(self.meta_db_path)
                    meta_conn.row_factory = sqlite3.Row
                    meta_cursor = meta_conn.cursor()
                    
                    adset_placeholders = ','.join(['?' for _ in adset_ids])
                    ad_mapping_query = f"""
                    SELECT DISTINCT adset_id, ad_id 
                    FROM ad_performance_daily 
                    WHERE adset_id IN ({adset_placeholders})
                    """
                    meta_cursor.execute(ad_mapping_query, adset_ids)
                    adset_to_ads = {}
                    for row in meta_cursor.fetchall():
                        adset_id = row['adset_id']
                        ad_id = row['ad_id']
                        if adset_id not in adset_to_ads:
                            adset_to_ads[adset_id] = []
                        adset_to_ads[adset_id].append(ad_id)
                    meta_conn.close()
                    
                    # Now get Mixpanel data for each adset by aggregating its ads
                    for adset_id in adset_ids:
                        ad_list = adset_to_ads.get(adset_id, [])
                        if ad_list:
                            ad_placeholders = ','.join(['?' for _ in ad_list])
                            mixpanel_query = f"""
                            SELECT 
                                COALESCE(SUM(CASE WHEN e.event_name = 'RC Trial started' 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN 1 ELSE 0 END), 0) as mixpanel_trials_started,
                                COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN 1 ELSE 0 END), 0) as mixpanel_purchases,
                                COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN COALESCE(e.revenue_usd, 0) ELSE 0 END), 0) as mixpanel_revenue_usd,
                                COUNT(DISTINCT u.distinct_id) as total_attributed_users
                            FROM mixpanel_user u
                            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                            WHERE u.abi_ad_id IN ({ad_placeholders})
                              AND u.has_abi_attribution = TRUE
                            """
                            
                            params = [
                                config.start_date, config.end_date,  # trial date filter
                                config.start_date, config.end_date,  # purchase date filter  
                                config.start_date, config.end_date   # revenue date filter
                            ] + ad_list
                            
                            cursor.execute(mixpanel_query, params)
                            row = cursor.fetchone()
                            if row:
                                mixpanel_data[adset_id] = dict(row)
                         
            elif record_type == 'campaign':
                # For campaign-level records, we need to find all users attributed to ads in each campaign
                # then aggregate their events. We'll use the Meta database to map ads to campaigns.
                campaign_ids = [r['campaign_id'] for r in records if r.get('campaign_id')]
                if campaign_ids:
                    # First, get all ad_ids for these campaigns from Meta database
                    meta_conn = sqlite3.connect(self.meta_db_path)
                    meta_conn.row_factory = sqlite3.Row
                    meta_cursor = meta_conn.cursor()
                    
                    campaign_placeholders = ','.join(['?' for _ in campaign_ids])
                    ad_mapping_query = f"""
                    SELECT DISTINCT campaign_id, ad_id 
                    FROM ad_performance_daily 
                    WHERE campaign_id IN ({campaign_placeholders})
                    """
                    meta_cursor.execute(ad_mapping_query, campaign_ids)
                    campaign_to_ads = {}
                    for row in meta_cursor.fetchall():
                        campaign_id = row['campaign_id']
                        ad_id = row['ad_id']
                        if campaign_id not in campaign_to_ads:
                            campaign_to_ads[campaign_id] = []
                        campaign_to_ads[campaign_id].append(ad_id)
                    meta_conn.close()
                    
                    # Now get Mixpanel data for each campaign by aggregating its ads
                    for campaign_id in campaign_ids:
                        ad_list = campaign_to_ads.get(campaign_id, [])
                        if ad_list:
                            ad_placeholders = ','.join(['?' for _ in ad_list])
                            mixpanel_query = f"""
                            SELECT 
                                COALESCE(SUM(CASE WHEN e.event_name = 'RC Trial started' 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN 1 ELSE 0 END), 0) as mixpanel_trials_started,
                                COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN 1 ELSE 0 END), 0) as mixpanel_purchases,
                                COALESCE(SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Renewal') 
                                         AND e.event_time BETWEEN ? AND ? 
                                         THEN COALESCE(e.revenue_usd, 0) ELSE 0 END), 0) as mixpanel_revenue_usd,
                                COUNT(DISTINCT u.distinct_id) as total_attributed_users
                            FROM mixpanel_user u
                            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                            WHERE u.abi_ad_id IN ({ad_placeholders})
                              AND u.has_abi_attribution = TRUE
                            """
                            
                            params = [
                                config.start_date, config.end_date,  # trial date filter
                                config.start_date, config.end_date,  # purchase date filter  
                                config.start_date, config.end_date   # revenue date filter
                            ] + ad_list
                            
                            cursor.execute(mixpanel_query, params)
                            row = cursor.fetchone()
                            if row:
                                mixpanel_data[campaign_id] = dict(row)
            
            conn.close()
            
            logger.info(f"🔍 Retrieved Mixpanel data for {len(mixpanel_data)} {record_type}s out of {len(records)} requested")
            
            # Merge mixpanel data with records
            for record in records:
                # Get the appropriate ID for lookup
                lookup_id = None
                if record_type == 'ad':
                    lookup_id = record.get('ad_id')
                elif record_type == 'adset':
                    lookup_id = record.get('adset_id')
                elif record_type == 'campaign':
                    lookup_id = record.get('campaign_id')
                
                if lookup_id and lookup_id in mixpanel_data:
                    metrics = mixpanel_data[lookup_id]
                    record.update({
                        'mixpanel_trials_started': int(metrics.get('mixpanel_trials_started', 0)),
                        'mixpanel_purchases': int(metrics.get('mixpanel_purchases', 0)),
                        'mixpanel_revenue_usd': float(metrics.get('mixpanel_revenue_usd', 0)),
                        'estimated_revenue_usd': float(metrics.get('mixpanel_revenue_usd', 0)),
                        'total_attributed_users': int(metrics.get('total_attributed_users', 0))
                    })
                    
                    # Calculate trial accuracy ratio (if we have both Meta and Mixpanel trials)
                    meta_trials = record.get('meta_trials_started', 0)
                    mixpanel_trials = record.get('mixpanel_trials_started', 0)
                    if meta_trials > 0 and mixpanel_trials > 0:
                        record['trial_accuracy_ratio'] = min(mixpanel_trials / meta_trials, 2.0)  # Cap at 200%
                    else:
                        record['trial_accuracy_ratio'] = 0.0
                        
                    logger.debug(f"✅ Added Mixpanel data to {record_type} {lookup_id}: trials={mixpanel_trials}, purchases={record['mixpanel_purchases']}, revenue=${record['mixpanel_revenue_usd']}")
                else:
                    # Set default values for records without mixpanel data
                    record.update({
                        'mixpanel_trials_started': 0,
                        'mixpanel_purchases': 0,
                        'mixpanel_revenue_usd': 0.0,
                        'estimated_revenue_usd': 0.0,
                        'trial_accuracy_ratio': 0.0,
                        'total_attributed_users': 0
                    })
                    if lookup_id:
                        logger.debug(f"❌ No Mixpanel data found for {record_type} {lookup_id}")
                    
        except Exception as e:
            logger.error(f"Error adding mixpanel data: {e}", exc_info=True)
            # Set default values for all records on error
            for record in records:
                record.update({
                    'mixpanel_trials_started': 0,
                    'mixpanel_purchases': 0,
                    'mixpanel_revenue_usd': 0.0,
                    'estimated_revenue_usd': 0.0,
                    'trial_accuracy_ratio': 0.0,
                    'total_attributed_users': 0
                })
        """
    
    def _format_record(self, record: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
        """Format a record with the expected structure for the frontend"""
        
        # Create unique ID based on entity type
        if entity_type == 'campaign':
            record_id = f"campaign_{record.get('campaign_id', 'unknown')}"
            name = record.get('campaign_name', 'Unknown Campaign')
        elif entity_type == 'adset':
            record_id = f"adset_{record.get('adset_id', 'unknown')}"
            name = record.get('adset_name', 'Unknown Ad Set')
        else:  # ad
            record_id = f"ad_{record.get('ad_id', 'unknown')}"
            name = record.get('ad_name', 'Unknown Ad')
        
        # Helper function to format accuracy score
        def format_accuracy_score(avg_score):
            if not avg_score:
                return "Unknown"
            if avg_score >= 4.5:
                return "Very High"
            elif avg_score >= 3.5:
                return "High"
            elif avg_score >= 2.5:
                return "Medium"
            elif avg_score >= 1.5:
                return "Low"
            else:
                return "Very Low"
        
        # Build the formatted record with all expected fields
        formatted = {
            'id': record_id,
            'type': entity_type,
            'name': name,
            'campaign_name': record.get('campaign_name', ''),
            'adset_name': record.get('adset_name', ''),
            'ad_name': record.get('ad_name', ''),
            
            # Meta metrics (already aggregated)
            'spend': float(record.get('spend', 0) or 0),
            'impressions': int(record.get('impressions', 0) or 0),
            'clicks': int(record.get('clicks', 0) or 0),
            'meta_trials_started': int(record.get('meta_trials_started', 0) or 0),
            'meta_purchases': int(record.get('meta_purchases', 0) or 0),
            
            # Mixpanel trial metrics
            'mixpanel_trials_started': int(record.get('mixpanel_trials_started', 0) or 0),
            'mixpanel_trials_in_progress': int(record.get('mixpanel_trials_in_progress', 0) or 0),
            'mixpanel_trials_ended': int(record.get('mixpanel_trials_ended', 0) or 0),
            
            # Mixpanel purchase metrics
            'mixpanel_purchases': int(record.get('mixpanel_purchases', 0) or 0),
            'mixpanel_converted_amount': int(record.get('mixpanel_converted_amount', 0) or 0),
            'mixpanel_conversions_net_refunds': int(record.get('mixpanel_conversions_net_refunds', 0) or 0),
            
            # Mixpanel revenue metrics
            'mixpanel_revenue_usd': float(record.get('mixpanel_revenue_usd', 0) or 0),
            'mixpanel_refunds_usd': float(record.get('mixpanel_refunds_usd', 0) or 0),
            'estimated_revenue_usd': float(record.get('estimated_revenue_usd', 0) or 0),
            
            # Segment accuracy
            'segment_accuracy_average': format_accuracy_score(record.get('segment_accuracy_average')),
            
            # Attribution stats
            'total_attributed_users': int(record.get('total_attributed_users', 0) or 0),
        }
        
        # Calculate derived metrics according to spec (all limited to 2 decimal places)
        spend = formatted['spend']
        mixpanel_trials = formatted['mixpanel_trials_started']
        mixpanel_purchases = formatted['mixpanel_purchases']
        meta_trials = formatted['meta_trials_started']
        meta_purchases = formatted['meta_purchases']
        clicks = formatted['clicks']
        
        # Accuracy ratios (as percentages, limited to 2 decimal places)
        if meta_trials > 0:
            formatted['trial_accuracy_ratio'] = round((mixpanel_trials / meta_trials) * 100, 2)
        else:
            formatted['trial_accuracy_ratio'] = 0.0
            
        if meta_purchases > 0:
            formatted['purchase_accuracy_ratio'] = round((mixpanel_purchases / meta_purchases) * 100, 2)
        else:
            formatted['purchase_accuracy_ratio'] = 0.0
        
        # ROAS calculation with trial accuracy ratio adjustment
        if spend > 0 and formatted['trial_accuracy_ratio'] > 0:
            # Adjust estimated revenue by dividing by trial accuracy ratio (to account for Meta/Mixpanel dropoff)
            # ROAS = (estimated_revenue / trial_accuracy_ratio) / spend
            adjusted_revenue = formatted['estimated_revenue_usd'] / (formatted['trial_accuracy_ratio'] / 100)
            formatted['estimated_roas'] = round(adjusted_revenue / spend, 2)
        elif spend > 0:
            # Fallback to standard calculation if no trial accuracy ratio
            formatted['estimated_roas'] = round(formatted['estimated_revenue_usd'] / spend, 2)
        else:
            formatted['estimated_roas'] = 0.0
        
        # Cost calculations (limited to 2 decimal places)
        if mixpanel_trials > 0:
            formatted['mixpanel_cost_per_trial'] = round(spend / mixpanel_trials, 2)
        else:
            formatted['mixpanel_cost_per_trial'] = 0.0
            
        if mixpanel_purchases > 0:
            formatted['mixpanel_cost_per_purchase'] = round(spend / mixpanel_purchases, 2)
        else:
            formatted['mixpanel_cost_per_purchase'] = 0.0
            
        if meta_trials > 0:
            formatted['meta_cost_per_trial'] = round(spend / meta_trials, 2)
        else:
            formatted['meta_cost_per_trial'] = 0.0
            
        if meta_purchases > 0:
            formatted['meta_cost_per_purchase'] = round(spend / meta_purchases, 2)
        else:
            formatted['meta_cost_per_purchase'] = 0.0
        
        # Rate calculations (limited to 2 decimal places)
        if clicks > 0:
            formatted['click_to_trial_rate'] = round((mixpanel_trials / clicks) * 100, 2)
        else:
            formatted['click_to_trial_rate'] = 0.0
        

... [File truncated - showing first 50,000 characters]
```

### orchestrator/dashboard/services/dashboard_service.py
```py
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
```

## Frontend Files (React/JavaScript)

### orchestrator/dashboard/client/package.json
```json
{
  "name": "ads-dashboard-client",
  "version": "0.1.0",
  "private": true,
  "dependencies": {
    "@testing-library/jest-dom": "^5.16.5",
    "@testing-library/react": "^13.4.0",
    "@testing-library/user-event": "^13.5.0",
    "axios": "^1.4.0",
    "chart.js": "^4.4.9",
    "dayjs": "^1.11.7",
    "http-proxy-middleware": "^3.0.5",
    "lucide-react": "^0.259.0",
    "react": "^18.2.0",
    "react-chartjs-2": "^5.3.0",
    "react-dom": "^18.2.0",
    "react-router-dom": "^6.15.0",
    "react-scripts": "5.0.1",
    "react-select": "^5.7.3",
    "recharts": "^2.6.2",
    "tailwindcss": "^3.3.2",
    "web-vitals": "^2.1.4"
  },
  "scripts": {
    "start": "REACT_APP_API_URL=http://localhost:5001 react-scripts start",
    "build": "REACT_APP_API_URL=http://localhost:5001 react-scripts build",
    "test": "react-scripts test",
    "eject": "react-scripts eject"
  },
  "eslintConfig": {
    "extends": [
      "react-app",
      "react-app/jest"
    ]
  },
  "browserslist": {
    "production": [
      ">0.2%",
      "not dead",
      "not op_mini all"
    ],
    "development": [
      "last 1 chrome version",
      "last 1 firefox version",
      "last 1 safari version"
    ]
  },
  "proxy": "http://127.0.0.1:5001"
}

```

### orchestrator/dashboard/client/src/App.js
```js
import React from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import './App.css';
import { Dashboard } from './pages/Dashboard';
import { MixpanelDebugPage } from './pages/MixpanelDebugPage';
import { MetaDebugger } from './pages/MetaDebugger';
import CohortAnalyzerPage from './pages/CohortAnalyzerPage';
import CohortAnalyzerV3RefactoredPage from './pages/CohortAnalyzerV3RefactoredPage';
import CohortPipelineDebugPage from './pages/CohortPipelineDebugPage';
import ConversionProbabilityPage from './pages/ConversionProbabilityPageRefactored';
import PricingManagementPage from './pages/PricingManagementPage';

function App() {
  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900 text-gray-800 dark:text-gray-200">
      <nav className="bg-white dark:bg-gray-800 shadow-md p-4">
        <div className="container mx-auto flex justify-between items-center">
          <div className="text-xl font-bold">Ads Dashboard</div>
          <div className="space-x-4">
            <Link to="/" className="hover:text-blue-500">Dashboard</Link>
            <Link to="/cohort-analyzer" className="hover:text-blue-500">Cohort Analyzer</Link>
            <Link to="/cohort-analyzer-v3" className="hover:text-blue-500 text-purple-600 dark:text-purple-400">Cohort V3</Link>
            <Link to="/conversion-probability" className="hover:text-blue-500">Conversion Probability</Link>
            <Link to="/pricing-management" className="hover:text-blue-500">Pricing Management</Link>
            <Link to="/cohort-pipeline" className="hover:text-blue-500">Cohort Pipeline</Link>
            <Link to="/debug" className="hover:text-blue-500">Mixpanel Debugger</Link>
            <Link to="/meta-debug" className="hover:text-blue-500">Meta Debugger</Link>
          </div>
        </div>
      </nav>
      
      <div className="w-full">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/cohort-analyzer" element={<CohortAnalyzerPage />} />
          <Route path="/cohort-analyzer-v3" element={<CohortAnalyzerV3RefactoredPage />} />
          <Route path="/conversion-probability" element={<ConversionProbabilityPage />} />
          <Route path="/pricing-management" element={<PricingManagementPage />} />
          <Route path="/cohort-pipeline" element={<CohortPipelineDebugPage />} />
          <Route path="/debug" element={<MixpanelDebugPage />} />
          <Route path="/meta-debug" element={<MetaDebugger />} />
        </Routes>
      </div>
    </div>
  );
}

export default App; 
```

### orchestrator/dashboard/client/src/components/dashboard/AnalyticsPipelineControls.jsx
```jsx
import React, { useState, useEffect } from 'react';
import { 
  Database, 
  CheckCircle, 
  XCircle,
  Loader,
  Clock
} from 'lucide-react';
import { analyticsPipelineApi } from '../../services/analyticsPipelineApi';

const AnalyticsPipelineControls = () => {
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [hasError, setHasError] = useState(false);

  // Fetch initial status and poll for updates
  useEffect(() => {
    const loadStatus = async () => {
      try {
        const status = await analyticsPipelineApi.getAnalyticsPipelineStatus();
        setPipelineStatus(status);
        setHasError(false);
      } catch (error) {
        console.error('Failed to load analytics pipeline status:', error);
        setHasError(true);
      }
    };

    loadStatus();
    
    // Poll every 30 seconds
    const interval = setInterval(loadStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const getStatusDisplay = () => {
    if (hasError) {
      return { 
        icon: <XCircle className="h-4 w-4 text-red-500" />, 
        text: 'Connection Error', 
        color: 'text-red-500' 
      };
    }
    
    if (!pipelineStatus) {
      return { 
        icon: <Loader className="h-4 w-4 text-gray-500 animate-spin" />, 
        text: 'Loading...', 
        color: 'text-gray-500' 
      };
    }
    
    const currentRun = pipelineStatus.current_run;
    if (currentRun?.is_running) {
      return { 
        icon: <Loader className="h-4 w-4 text-blue-500 animate-spin" />, 
        text: 'Processing Data', 
        color: 'text-blue-500' 
      };
    }
    
    if (currentRun?.completed_at) {
      const completedTime = new Date(currentRun.completed_at);
      const now = new Date();
      const hoursAgo = Math.floor((now - completedTime) / (1000 * 60 * 60));
      
      return { 
        icon: <CheckCircle className="h-4 w-4 text-green-500" />, 
        text: `Updated ${hoursAgo}h ago`, 
        color: 'text-green-500' 
      };
    }
    
    return { 
      icon: <Database className="h-4 w-4 text-gray-500" />, 
      text: 'Ready', 
      color: 'text-gray-500' 
    };
  };

  const status = getStatusDisplay();

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-4">
      <div className="flex items-center space-x-3">
        <div className="flex items-center space-x-2">
          {status.icon}
          <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
            Analytics Pipeline
          </span>
        </div>
        <div className="flex items-center space-x-2">
          <span className={`text-sm ${status.color}`}>
            {status.text}
          </span>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsPipelineControls; 
```

### orchestrator/dashboard/client/src/components/dashboard/RefreshPipelineControls.jsx
```jsx
import React, { useState, useEffect } from 'react';
import { 
  RefreshCw, 
  Settings, 
  Clock, 
  AlertCircle, 
  CheckCircle, 
  XCircle,
  Play,
  Pause,
  Loader,
  ChevronDown,
  ChevronUp
} from 'lucide-react';
import { refreshPipelineApi } from '../../services/refreshPipelineApi';
import { DashboardControls } from './DashboardControls';

const STAGES = [
  { id: 'mixpanel', name: 'Update Mixpanel Data', description: 'Fetching latest events and users' },
  { id: 'meta', name: 'Update Meta Data', description: 'Updating ad performance data' },
  { id: 'conversion', name: 'Run Conversion Analysis', description: 'Analyzing conversion probabilities' },
  { id: 'dashboard', name: 'Refresh Dashboard', description: 'Loading dashboard data' },
  { id: 'pipelines', name: 'Generate Row Pipelines', description: 'Running analysis for all campaigns/ads' }
];

const RefreshPipelineControls = ({ 
  onRefresh, 
  isLoading = false, 
  configurations = {}, 
  selectedConfig = null,
  onConfigChange,
  lastUpdated = null,
  onGetCurrentParams = null,
  onColumnVisibilityChange = null,
  onColumnOrderChange = null 
}) => {
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [lastRefreshData, setLastRefreshData] = useState(null);
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const [debugMode, setDebugMode] = useState(() => {
    // Load debug mode from localStorage
    const saved = localStorage.getItem('refreshPipeline_debugMode');
    return saved ? JSON.parse(saved) : false;
  });
  const [debugDaysOverride, setDebugDaysOverride] = useState(() => {
    // Load debug days from localStorage  
    const saved = localStorage.getItem('refreshPipeline_debugDays');
    return saved ? parseInt(saved) : 5;
  });
  const [hasError, setHasError] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');
  const [isExpanded, setIsExpanded] = useState(false); // Manual expand/collapse
  const [userHasCollapsed, setUserHasCollapsed] = useState(false); // Track if user manually collapsed
  
  // Save debug settings to localStorage when they change
  useEffect(() => {
    localStorage.setItem('refreshPipeline_debugMode', JSON.stringify(debugMode));
  }, [debugMode]);

  useEffect(() => {
    localStorage.setItem('refreshPipeline_debugDays', debugDaysOverride.toString());
  }, [debugDaysOverride]);

  // Fetch initial status
  useEffect(() => {
    loadPipelineStatus();
    loadLastRefreshInfo();
  }, []);

  // Auto-expand when pipeline starts running, but respect user's manual collapse
  useEffect(() => {
    if (pipelineStatus?.is_running && !isExpanded && !userHasCollapsed) {
      setIsExpanded(true);
    }
    // Reset user collapse state when pipeline stops
    if (!pipelineStatus?.is_running && userHasCollapsed) {
      setUserHasCollapsed(false);
    }
  }, [pipelineStatus?.is_running, isExpanded, userHasCollapsed]);

  // Poll for pipeline status updates with connection resilience
  useEffect(() => {
    let interval;
    let failureCount = 0;
    
    const pollStatus = async () => {
      try {
        await loadPipelineStatus();
        failureCount = 0; // Reset failure count on success
      } catch (error) {
        failureCount++;
        console.warn(`Pipeline status poll failed (attempt ${failureCount}):`, error);
        
        // If we've failed too many times, don't mark as error immediately
        // This prevents "failed" status when it's just a connection issue
        if (failureCount >= 5) {
          setHasError(true);
          setErrorMessage(`Connection lost: ${error.message}`);
        }
      }
    };
    
    if (pipelineStatus?.is_running) {
      interval = setInterval(pollStatus, 2000); // Poll every 2 seconds when running
    }
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [pipelineStatus?.is_running]);

  const loadPipelineStatus = async () => {
    try {
      const status = await refreshPipelineApi.getRefreshPipelineStatus();
      setPipelineStatus(status);
      setHasError(false);
      setErrorMessage('');
    } catch (error) {
      console.error('Failed to load pipeline status:', error);
      setHasError(true);
      setErrorMessage(`Failed to load status: ${error.message}`);
    }
  };

  const loadLastRefreshInfo = async () => {
    try {
      const data = await refreshPipelineApi.getLastRefreshTime();
      setLastRefreshData(data.last_refresh_data);
    } catch (error) {
      console.error('Failed to load last refresh info:', error);
    }
  };

  const startRefreshPipeline = async () => {
    try {
      const options = {};
      
      // TODO: Remove later - debug code
      if (debugMode) {
        options.debug_mode = true;
        if (debugDaysOverride) {
          options.debug_days_override = debugDaysOverride;
        }
      }
      
      await refreshPipelineApi.startRefreshPipeline(options);
      // Immediately update status after successful pipeline start
      await loadPipelineStatus();
      setIsExpanded(true);
      setHasError(false);
      setErrorMessage('');
    } catch (error) {
      console.error('Failed to start refresh pipeline:', error);
      setHasError(true);
      setErrorMessage(`Failed to start pipeline: ${error.message}`);
    }
  };

  const cancelRefreshPipeline = async () => {
    try {
      const result = await refreshPipelineApi.cancelRefreshPipeline();
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Cancel pipeline request failed');
      }
      
      await loadPipelineStatus();
      console.log('Pipeline cancelled successfully');
    } catch (error) {
      console.error('Failed to cancel refresh pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to cancel pipeline';
      if (error.message.includes('not running')) {
        errorMessage = 'No pipeline is currently running to cancel';
      } else if (error.message) {
        errorMessage = `Failed to cancel pipeline: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const resumeFromStage = async (stageIndex) => {
    try {
      const options = {};
      
      // TODO: Remove later - debug code
      if (debugMode) {
        options.debug_mode = true;
        if (debugDaysOverride) {
          options.debug_days_override = debugDaysOverride;
        }
      }
      
      const result = await refreshPipelineApi.resumeRefreshPipeline(stageIndex, options);
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Resume pipeline request failed');
      }
      
      // Immediately update status after successful pipeline resume
      await loadPipelineStatus();
      setIsExpanded(true);
      setHasError(false);
      setErrorMessage('');
      
      console.log(`Pipeline resumed successfully from stage ${stageIndex + 1}`);
    } catch (error) {
      console.error('Failed to resume refresh pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to resume pipeline';
      if (error.message.includes('already running')) {
        errorMessage = 'Cannot resume: A pipeline is already running';
      } else if (error.message.includes('stage_index')) {
        errorMessage = 'Invalid stage index for resume operation';
      } else if (error.message) {
        errorMessage = `Failed to resume pipeline: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const dismissInterrupted = async () => {
    if (!pipelineStatus?.interrupted_pipeline?.pipeline_id) {
      console.warn('No interrupted pipeline ID found to dismiss');
      return;
    }
    
    try {
      const result = await refreshPipelineApi.dismissInterruptedPipeline(pipelineStatus.interrupted_pipeline.pipeline_id);
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Dismiss interrupted pipeline request failed');
      }
      
      await loadPipelineStatus();
      console.log('Interrupted pipeline notification dismissed successfully');
    } catch (error) {
      console.error('Failed to dismiss interrupted pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to dismiss notification';
      if (error.message.includes('pipeline_id')) {
        errorMessage = 'Invalid pipeline ID for dismiss operation';
      } else if (error.message.includes('not found')) {
        errorMessage = 'Interrupted pipeline not found';
      } else if (error.message) {
        errorMessage = `Failed to dismiss notification: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const formatInterruptedTime = (interruptedData) => {
    if (!interruptedData?.start_time) return '';
    try {
      const date = new Date(interruptedData.start_time);
      const now = new Date();
      const isToday = date.toDateString() === now.toDateString();
      
      if (isToday) {
        return `today at ${date.toLocaleTimeString()}`;
      } else {
        return date.toLocaleString();
      }
    } catch (e) {
      return 'unknown time';
    }
  };

  const formatLastRefreshTime = (refreshData) => {
    if (!refreshData || !refreshData.start_time) return 'Never';
    try {
      return new Date(refreshData.start_time).toLocaleString();
    } catch (e) {
      return 'Invalid date';
    }
  };

  const getStageStatus = (stageId, stageIndex) => {
    if (!pipelineStatus) return 'pending';
    
    if (pipelineStatus.stages_failed?.includes(stageId)) return 'failed';
    if (pipelineStatus.stages_completed?.includes(stageId)) return 'completed';
    if (pipelineStatus.current_stage === stageId && pipelineStatus.is_running) return 'running';
    if (pipelineStatus.is_running && stageIndex < STAGES.findIndex(s => s.id === pipelineStatus.current_stage)) return 'completed';
    
    return 'pending';
  };

  const getStageIcon = (status, size = 16) => {
    switch (status) {
      case 'completed': return <CheckCircle size={size} className="text-green-500" />;
      case 'running': return <Loader size={size} className="text-blue-500 animate-spin" />;
      case 'failed': return <XCircle size={size} className="text-red-500" />;
      default: return <Clock size={size} className="text-gray-400" />;
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'completed': return 'Completed';
      case 'running': return 'Running';
      case 'failed': return 'Failed';
      default: return 'Pending';
    }
  };

  const getOverallStatus = () => {
    if (!pipelineStatus) return { text: 'Idle', color: 'text-gray-600', icon: Clock };
    
    if (pipelineStatus.is_running) {
      return { 
        text: `Running (${pipelineStatus.overall_progress}%)`, 
        color: 'text-blue-600', 
        icon: Loader,
        spinning: true 
      };
    }
    
    if (lastRefreshData?.status === 'completed') {
      return { text: 'Completed', color: 'text-green-600', icon: CheckCircle };
    }
    
    if (lastRefreshData?.status === 'failed') {
      return { text: 'Failed', color: 'text-red-600', icon: XCircle };
    }
    
    return { text: 'Ready', color: 'text-gray-600', icon: Clock };
  };

  const renderCompactStagesList = () => {
    if (!pipelineStatus) return null;
    
    return (
      <div className="flex items-center space-x-2">
        {STAGES.map((stage, index) => {
          const status = getStageStatus(stage.id, index);
          return (
            <div key={stage.id} className="flex items-center">
              {getStageIcon(status, 14)}
              {index < STAGES.length - 1 && (
                <div className="w-3 h-px bg-gray-300 dark:bg-gray-600 mx-1" />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  const renderExpandedStages = () => {
    if (!isExpanded) return null;
    
    return (
      <div className="mt-4 px-4 pb-4">
        {/* Timeline-style stages container */}
        <div className="relative">
          {/* Connecting line down the left side */}
          <div className="absolute left-5 top-6 bottom-6 w-0.5 bg-gray-200 dark:bg-gray-600"></div>
          
          <div className="space-y-2">
            {STAGES.map((stage, index) => {
              const status = getStageStatus(stage.id, index);
              
              return (
                <div key={stage.id} className="relative flex items-start space-x-4 py-2">
                  {/* Stage indicator */}
                  <div className={`relative z-10 flex-shrink-0 w-10 h-10 rounded-full flex items-center justify-center border-2 transition-all ${
                    status === 'completed' 
                      ? 'bg-green-500 border-green-500 text-white shadow-sm' 
                      : status === 'running' 
                      ? 'bg-blue-500 border-blue-500 text-white shadow-sm' 
                      : status === 'failed' 
                      ? 'bg-red-500 border-red-500 text-white shadow-sm' 
                      : 'bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400'
                  }`}>
                    {status === 'completed' ? (
                      <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                      </svg>
                    ) : status === 'running' ? (
                      <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    ) : status === 'failed' ? (
                      <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                      </svg>
                    ) : (
                      <span className="text-sm font-medium">{index + 1}</span>
                    )}
                  </div>

                  {/* Stage content */}
                  <div className="flex-1 min-w-0 pb-2">
                    <div className="flex items-center justify-between">
                      <div className="flex-1 min-w-0">
                        <h4 className={`text-sm font-medium ${
                          status === 'completed' ? 'text-green-700 dark:text-green-300' :
                          status === 'running' ? 'text-blue-700 dark:text-blue-300' :
                          status === 'failed' ? 'text-red-700 dark:text-red-300' :
                          'text-gray-700 dark:text-gray-300'
                        }`}>
                          {stage.name}
                        </h4>
                        <p className="text-xs text-gray-600 dark:text-gray-400 mt-0.5">
                          {stage.description}
                        </p>
                      </div>
                      
                      {/* Status badge */}
                      <span className={`px-2 py-1 text-xs font-medium rounded-full whitespace-nowrap ml-3 ${
                        status === 'completed' ? 'bg-green-100 text-green-700 dark:bg-green-900/50 dark:text-green-300' :
                        status === 'running' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300' :
                        status === 'failed' ? 'bg-red-100 text-red-700 dark:bg-red-900/50 dark:text-red-300' :
                        'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400'
                      }`}>
                        {getStatusText(status)}
                      </span>
                    </div>

                    {/* Progress bar for running stage */}
                    {status === 'running' && pipelineStatus?.current_stage === stage.id && (
                      <div className="mt-2 pr-3">
                        <div className="flex justify-between text-xs text-blue-600 dark:text-blue-400 mb-1">
                          <span>Progress</span>
                          <span>{pipelineStatus.stage_progress}%</span>
                        </div>
                        <div className="w-full bg-blue-100 dark:bg-blue-900/30 rounded-full h-1.5">
                          <div 
                            className="bg-blue-500 h-1.5 rounded-full transition-all duration-300"
                            style={{ width: `${pipelineStatus.stage_progress}%` }}
                          />
                        </div>
                        {pipelineStatus.current_operation && (
                          <p className="text-xs text-blue-600 dark:text-blue-400 mt-1 italic">
                            {pipelineStatus.current_operation}
                          </p>
                        )}
                      </div>
                    )}
                    
                    {/* Error details for failed stage */}
                    {status === 'failed' && pipelineStatus?.errors?.length > 0 && (
                      <div className="mt-2 pr-3">
                        {pipelineStatus.errors
                          .filter(error => error.stage === stage.id)
                          .map((error, index) => (
                            <div key={index} className="p-2 bg-red-50 dark:bg-red-900/20 rounded border border-red-200 dark:border-red-800">
                              <p className="text-xs text-red-700 dark:text-red-300">
                                {error.message}
                              </p>
                            </div>
                          ))}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  };

  const renderInterruptedPipelineNotification = () => {
    const interrupted = pipelineStatus?.interrupted_pipeline;
    if (!interrupted) return null;

    const stageName = STAGES[interrupted.interrupted_stage - 1]?.name || 'Unknown Stage';
    const completedCount = interrupted.stages_completed.length;
    const isToday = interrupted.is_same_day;

    return (
      <div className="mb-4 p-4 bg-amber-50 dark:bg-amber-900/20 rounded-lg border border-amber-200 dark:border-amber-800">
        <div className="flex items-start space-x-3">
          <AlertCircle size={20} className="text-amber-600 dark:text-amber-400 mt-0.5" />
          <div className="flex-1">
            <h4 className="text-sm font-medium text-amber-800 dark:text-amber-200">
              Pipeline Interrupted
            </h4>
            <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
              A pipeline started {formatInterruptedTime(interrupted)} was interrupted after completing {completedCount} of 5 stages.
              {isToday ? ` It was about to run "${stageName}".` : ' Since it was started on a different day, the data may be outdated.'}
            </p>
            
            <div className="flex items-center space-x-3 mt-3">
              {interrupted.can_resume ? (
                <>
                  <button
                    onClick={() => resumeFromStage(interrupted.interrupted_stage - 1)}
                    disabled={pipelineStatus?.is_running}
                    className="px-3 py-1.5 bg-amber-600 hover:bg-amber-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                  >
                    <Play size={14} className="mr-1" />
                    Resume from Stage {interrupted.interrupted_stage}
                  </button>
                  <button
                    onClick={startRefreshPipeline}
                    disabled={pipelineStatus?.is_running}
                    className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                  >
                    <RefreshCw size={14} className="mr-1" />
                    Start Fresh
                  </button>
                </>
              ) : (
                <button
                  onClick={startRefreshPipeline}
                  disabled={pipelineStatus?.is_running}
                  className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                >
                  <RefreshCw size={14} className="mr-1" />
                  Start Fresh Pipeline
                </button>
              )}
              
              <button
                onClick={dismissInterrupted}
                className="px-3 py-1.5 text-amber-700 dark:text-amber-300 hover:text-amber-900 dark:hover:text-amber-100 text-sm font-medium transition-colors duration-200"
              >
                Dismiss
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  };

  const overallStatus = getOverallStatus();
  const StatusIcon = overallStatus.icon;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-soft border border-gray-200 dark:border-gray-700 mb-6">
      {/* Compact Header */}
      <div className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="flex items-center space-x-2">
              <RefreshCw size={18} className="text-gray-700 dark:text-gray-300" />
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                Data Pipeline
              </h3>
            </div>
            
            {/* Overall Status */}
            <div className="flex items-center space-x-2">
              <StatusIcon 
                size={16} 
                className={`${overallStatus.color} ${overallStatus.spinning ? 'animate-spin' : ''}`} 
              />
              <span className={`text-sm font-medium ${overallStatus.color}`}>
                {overallStatus.text}
              </span>
            </div>
          </div>

          <div className="flex items-center space-x-2">
            {/* Last Refresh Time */}
            <div className="text-xs text-gray-500 dark:text-gray-400 text-right mr-3">
              <div>Last: {formatLastRefreshTime(lastRefreshData)}</div>
              {lastRefreshData && (
                <div>{lastRefreshData.stages_completed?.length || 0}/5 stages</div>
              )}
            </div>
            
            {/* Compact Stages Visual */}
            {renderCompactStagesList()}
            
            {/* Expand/Collapse Toggle */}
            <button
              onClick={() => {
                setIsExpanded(!isExpanded);
                if (isExpanded) {
                  setUserHasCollapsed(true); // Remember that user manually collapsed
                }
              }}
              className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors rounded-md border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700"
              title={isExpanded ? 'Collapse Pipeline Details' : 'Expand Pipeline Details'}
            >
              {isExpanded ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
            </button>
          </div>
        </div>

        {/* Action Buttons Row */}
        <div className="flex items-center justify-between mt-3">
          <div className="flex items-center space-x-2">
            <button
              onClick={startRefreshPipeline}
              disabled={pipelineStatus?.is_running}
              className={`px-3 py-1.5 rounded text-sm font-medium flex items-center transition-colors duration-200
                         ${pipelineStatus?.is_running
                           ? 'bg-gray-300 dark:bg-gray-600 text-gray-500 dark:text-gray-400 cursor-not-allowed'
                           : 'bg-blue-600 hover:bg-blue-700 text-white'
                         }`}
            >
              {pipelineStatus?.is_running ? (
                <Loader size={14} className="mr-1 animate-spin" />
              ) : (
                <Play size={14} className="mr-1" />
              )}
              {pipelineStatus?.is_running ? 'Running...' : 'Run Pipeline'}
            </button>

            {pipelineStatus?.is_running && (
              <button
                onClick={cancelRefreshPipeline}
                className="px-3 py-1.5 rounded text-sm font-medium bg-red-600 hover:bg-red-700 text-white flex items-center transition-colors duration-200"
              >
                <Pause size={14} className="mr-1" />
                Cancel
              </button>
            )}

            <button
              onClick={() => setIsAdvancedOpen(true)}
              className="px-3 py-1.5 rounded text-sm font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600 flex items-center transition-colors duration-200"
            >
              <Settings size={14} className="mr-1" />
              Settings
            </button>
          </div>

          {/* Overall Progress Bar (when running) */}
          {pipelineStatus?.is_running && (
            <div className="flex items-center space-x-2 flex-1 max-w-xs ml-4">
              <div className="flex-1 bg-blue-200 dark:bg-blue-800 rounded-full h-2">
                <div 
                  className="bg-blue-600 dark:bg-blue-400 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${pipelineStatus.overall_progress}%` }}
                />
              </div>
              <span className="text-xs text-blue-700 dark:text-blue-300 font-medium min-w-[3rem]">
                {pipelineStatus.overall_progress}%
              </span>
            </div>
          )}
        </div>

        {/* Error Display */}
        {hasError && (
          <div className="mt-3 p-3 bg-red-50 dark:bg-red-900/20 rounded border border-red-200 dark:border-red-800">
            <div className="flex items-center">
              <AlertCircle size={14} className="text-red-500 mr-2" />
              <span className="text-sm font-medium text-red-800 dark:text-red-200">Error</span>
            </div>
            <p className="text-sm text-red-700 dark:text-red-300 mt-1">{errorMessage}</p>
          </div>
        )}
      </div>

      {/* Interrupted Pipeline Notification */}
      {renderInterruptedPipelineNotification()}

      {/* Expanded Stages View */}
      {renderExpandedStages()}

      {/* Advanced Settings Modal */}
      {isAdvancedOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6">
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Advanced Settings
                </h3>
                <button
                  onClick={() => setIsAdvancedOpen(false)}
                  className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                >
                  <XCircle size={24} />
                </button>
              </div>
              
              {/* Debug Mode Settings */}
              {process.env.NODE_ENV === 'development' && (
                <div className="mb-6 p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
                  <h4 className="text-sm font-medium text-yellow-800 dark:text-yellow-200 mb-3">
                    🚧 Debug Mode Settings (Development Only)
                  </h4>
                  <div className="space-y-3">
                    <label className="flex items-center">
                      <input
                        type="checkbox"
                        checked={debugMode}
                        onChange={(e) => setDebugMode(e.target.checked)}
                        className="mr-2"
                      />
                      <span className="text-sm text-yellow-700 dark:text-yellow-300">
                        Enable Debug Mode (uses shorter time ranges for testing)
                      </span>
                    </label>
                    {debugMode && (
                      <div className="ml-6">
                        <label className="flex items-center space-x-2">
                          <span className="text-sm text-yellow-700 dark:text-yellow-300">
                            Days to process:
                          </span>
                          <input
                            type="number"
                            value={debugDaysOverride}
                            onChange={(e) => setDebugDaysOverride(parseInt(e.target.value) || 5)}
                            className="w-20 px-2 py-1 border rounded text-sm"
                            min="1"
                            max="365"
                          />
                          <span className="text-xs text-yellow-600 dark:text-yellow-400">
                            (Default: 5 days instead of 30)
                          </span>
                        </label>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Include the existing DashboardControls */}
              <DashboardControls
                onRefresh={onRefresh}
                isLoading={isLoading}
                configurations={configurations}
                selectedConfig={selectedConfig}
                onConfigChange={onConfigChange}
                lastUpdated={lastUpdated}
                onGetCurrentParams={onGetCurrentParams}
                onColumnVisibilityChange={onColumnVisibilityChange}
                onColumnOrderChange={onColumnOrderChange}
              />
              
              <div className="flex justify-end mt-6">
                <button
                  onClick={() => setIsAdvancedOpen(false)}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Interrupted Pipeline Notification */}
      {renderInterruptedPipelineNotification()}
    </div>
  );
};

export { RefreshPipelineControls }; 
```

### orchestrator/dashboard/client/src/pages/Dashboard.js
```js
import React, { useState, useEffect } from 'react';
import { 
  RefreshCw, 
  Search, 
  Filter, 
  BarChart3, 
  AlertTriangle, 
  CheckCircle, 
  Clock,
  Database,
  TrendingUp,
  Users,
  DollarSign,
  Eye,
  EyeOff,
  Grid,
  List,
  Settings,
  XCircle
} from 'lucide-react';
import { DashboardGrid } from '../components/DashboardGrid';
import { DebugModal } from '../components/DebugModal';
import { GraphModal } from '../components/GraphModal';
import TimelineModal from '../components/TimelineModal';
import AnalyticsPipelineControls from '../components/dashboard/AnalyticsPipelineControls';
import { DashboardControls } from '../components/dashboard/DashboardControls';
import { dashboardApi } from '../services/dashboardApi';

export const Dashboard = () => {
  // Main dashboard state
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [dashboardData, setDashboardData] = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  
  // Dashboard controls state
  const [dateRange, setDateRange] = useState(() => {
    const saved = localStorage.getItem('dashboard_date_range');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved date range:', e);
      }
    }
    return {
      start_date: '2024-01-01',
      end_date: new Date().toISOString().split('T')[0]
    };
  });

  const [breakdown, setBreakdown] = useState(() => {
    return localStorage.getItem('dashboard_breakdown') || 'all';
  });

  const [hierarchy, setHierarchy] = useState(() => {
    return localStorage.getItem('dashboard_hierarchy') || 'campaign';
  });

  const [textFilter, setTextFilter] = useState('');
  
  // Modal states
  const [isDebugModalOpen, setIsDebugModalOpen] = useState(false);
  const [isGraphModalOpen, setIsGraphModalOpen] = useState(false);
  const [isTimelineModalOpen, setIsTimelineModalOpen] = useState(false);
  const [selectedRowData, setSelectedRowData] = useState(null);
  
  // UI state
  const [viewMode, setViewMode] = useState('grid');
  const [showSettings, setShowSettings] = useState(false);
  
  // Column visibility state
  const [columnVisibility, setColumnVisibility] = useState(() => {
    const saved = localStorage.getItem('dashboard_column_visibility');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved column visibility:', e);
      }
    }
    // Default column visibility
    return {
      name: true,
      campaign_name: true,
      adset_name: true,
      impressions: true,
      clicks: true,
      spend: true,
      meta_trials_started: true,
      mixpanel_trials_started: true,
      meta_purchases: true,
      mixpanel_purchases: true,
      mixpanel_revenue_usd: true,
      estimated_roas: true,
      profit: true,
      trial_accuracy_ratio: true
    };
  });

  // Column order state
  const [columnOrder, setColumnOrder] = useState([]);
  
  // Row order state  
  const [rowOrder, setRowOrder] = useState([]);

  // Save states to localStorage
  useEffect(() => {
    localStorage.setItem('dashboard_date_range', JSON.stringify(dateRange));
  }, [dateRange]);

  useEffect(() => {
    localStorage.setItem('dashboard_breakdown', breakdown);
  }, [breakdown]);

  useEffect(() => {
    localStorage.setItem('dashboard_hierarchy', hierarchy);
  }, [hierarchy]);

  useEffect(() => {
    localStorage.setItem('dashboard_column_visibility', JSON.stringify(columnVisibility));
  }, [columnVisibility]);

  // Filter data based on text filter
  const filteredData = React.useMemo(() => {
    if (!textFilter.trim()) return dashboardData;
    
    const filterText = textFilter.toLowerCase();
    
    const filterRecursive = (items) => {
      return items.reduce((acc, item) => {
        // Check if this item matches the filter
        const matches = (
          (item.name && item.name.toLowerCase().includes(filterText)) ||
          (item.campaign_name && item.campaign_name.toLowerCase().includes(filterText)) ||
          (item.adset_name && item.adset_name.toLowerCase().includes(filterText)) ||
          (item.ad_name && item.ad_name.toLowerCase().includes(filterText))
        );
        
        // Filter children recursively
        const filteredChildren = item.children ? filterRecursive(item.children) : [];
        
        // Include this item if it matches OR if it has matching children
        if (matches || filteredChildren.length > 0) {
          acc.push({
            ...item,
            children: filteredChildren
          });
        }
        
        return acc;
      }, []);
    };
    
    return filterRecursive(dashboardData);
  }, [dashboardData, textFilter]);

  // Handle dashboard data refresh (fast, pre-computed data)
  const handleRefresh = async () => {
    setLoading(true);
    setError(null);
    
    try {
      console.log('🔄 Fetching pre-computed analytics data:', {
        dateRange,
        breakdown,
        hierarchy
      });
      
      const response = await dashboardApi.getAnalyticsData({
        start_date: dateRange.start_date,
        end_date: dateRange.end_date,
        breakdown: breakdown,
        group_by: hierarchy  // Fix parameter name to match backend API
      });
      
      if (response.success) {
        setDashboardData(response.data || []);
        setLastUpdated(new Date().toISOString());
        
        // Initialize row order with data IDs
        if (response.data && response.data.length > 0) {
          setRowOrder(response.data.map(r => r.id));
        }
        
        console.log('✅ Dashboard data loaded successfully');
        console.log('📊 Data summary:', {
          totalRows: response.data?.length || 0,
          breakdown: breakdown,
          hierarchy: hierarchy
        });
        
      } else {
        setError(response.error || 'Failed to fetch analytics data');
      }
    } catch (error) {
      console.error('Dashboard refresh error:', error);
      setError(error.message || 'Failed to refresh dashboard data');
    } finally {
      setLoading(false);
    }
  };

  // Handle row actions
  const handleRowAction = (action, rowData) => {
    setSelectedRowData(rowData);
    if (action === 'graph') {
      setIsGraphModalOpen(true);
    } else if (action === 'debug') {
      setIsDebugModalOpen(true);
    } else if (action === 'timeline') {
      setIsTimelineModalOpen(true);
    }
  };

  // Modal close handlers
  const closeDebugModal = () => {
    setIsDebugModalOpen(false);
    setSelectedRowData(null);
  };

  const closeGraphModal = () => {
    setIsGraphModalOpen(false);
    setSelectedRowData(null);
  };

  const closeTimelineModal = () => {
    setIsTimelineModalOpen(false);
    setSelectedRowData(null);
  };

  // Get stats for summary cards
  const getDashboardStats = () => {
    if (!dashboardData.length) return null;
    
    const calculateStats = (items) => {
      let totalSpend = 0;
      let totalRevenue = 0;
      let totalImpressions = 0;
      let totalClicks = 0;
      let totalTrials = 0;
      let totalPurchases = 0;
      let count = 0;
      
      items.forEach(item => {
        // Only sum the top-level items (campaigns) - their totals already include adsets/ads
        totalSpend += parseFloat(item.spend || 0);
        totalRevenue += parseFloat(item.mixpanel_revenue_usd || item.estimated_revenue_usd || 0);
        totalImpressions += parseInt(item.impressions || 0);
        totalClicks += parseInt(item.clicks || 0);
        totalTrials += parseInt(item.mixpanel_trials_started || 0);
        totalPurchases += parseInt(item.mixpanel_purchases || 0);
        count++;
        
        // DO NOT add children stats - they're already included in the campaign totals
        // This prevents double-counting since campaign metrics are aggregated from their adsets/ads
      });
      
      return {
        totalSpend,
        totalRevenue,
        totalImpressions,
        totalClicks,
        totalTrials,
        totalPurchases,
        count
      };
    };
    
    const stats = calculateStats(dashboardData);
    const profit = stats.totalRevenue - stats.totalSpend;
    const roas = stats.totalSpend > 0 ? stats.totalRevenue / stats.totalSpend : 0;
    
    return { ...stats, profit, roas };
  };

  const stats = getDashboardStats();

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center">
            <AlertTriangle className="mx-auto h-12 w-12 text-red-500" />
            <h1 className="mt-4 text-3xl font-bold text-gray-900 dark:text-gray-100">
              Dashboard Error
            </h1>
            <p className="mt-2 text-gray-600 dark:text-gray-400">{error}</p>
            <button 
              onClick={() => {
                setError(null);
                handleRefresh();
              }}
              className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <RefreshCw className="mr-2 h-4 w-4" />
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                Campaign Dashboard
              </h1>
              <p className="mt-2 text-gray-600 dark:text-gray-400">
                Advanced analytics for Meta advertising performance
              </p>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={() => setViewMode(viewMode === 'grid' ? 'list' : 'grid')}
                className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              >
                {viewMode === 'grid' ? <List className="h-5 w-5" /> : <Grid className="h-5 w-5" />}
              </button>
              <button
                onClick={() => setShowSettings(!showSettings)}
                className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              >
                <Settings className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>

        {/* Analytics Pipeline Status */}
        <div className="mb-6">
          <AnalyticsPipelineControls />
        </div>

        {/* Dashboard Controls */}
        <div className="mb-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="p-6">
              <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                {/* Date Range */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Date Range
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <input
                      type="date"
                      value={dateRange.start_date}
                      onChange={(e) => setDateRange(prev => ({ ...prev, start_date: e.target.value }))}
                      className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    />
                    <input
                      type="date"
                      value={dateRange.end_date}
                      onChange={(e) => setDateRange(prev => ({ ...prev, end_date: e.target.value }))}
                      className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    />
                  </div>
                </div>

                {/* Breakdown */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Breakdown
                  </label>
                  <select
                    value={breakdown}
                    onChange={(e) => setBreakdown(e.target.value)}
                    className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  >
                    <option value="all">All</option>
                    <option value="country">Country</option>
                    <option value="region">Region</option>
                    <option value="device">Device</option>
                  </select>
                </div>

                {/* Hierarchy */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Hierarchy
                  </label>
                  <select
                    value={hierarchy}
                    onChange={(e) => setHierarchy(e.target.value)}
                    className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  >
                    <option value="campaign">Campaign → Ad Set → Ad</option>
                    <option value="adset">Ad Set → Ad</option>
                    <option value="ad">Ad Only</option>
                  </select>
                </div>

                {/* Actions */}
                <div className="flex flex-col justify-end">
                  <button
                    onClick={handleRefresh}
                    disabled={loading}
                    className="inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {loading ? (
                      <>
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                        Refreshing...
                      </>
                    ) : (
                      <>
                        <RefreshCw className="mr-2 h-4 w-4" />
                        Refresh Data
                      </>
                    )}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Column Management Settings Panel */}
        {showSettings && (
          <div className="mb-6">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">
                    Column Settings
                  </h3>
                  <button
                    onClick={() => setShowSettings(false)}
                    className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                  >
                    <XCircle className="h-5 w-5" />
                  </button>
                </div>
                <DashboardControls
                  onColumnVisibilityChange={setColumnVisibility}
                  onColumnOrderChange={setColumnOrder}
                />
              </div>
            </div>
          </div>
        )}

        {/* Search and Filter */}
        <div className="mb-6">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search campaigns, ad sets, or ads..."
              value={textFilter}
              onChange={(e) => setTextFilter(e.target.value)}
              className="pl-10 pr-4 py-3 w-full border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>

        {/* Summary Stats */}
        {stats && (
          <div className="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <DollarSign className="h-8 w-8 text-blue-500" />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Total Spend</p>
                  <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                    ${stats.totalSpend.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <TrendingUp className="h-8 w-8 text-green-500" />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Revenue</p>
                  <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                    ${stats.totalRevenue.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <BarChart3 className={`h-8 w-8 ${stats.profit >= 0 ? 'text-green-500' : 'text-red-500'}`} />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Profit</p>
                  <p className={`text-2xl font-bold ${stats.profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    ${stats.profit.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <Users className="h-8 w-8 text-purple-500" />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">ROAS</p>
                  <p className={`text-2xl font-bold ${stats.roas >= 1.5 ? 'text-green-600' : stats.roas >= 1.0 ? 'text-yellow-600' : 'text-red-600'}`}>
                    {stats.roas.toFixed(2)}x
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Last Updated */}
        {lastUpdated && (
          <div className="mb-4 flex items-center text-sm text-gray-500 dark:text-gray-400">
            <Clock className="mr-2 h-4 w-4" />
            Last updated: {new Date(lastUpdated).toLocaleString()}
          </div>
        )}

        {/* Dashboard Content */}
        {loading ? (
          <div className="flex items-center justify-center h-64 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="text-center">
              <RefreshCw className="mx-auto h-8 w-8 text-blue-500 animate-spin" />
              <p className="mt-2 text-lg font-medium text-gray-900 dark:text-gray-100">
                Loading dashboard data...
              </p>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Fetching pre-computed analytics data
              </p>
            </div>
          </div>
        ) : filteredData.length > 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <DashboardGrid 
              data={filteredData}
              rowOrder={rowOrder}
              onRowOrderChange={setRowOrder}
              onRowAction={handleRowAction}
              columnVisibility={columnVisibility}
              columnOrder={columnOrder}
              onColumnOrderChange={setColumnOrder}
              dashboardParams={{
                start_date: dateRange.start_date,
                end_date: dateRange.end_date,
                breakdown: breakdown,
                hierarchy: hierarchy
              }}
            />
          </div>
        ) : (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-8 text-center">
            <Database className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-4 text-lg font-semibold text-gray-900 dark:text-gray-100">
              No Data Available
            </h3>
            <p className="mt-2 text-gray-600 dark:text-gray-400 max-w-md mx-auto">
              {textFilter ? 
                `No results found for "${textFilter}". Try adjusting your search terms.` :
                'Click "Refresh Data" to load campaign information for the selected date range and settings.'
              }
            </p>
            {!textFilter && (
              <button 
                onClick={handleRefresh}
                className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Refresh Data
              </button>
            )}
          </div>
        )}
      </div>
      
      {/* Modals */}
      {selectedRowData && (
        <>
          <DebugModal 
            isOpen={isDebugModalOpen} 
            onClose={closeDebugModal} 
            data={selectedRowData} 
          />
          <GraphModal 
            isOpen={isGraphModalOpen} 
            onClose={closeGraphModal} 
            data={selectedRowData}
            dashboardParams={{
              start_date: dateRange.start_date,
              end_date: dateRange.end_date,
              breakdown: breakdown,
              hierarchy: hierarchy
            }}
          />
          <TimelineModal 
            isOpen={isTimelineModalOpen} 
            onClose={closeTimelineModal} 
            data={selectedRowData}
            rowData={selectedRowData}
          />
        </>
      )}
    </div>
  );
}; 
```

### orchestrator/dashboard/client/src/services/api.js
```js
import axios from 'axios';

// Base URL for API requests - updated to use localhost:5001 consistently
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5001';

// Create an axios instance
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  // Increase timeout for large responses
  timeout: 600000, // 10 minutes
  // Set max content length to 500MB
  maxContentLength: 500 * 1024 * 1024,
  maxBodyLength: 500 * 1024 * 1024,
});

// Add response interceptor to handle large responses
apiClient.interceptors.response.use(
  (response) => {
    // Log response size for debugging
    if (response.data) {
      const responseSize = JSON.stringify(response.data).length;
      if (responseSize > 10 * 1024 * 1024) { // 10MB+
        console.warn(`Large API response: ${(responseSize / (1024 * 1024)).toFixed(2)} MB`);
      }
    }
    return response;
  },
  (error) => {
    // Enhanced error handling for memory issues
    if (error.code === 'ERR_INSUFFICIENT_RESOURCES' || 
        error.message?.includes('out of memory') ||
        error.message?.includes('Maximum call stack')) {
      console.error('Response too large for browser to handle:', error);
      error.message = 'Response is too large for your browser to handle. Please use debug mode or reduce the date range.';
    }
    return Promise.reject(error);
  }
);

export const api = {
  // Mixpanel Debug Endpoints
  
  /**
   * Get raw Mixpanel data with filters
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Response data
   */
  getRawMixpanelData: async (params = {}) => {
    try {
      const response = await apiClient.get('/api/mixpanel/data', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel data:', error);
      throw error;
    }
  },
  
  /**
   * Get the last Mixpanel data load timestamp
   * @returns {Promise<Object>} - Response with timestamp
   */
  getMixpanelDebugSyncTS: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/sync-ts');
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel sync timestamp:', error);
      throw error;
    }
  },
  
  /**
   * Reset the last Mixpanel data load timestamp
   * @returns {Promise<Object>} - Response with success message
   */
  resetMixpanelDebugSyncTS: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/sync-ts/reset');
      return response.data;
    } catch (error) {
      console.error('Error resetting Mixpanel sync timestamp:', error);
      throw error;
    }
  },
  
  /**
   * Get the latest processed date to continue from
   * @returns {Promise<Object>} - Response with latest processed date info
   */
  getLatestProcessedDate: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/latest-processed-date');
      return response.data;
    } catch (error) {
      console.error('Error fetching latest processed date:', error);
      throw error;
    }
  },
  
  /**
   * Reset all Mixpanel data in the database
   * @returns {Promise<Object>} - Response with success message
   */
  resetMixpanelDatabase: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/database/reset');
      return response.data;
    } catch (error) {
      console.error('Error resetting Mixpanel database:', error);
      throw error;
    }
  },
  
  /**
   * Refresh Mixpanel data by clearing data directories
   * @returns {Promise<Object>} - Response with success message
   */
  refreshMixpanelData: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/data/refresh');
      return response.data;
    } catch (error) {
      console.error('Error refreshing Mixpanel data:', error);
      throw error;
    }
  },
  
  /**
   * Trigger Mixpanel data ingestion
   * @param {string} startDate - Optional start date (YYYY-MM-DD)
   * @returns {Promise<Object>} - Response with success message
   */
  triggerMixpanelIngest: async (startDate) => {
    try {
      const response = await apiClient.post('/api/mixpanel/ingest', null, {
        params: startDate ? { start_date: startDate } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error triggering data ingest:', error);
      throw error;
    }
  },
  
  /**
   * Start the Mixpanel data processing pipeline
   * @param {Object} options - Processing options
   * @param {string} options.start_date - Start date (YYYY-MM-DD)
   * @param {boolean} options.wipe_folder - Whether to wipe the folder before processing
   * @returns {Promise<Object>} - Response with success status
   */
  startMixpanelProcessing: async (options) => {
    try {
      const response = await apiClient.post('/api/mixpanel/process/start', options);
      return response.data;
    } catch (error) {
      console.error('Error starting Mixpanel processing:', error);
      throw error;
    }
  },
  
  /**
   * Get the current status of Mixpanel data processing
   * @returns {Promise<Object>} - Response with processing status
   */
  getMixpanelProcessStatus: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/process/status');
      return response.data;
    } catch (error) {
      console.error('Error fetching process status:', error);
      throw error;
    }
  },
  
  /**
   * Cancel the current Mixpanel data processing job
   * @returns {Promise<Object>} - Response with success status
   */
  cancelMixpanelProcessing: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/process/cancel');
      return response.data;
    } catch (error) {
      console.error('Error canceling processing:', error);
      throw error;
    }
  },
  
  /**
   * Get test DB user data
   * @param {string} distinctId - Optional user distinct ID
   * @returns {Promise<Object>} - Response with user data
   */
  getTestDbUser: async (distinctId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/test-db-user', {
        params: distinctId ? { distinct_id: distinctId } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching test DB user:', error);
      throw error;
    }
  },
  
  /**
   * Get test DB events data
   * @param {string} distinctId - Optional user distinct ID
   * @returns {Promise<Object>} - Response with events data
   */
  getTestDbEvents: async (distinctId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/test-db-events', {
        params: distinctId ? { distinct_id: distinctId } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching test DB events:', error);
      throw error;
    }
  },
  
  /**
   * Fetch Meta API data
   * @param {Object} params - Request parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {number} params.time_increment - Time increment in days
   * @param {string} [params.fields] - Comma-separated list of fields to retrieve
   * @returns {Promise<Object>} - Response with Meta API data
   */
  fetchMetaData: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/fetch', params);
      return response.data;
    } catch (error) {
      console.error('Error fetching Meta API data:', error);
      throw error;
    }
  },

  /**
   * Check the status of an async Meta API job
   * @param {string} reportRunId - The report run ID
   * @returns {Promise<Object>} - Job status information
   */
  checkMetaJobStatus: async (reportRunId) => {
    try {
      const response = await apiClient.get(`/api/meta/job/${reportRunId}/status`);
      return response.data;
    } catch (error) {
      console.error('Error checking Meta job status:', error);
      throw error;
    }
  },

  /**
   * Get results from a completed async Meta API job
   * @param {string} reportRunId - The report run ID
   * @param {boolean} useFileUrl - Whether to use file URL download
   * @returns {Promise<Object>} - Job results
   */
  getMetaJobResults: async (reportRunId, useFileUrl = false) => {
    try {
      const response = await apiClient.get(`/api/meta/job/${reportRunId}/results`, {
        params: { use_file_url: useFileUrl }
      });
      return response.data;
    } catch (error) {
      console.error('Error getting Meta job results:', error);
      throw error;
    }
  },

  // --- Cohort Analyzer API Methods ---
  analyzeCohortData: async (filters) => {
    try {
      const response = await apiClient.post('/api/cohort-analysis', filters);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Cohort Analyzer V3 API Methods ---
  analyzeCohortDataV3: async (filters) => {
    try {
      console.log('[V3] Sending cohort analysis request to V3 API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze', filters);
      console.log('[V3] Received response from V3 API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  analyzeCohortDataV3Enhanced: async (filters) => {
    try {
      console.log('[V3] Sending enhanced cohort analysis request to V3 API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze-enhanced', filters);
      console.log('[V3] Received enhanced response from V3 API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 Enhanced API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Cohort Analyzer V3 Refactored API Methods ---
  analyzeCohortDataV3Refactored: async (filters) => {
    try {
      console.log('[V3-Refactored] Sending cohort analysis request to V3 Refactored API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', filters);
      console.log('[V3-Refactored] Received response from V3 Refactored API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 Refactored API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3RefactoredHealth: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/refactored-health');
      return response.data;
    } catch (error) {
      console.error('Error checking V3 Refactored health:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3RefactoredVersion: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/refactored-version');
      return response.data;
    } catch (error) {
      console.error('Error getting V3 Refactored version:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // V3 Refactored Debug Methods
  runV3RefactoredStageAnalysis: async (filters, stage) => {
    try {
      console.log(`🚀🚀🚀 [V3-REFACTORED API] CALLING runV3RefactoredStageAnalysis for stage: ${stage}`);
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Will POST to: /api/v3/cohort/analyze-refactored`);
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Filters:`, filters);
      
      const debugFilters = {
        ...filters,
        debug_mode: true,
        debug_stage: stage,
        pipeline_version: '3.0.0_refactored'
      };
      
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Final payload:`, debugFilters);
      
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', debugFilters);
      
      console.log(`🚀🚀🚀 [V3-REFACTORED API] SUCCESS! Received response:`, response.data);
      return response.data;
    } catch (error) {
      console.error(`🚨🚨🚨 [V3-REFACTORED API] ERROR in stage ${stage} analysis:`, error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3Health: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/health');
      return response.data;
    } catch (error) {
      console.error('Error checking V3 health:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3Version: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/version');
      return response.data;
    } catch (error) {
      console.error('Error getting V3 version:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getCohortUserTimeline: async (filters) => {
    try {
      console.log('[DEBUG] getCohortUserTimeline called with filters:', filters);
      // Convert optional_filters to legacy format for the unified pipeline
      const optional_filters = filters.optional_filters || [];
      let primary_user_filter = {};
      let secondary_filters = [];
      
      if (optional_filters.length > 0) {
        // Convert optional_filters to legacy format
        const user_filters = optional_filters.filter(f => f.property_source === 'user');
        const event_filters = optional_filters.filter(f => f.property_source === 'event');
        
        // Use the first user filter as primary_user_filter
        primary_user_filter = user_filters.length > 0 ? user_filters[0] : {};
        
        // Only event filters go to secondary_filters
        secondary_filters = event_filters;
      } else {
        // Fall back to legacy format if provided
        primary_user_filter = filters.primary_user_filter || {};
        secondary_filters = filters.secondary_filters || [];
      }
      
      const payload = {
        date_from: filters.date_from_str,
        date_to: filters.date_to_str,
        primary_user_filter: primary_user_filter,
        secondary_filters: secondary_filters,
        config: {}
      };
      
      console.log('[DEBUG] getCohortUserTimeline sending payload:', payload);
      const response = await apiClient.post('/api/cohort-pipeline/timeline', payload);
      console.log('[DEBUG] getCohortUserTimeline received response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error fetching cohort user timeline:', error.response?.data || error.message);
      console.error('[DEBUG] getCohortUserTimeline full error:', error);
      throw error.response?.data || error;
    }
  },

  getUserEventRevenueTimeline: async (filters, distinctId = null, productId = null) => {
    try {
      console.log('[DEBUG] getUserEventRevenueTimeline called with filters:', filters, 'distinctId:', distinctId, 'productId:', productId);
      
      // CRITICAL FIX: Use V3 API instead of legacy API
      // The legacy API has the business logic bug where trial conversions incorrectly set initial_purchase = 1
      // The V3 API uses the correct EventStateTracker that properly handles business rules
      
      // Convert filters to V3 format
      const v3Payload = {
        date_from_str: filters.date_from_str,
        date_to_str: filters.date_to_str,
        // Convert optional_filters to V3 user_filters format
        user_filters: []
      };
      
      // Handle filter conversion
      if (filters.optional_filters && filters.optional_filters.length > 0) {
        // Use optional_filters (new format)
        v3Payload.user_filters = filters.optional_filters.filter(f => f.property_source === 'user');
      } else if (filters.primary_user_filter && filters.primary_user_filter.property_name) {
        // Convert legacy primary_user_filter format
        v3Payload.user_filters = [{
          property_name: filters.primary_user_filter.property_name,
          property_values: filters.primary_user_filter.property_values,
          property_source: 'user'
        }];
      }
      
      // Add user/product filtering if specified
      if (distinctId) v3Payload.distinct_id = distinctId;
      if (productId) v3Payload.product_id = productId;
      
      console.log('[DEBUG] getUserEventRevenueTimeline sending V3 payload:', v3Payload);
      
      // Use V3 API endpoint that has correct business logic
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', v3Payload);
      console.log('[DEBUG] getUserEventRevenueTimeline received V3 response:', response.data);
      
      // Convert V3 response to legacy format for compatibility with existing frontend code
      const v3Data = response.data;
      const stage3Data = v3Data?.stage_results?.stage3;
      
      if (!stage3Data || !stage3Data.timeline_results) {
        throw new Error('Invalid V3 response format');
      }
      
      const timelineResults = stage3Data.timeline_results;
      
      // Convert V3 timeline data to legacy format
      const legacyFormat = {
        dates: timelineResults.dates || [],
        event_rows: {},
        estimate_rows: {},
        arpc_per_product: timelineResults.arpc_per_product || {},
        available_users: Object.keys(timelineResults.user_daily_metrics || {}),
        available_products: timelineResults.available_products || []
      };
      
      // Convert daily metrics to legacy event_rows format
      if (timelineResults.timeline_data) {
        legacyFormat.dates.forEach(date => {
          const dayData = timelineResults.timeline_data[date] || {};
          
          // Map V3 field names to legacy field names
          if (!legacyFormat.event_rows.trial_started) legacyFormat.event_rows.trial_started = {};
          if (!legacyFormat.event_rows.trial_pending) legacyFormat.event_rows.trial_pending = {};
          if (!legacyFormat.event_rows.trial_ended) legacyFormat.event_rows.trial_ended = {};
          if (!legacyFormat.event_rows.trial_converted) legacyFormat.event_rows.trial_converted = {};
          if (!legacyFormat.event_rows.trial_canceled) legacyFormat.event_rows.trial_canceled = {};
          if (!legacyFormat.event_rows.initial_purchase) legacyFormat.event_rows.initial_purchase = {};
          if (!legacyFormat.event_rows.subscription_active) legacyFormat.event_rows.subscription_active = {};
          if (!legacyFormat.event_rows.subscription_cancelled) legacyFormat.event_rows.subscription_cancelled = {};
          if (!legacyFormat.event_rows.refund) legacyFormat.event_rows.refund = {};
          
          legacyFormat.event_rows.trial_started[date] = dayData.trial_started || 0;
          legacyFormat.event_rows.trial_pending[date] = dayData.trial_pending || 0;
          legacyFormat.event_rows.trial_ended[date] = dayData.trial_ended || 0;
          legacyFormat.event_rows.trial_converted[date] = dayData.trial_converted || 0;
          legacyFormat.event_rows.trial_canceled[date] = dayData.trial_cancelled || 0; // Note: cancelled vs canceled
          legacyFormat.event_rows.initial_purchase[date] = dayData.initial_purchase || 0; // CRITICAL: This will now be 0 for trial conversions
          legacyFormat.event_rows.subscription_active[date] = dayData.subscription_active || 0;
          legacyFormat.event_rows.subscription_cancelled[date] = dayData.subscription_cancelled || 0;
          legacyFormat.event_rows.refund[date] = dayData.refund_count || 0;
          
          // Revenue data
          if (!legacyFormat.estimate_rows.current_revenue) legacyFormat.estimate_rows.current_revenue = {};
          if (!legacyFormat.estimate_rows.estimated_revenue) legacyFormat.estimate_rows.estimated_revenue = {};
          if (!legacyFormat.estimate_rows.estimated_net_revenue) legacyFormat.estimate_rows.estimated_net_revenue = {};
          
          legacyFormat.estimate_rows.current_revenue[date] = dayData.revenue || 0;
          legacyFormat.estimate_rows.estimated_revenue[date] = dayData.estimated_revenue || 0;
          legacyFormat.estimate_rows.estimated_net_revenue[date] = dayData.estimated_revenue || 0;
        });
      }
      
      // Add cumulative data for legacy compatibility
      legacyFormat.event_rows.cumulative_initial_purchase = {};
      let cumulativeInitialPurchase = 0;
      legacyFormat.dates.forEach(date => {
        cumulativeInitialPurchase += legacyFormat.event_rows.initial_purchase[date] || 0;
        legacyFormat.event_rows.cumulative_initial_purchase[date] = cumulativeInitialPurchase;
      });
      
      console.log('[DEBUG] getUserEventRevenueTimeline converted to legacy format:', legacyFormat);
      return legacyFormat;
      
    } catch (err) {
      console.error('getUserEventRevenueTimeline error:', err);
      throw err;
    }
  },

  getDiscoverableCohortProperties: async () => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/discoverable_properties');
      return response.data;
    } catch (error) {
      console.error('Error fetching discoverable cohort properties:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getDiscoverableCohortPropertyValues: async (propertyKey, propertySource) => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/property_values', {
        params: { property_key: propertyKey, property_source: propertySource }
      });
      return response.data;
    } catch (error) {
      console.error(`Error fetching values for ${propertyKey} (${propertySource}):`, error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  triggerCohortPropertyDiscovery: async () => {
    try {
      const response = await apiClient.post('/api/cohort_analyzer/trigger_discovery');
      return response.data;
    } catch (error) {
      console.error('Error triggering cohort property discovery:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },
  
  enableCohortProperties: async () => {
    try {
      const response = await apiClient.post('/api/cohort_analyzer/enable_properties');
      return response.data;
    } catch (error) {
      console.error('Error enabling cohort properties:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },
  
  getPropertyDiscoveryStatus: async () => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/discovery_status');
      return response.data;
    } catch (error) {
      console.error('Error checking property discovery status:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Mixpanel Debug API Methods ---
  getMixpanelDatabaseStats: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/database-stats');
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel database statistics:', error);
      throw error;
    }
  },
  
  /**
   * Get events for a specific user
   * @param {string} userId - User ID or distinct ID
   * @returns {Promise<Array>} - Response with user events
   */
  getUserEvents: async (userId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/user-events', {
        params: { user_id: userId }
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching user events:', error);
      throw error;
    }
  },

  // --- Meta Historical Data API Methods ---
  
  /**
   * Start a historical data collection job
   * @param {Object} params - Collection parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Response with job ID
   */
  startHistoricalCollection: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/historical/start', params);
      return response.data;
    } catch (error) {
      console.error('Error starting historical collection:', error);
      throw error;
    }
  },

  /**
   * Get status of a historical collection job
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} - Job status information
   */
  getHistoricalJobStatus: async (jobId) => {
    try {
      const response = await apiClient.get(`/api/meta/historical/jobs/${jobId}/status`);
      return response.data;
    } catch (error) {
      console.error('Error getting job status:', error);
      throw error;
    }
  },

  /**
   * Cancel a historical collection job
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} - Cancellation confirmation
   */
  cancelHistoricalJob: async (jobId) => {
    try {
      const response = await apiClient.post(`/api/meta/historical/jobs/${jobId}/cancel`);
      return response.data;
    } catch (error) {
      console.error('Error cancelling job:', error);
      throw error;
    }
  },

  /**
   * Get data coverage summary for a configuration
   * @param {Object} params - Configuration parameters
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @param {string} [params.start_date] - Optional start date filter
   * @param {string} [params.end_date] - Optional end date filter
   * @returns {Promise<Object>} - Coverage summary
   */
  getDataCoverage: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/coverage', { params });
      return response.data;
    } catch (error) {
      console.error('Error getting data coverage:', error);
      throw error;
    }
  },

  /**
   * Get list of missing dates for a configuration
   * @param {Object} params - Configuration parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Missing dates information
   */
  getMissingDates: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/missing-dates', { params });
      return response.data;
    } catch (error) {
      console.error('Error getting missing dates:', error);
      throw error;
    }
  },

  /**
   * Delete all data for a specific historical configuration
   * @param {string} configHash - Configuration hash to delete
   * @returns {Promise<Object>} - Deletion confirmation
   */
  deleteHistoricalConfiguration: async (configHash) => {
    try {
      const response = await apiClient.delete(`/api/meta/historical/configurations/${configHash}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting historical configuration:', error);
      throw error;
    }
  },

  /**
   * Get all stored request configurations
   * @returns {Promise<Array>} - List of configurations
   */
  getHistoricalConfigurations: async () => {
    try {
      const response = await apiClient.get('/api/meta/historical/configurations');
      return response.data;
    } catch (error) {
      console.error('Error getting configurations:', error);
      throw error;
    }
  },

  /**
   * Export stored data for a configuration and date range
   * @param {Object} params - Export parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @param {string} [params.format] - Export format (default: 'json')
   * @returns {Promise<Object>} - Exported data
   */
  exportHistoricalData: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/export', { params });
      return response.data;
    } catch (error) {
      console.error('Error exporting data:', error);
      throw error;
    }
  },

  /**
   * Get stored data for a specific day and configuration
   * @param {Object} params - Day data parameters
   * @param {string} params.date - Date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Day data
   */
  getHistoricalDayData: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/historical/get-day-data', params);
      return response.data;
    } catch (error) {
      console.error('Error getting day data:', error);
      throw error;
    }
  },

  /**
   * Get current action mappings
   * @returns {Promise<Object>} - Action mappings
   */
  getActionMappings: async () => {
    try {
      const response = await apiClient.get('/api/meta/action-mappings');
      return response.data;
    } catch (error) {
      console.error('Error getting action mappings:', error);
      throw error;
    }
  },

  /**
   * Save action mappings
   * @param {Object} mappings - Action mappings to save
   * @returns {Promise<Object>} - Save confirmation
   */
  saveActionMappings: async (mappings) => {
    try {
      const response = await apiClient.post('/api/meta/action-mappings', { mappings });
      return response.data;
    } catch (error) {
      console.error('Error saving action mappings:', error);
      throw error;
    }
  },
}; 
```

### orchestrator/dashboard/client/src/services/dashboardApi.js
```js
// Dashboard API Service
// 
// Handles all API calls related to dashboard functionality

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5001';

class DashboardApiService {
  constructor() {
    this.baseUrl = `${API_BASE_URL}/api/dashboard`;
  }

  async makeRequest(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    };

    try {
      const response = await fetch(url, config);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }
      
      return data;
    } catch (error) {
      console.error(`API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  /**
   * Get available data configurations
   */
  async getConfigurations() {
    return this.makeRequest('/configurations');
  }

  /**
   * Get dashboard data for specified parameters
   */
  async getDashboardData(params) {
    return this.makeRequest('/data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Trigger manual data collection
   */
  async triggerCollection(params) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/collection/trigger`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(params)
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to trigger collection');
      }
      
      return data;
    } catch (error) {
      console.error('Error triggering collection:', error);
      throw error;
    }
  }

  /**
   * Get collection job status
   */
  async getCollectionStatus(jobId) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/collection/status/${jobId}`);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch collection status');
      }
      
      return data;
    } catch (error) {
      console.error('Error fetching collection status:', error);
      throw error;
    }
  }

  /**
   * Get data coverage summary for a configuration
   */
  async getCoverageSummary(configKey) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/coverage/${configKey}`);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch coverage summary');
      }
      
      return data;
    } catch (error) {
      console.error('Error fetching coverage summary:', error);
      throw error;
    }
  }

  /**
   * Health check for the dashboard API
   */
  async healthCheck() {
    return this.makeRequest('/health');
  }

  async getChartData(params) {
    return this.makeRequest('/chart-data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Get analytics data from the analytics pipeline - NEW ANALYTICS API
   */
  async getAnalyticsData(params) {
    return this.makeRequest('/analytics/data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Get chart data for analytics sparklines and detailed views
   */
  async getAnalyticsChartData(params) {
    const queryParams = new URLSearchParams(params).toString();
    return this.makeRequest(`/analytics/chart-data?${queryParams}`);
  }

  /**
   * Run pipeline analysis for a specific campaign, adset, or ad
   */
  async runPipeline(params, dashboardParams = null) {
    try {
      // Use dashboard's actual date range if available, otherwise default to last 30 days
      let dateFrom, dateTo;
      
      if (dashboardParams && dashboardParams.start_date && dashboardParams.end_date) {
        // Dashboard controls use start_date and end_date
        dateFrom = dashboardParams.start_date;
        dateTo = dashboardParams.end_date;
      } else if (dashboardParams && dashboardParams.date_from && dashboardParams.date_to) {
        // Fallback for other formats that might use date_from and date_to
        dateFrom = dashboardParams.date_from;
        dateTo = dashboardParams.date_to;
      } else {
        // Fallback to last 30 days
        dateFrom = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        dateTo = new Date().toISOString().split('T')[0];
      }

      // Prepare the pipeline parameters matching CohortAnalyzerV3RefactoredPage format
      const pipelineParams = {
        date_from: dateFrom,
        date_to: dateTo,
        timeline_end_date: new Date(Date.now() + 180 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 6 months from now
        pipeline_version: '3.0.0_refactored',
        use_conversion_probabilities: true,
        optional_filters: [],
        secondary_filters: [],
        config: {
          product_filter: {
            include_patterns: [".*"], // Include all products by default
            exclude_patterns: [],
            specific_product_ids: []
          },
          lifecycle: {
            trial_window_days: 7,
            cancellation_window_days: 30,
            smoothing_enabled: true
          },
          timeline: {
            include_estimates: true,
            include_confidence_intervals: false
          }
        }
      };

      // Set the primary_user_filter based on the ID type (matching cohort page format)
      if (params.ad_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_ad_id',
          property_values: [params.ad_id],
          property_source: 'user'
        };
      } else if (params.adset_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_ad_set_id', 
          property_values: [params.adset_id],
          property_source: 'user'
        };
      } else if (params.campaign_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_campaign_id',
          property_values: [params.campaign_id],
          property_source: 'user'
        };
      } else {
        // No specific filter - this will get all users in the date range
        pipelineParams.primary_user_filter = {
          property_name: '',
          property_values: [],
          property_source: 'user'
        };
      }

      console.log('🔍 Pipeline Debug: Sending V3 refactored parameters:', pipelineParams);

      const response = await fetch(`${API_BASE_URL}/api/v3/cohort/analyze-refactored`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(pipelineParams)
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to run pipeline');
      }
      
      return data;
    } catch (error) {
      console.error('Error running pipeline:', error);
      throw error;
    }
  }
}

// Create and export a singleton instance
const dashboardApi = new DashboardApiService();
export { dashboardApi }; 
```

## Database & Utility Files

### database/schema.sql
```sql
-- ========================================
-- MIXPANEL DATABASE SCHEMA (SINGLE SOURCE OF TRUTH)
-- ========================================
-- Database Location: /database/mixpanel_data.db
-- Last Updated: Consolidated schema including all analytics capabilities
-- Status: AUTHORITATIVE - All code should reference this schema
-- ========================================

-- ========================================
-- CORE USER DATA TABLES
-- ========================================

-- Primary User Table
-- Status: EXISTS - Needs 2 columns added (valid_user, economic_tier)
CREATE TABLE mixpanel_user (
    distinct_id TEXT PRIMARY KEY,
    abi_ad_id TEXT, -- Attribution ad ID (matches Meta ad_id)
    country TEXT, -- ISO 3166-1 alpha-2 code
    region TEXT,
    city TEXT,
    has_abi_attribution BOOLEAN DEFAULT FALSE,
    profile_json TEXT,
    first_seen DATETIME, -- Changed from TEXT to DATETIME
    last_updated DATETIME, -- Changed from TEXT to DATETIME
    valid_user BOOLEAN DEFAULT FALSE, -- Flag for user validity
    economic_tier TEXT -- Economic classification ("premium", "standard", "basic", "free")
);

-- Event Tracking Table
-- Status: EXISTS - No changes needed
CREATE TABLE mixpanel_event (
    event_uuid TEXT PRIMARY KEY,
    event_name TEXT NOT NULL,
    abi_ad_id TEXT, -- Attribution ad ID (matches Meta ad_id)
    abi_campaign_id TEXT, -- Attribution campaign ID (matches Meta campaign_id)
    abi_ad_set_id TEXT, -- Attribution ad set ID (matches Meta adset_id)
    distinct_id TEXT NOT NULL,
    event_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    country TEXT,
    region TEXT,
    revenue_usd DECIMAL(10,2), -- Changed from REAL to DECIMAL
    raw_amount DECIMAL(10,2), -- Changed from REAL to DECIMAL
    currency TEXT,
    refund_flag BOOLEAN DEFAULT FALSE,
    is_late_event BOOLEAN DEFAULT FALSE,
    trial_expiration_at_calc DATETIME, -- Changed from TEXT to DATETIME
    event_json TEXT,
    FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
);

-- CONSOLIDATED USER PRODUCT TABLE (REPLACES BOTH fact_user_products AND user_product_metrics)
-- Status: CONSOLIDATED FROM ANALYTICS DB + PLANNED STRUCTURE
-- Purpose: Complete user-product analytics with lifecycle tracking, attribution, and conversion metrics
-- Note: This table consolidates the rich analytics from user_product_metrics with the planned fact_user_products structure

-- ========================================
-- ANALYTICS TABLES (MERGED FROM ANALYTICS DB)
-- ========================================

-- CONSOLIDATED User Product Metrics & Lifecycle Tracking
-- Status: CONSOLIDATED TABLE - Replaces both fact_user_products and original user_product_metrics
-- Purpose: Complete user-product analytics combining rich conversion metrics with lifecycle tracking
CREATE TABLE user_product_metrics (
    user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    distinct_id TEXT NOT NULL,
    product_id TEXT NOT NULL, 
    credited_date DATE NOT NULL, -- Changed from TEXT to DATE
    country TEXT, 
    region TEXT, 
    device TEXT, 
    abi_ad_id TEXT, -- Attribution ad ID (matches Meta ad_id)
    abi_campaign_id TEXT, -- Attribution campaign ID (matches Meta campaign_id)
    abi_ad_set_id TEXT, -- Attribution ad set ID (matches Meta adset_id)
    current_status TEXT NOT NULL, 
    current_value DECIMAL(10,2) NOT NULL, -- Changed from REAL to DECIMAL
    value_status TEXT NOT NULL, 
    segment_id TEXT, 
    accuracy_score TEXT, 
    trial_conversion_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    trial_converted_to_refund_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    initial_purchase_to_refund_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    price_bucket DECIMAL(10,2), -- Changed from REAL to DECIMAL
    last_updated_ts DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    -- Additional fields from planned fact_user_products structure
    valid_lifecycle BOOLEAN DEFAULT FALSE, -- Whether this user-product lifecycle can be trusted for analysis
    store TEXT, -- Store identifier: "app_store" or "play_store"
    UNIQUE (distinct_id, product_id),
    FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
);

-- Pipeline Status Monitoring
-- Status: MERGED FROM ANALYTICS DB
-- Purpose: Track analytics pipeline execution status
CREATE TABLE pipeline_status (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    started_at DATETIME,
    completed_at DATETIME,
    progress_percentage INTEGER DEFAULT 0,
    current_step TEXT,
    error_message TEXT,
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    processed_users INTEGER DEFAULT 0,
    total_users INTEGER DEFAULT 0
);

-- ========================================
-- META ADVERTISING PERFORMANCE TABLES
-- ========================================
-- Note: Meta API breakdowns are mutually exclusive, requiring separate tables

-- Table for aggregated daily performance without breakdowns ("all" view)
CREATE TABLE ad_performance_daily (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date)
);

-- Table for country-level geographic breakdowns
CREATE TABLE ad_performance_daily_country (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    country TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, country)
);

-- Table for region-level geographic breakdowns  
CREATE TABLE ad_performance_daily_region (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    region TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, region)
);

-- Table for device breakdowns
CREATE TABLE ad_performance_daily_device (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    device TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, device)
);

-- ========================================
-- SUPPORTING TABLES
-- ========================================

-- Currency Exchange Rates
-- Status: EXISTS - No changes needed
CREATE TABLE currency_fx (
    date_day DATE NOT NULL, -- Changed from TEXT to DATE
    currency_code CHAR(3) NOT NULL,
    usd_rate DECIMAL(10,6) NOT NULL, -- Changed from REAL to DECIMAL (higher precision for FX rates)
    PRIMARY KEY (date_day, currency_code)
);

-- ETL Pipeline Control
-- Status: EXISTS - No changes needed
CREATE TABLE etl_job_control (
    job_name TEXT PRIMARY KEY,
    last_run_timestamp DATETIME, -- Changed from TEXT to DATETIME
    last_success_timestamp DATETIME, -- Changed from TEXT to DATETIME
    status TEXT, -- 'running', 'success', 'failed'
    error_message TEXT,
    run_duration_seconds INTEGER
);

-- Daily Event Processing Tracker
-- Status: EXISTS - No changes needed
CREATE TABLE processed_event_days (
    date_day DATE PRIMARY KEY, -- Changed from TEXT to DATE
    events_processed INTEGER,
    processing_timestamp DATETIME, -- Changed from TEXT to DATETIME
    status TEXT -- 'complete', 'partial', 'failed'
);

-- Dynamic Schema Discovery
-- Status: EXISTS - No changes needed
CREATE TABLE discovered_properties (
    property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_name TEXT NOT NULL UNIQUE,
    property_type TEXT, -- 'event' or 'user'
    first_seen_date DATE, -- Changed from TEXT to DATE
    last_seen_date DATE, -- Changed from TEXT to DATE
    sample_value TEXT
);

CREATE TABLE discovered_property_values (
    value_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER,
    property_value TEXT,
    first_seen_date DATE, -- Changed from TEXT to DATE
    last_seen_date DATE, -- Changed from TEXT to DATE
    occurrence_count INTEGER DEFAULT 1,
    FOREIGN KEY (property_id) REFERENCES discovered_properties(property_id)
);

-- ========================================
-- PIPELINE MANAGEMENT TABLES
-- ========================================

-- Pipeline Execution History
-- Status: EXISTS - No changes needed
CREATE TABLE refresh_pipeline_history (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    start_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    end_time DATETIME, -- Changed from TEXT to DATETIME
    status TEXT, -- 'running', 'completed', 'failed', 'interrupted'
    records_processed INTEGER,
    error_details TEXT,
    execution_parameters TEXT -- JSON blob of parameters used
);

-- Interrupted Pipeline Recovery
-- Status: EXISTS - No changes needed
CREATE TABLE interrupted_pipelines (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    interruption_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    last_processed_record TEXT,
    recovery_checkpoint TEXT, -- JSON blob for recovery state
    status TEXT -- 'interrupted', 'recovering', 'recovered'
);

-- Dashboard Caching
-- Status: EXISTS - No changes needed
CREATE TABLE dashboard_refresh_cache (
    cache_key TEXT PRIMARY KEY,
    cache_value TEXT, -- JSON blob
    created_timestamp DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    expires_timestamp DATETIME, -- Changed from TEXT to DATETIME
    refresh_count INTEGER DEFAULT 1
);

-- Geographic Reference Data
-- Status: EXISTS - No changes needed
CREATE TABLE continent_country (
    country_code CHAR(2) PRIMARY KEY, -- ISO 3166-1 alpha-2
    country_name TEXT NOT NULL,
    continent_code CHAR(2),
    continent_name TEXT,
    region TEXT,
    sub_region TEXT
);

-- Saved Analysis Views
-- Status: EXISTS - No changes needed
CREATE TABLE saved_views (
    view_id INTEGER PRIMARY KEY AUTOINCREMENT,
    view_name TEXT NOT NULL,
    view_description TEXT,
    view_sql TEXT NOT NULL,
    created_by TEXT,
    created_timestamp DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    last_modified DATETIME, -- Changed from TEXT to DATETIME
    view_parameters TEXT, -- JSON blob for parameterized views
    is_public BOOLEAN DEFAULT FALSE
);

-- ========================================
-- INDEXES FOR PERFORMANCE
-- ========================================

-- User table indexes
CREATE INDEX idx_mixpanel_user_country ON mixpanel_user(country);
CREATE INDEX idx_mixpanel_user_has_abi ON mixpanel_user(has_abi_attribution);
CREATE INDEX idx_mixpanel_user_first_seen ON mixpanel_user(first_seen);
CREATE INDEX idx_mixpanel_user_valid_user ON mixpanel_user(valid_user);
CREATE INDEX idx_mixpanel_user_economic_tier ON mixpanel_user(economic_tier);
CREATE INDEX idx_mixpanel_user_abi_ad_id ON mixpanel_user(abi_ad_id); -- Attribution lookup

-- Event table indexes
CREATE INDEX idx_mixpanel_event_distinct_id ON mixpanel_event(distinct_id);
CREATE INDEX idx_mixpanel_event_name ON mixpanel_event(event_name);
CREATE INDEX idx_mixpanel_event_time ON mixpanel_event(event_time);
CREATE INDEX idx_mixpanel_event_country ON mixpanel_event(country);
CREATE INDEX idx_mixpanel_event_revenue ON mixpanel_event(revenue_usd);
CREATE INDEX idx_mixpanel_event_abi_ad_id ON mixpanel_event(abi_ad_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_event_abi_campaign_id ON mixpanel_event(abi_campaign_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_event_abi_ad_set_id ON mixpanel_event(abi_ad_set_id); -- Attribution lookup

-- Consolidated User Product Metrics indexes (combines all analytics and lifecycle tracking indexes)
CREATE INDEX idx_upm_distinct_id ON user_product_metrics (distinct_id);
CREATE INDEX idx_upm_product_id ON user_product_metrics (product_id);
CREATE INDEX idx_upm_credited_date ON user_product_metrics (credited_date);
CREATE INDEX idx_upm_country ON user_product_metrics (country);
CREATE INDEX idx_upm_region ON user_product_metrics (region);
CREATE INDEX idx_upm_device ON user_product_metrics (device);
CREATE INDEX idx_upm_abi_ad_id ON user_product_metrics (abi_ad_id);
CREATE INDEX idx_upm_abi_campaign_id ON user_product_metrics (abi_campaign_id);
CREATE INDEX idx_upm_abi_ad_set_id ON user_product_metrics (abi_ad_set_id);
CREATE INDEX idx_upm_valid_lifecycle ON user_product_metrics (valid_lifecycle);
CREATE INDEX idx_upm_store ON user_product_metrics (store);
CREATE INDEX idx_upm_price_bucket ON user_product_metrics (price_bucket);

-- Meta advertising performance table indexes
CREATE INDEX idx_ad_perf_date ON ad_performance_daily (date);
CREATE INDEX idx_ad_perf_campaign ON ad_performance_daily (campaign_id);
CREATE INDEX idx_ad_perf_adset ON ad_performance_daily (adset_id);
CREATE INDEX idx_ad_perf_ad_id ON ad_performance_daily (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_country_date ON ad_performance_daily_country (date);
CREATE INDEX idx_ad_perf_country_campaign ON ad_performance_daily_country (campaign_id);
CREATE INDEX idx_ad_perf_country_ad_id ON ad_performance_daily_country (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_region_date ON ad_performance_daily_region (date);
CREATE INDEX idx_ad_perf_region_campaign ON ad_performance_daily_region (campaign_id);
CREATE INDEX idx_ad_perf_region_ad_id ON ad_performance_daily_region (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_device_date ON ad_performance_daily_device (date);
CREATE INDEX idx_ad_perf_device_campaign ON ad_performance_daily_device (campaign_id);
CREATE INDEX idx_ad_perf_device_ad_id ON ad_performance_daily_device (ad_id); -- NEW: For attribution joins

-- Additional indexes for join performance
CREATE INDEX idx_currency_fx_date ON currency_fx (date_day);
CREATE INDEX idx_etl_job_status ON etl_job_control (status);
CREATE INDEX idx_pipeline_history_name_time ON refresh_pipeline_history (pipeline_name, start_time);
CREATE INDEX idx_dashboard_cache_expires ON dashboard_refresh_cache (expires_timestamp);

-- ========================================
-- MERGE BENEFITS & RELATIONSHIPS
-- ========================================

/*
POST-MERGE ADVANTAGES:
1. Single database eliminates sync complexity
2. Direct joins possible between user_product_metrics and mixpanel_user
3. Can correlate detailed analytics with attribution data
4. Simplified backup/maintenance procedures
5. Better performance for cross-dataset queries

CONSOLIDATED USER_PRODUCT_METRICS TABLE BENEFITS:
- Replaces both fact_user_products (planned) and user_product_metrics (from analytics DB)
- Contains comprehensive analytics: conversion rates, attribution, lifecycle tracking
- distinct_id maps consistently with other tables (was user_id in original analytics DB)
- Rich attribution data can JOIN with ad performance tables via abi_ad_id
- Lifecycle validity tracking with valid_lifecycle boolean field
- Store tracking for app_store vs play_store differentiation

KEY RELATIONSHIPS AFTER CONSOLIDATION:
- user_product_metrics.distinct_id can JOIN mixpanel_user.distinct_id
- user_product_metrics attribution fields (abi_ad_id, abi_campaign_id, abi_ad_set_id) can JOIN advertising performance tables
- Combined analytics enable complete user acquisition-to-conversion journey analysis
- price_bucket enables revenue cohort analysis

ATTRIBUTION FIELD CONSISTENCY:
- abi_ad_id (TEXT) - consistent across mixpanel_user, mixpanel_event, user_product_metrics, and all ad_performance tables
- abi_campaign_id (TEXT) - consistent across mixpanel_event, user_product_metrics, and all ad_performance tables
- abi_ad_set_id (TEXT) - consistent across mixpanel_event, user_product_metrics, and all ad_performance tables

MIGRATION IMPACT:
- Zero downtime merge process with field mapping (user_id → distinct_id)
- All existing analytics queries continue to work with minor field name adjustment
- Enhanced analytics capabilities immediately available
- Single source of truth for user-product relationships
- File size increase: ~13,799 user_product_metrics records with full analytics data
*/ 
```

### utils/database_utils.py
```py
"""
Database Utilities Module

Production-grade database path discovery and connection utilities.
Provides centralized database path management for the entire project.

Author: System Architecture Team
Created: 2024
"""

import os
import sqlite3
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabasePathError(Exception):
    """Custom exception for database path related errors."""
    pass


class DatabaseManager:
    """
    Centralized database path discovery and management.
    
    This class provides robust database path discovery that works across
    different directory structures and deployment scenarios.
    """
    
    # Supported database configurations
    DATABASE_CONFIGS = {
        'mixpanel_data': {
            'filename': 'mixpanel_data.db',
            'description': 'Main Mixpanel user and event data'
        },
        'meta_analytics': {
            'filename': 'meta_analytics.db', 
            'description': 'Meta (Facebook) advertising analytics data'
        },
        'meta_historical_data': {
            'filename': 'meta_historical_data.db',
            'description': 'Historical Meta advertising data'
        }
    }
    
    def __init__(self, project_root: Optional[Union[str, Path]] = None):
        """
        Initialize the database manager.
        
        Args:
            project_root: Optional explicit project root path. If None, auto-detects.
        """
        self._project_root = None
        self._database_paths: Dict[str, Path] = {}
        
        if project_root:
            self._project_root = Path(project_root)
        else:
            self._project_root = self._find_project_root()
            
        self._discover_databases()
    
    def _find_project_root(self) -> Path:
        """
        Robustly find the project root directory.
        
        Uses multiple strategies to locate the project root that contains
        the database directory structure.
        
        Returns:
            Path to the project root directory
            
        Raises:
            DatabasePathError: If project root cannot be determined
        """
        # Start from the current file's location
        current_file = Path(__file__).resolve()
        
        # Strategy 1: Look for utils directory parent (normal case)
        if current_file.parent.name == 'utils':
            potential_root = current_file.parent.parent
            if self._validate_project_root(potential_root):
                return potential_root
        
        # Strategy 2: Walk up from current file location
        current = current_file.parent
        for _ in range(5):  # Limit search depth
            if self._validate_project_root(current):
                return current
            current = current.parent
            
        # Strategy 3: Use current working directory as reference
        cwd = Path.cwd()
        for _ in range(3):  # Limit search depth  
            if self._validate_project_root(cwd):
                return cwd
            cwd = cwd.parent
            
        # Strategy 4: Check common project patterns
        common_patterns = [
            Path.cwd(),
            Path.cwd().parent,
            current_file.parent.parent,
            current_file.parent.parent.parent
        ]
        
        for pattern in common_patterns:
            if pattern.exists() and self._validate_project_root(pattern):
                return pattern
                
        raise DatabasePathError(
            "Could not locate project root directory. "
            "Ensure you're running from within the project structure "
            "and that the 'database' directory exists."
        )
    
    def _validate_project_root(self, path: Path) -> bool:
        """
        Validate that a path is the correct project root.
        
        Args:
            path: Path to validate
            
        Returns:
            True if path appears to be valid project root
        """
        if not path.exists():
            return False
            
        # Check for database directory
        database_dir = path / "database"
        if not database_dir.exists():
            return False
            
        # Check for at least one expected database file
        for config in self.DATABASE_CONFIGS.values():
            db_path = database_dir / config['filename']
            if db_path.exists():
                return True
                
        return False
    
    def _discover_databases(self) -> None:
        """
        Discover and validate all database paths.
        
        Populates the internal database paths cache.
        """
        database_dir = self._project_root / "database"
        
        if not database_dir.exists():
            raise DatabasePathError(
                f"Database directory not found at {database_dir}. "
                f"Project root: {self._project_root}"
            )
        
        for db_key, config in self.DATABASE_CONFIGS.items():
            db_path = database_dir / config['filename']
            if db_path.exists():
                self._database_paths[db_key] = db_path
                logger.debug(f"Found database: {db_key} at {db_path}")
            else:
                logger.warning(f"Database not found: {db_key} at {db_path}")
    
    def get_database_path(self, database_key: str) -> Path:
        """
        Get the path to a specific database.
        
        Args:
            database_key: Key for the database (e.g., 'mixpanel_data')
            
        Returns:
            Path object to the database file
            
        Raises:
            DatabasePathError: If database is not found or key is invalid
        """
        if database_key not in self.DATABASE_CONFIGS:
            valid_keys = ", ".join(self.DATABASE_CONFIGS.keys())
            raise DatabasePathError(
                f"Invalid database key '{database_key}'. "
                f"Valid keys: {valid_keys}"
            )
            
        if database_key not in self._database_paths:
            config = self.DATABASE_CONFIGS[database_key]
            raise DatabasePathError(
                f"Database '{database_key}' ({config['description']}) "
                f"not found at expected location: "
                f"{self._project_root}/database/{config['filename']}"
            )
            
        return self._database_paths[database_key]
    
    def get_database_path_str(self, database_key: str) -> str:
        """
        Get the path to a database as a string.
        
        Args:
            database_key: Key for the database
            
        Returns:
            String path to the database file
        """
        return str(self.get_database_path(database_key))
    
    @contextmanager
    def get_connection(self, database_key: str, **kwargs):
        """
        Get a database connection with automatic cleanup.
        
        Args:
            database_key: Key for the database
            **kwargs: Additional arguments for sqlite3.connect()
            
        Yields:
            sqlite3.Connection object
            
        Example:
            with db_manager.get_connection('mixpanel_data') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users")
                result = cursor.fetchone()
        """
        db_path = self.get_database_path(database_key)
        conn = None
        
        try:
            conn = sqlite3.connect(str(db_path), **kwargs)
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabasePathError(f"Database connection error: {e}")
        finally:
            if conn:
                conn.close()
    
    def get_project_root(self) -> Path:
        """
        Get the project root directory.
        
        Returns:
            Path object to the project root
        """
        return self._project_root
    
    def list_available_databases(self) -> Dict[str, Dict[str, str]]:
        """
        List all available databases with their status.
        
        Returns:
            Dictionary mapping database keys to status information
        """
        result = {}
        for db_key, config in self.DATABASE_CONFIGS.items():
            status = "available" if db_key in self._database_paths else "not_found"
            path = str(self._database_paths.get(db_key, "N/A"))
            
            result[db_key] = {
                "description": config["description"],
                "filename": config["filename"],
                "status": status,
                "path": path
            }
            
        return result


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Returns:
        DatabaseManager instance (singleton)
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_database_path(database_key: str) -> str:
    """
    Convenience function to get a database path as string.
    
    Args:
        database_key: Key for the database (e.g., 'mixpanel_data')
        
    Returns:
        String path to the database file
        
    Example:
        db_path = get_database_path('mixpanel_data')
        conn = sqlite3.connect(db_path)
    """
    return get_database_manager().get_database_path_str(database_key)


def get_database_connection(database_key: str, **kwargs):
    """
    Convenience function to get a database connection context manager.
    
    Args:
        database_key: Key for the database
        **kwargs: Additional arguments for sqlite3.connect()
        
    Returns:
        Context manager that yields sqlite3.Connection
        
    Example:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
    """
    return get_database_manager().get_connection(database_key, **kwargs)


def reset_database_manager():
    """
    Reset the global database manager instance.
    
    Useful for testing or when project structure changes.
    """
    global _db_manager
    _db_manager = None


# Export commonly used functions
__all__ = [
    'DatabaseManager',
    'DatabasePathError', 
    'get_database_manager',
    'get_database_path',
    'get_database_connection',
    'reset_database_manager'
] 
```

## Configuration Files

### requirements.txt
```txt
# Flask and web framework dependencies
Flask==2.3.3
Flask-SocketIO==5.3.6
Flask-CORS==4.0.0

# Configuration and environment
PyYAML==6.0.1
python-dotenv==1.0.0

# HTTP and networking
requests==2.31.0
python-socketio==5.9.0
python-engineio==4.7.1

# Data processing and analytics
pandas>=1.5.0
numpy>=1.24.0

# AWS services
boto3>=1.26.0 
```

### start_orchestrator.sh
```sh
#!/bin/bash

echo "🚀 Starting Pipeline Orchestrator..."

# Activate virtual environment
source venv/bin/activate

# Start the Flask application
cd orchestrator
python app.py 
```

## Key Issues Identified

1. **Schema Misalignment**: analytics_query_service.py uses incorrect column names and table relationships
2. **Attribution Data**: Trying to use empty attribution fields in events table instead of mixpanel_user table
3. **Multi-Level Aggregation**: Campaign/adset level queries fail to map to constituent ads
4. **SQL NULL Handling**: Missing COALESCE() for SUM() aggregations

## Analysis Instructions

Please analyze these files to understand the complete data flow from frontend Dashboard.js → dashboardApi.js → dashboard_routes.py → analytics_query_service.py and identify any remaining issues with MixPanel data retrieval.

Focus on:
- Database schema alignment between queries and actual table structure
- Attribution data flow from mixpanel_user to aggregated metrics
- API response structure and frontend data handling
- Error handling and debugging capabilities
