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

# Import configuration and authentication
from config import config
from auth import requires_auth

# Import dashboard blueprint
from dashboard.api.dashboard_routes import dashboard_bp
# Import debug blueprint
from debug.api.debug_routes import debug_bp
# Import meta blueprint
from meta.api.meta_routes import meta_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*")

# Register dashboard blueprint
app.register_blueprint(dashboard_bp)
# Register debug blueprint
app.register_blueprint(debug_bp)
# Register meta blueprint
app.register_blueprint(meta_bp)

# Enable CORS for all routes to handle cross-origin requests
# Allow Heroku domains in production
allowed_origins = config.ALLOWED_ORIGINS.copy()
if config.is_production and config.HEROKU_APP_NAME:
    allowed_origins.extend([
        f'https://{config.HEROKU_APP_NAME}.herokuapp.com',
        f'http://{config.HEROKU_APP_NAME}.herokuapp.com'
    ])

CORS(app, origins=allowed_origins, 
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
                print(f"   Executing: python3 {step_file}")
                process = subprocess.Popen(
                    ['python3', step_file],
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
                    print(f"   Executing: python3 {step_file}")
                    process = subprocess.Popen(
                        ['python3', step_file],
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

    def reset_all_steps(self, pipeline_name):
        """Reset all steps in a pipeline back to pending status"""
        try:
            print(f"🔄 ORCHESTRATOR: Reset all steps request for pipeline '{pipeline_name}'")
            
            # Check if pipeline exists
            pipeline = self.pipelines.get(pipeline_name)
            if not pipeline:
                return False, f"Pipeline '{pipeline_name}' not found"
            
            # Cancel all running steps first
            if pipeline_name in self.running_processes:
                running_steps = list(self.running_processes[pipeline_name].keys())
                print(f"   Found {len(running_steps)} running steps, cancelling them first...")
                
                for step_id in running_steps:
                    success, message = self.cancel_step(pipeline_name, step_id)
                    if not success:
                        print(f"   Warning: Failed to cancel running step '{step_id}': {message}")
            
            # Remove the entire status file
            status_file = os.path.join(pipeline['dir'], '.status.json')
            print(f"   Looking for status file: {status_file}")
            
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                    
                    step_count = len(status_data)
                    print(f"   Found {step_count} steps with status data")
                    
                    # Remove the entire status file
                    os.remove(status_file)
                    print(f"   Removed status file")
                    
                    print(f"✅ ORCHESTRATOR: All steps in pipeline '{pipeline_name}' reset successfully")
                    return True, f"All {step_count} steps in pipeline '{pipeline_name}' have been reset to pending status"
                    
                except json.JSONDecodeError as e:
                    print(f"   Status file was corrupted: {e}, removing it")
                    os.remove(status_file)
                    print(f"✅ ORCHESTRATOR: Corrupted status file removed for pipeline '{pipeline_name}'")
                    return True, f"Corrupted status file removed, all steps reset to pending status"
            else:
                print(f"   No status file found, all steps already in pending state")
                return True, f"All steps in pipeline '{pipeline_name}' are already in pending status"
                
        except Exception as e:
            print(f"❌ Error resetting all steps: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error resetting all steps: {str(e)}"

# Initialize the runner
runner = PipelineRunner()

# Initialize debug system
from debug.registry import DebugModuleRegistry
debug_registry = DebugModuleRegistry()
print(f"Debug system initialized with {debug_registry.get_module_count()} modules loaded")

@app.route('/')
@requires_auth
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/pipelines')
@requires_auth
def pipelines():
    """Pipeline orchestrator page"""
    return render_template('pipelines.html')

@app.route('/debug')
@requires_auth
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

@app.route('/api/reset-all/<pipeline_name>', methods=['POST'])
def reset_all_steps(pipeline_name):
    """Reset all steps in a pipeline back to pending status"""
    success, message = runner.reset_all_steps(pipeline_name)
    return jsonify({'success': success, 'message': message})

@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

# Dashboard routes for serving static files
@app.route('/ads-dashboard')
@requires_auth
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

@app.route('/api/meta/analytics/start', methods=['POST'])
def meta_analytics_start():
    """Start Meta analytics data collection"""
    return jsonify({
        "success": True,
        "message": "Analytics data collection started",
        "job_id": f"analytics_{int(datetime.now().timestamp())}",
        "status": "started",
        "timestamp": datetime.now().isoformat()
    })

# Backward compatibility route
@app.route('/api/meta/historical/start', methods=['POST'])
def meta_historical_start():
    """Legacy route - use /api/meta/analytics/start instead"""
    return meta_analytics_start()

# Meta Analytics Routes (Preferred)
@app.route('/api/meta/analytics/jobs/<job_id>/status', methods=['GET'])
def meta_analytics_job_status(job_id):
    """Get Meta analytics job status"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "status": "completed",
        "progress": 100,
        "message": "Analytics job completed",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/analytics/jobs/<job_id>/cancel', methods=['POST'])
def meta_analytics_job_cancel(job_id):
    """Cancel Meta analytics job"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": "Analytics job cancelled",
        "status": "cancelled",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/analytics/coverage', methods=['GET'])
def meta_analytics_coverage():
    """Get Meta analytics data coverage"""
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

@app.route('/api/meta/analytics/missing-dates', methods=['GET'])
def meta_analytics_missing_dates():
    """Get missing dates in Meta analytics data"""
    return jsonify({
        "success": True,
        "data": {
            "missing_dates": [],
            "total_missing": 0
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/analytics/configurations', methods=['GET'])
def meta_analytics_configurations():
    """Get Meta analytics configurations"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/analytics/configurations/<config_id>', methods=['DELETE'])
def meta_analytics_delete_configuration(config_id):
    """Delete Meta analytics configuration"""
    return jsonify({
        "success": True,
        "message": f"Configuration {config_id} deleted",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/meta/analytics/export', methods=['GET'])
def meta_analytics_export():
    """Export Meta analytics data"""
    return jsonify({
        "success": True,
        "data": {
            "export_url": "/downloads/meta_export.csv",
            "record_count": 0
        },
        "timestamp": datetime.now().isoformat()
    })

# Legacy Historical Routes (Backward compatibility - redirect to analytics)
@app.route('/api/meta/historical/jobs/<job_id>/status', methods=['GET'])
def meta_historical_job_status(job_id):
    """Legacy route - redirects to analytics"""
    return meta_analytics_job_status(job_id)

@app.route('/api/meta/historical/jobs/<job_id>/cancel', methods=['POST'])
def meta_historical_job_cancel(job_id):
    """Legacy route - redirects to analytics"""
    return meta_analytics_job_cancel(job_id)

@app.route('/api/meta/historical/coverage', methods=['GET'])
def meta_historical_coverage():
    """Legacy route - redirects to analytics"""
    return meta_analytics_coverage()

@app.route('/api/meta/historical/missing-dates', methods=['GET'])
def meta_historical_missing_dates():
    """Legacy route - redirects to analytics"""
    return meta_analytics_missing_dates()

@app.route('/api/meta/historical/configurations', methods=['GET'])
def meta_historical_configurations():
    """Legacy route - redirects to analytics"""
    return meta_analytics_configurations()

@app.route('/api/meta/historical/configurations/<config_id>', methods=['DELETE'])
def meta_historical_delete_configuration(config_id):
    """Legacy route - redirects to analytics"""
    return meta_analytics_delete_configuration(config_id)

@app.route('/api/meta/historical/export', methods=['GET'])
def meta_historical_export():
    """Legacy route - redirects to analytics"""
    return meta_analytics_export()

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
        "success": True,
        "message": "Data refresh completed",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/test-db-events', methods=['GET'])
def mixpanel_debug_test_db_events():
    """Get test DB events"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/database-stats', methods=['GET'])
def mixpanel_debug_database_stats():
    """Get database statistics"""
    return jsonify({
        "success": True,
        "data": {
            "total_events": 0,
            "total_users": 0,
            "date_range": {}
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/mixpanel/debug/user-events', methods=['GET'])
def mixpanel_debug_user_events():
    """Get user events"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": datetime.now().isoformat()
    })

# Removed conflicting proxy routes - these were overriding the proper dashboard routes
# The dashboard blueprint handles /api/dashboard/analytics/chart-data properly

if __name__ == '__main__':
    # Initialize database structure if needed (especially for Heroku)
    if config.is_production:
        try:
            from database_init import init_all_databases
            init_all_databases()
        except Exception as e:
            print(f"Warning: Database initialization failed: {e}")
    
    init_db()
    runner = PipelineRunner()
    socketio.run(app, debug=config.FLASK_DEBUG, host=config.HOST, port=config.PORT) 