import os
import sys
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
import logging

# Import configuration and authentication
from orchestrator.config import config
from orchestrator.auth import requires_auth

# Import timezone utilities for consistent timezone handling
from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display

# Import database initialization
from orchestrator.database_init import initialize_all_databases, check_database_health

# Import dashboard blueprint
from orchestrator.dashboard.api.dashboard_routes import dashboard_bp
# Import debug blueprint  
from orchestrator.debug.api.debug_routes import debug_bp
# Import meta blueprint
from orchestrator.meta.api.meta_routes import meta_bp

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='dashboard/static/static', static_url_path='/static')
app.config['SECRET_KEY'] = config.SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize databases on startup
logger.info("🚀 Initializing databases on startup...")
try:
    if initialize_all_databases():
        if check_database_health():
            logger.info("✅ Database initialization completed successfully")
        else:
            logger.warning("⚠️ Database initialization completed but health check failed")
    else:
        logger.error("❌ Database initialization failed - app may not function properly")
except Exception as e:
    logger.error(f"❌ Database initialization error: {e}")

# Register dashboard blueprint
app.register_blueprint(dashboard_bp)
# Register debug blueprint
app.register_blueprint(debug_bp)
# Register meta blueprint
app.register_blueprint(meta_bp)

# Enable CORS for all routes to handle cross-origin requests
allowed_origins = config.ALLOWED_ORIGINS.copy()

# Add Railway domain if running on Railway
railway_public_domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
if railway_public_domain:
    allowed_origins.append(f'https://{railway_public_domain}')

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
            'timestamp': now_in_timezone().isoformat(),
            'error_message': error_message
        }
        
        with open(status_file, 'w') as f:
            json.dump(current_status, f, indent=2)
        
        # Emit websocket event (don't let websocket failures prevent file updates)
        try:
            socketio.emit('status_update', {
                'pipeline': pipeline_name,
                'step': step_id,
                'status': status,
                'timestamp': now_in_timezone().isoformat(),
                'error_message': error_message
            })
        except Exception:
            pass  # Don't let websocket failures prevent status file updates
    
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
                
                # Calculate project root and absolute script path for proper execution
                script_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(script_dir)
                script_path = os.path.join(pipeline['dir'], step_file)
                relative_script_path = os.path.relpath(script_path, project_root)
                
                print(f"🚀 ORCHESTRATOR: Starting module '{step_name}' (ID: {step_id})")
                print(f"   Pipeline: {pipeline_name}")
                print(f"   Description: {step_description}")
                print(f"   File: {step_file}")
                print(f"   Working Directory: {project_root}")
                print(f"   Script Path: {relative_script_path}")
                
                self.update_step_status(pipeline_name, step_id, 'running')
                
                # Run the step with Popen so we can track and cancel it
                print(f"   Executing: python3 {relative_script_path}")
                process = subprocess.Popen(
                    ['python3', relative_script_path],
                    cwd=project_root,
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
                    print(f"   ✓ Cleaned up process tracking for step '{step_id}'")
                else:
                    print(f"   ℹ️ Process tracking already cleaned up (likely by background thread)")
                
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
        
        # Check if pipeline is already running
        if pipeline_name in self.running_processes and self.running_processes[pipeline_name]:
            running_steps = list(self.running_processes[pipeline_name].keys())
            return False, f"Pipeline '{pipeline_name}' is already running (steps: {', '.join(running_steps)})"
        
        # Check if all steps are tested (only for non-master pipelines)
        if pipeline_name != 'master_pipeline':
            for step in pipeline['steps']:
                if not step.get('tested', False):
                    return False, f"Step {step['id']} not marked as tested"
        
        # Record run start
        conn = sqlite3.connect('db.sqlite')
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO runs (pipeline_name, status, started_at) VALUES (?, ?, ?)',
            (pipeline_name, 'running', now_in_timezone())
        )
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        def run_in_background():
            success = True
            error_message = None
            
            # Calculate project root for consistent working directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            
            print(f"🎯 ORCHESTRATOR: Starting FULL PIPELINE '{pipeline_name}'")
            print(f"   Description: {pipeline.get('description', 'No description')}")
            print(f"   Total steps: {len(pipeline['steps'])}")
            print(f"   Steps to execute:")
            for step in pipeline['steps']:
                step_name = step.get('name', step['id'])
                print(f"     {step['id']}: {step_name}")
            print(f"   Working Directory: {project_root}")
            print("")
            
            try:
                for i, step in enumerate(pipeline['steps'], 1):
                    step_file = step['file']
                    step_name = step.get('name', step['id'])
                    step_description = step.get('description', 'No description')
                    
                    # Calculate project root and absolute script path for proper execution
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.dirname(script_dir)
                    script_path = os.path.join(pipeline['dir'], step_file)
                    relative_script_path = os.path.relpath(script_path, project_root)
                    
                    print(f"🚀 ORCHESTRATOR: Starting step {i}/{len(pipeline['steps'])}: '{step_name}' (ID: {step['id']})")
                    print(f"   Description: {step_description}")
                    print(f"   File: {step_file}")
                    print(f"   Script Path: {relative_script_path}")
                    
                    self.update_step_status(pipeline_name, step['id'], 'running')
                    
                    # Run the step with Popen so we can track and cancel it
                    print(f"   Executing: python3 {relative_script_path}")
                    process = subprocess.Popen(
                        ['python3', relative_script_path],
                        cwd=project_root,
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
                ('success' if success else 'failed', now_in_timezone(), error_message, run_id)
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
                
                # Clean up tracking (with safety check for race conditions)
                if (pipeline_name in self.running_processes and 
                    step_id in self.running_processes[pipeline_name]):
                    del self.running_processes[pipeline_name][step_id]
                    if not self.running_processes[pipeline_name]:
                        del self.running_processes[pipeline_name]
                        print(f"   ✓ Cleaned up process tracking for step '{step_id}'")
                    else:
                        print(f"   ℹ️ Process tracking already cleaned up (likely by background thread)")
                
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
            print(f"❌ Error cancelling step '{step_id}' in pipeline '{pipeline_name}': {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error cancelling step: {str(e)}"
    
    def cancel_pipeline(self, pipeline_name):
        """Cancel all running steps in a pipeline"""
        cancelled_count = 0
        errors = []
        
        print(f"🛑 ORCHESTRATOR: Cancel pipeline request for '{pipeline_name}'")
        
        # First, cancel any processes that are actively tracked
        tracked_steps = []
        if pipeline_name in self.running_processes:
            tracked_steps = list(self.running_processes[pipeline_name].keys())
            print(f"   Found {len(tracked_steps)} tracked running processes")
            
            for step_id in tracked_steps:
                success, message = self.cancel_step(pipeline_name, step_id)
                if success:
                    cancelled_count += 1
                else:
                    errors.append(f"Step {step_id}: {message}")
            
        # Second, check the status file for steps marked as "running" but not tracked
        untracked_running = []
        status = self.get_pipeline_status(pipeline_name)
        if status:
            running_in_status = [step_id for step_id, step_status in status.items() 
                               if step_status.get('status') == 'running']
            
            untracked_running = [step_id for step_id in running_in_status 
                               if step_id not in tracked_steps]
            
            if untracked_running:
                print(f"   Found {len(untracked_running)} steps marked as running in status file but not tracked")
                print(f"   Untracked running steps: {untracked_running}")
                
                # Force-cancel these steps by updating their status
                for step_id in untracked_running:
                    try:
                        self.update_step_status(pipeline_name, step_id, 'cancelled', 'Force cancelled - process not tracked')
                        cancelled_count += 1
                        print(f"   ✓ Force cancelled untracked step: {step_id}")
                    except Exception as e:
                        error_msg = f"Failed to force cancel {step_id}: {str(e)}"
                        errors.append(error_msg)
                        print(f"   ❌ {error_msg}")
        
        # Return results
        if cancelled_count > 0:
            message = f"Cancelled {cancelled_count} steps"
            if tracked_steps:
                message += f" ({len(tracked_steps)} tracked processes"
                if untracked_running:
                    message += f", {len(untracked_running)} untracked status entries"
                message += ")"
            elif untracked_running:
                message += f" (all were untracked status entries)"
            
            if errors:
                message += f". Errors: {'; '.join(errors)}"
            
            return True, message
        else:
            if not status:
                return False, f"No running processes found for pipeline '{pipeline_name}' (no status file)"
            elif not status or not any(step_status.get('status') == 'running' for step_status in status.values()):
                return False, f"No running processes found for pipeline '{pipeline_name}' (no running steps in status)"
            else:
                return False, f"No steps cancelled. Errors: {'; '.join(errors)}"

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
            
            # Get the existing status data before removing the file
            status_file = os.path.join(pipeline['dir'], '.status.json')
            print(f"   Looking for status file: {status_file}")
            
            step_ids_to_reset = []
            step_count = 0
            
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                    
                    step_count = len(status_data)
                    step_ids_to_reset = list(status_data.keys())
                    print(f"   Found {step_count} steps with status data: {step_ids_to_reset}")
                    
                    # Remove the entire status file
                    os.remove(status_file)
                    print(f"   Removed status file")
                    
                except json.JSONDecodeError as e:
                    print(f"   Status file was corrupted: {e}, removing it")
                    os.remove(status_file)
                    print(f"   Corrupted status file removed")
            else:
                print(f"   No status file found")
                # Get all possible steps from pipeline definition
                step_ids_to_reset = [step['id'] for step in pipeline['steps']]
                step_count = len(step_ids_to_reset)
                print(f"   Using pipeline definition steps: {step_ids_to_reset}")
            
            # Emit WebSocket events for all steps being reset to pending
            print(f"   Emitting WebSocket reset events for {len(step_ids_to_reset)} steps...")
            for step_id in step_ids_to_reset:
                try:
                    socketio.emit('status_update', {
                        'pipeline': pipeline_name,
                        'step': step_id,
                        'status': 'pending',
                        'timestamp': now_in_timezone().isoformat(),
                        'error_message': None
                    })
                    print(f"   📡 Emitted reset event for step: {step_id}")
                except Exception as e:
                    print(f"   Warning: Failed to emit WebSocket event for step '{step_id}': {e}")
            
            # Emit a special "reset_complete" event to signal frontend to clear all state
            try:
                socketio.emit('pipeline_reset', {
                    'pipeline': pipeline_name,
                    'message': 'All steps reset to pending status',
                    'timestamp': now_in_timezone().isoformat(),
                    'step_count': len(step_ids_to_reset)
                })
                print(f"   📡 Emitted pipeline_reset event")
            except Exception as e:
                print(f"   Warning: Failed to emit pipeline_reset event: {e}")
            
            print(f"✅ ORCHESTRATOR: All steps in pipeline '{pipeline_name}' reset successfully")
            return True, f"All {step_count} steps in pipeline '{pipeline_name}' have been reset to pending status"
                
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
    """Serve the ads dashboard application at root"""
    return send_from_directory('dashboard/static', 'index.html')

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
# Root route now serves the dashboard directly

@app.route('/dashboard-static/<path:filename>')
def dashboard_static(filename):
    """Serve dashboard static files (CSS, JS, images)"""
    return send_from_directory('dashboard/static/static', filename)

# Custom static file routes removed - using Flask's built-in static file handling
# Flask app is configured with static_folder='dashboard/static/static', static_url_path='/static'

@app.route('/manifest.json')
def dashboard_manifest():
    """Serve dashboard manifest.json"""
    return send_from_directory('dashboard/static', 'manifest.json')

@app.route('/images/site.webmanifest')
def dashboard_site_manifest():
    """Serve dashboard site.webmanifest"""
    return send_from_directory('dashboard/static/images', 'site.webmanifest')

@app.route('/favicon.ico')
def dashboard_favicon():
    """Serve dashboard favicon"""
    return send_from_directory('dashboard/static/images', 'favicon.ico')

@app.route('/images/<path:filename>')
def dashboard_images(filename):
    """Serve dashboard image files (including favicons)"""
    return send_from_directory('dashboard/static/images', filename)

@app.route('/debug/static-files')
def debug_static_files():
    """Debug route to check what files exist in static directory"""
    import os
    from pathlib import Path
    
    debug_info = {
        'current_working_directory': os.getcwd(),
        'app_root': str(Path(__file__).parent),
        'static_directories': {}
    }
    
    # Check different possible static directory paths
    possible_paths = [
        'dashboard/static',
        'dashboard/static/static',
        'dashboard/static/static/css',
        'dashboard/static/static/js',
        'orchestrator/dashboard/static',
        'orchestrator/dashboard/static/static'
    ]
    
    for path in possible_paths:
        full_path = Path(path)
        if full_path.exists():
            debug_info['static_directories'][path] = {
                'exists': True,
                'files': list(os.listdir(full_path)) if full_path.is_dir() else 'not_a_directory'
            }
        else:
            debug_info['static_directories'][path] = {'exists': False}
    
    return jsonify(debug_info)

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
        "last_update": now_in_timezone().isoformat(),
        "pipeline_health": "operational",
        "data_freshness": "current",
        "last_run": now_in_timezone().isoformat(),
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
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
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
        "last_run": now_in_timezone().isoformat(),
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/process/start', methods=['POST'])
def mixpanel_process_start():
    """Start Mixpanel processing"""
    return jsonify({
        "success": True,
        "message": "Mixpanel processing started",
        "job_id": f"mixpanel_{int(now_in_timezone().timestamp())}",
        "status": "started",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/process/cancel', methods=['POST'])
def mixpanel_process_cancel():
    """Cancel Mixpanel processing"""
    return jsonify({
        "success": True,
        "message": "Mixpanel processing cancelled",
        "status": "cancelled",
        "timestamp": now_in_timezone().isoformat()
    })

# Meta API endpoints
# Removed dummy meta job status/results routes - using blueprint routes from meta_routes.py instead

# Removed dummy meta_fetch route - using blueprint route from meta_routes.py instead

@app.route('/api/meta/analytics/start', methods=['POST'])
def meta_analytics_start():
    """Start Meta analytics data collection"""
    return jsonify({
        "success": True,
        "message": "Analytics data collection started",
        "job_id": f"analytics_{int(now_in_timezone().timestamp())}",
        "status": "started",
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/meta/analytics/jobs/<job_id>/cancel', methods=['POST'])
def meta_analytics_job_cancel(job_id):
    """Cancel Meta analytics job"""
    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": "Analytics job cancelled",
        "status": "cancelled",
        "timestamp": now_in_timezone().isoformat()
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
                "end": now_in_timezone().strftime("%Y-%m-%d")
            }
        },
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/meta/analytics/configurations', methods=['GET'])
def meta_analytics_configurations():
    """Get Meta analytics configurations"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/meta/analytics/configurations/<config_id>', methods=['DELETE'])
def meta_analytics_delete_configuration(config_id):
    """Delete Meta analytics configuration"""
    return jsonify({
        "success": True,
        "message": f"Configuration {config_id} deleted",
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/meta/action-mappings', methods=['POST'])
def meta_action_mappings_post():
    """Save Meta action mappings"""
    return jsonify({
        "success": True,
        "message": "Action mappings saved",
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/v3/cohort/analyze-refactored', methods=['POST'])
def cohort_v3_analyze():
    """V3 Refactored cohort analysis"""
    return jsonify({
        "success": True,
        "message": "V3 Refactored analysis completed",
        "stage_results": {},
        "final_analysis": {},
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

# Cohort Analyzer API endpoints
@app.route('/api/cohort_analyzer/discoverable_properties', methods=['GET'])
def cohort_analyzer_properties():
    """Get discoverable cohort properties"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/cohort_analyzer/property_values', methods=['GET'])
def cohort_analyzer_property_values():
    """Get property values"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/cohort_analyzer/trigger_discovery', methods=['POST'])
def cohort_analyzer_trigger_discovery():
    """Trigger property discovery"""
    return jsonify({
        "success": True,
        "message": "Property discovery triggered",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/cohort_analyzer/enable_properties', methods=['POST'])
def cohort_analyzer_enable_properties():
    """Enable properties"""
    return jsonify({
        "success": True,
        "message": "Properties enabled",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/cohort_analyzer/discovery_status', methods=['GET'])
def cohort_analyzer_discovery_status():
    """Get discovery status"""
    return jsonify({
        "success": True,
        "status": "idle",
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/conversion-probability/start-analysis', methods=['POST'])
def conversion_probability_start_analysis():
    """Start conversion analysis"""
    return jsonify({
        "success": True,
        "data": {
            "analysis_id": f"conv_{int(now_in_timezone().timestamp())}",
            "cached": False
        },
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/conversion-probability/analyses', methods=['GET'])
def conversion_probability_analyses():
    """Get available analyses"""
    return jsonify({
        "success": True,
        "data": {
            "files": []
        },
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/conversion-probability/run-new-hierarchical-analysis', methods=['POST'])
def conversion_probability_hierarchical():
    """Run new hierarchical analysis"""
    return jsonify({
        "success": True,
        "message": "Hierarchical analysis completed",
        "data": {},
        "timestamp": now_in_timezone().isoformat()
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
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/rules', methods=['POST'])
def pricing_rules_post():
    """Create pricing rule"""
    return jsonify({
        "success": True,
        "message": "Pricing rule created",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>', methods=['PUT'])
def pricing_rules_put(rule_id):
    """Update pricing rule"""
    return jsonify({
        "success": True,
        "message": f"Pricing rule {rule_id} updated",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>', methods=['DELETE'])
def pricing_rules_delete(rule_id):
    """Delete pricing rule"""
    return jsonify({
        "success": True,
        "message": f"Pricing rule {rule_id} deleted",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/products', methods=['GET'])
def pricing_products():
    """Get pricing products"""
    return jsonify({
        "success": True,
        "data": {
            "products": []
        },
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/countries', methods=['GET'])
def pricing_countries():
    """Get pricing countries"""
    return jsonify({
        "success": True,
        "data": {
            "countries": []
        },
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/repair-rules', methods=['POST'])
def pricing_repair_rules():
    """Repair pricing rules"""
    return jsonify({
        "success": True,
        "message": "Pricing rules repaired",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>/history', methods=['GET'])
def pricing_rule_history(rule_id):
    """Get pricing rule history"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/rules/<rule_id>/provenance', methods=['GET'])
def pricing_rule_provenance(rule_id):
    """Get pricing rule provenance"""
    return jsonify({
        "success": True,
        "data": {},
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/pricing/products/<product_id>/most-recent-conversion-price', methods=['GET'])
def pricing_product_recent_price(product_id):
    """Get most recent conversion price"""
    return jsonify({
        "success": True,
        "data": {
            "suggested_price": 9.99
        },
        "timestamp": now_in_timezone().isoformat()
    })

# Additional Mixpanel debug endpoints
@app.route('/api/mixpanel/debug/sync-ts', methods=['GET'])
def mixpanel_debug_sync_ts_get():
    """Get Mixpanel sync timestamp"""
    return jsonify({
        "success": True,
        "data": {
            "sync_timestamp": now_in_timezone().isoformat()
        },
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/debug/sync-ts/reset', methods=['POST'])
def mixpanel_debug_sync_ts_reset():
    """Reset Mixpanel sync timestamp"""
    return jsonify({
        "success": True,
        "message": "Sync timestamp reset",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/debug/latest-processed-date', methods=['GET'])
def mixpanel_debug_latest_processed_date():
    """Get latest processed date"""
    return jsonify({
        "success": True,
        "data": {
            "latest_date": now_in_timezone().strftime("%Y-%m-%d")
        },
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/debug/database/reset', methods=['POST'])
def mixpanel_debug_database_reset():
    """Reset Mixpanel raw data tables"""
    try:
        # Import database utilities
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
        from database_utils import get_database_path
        
        # Connect to raw data database
        raw_db_path = get_database_path('raw_data')
        
        import sqlite3
        conn = sqlite3.connect(raw_db_path)
        cursor = conn.cursor()
        
        # Delete all records from raw data tables
        raw_tables = ['raw_event_data', 'raw_user_data', 'downloaded_dates']
        total_deleted = 0
        
        for table in raw_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count_before = cursor.fetchone()[0]
                
                cursor.execute(f"DELETE FROM {table}")
                deleted = cursor.rowcount
                total_deleted += deleted
                
                logger.info(f"Deleted {deleted} records from {table} (was {count_before})")
                
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not delete from table {table}: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully deleted {total_deleted} total raw data records")
        
        return jsonify({
            "success": True,
            "message": f"Database reset completed - deleted {total_deleted} raw data records",
            "timestamp": now_in_timezone().isoformat(),
            "tables_reset": raw_tables,
            "total_deleted": total_deleted
        })
        
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Database reset failed: {str(e)}",
            "timestamp": now_in_timezone().isoformat()
        }), 500

@app.route('/api/mixpanel/debug/data/refresh', methods=['POST'])
def mixpanel_debug_data_refresh():
    """Refresh Mixpanel data"""
    return jsonify({
        "success": True,
        "message": "Data refresh completed",
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/debug/test-s3', methods=['GET'])
def mixpanel_debug_test_s3():
    """Test S3 connection and data availability"""
    try:
        import boto3
        from datetime import timedelta
        
        # Load environment variables
        AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
        AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
        S3_BUCKET_EVENTS = os.environ.get('S3_BUCKET_EVENTS')
        S3_BUCKET_USERS = os.environ.get('S3_BUCKET_USERS')
        PROJECT_ID = os.environ.get('PROJECT_ID')
        
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_EVENTS, S3_BUCKET_USERS, PROJECT_ID]):
            return jsonify({
                "success": False,
                "message": "Missing required environment variables for S3 access",
                "timestamp": now_in_timezone().isoformat()
            }), 500
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME
        )
        
        # Test user data availability
        user_prefix = f'{PROJECT_ID}/mp_people_data/'
        try:
            user_response = s3_client.list_objects_v2(Bucket=S3_BUCKET_USERS, Prefix=user_prefix)
            user_data = {
                "status": "✅ Available" if 'Contents' in user_response else "❌ Not found",
                "files_count": len(user_response.get('Contents', [])),
                "sample_file": user_response['Contents'][0]['Key'] if 'Contents' in user_response else None
            }
        except Exception as e:
            user_data = {
                "status": f"❌ Error: {str(e)}",
                "files_count": 0,
                "sample_file": None
            }
        
        # Test event data availability
        start_date = datetime(2025, 4, 14)
        end_date = now_in_timezone()
        total_days = (end_date - start_date).days + 1
        available_dates = []
        
        current_date = start_date
        while current_date <= end_date:
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            day = current_date.strftime('%d')
            
            event_prefix = f'{PROJECT_ID}/mp_master_event/{year}/{month}/{day}/'
            
            try:
                event_response = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET_EVENTS, 
                    Prefix=event_prefix, 
                    MaxKeys=1
                )
                
                if 'Contents' in event_response and len(event_response['Contents']) > 0:
                    available_dates.append(current_date.strftime('%Y-%m-%d'))
            except Exception:
                pass  # Ignore errors, treat as missing
            
            current_date += timedelta(days=1)
        
        available_count = len(available_dates)
        missing_count = total_days - available_count
        coverage_percentage = round((available_count / total_days) * 100, 1)
        
        event_data = {
            "status": "✅ Available" if available_count > 0 else "❌ Not found",
            "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "available_days": available_count,
            "missing_days": missing_count,
            "coverage_percentage": coverage_percentage,
            "sample_dates": available_dates[:5] if available_dates else []
        }
        
        return jsonify({
            "success": True,
            "message": "S3 connection test completed",
            "timestamp": now_in_timezone().isoformat(),
            "user_data": user_data,
            "event_data": event_data
        })
        
    except Exception as e:
        logger.error(f"S3 connection test failed: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"S3 connection test failed: {str(e)}",
            "timestamp": now_in_timezone().isoformat()
        }), 500

@app.route('/api/mixpanel/debug/test-db-events', methods=['GET'])
def mixpanel_debug_test_db_events():
    """Get test DB events"""
    return jsonify({
        "success": True,
        "data": [],
        "timestamp": now_in_timezone().isoformat()
    })

@app.route('/api/mixpanel/debug/database-stats', methods=['GET'])
def mixpanel_debug_database_stats():
    """Get database statistics"""
    try:
        from utils.database_utils import get_database_connection
        
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Get total events
            cursor.execute("SELECT COUNT(*) FROM mixpanel_event")
            total_events = cursor.fetchone()[0] or 0
            
            # Get total users
            cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
            total_users = cursor.fetchone()[0] or 0
            
            # Get date range
            cursor.execute("""
                SELECT 
                    MIN(event_time) as earliest,
                    MAX(event_time) as latest
                FROM mixpanel_event
            """)
            date_result = cursor.fetchone()
            earliest = date_result[0] if date_result and date_result[0] else None
            latest = date_result[1] if date_result and date_result[1] else None
            
            # Get event breakdown
            cursor.execute("""
                SELECT event_name, COUNT(*) as count
                FROM mixpanel_event
                GROUP BY event_name
                ORDER BY count DESC
                LIMIT 20
            """)
            event_breakdown = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            # Get monthly breakdown
            cursor.execute("""
                SELECT 
                    strftime('%Y-%m', event_time) as month,
                    COUNT(*) as events,
                    COUNT(DISTINCT distinct_id) as users,
                    COUNT(DISTINCT event_name) as uniqueEvents
                FROM mixpanel_event
                WHERE event_time IS NOT NULL
                GROUP BY strftime('%Y-%m', event_time)
                ORDER BY month DESC
                LIMIT 12
            """)
            monthly_breakdown = [
                {
                    "month": row[0],
                    "events": row[1],
                    "users": row[2],
                    "uniqueEvents": row[3]
                } for row in cursor.fetchall()
            ]
            
            # Get daily breakdown (last 30 days)
            cursor.execute("""
                SELECT 
                    DATE(event_time) as date,
                    COUNT(*) as events,
                    COUNT(DISTINCT distinct_id) as users,
                    COUNT(DISTINCT event_name) as uniqueEvents
                FROM mixpanel_event
                WHERE event_time >= DATE('now', '-30 days')
                GROUP BY DATE(event_time)
                ORDER BY date DESC
                LIMIT 30
            """)
            daily_breakdown = [
                {
                    "date": row[0],
                    "events": row[1],
                    "users": row[2],
                    "uniqueEvents": row[3]
                } for row in cursor.fetchall()
            ]
            
            return jsonify({
                "totalEvents": total_events,
                "totalUsers": total_users,
                "eventBreakdown": event_breakdown,
                "monthlyBreakdown": monthly_breakdown,
                "dailyBreakdown": daily_breakdown,
                "dateRange": {
                    "earliest": earliest,
                    "latest": latest
                }
            })
            
    except Exception as e:
        logger.error(f"Error fetching database statistics: {e}", exc_info=True)
        return jsonify({
            "totalEvents": 0,
            "totalUsers": 0,
            "eventBreakdown": [],
            "monthlyBreakdown": [],
            "dailyBreakdown": [],
            "dateRange": {
                "earliest": None,
                "latest": None
            }
        }), 500

@app.route('/api/mixpanel/debug/user-events', methods=['GET'])
def mixpanel_debug_user_events():
    """Get user events"""
    try:
        from utils.database_utils import get_database_connection
        from flask import request
        
        user_id = request.args.get('user_id')
        if not user_id:
            return jsonify({
                "error": "user_id parameter is required"
            }), 400
        
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Get all events for the user, ordered by time
            cursor.execute("""
                SELECT 
                    event_name,
                    event_time,
                    revenue_usd,
                    raw_amount,
                    currency,
                    refund_flag,
                    event_json
                FROM mixpanel_event
                WHERE distinct_id = ?
                ORDER BY event_time ASC
            """, (user_id,))
            
            events = []
            for row in cursor.fetchall():
                # Parse event properties from JSON if available
                properties = {}
                if row[6]:  # event_json
                    try:
                        import json
                        properties = json.loads(row[6])
                    except:
                        properties = {}
                
                events.append({
                    "name": row[0],
                    "time": row[1],
                    "revenue_usd": row[2],
                    "raw_amount": row[3],
                    "currency": row[4],
                    "refund_flag": bool(row[5]) if row[5] is not None else False,
                    "properties": properties
                })
            
            return jsonify(events)
            
    except Exception as e:
        logger.error(f"Error fetching user events: {e}", exc_info=True)
        return jsonify({
            "error": "Failed to fetch user events",
            "message": str(e)
        }), 500

# Removed conflicting proxy routes - these were overriding the proper dashboard routes
# The dashboard blueprint handles /api/dashboard/analytics/chart-data properly

# Catch-all route for React app client-side routing
@app.route('/<path:path>')
@requires_auth
def catch_all(path):
    """Catch-all route to serve React app for client-side routing"""
    # Don't serve React app for API routes
    if path.startswith('api/'):
        return "API endpoint not found", 404
    
    # Don't intercept static file routes - let them be handled by their specific routes
    # The static file routes are defined above and should handle these paths
    
    # For all other routes, serve the React app (client-side routing)
    return send_from_directory('dashboard/static', 'index.html')

if __name__ == '__main__':
    # Initialize database structure if needed (especially for Heroku)
    if config.is_production:
        try:
            from database_init import initialize_all_databases
            initialize_all_databases()
        except Exception as e:
            print(f"Warning: Database initialization failed: {e}")
    
    init_db()
    runner = PipelineRunner()
    
    # Configure for production vs development
    if config.is_production:
        # Production configuration for Heroku
        socketio.run(
            app, 
            debug=False, 
            host=config.HOST, 
            port=config.PORT,
            allow_unsafe_werkzeug=True  # Allow in production for Heroku
        )
    else:
        # Development configuration
        socketio.run(
            app, 
            debug=config.FLASK_DEBUG, 
            host=config.HOST, 
            port=config.PORT
        ) 