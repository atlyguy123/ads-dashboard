#!/usr/bin/env python3
"""
Simple launcher script for Railway deployment.
This runs the orchestrator app as a module with proper Python path setup.
"""

import sys
import os
import subprocess

if __name__ == "__main__":
    # Get environment variables
    port = os.environ.get('PORT', '5000')
    host = os.environ.get('HOST', '0.0.0.0')
    
    # Set environment variables for the Flask app
    env = os.environ.copy()
    env['PORT'] = port
    env['HOST'] = host
    env['FLASK_ENV'] = 'production'
    env['PYTHONPATH'] = '/app'
    
    # Run as module from project root
    result = subprocess.run([
        sys.executable, '-m', 'orchestrator.app'
    ], cwd='/app', env=env)
    
    sys.exit(result.returncode) 