#!/usr/bin/env python3
"""
Simple launcher script for Railway deployment.
This executes the orchestrator app from the correct directory context.
"""

import sys
import os
import subprocess

if __name__ == "__main__":
    # Get port from environment (Railway sets this automatically)
    port = os.environ.get('PORT', '5000')
    host = os.environ.get('HOST', '0.0.0.0')
    
    # Set environment variables for the Flask app
    os.environ['PORT'] = port
    os.environ['HOST'] = host
    os.environ['FLASK_ENV'] = 'production'
    
    # Change to orchestrator directory and run app.py
    os.chdir('orchestrator')
    
    # Execute the app.py directly
    result = subprocess.run([sys.executable, 'app.py'], 
                          cwd=os.getcwd(), 
                          env=os.environ)
    
    sys.exit(result.returncode) 