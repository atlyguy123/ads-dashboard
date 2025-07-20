#!/usr/bin/env python3
"""
Simple launcher script for Railway deployment.
This sets up the Python path and imports the orchestrator app directly.
"""

import sys
import os

if __name__ == "__main__":
    # Add the project root to Python path
    sys.path.insert(0, '/app')
    
    # Get environment variables
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    # Import and run the Flask app
    from orchestrator.app import app
    
    # Run the Flask app
    app.run(host=host, port=port, debug=False) 