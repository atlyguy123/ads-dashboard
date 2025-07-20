#!/usr/bin/env python3
"""
Simple launcher script for Railway deployment.
This ensures the orchestrator app runs correctly without changing any existing imports.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import and run the orchestrator app
if __name__ == "__main__":
    from orchestrator.app import app
    
    # Get port from environment (Railway sets this automatically)
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    # Run the Flask app
    app.run(host=host, port=port, debug=False) 