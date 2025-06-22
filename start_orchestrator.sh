#!/bin/bash

echo "🚀 Starting Pipeline Orchestrator..."

# Activate virtual environment
source venv/bin/activate

# Start the Flask application
cd orchestrator
python3 app.py 