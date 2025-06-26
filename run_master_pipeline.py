#!/usr/bin/env python3
"""
Scheduler script to run the master pipeline
Usage: python run_master_pipeline.py
"""

import sys
import os

# Add the orchestrator directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'orchestrator'))

def run_master_pipeline():
    """Run the master pipeline and return success status"""
    try:
        # Import the runner from the orchestrator
        from app import runner
        
        print("🚀 Scheduler: Starting master pipeline...")
        
        # Run the master pipeline
        success, message = runner.run_pipeline('master_pipeline')
        
        if success:
            print(f"✅ Scheduler: Pipeline started successfully - {message}")
            return True
        else:
            print(f"❌ Scheduler: Pipeline failed to start - {message}")
            return False
            
    except Exception as e:
        print(f"💥 Scheduler: Exception occurred - {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_master_pipeline()
    sys.exit(0 if success else 1) 