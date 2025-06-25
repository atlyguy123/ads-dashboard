#!/usr/bin/env python3
"""
Daily scheduler script for Heroku Scheduler
This script runs the master pipeline once per day
"""

import os
import sys
import logging
from datetime import datetime

# Add the orchestrator directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the pipeline runner
from app import PipelineRunner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_daily_pipeline():
    """Run the daily master pipeline"""
    logger.info("🚀 HEROKU SCHEDULER: Starting daily master pipeline...")
    logger.info(f"   Time: {datetime.now()}")
    
    try:
        # Initialize pipeline runner
        runner = PipelineRunner()
        
        # IMPORTANT: Reset pipeline state before running to ensure clean start
        logger.info("🧹 HEROKU SCHEDULER: Resetting pipeline state for clean start...")
        reset_success, reset_message = runner.reset_all_steps('master_pipeline')
        
        if reset_success:
            logger.info(f"✅ HEROKU SCHEDULER: Pipeline reset successful - {reset_message}")
        else:
            logger.warning(f"⚠️ HEROKU SCHEDULER: Pipeline reset failed - {reset_message}")
            # Continue anyway - reset failure shouldn't stop the pipeline
        
        # Run the master pipeline
        logger.info("🚀 HEROKU SCHEDULER: Starting master pipeline execution...")
        success, message = runner.run_pipeline('master_pipeline')
        
        if success:
            logger.info("✅ HEROKU SCHEDULER: Daily master pipeline started successfully")
            logger.info("   Pipeline is now running in background - check dashboard for progress")
            print("SUCCESS: Daily pipeline started")
            sys.exit(0)
        else:
            logger.error(f"❌ HEROKU SCHEDULER: Daily master pipeline failed to start: {message}")
            print(f"FAILED: {message}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ HEROKU SCHEDULER: Error running daily pipeline: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_daily_pipeline() 