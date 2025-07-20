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

# Import timezone utilities for consistent timezone handling
from utils.timezone_utils import now_in_timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_daily_pipeline():
    """Run the daily master pipeline"""
    logger.info("🚀 HEROKU SCHEDULER: Starting daily master pipeline...")
    logger.info(f"   Time: {now_in_timezone()}")
    
    try:
        # Initialize pipeline runner
        runner = PipelineRunner()
        
        # Run the master pipeline
        success, message = runner.run_pipeline('master_pipeline')
        
        if success:
            logger.info("✅ HEROKU SCHEDULER: Daily master pipeline completed successfully")
            print("SUCCESS: Daily pipeline completed")
            sys.exit(0)
        else:
            logger.error(f"❌ HEROKU SCHEDULER: Daily master pipeline failed: {message}")
            print(f"FAILED: {message}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ HEROKU SCHEDULER: Error running daily pipeline: {e}")
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_daily_pipeline() 