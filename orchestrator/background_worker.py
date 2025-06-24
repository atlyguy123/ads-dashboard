#!/usr/bin/env python3
"""
Background worker for processing pipeline jobs
This runs independently of the web interface
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta
import threading
import signal
import sys

# Import the pipeline runner
from app import PipelineRunner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BackgroundWorker:
    def __init__(self):
        self.runner = PipelineRunner()
        self.running = True
        self.last_daily_run = None
        
        # Load last run time from file
        self.load_last_run_time()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        
    def load_last_run_time(self):
        """Load the last daily run time from file"""
        try:
            if os.path.exists('last_daily_run.json'):
                with open('last_daily_run.json', 'r') as f:
                    data = json.load(f)
                    self.last_daily_run = datetime.fromisoformat(data['last_run'])
                    logger.info(f"Loaded last daily run time: {self.last_daily_run}")
        except Exception as e:
            logger.error(f"Error loading last run time: {e}")
            self.last_daily_run = None
    
    def save_last_run_time(self):
        """Save the last daily run time to file"""
        try:
            with open('last_daily_run.json', 'w') as f:
                json.dump({
                    'last_run': datetime.now().isoformat()
                }, f)
            logger.info("Saved last daily run time")
        except Exception as e:
            logger.error(f"Error saving last run time: {e}")
    
    def should_run_daily_job(self):
        """Check if daily job should run"""
        if self.last_daily_run is None:
            return True
        
        # Run if it's been more than 22 hours (allows for some flexibility)
        time_since_last_run = datetime.now() - self.last_daily_run
        return time_since_last_run > timedelta(hours=22)
    
    def run_daily_pipeline(self):
        """Run the daily master pipeline"""
        logger.info("🚀 Starting daily master pipeline...")
        
        try:
            # Run the master pipeline
            success, message = self.runner.run_pipeline('master_pipeline')
            
            if success:
                logger.info("✅ Daily master pipeline completed successfully")
                self.save_last_run_time()
                self.last_daily_run = datetime.now()
            else:
                logger.error(f"❌ Daily master pipeline failed: {message}")
                
        except Exception as e:
            logger.error(f"❌ Error running daily pipeline: {e}")
    
    def process_job_queue(self):
        """Process any queued jobs"""
        # This could be extended to handle a job queue
        # For now, we'll just check for daily jobs
        pass
    
    def health_check(self):
        """Perform health check and log status"""
        logger.info("💓 Background worker health check - running normally")
        
        # Log some basic stats
        if self.last_daily_run:
            hours_since_last = (datetime.now() - self.last_daily_run).total_seconds() / 3600
            logger.info(f"   Last daily run: {hours_since_last:.1f} hours ago")
        else:
            logger.info("   No daily run recorded yet")
    
    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run(self):
        """Main worker loop"""
        logger.info("🔄 Background worker starting...")
        
        while self.running:
            try:
                # Check if daily job should run
                if self.should_run_daily_job():
                    logger.info("⏰ Time for daily pipeline run")
                    self.run_daily_pipeline()
                
                # Process any other queued jobs
                self.process_job_queue()
                
                # Health check every hour
                current_minute = datetime.now().minute
                if current_minute == 0:  # Top of the hour
                    self.health_check()
                
                # Sleep for 1 minute before next check
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(60)  # Sleep before retrying
        
        logger.info("🛑 Background worker stopped")

if __name__ == '__main__':
    worker = BackgroundWorker()
    worker.run() 