#!/usr/bin/env python3
"""
Pipeline status checker - Use this to quickly check if the daily pipeline completed
Usage: python check_pipeline_status.py
"""

import os
import sys
import json
from datetime import datetime

# Add the orchestrator directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import PipelineRunner

def check_pipeline_status():
    """Check the current status of the master pipeline"""
    try:
        runner = PipelineRunner()
        status = runner.get_pipeline_status('master_pipeline')
        
        print("🔍 PIPELINE STATUS CHECK")
        print(f"   Time: {datetime.now()}")
        print("=" * 50)
        
        if not status:
            print("✅ STATUS: All steps are PENDING (ready to run)")
            print("   This means either:")
            print("   • Pipeline hasn't run yet today")
            print("   • Pipeline was reset")
            return
        
        # Count step statuses
        total_steps = len(status)
        success_count = sum(1 for s in status.values() if s.get('status') == 'success')
        failed_count = sum(1 for s in status.values() if s.get('status') == 'failed')
        running_count = sum(1 for s in status.values() if s.get('status') == 'running')
        
        print(f"📊 OVERVIEW: {success_count}/{total_steps} completed")
        print(f"   ✅ Success: {success_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   🔄 Running: {running_count}")
        print("")
        
        # Overall status
        if success_count == total_steps:
            print("🎉 RESULT: Pipeline COMPLETED SUCCESSFULLY!")
            # Find the latest completion time
            latest_time = None
            for step_status in status.values():
                if step_status.get('timestamp'):
                    step_time = datetime.fromisoformat(step_status['timestamp'].replace('Z', '+00:00'))
                    if not latest_time or step_time > latest_time:
                        latest_time = step_time
            if latest_time:
                print(f"   Completed at: {latest_time}")
                
        elif failed_count > 0:
            print("❌ RESULT: Pipeline FAILED")
            # Show which steps failed
            for step_id, step_status in status.items():
                if step_status.get('status') == 'failed':
                    print(f"   Failed step: {step_id}")
                    if step_status.get('error_message'):
                        print(f"   Error: {step_status['error_message']}")
                        
        elif running_count > 0:
            print("🔄 RESULT: Pipeline is RUNNING")
            # Show which steps are running
            for step_id, step_status in status.items():
                if step_status.get('status') == 'running':
                    print(f"   Running step: {step_id}")
                    if step_status.get('timestamp'):
                        print(f"   Started at: {step_status['timestamp']}")
        else:
            print("⏸️ RESULT: Pipeline is PARTIALLY COMPLETE")
            print("   Some steps completed, others pending")
        
        print("")
        print("📋 DETAILED STATUS:")
        for step_id, step_status in status.items():
            status_emoji = {
                'success': '✅',
                'failed': '❌', 
                'running': '🔄',
                'pending': '⏸️'
            }.get(step_status.get('status', 'pending'), '❓')
            
            print(f"   {status_emoji} {step_id}: {step_status.get('status', 'pending')}")
            if step_status.get('timestamp'):
                print(f"      Time: {step_status['timestamp']}")
        
    except Exception as e:
        print(f"❌ ERROR checking pipeline status: {e}")
        sys.exit(1)

if __name__ == '__main__':
    check_pipeline_status() 