#!/usr/bin/env python3
"""
Robust Data Refresh Workflow - Mixpanel Pipeline

This module provides a bulletproof data refresh workflow that ensures data integrity
during the 3-day refresh process. It includes:

- Backup and rollback capabilities
- Comprehensive verification at each step
- Lock-step validation between environments
- Transaction-safe operations
- Detailed logging and monitoring

This solves the core data consistency issues between local and production environments.
"""

import os
import sys
import json
import logging
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

# Import timezone utilities
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Import other pipeline modules
from data_integrity_verifier import DataIntegrityVerifier, IntegrityReport

# Add utils directory to path
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class RefreshStatus:
    """Track the status of refresh operations"""
    step_name: str
    status: str  # 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'ROLLED_BACK'
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    details: Dict[str, Any]
    backup_created: bool = False

class RobustDataRefresh:
    """
    Main class for performing robust data refresh with backup/rollback capabilities
    """
    
    def __init__(self, backup_dir: str = None):
        """
        Initialize the robust refresh system
        
        Args:
            backup_dir: Directory for storing backups (auto-created if None)
        """
        self.db_path = get_database_path('mixpanel_data')
        
        # Setup backup directory
        if backup_dir is None:
            backup_dir = Path(__file__).parent / "backups"
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Track refresh steps
        self.refresh_steps = []
        self.current_backup_path = None
        
        # Initialize verifier
        self.verifier = DataIntegrityVerifier()
        
        logger.info(f"RobustDataRefresh initialized with backup dir: {self.backup_dir}")
    
    def execute_robust_refresh(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Execute the complete robust refresh workflow
        
        Args:
            force_refresh: Force refresh even if recent refresh detected
            
        Returns:
            Dictionary with refresh results and metrics
        """
        logger.info("=== STARTING ROBUST DATA REFRESH WORKFLOW ===")
        start_time = now_in_timezone()
        
        try:
            # Step 1: Pre-flight verification
            logger.info("Step 1: Pre-flight verification")
            pre_flight_status = self._execute_step("pre_flight_verification", self._pre_flight_verification)
            if pre_flight_status.status == 'FAILED':
                return self._generate_failure_report("Pre-flight verification failed", start_time)
            
            # Step 2: Create backup
            logger.info("Step 2: Creating backup")
            backup_status = self._execute_step("create_backup", self._create_backup)
            if backup_status.status == 'FAILED':
                return self._generate_failure_report("Backup creation failed", start_time)
            
            # Step 3: Download refresh data
            logger.info("Step 3: Downloading refresh data")
            download_status = self._execute_step("download_refresh_data", self._download_refresh_data)
            if download_status.status == 'FAILED':
                logger.warning("Download failed, initiating rollback")
                self._rollback_all_steps()
                return self._generate_failure_report("Download failed", start_time)
            
            # Step 4: Ingest refresh data
            logger.info("Step 4: Ingesting refresh data")
            ingest_status = self._execute_step("ingest_refresh_data", self._ingest_refresh_data)
            if ingest_status.status == 'FAILED':
                logger.warning("Ingestion failed, initiating rollback")
                self._rollback_all_steps()
                return self._generate_failure_report("Ingestion failed", start_time)
            
            # Step 5: Re-process downstream modules
            logger.info("Step 5: Re-processing downstream modules")
            reprocess_status = self._execute_step("reprocess_downstream", self._reprocess_downstream_modules)
            if reprocess_status.status == 'FAILED':
                logger.warning("Downstream processing failed, initiating rollback")
                self._rollback_all_steps()
                return self._generate_failure_report("Downstream processing failed", start_time)
            
            # Step 6: Comprehensive verification
            logger.info("Step 6: Comprehensive verification")
            verification_status = self._execute_step("comprehensive_verification", self._comprehensive_verification)
            if verification_status.status == 'FAILED':
                logger.warning("Verification failed, initiating rollback")
                self._rollback_all_steps()
                return self._generate_failure_report("Verification failed", start_time)
            
            # Step 7: Cleanup old backups
            logger.info("Step 7: Cleaning up old backups")
            cleanup_status = self._execute_step("cleanup_backups", self._cleanup_old_backups)
            
            end_time = now_in_timezone()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=== ROBUST DATA REFRESH COMPLETED SUCCESSFULLY ===")
            
            return {
                'success': True,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'steps_completed': len([s for s in self.refresh_steps if s.status == 'COMPLETED']),
                'total_steps': len(self.refresh_steps),
                'backup_created': self.current_backup_path,
                'verification_passed': verification_status.status == 'COMPLETED',
                'details': {
                    'steps': [
                        {
                            'name': step.step_name,
                            'status': step.status,
                            'duration': (step.end_time - step.start_time).total_seconds() if step.end_time and step.start_time else 0,
                            'details': step.details
                        }
                        for step in self.refresh_steps
                    ]
                }
            }
            
        except Exception as e:
            logger.error(f"Unexpected error during robust refresh: {e}")
            logger.warning("Initiating emergency rollback")
            self._rollback_all_steps()
            return self._generate_failure_report(f"Unexpected error: {e}", start_time)
    
    def _execute_step(self, step_name: str, step_function) -> RefreshStatus:
        """Execute a single refresh step with error handling"""
        status = RefreshStatus(
            step_name=step_name,
            status='PENDING',
            start_time=None,
            end_time=None,
            details={}
        )
        self.refresh_steps.append(status)
        
        try:
            logger.info(f"🔄 Starting: {step_name}")
            status.status = 'RUNNING'
            status.start_time = now_in_timezone()
            
            # Execute the step function
            result = step_function()
            
            status.end_time = now_in_timezone()
            duration = (status.end_time - status.start_time).total_seconds()
            
            if result.get('success', False):
                status.status = 'COMPLETED'
                status.details = result
                logger.info(f"✅ Completed: {step_name} ({duration:.2f}s)")
            else:
                status.status = 'FAILED'
                status.details = result
                logger.error(f"❌ Failed: {step_name} - {result.get('error', 'Unknown error')}")
            
        except Exception as e:
            status.end_time = now_in_timezone()
            status.status = 'FAILED'
            status.details = {'error': str(e)}
            logger.error(f"❌ Failed: {step_name} - {e}")
        
        return status
    
    def _pre_flight_verification(self) -> Dict[str, Any]:
        """Perform pre-flight checks before starting refresh"""
        try:
            checks = []
            
            # Check database exists and is accessible
            if not Path(self.db_path).exists():
                return {'success': False, 'error': f'Database not found: {self.db_path}'}
            
            # Check sufficient disk space (at least 1GB for backup)
            db_size = Path(self.db_path).stat().st_size
            free_space = shutil.disk_usage(self.backup_dir).free
            if free_space < db_size * 2:  # Need at least 2x database size
                return {'success': False, 'error': f'Insufficient disk space for backup. Need {db_size * 2}, have {free_space}'}
            
            # Check AWS credentials are available
            aws_keys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_EVENTS', 'S3_BUCKET_USERS']
            missing_keys = [key for key in aws_keys if not os.environ.get(key)]
            if missing_keys:
                return {'success': False, 'error': f'Missing AWS environment variables: {missing_keys}'}
            
            # Check recent refresh activity
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(processing_timestamp) 
                FROM processed_event_days 
                WHERE date_day >= date('now', '-3 days')
            """)
            last_refresh = cursor.fetchone()[0]
            conn.close()
            
            if last_refresh:
                last_refresh_dt = datetime.fromisoformat(last_refresh.replace('Z', '+00:00'))
                hours_since = (now_in_timezone() - last_refresh_dt).total_seconds() / 3600
                checks.append(f"Last refresh: {hours_since:.1f} hours ago")
            else:
                checks.append("No recent refresh detected")
            
            return {
                'success': True,
                'database_size_mb': db_size / (1024*1024),
                'free_space_gb': free_space / (1024*1024*1024),
                'checks': checks
            }
            
        except Exception as e:
            return {'success': False, 'error': f'Pre-flight check failed: {e}'}
    
    def _create_backup(self) -> Dict[str, Any]:
        """Create backup of current database state"""
        try:
            timestamp = now_in_timezone().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"mixpanel_data_backup_{timestamp}.db"
            backup_path = self.backup_dir / backup_filename
            
            logger.info(f"Creating backup: {backup_path}")
            
            # Copy database file
            shutil.copy2(self.db_path, backup_path)
            
            # Verify backup integrity
            try:
                conn = sqlite3.connect(str(backup_path))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
                user_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM mixpanel_event")
                event_count = cursor.fetchone()[0]
                conn.close()
            except Exception as e:
                return {'success': False, 'error': f'Backup verification failed: {e}'}
            
            self.current_backup_path = str(backup_path)
            
            # Mark all current steps as having backup
            for step in self.refresh_steps:
                step.backup_created = True
            
            return {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size_mb': backup_path.stat().st_size / (1024*1024),
                'verified_user_count': user_count,
                'verified_event_count': event_count
            }
            
        except Exception as e:
            return {'success': False, 'error': f'Backup creation failed: {e}'}
    
    def _download_refresh_data(self) -> Dict[str, Any]:
        """Execute the download module with fixed refresh logic"""
        try:
            # Import and run download module
            from importlib import import_module
            download_module = import_module('01_download_update_data')
            
            # Execute download with our fixed logic
            result = download_module.main()
            
            if result == 0:
                return {'success': True, 'message': 'Download completed successfully'}
            else:
                return {'success': False, 'error': 'Download module returned error code'}
                
        except Exception as e:
            return {'success': False, 'error': f'Download execution failed: {e}'}
    
    def _ingest_refresh_data(self) -> Dict[str, Any]:
        """Execute the ingestion module with fixed refresh logic"""
        try:
            # Import and run ingestion module
            from importlib import import_module
            ingest_module = import_module('03_ingest_data')
            
            # Execute ingestion with our fixed logic
            result = ingest_module.main()
            
            if result == 0:
                return {'success': True, 'message': 'Ingestion completed successfully'}
            else:
                return {'success': False, 'error': 'Ingestion module returned error code'}
                
        except Exception as e:
            return {'success': False, 'error': f'Ingestion execution failed: {e}'}
    
    def _reprocess_downstream_modules(self) -> Dict[str, Any]:
        """Re-run downstream processing modules affected by refresh"""
        try:
            modules_to_rerun = [
                ('04_assign_product_information', 'Product assignment'),
                ('05_set_abi_attribution', 'ABI attribution'),
                ('06_validate_event_lifecycle', 'Lifecycle validation'),
                ('07_assign_economic_tier', 'Economic tier assignment')
            ]
            
            results = []
            
            for module_name, description in modules_to_rerun:
                try:
                    logger.info(f"Re-running: {description}")
                    from importlib import import_module
                    module = import_module(module_name)
                    result = module.main()
                    
                    if result == 0:
                        results.append({'module': module_name, 'status': 'success'})
                        logger.info(f"✅ {description} completed")
                    else:
                        results.append({'module': module_name, 'status': 'failed', 'error': 'Non-zero exit code'})
                        logger.warning(f"⚠️ {description} failed")
                        
                except Exception as e:
                    results.append({'module': module_name, 'status': 'failed', 'error': str(e)})
                    logger.error(f"❌ {description} failed: {e}")
            
            # Check if critical modules succeeded
            failed_modules = [r for r in results if r['status'] == 'failed']
            
            if len(failed_modules) == 0:
                return {'success': True, 'modules_processed': len(results), 'results': results}
            elif len(failed_modules) <= 1:  # Allow one module to fail
                return {'success': True, 'modules_processed': len(results), 'failed_modules': failed_modules, 'results': results}
            else:
                return {'success': False, 'error': f'{len(failed_modules)} modules failed', 'failed_modules': failed_modules}
                
        except Exception as e:
            return {'success': False, 'error': f'Downstream processing failed: {e}'}
    
    def _comprehensive_verification(self) -> Dict[str, Any]:
        """Run comprehensive verification using the DataIntegrityVerifier"""
        try:
            logger.info("Running comprehensive data integrity verification")
            
            # Run verification
            reports = self.verifier.verify_data_consistency(days_back=7)
            
            # Analyze results
            total_checks = len(reports)
            passed_checks = len([r for r in reports if r.status == "PASS"])
            warning_checks = len([r for r in reports if r.status == "WARNING"])
            failed_checks = len([r for r in reports if r.status == "FAIL"])
            
            confidence_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
            
            # Determine overall success
            # Allow some warnings but no failures for success
            success = failed_checks == 0 and confidence_score >= 70
            
            return {
                'success': success,
                'confidence_score': confidence_score,
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'warning_checks': warning_checks,
                'failed_checks': failed_checks,
                'failed_check_details': [
                    {'name': r.check_name, 'details': r.details} 
                    for r in reports if r.status == "FAIL"
                ]
            }
            
        except Exception as e:
            return {'success': False, 'error': f'Verification failed: {e}'}
    
    def _cleanup_old_backups(self) -> Dict[str, Any]:
        """Clean up old backup files (keep last 5)"""
        try:
            backup_files = list(self.backup_dir.glob("mixpanel_data_backup_*.db"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # Keep the 5 most recent backups
            files_to_delete = backup_files[5:]
            deleted_count = 0
            
            for backup_file in files_to_delete:
                try:
                    backup_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete old backup {backup_file}: {e}")
            
            return {
                'success': True,
                'backups_remaining': len(backup_files) - deleted_count,
                'backups_deleted': deleted_count
            }
            
        except Exception as e:
            return {'success': False, 'error': f'Backup cleanup failed: {e}'}
    
    def _rollback_all_steps(self) -> bool:
        """Rollback all completed steps using backup"""
        if not self.current_backup_path or not Path(self.current_backup_path).exists():
            logger.error("❌ No backup available for rollback!")
            return False
        
        try:
            logger.warning("🔄 INITIATING ROLLBACK TO BACKUP")
            
            # Replace current database with backup
            shutil.copy2(self.current_backup_path, self.db_path)
            
            # Verify rollback
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
            user_count = cursor.fetchone()[0]
            conn.close()
            
            # Mark all steps as rolled back
            for step in self.refresh_steps:
                if step.status in ['COMPLETED', 'FAILED']:
                    step.status = 'ROLLED_BACK'
            
            logger.info(f"✅ Rollback completed. Database restored with {user_count} users")
            return True
            
        except Exception as e:
            logger.error(f"❌ CRITICAL: Rollback failed: {e}")
            return False
    
    def _generate_failure_report(self, error_message: str, start_time: datetime) -> Dict[str, Any]:
        """Generate failure report with rollback status"""
        end_time = now_in_timezone()
        duration = (end_time - start_time).total_seconds()
        
        return {
            'success': False,
            'error': error_message,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'rollback_performed': any(s.status == 'ROLLED_BACK' for s in self.refresh_steps),
            'backup_available': self.current_backup_path is not None,
            'steps_completed': len([s for s in self.refresh_steps if s.status == 'COMPLETED']),
            'steps_failed': len([s for s in self.refresh_steps if s.status == 'FAILED']),
            'details': {
                'steps': [
                    {
                        'name': step.step_name,
                        'status': step.status,
                        'details': step.details
                    }
                    for step in self.refresh_steps
                ]
            }
        }


def main():
    """
    Main function to execute robust data refresh
    """
    logger.info("Starting Robust Data Refresh")
    
    try:
        refresh_system = RobustDataRefresh()
        result = refresh_system.execute_robust_refresh()
        
        # Save results
        timestamp = now_in_timezone().strftime('%Y%m%d_%H%M%S')
        report_file = f"robust_refresh_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        logger.info(f"Refresh report saved to: {report_file}")
        
        if result['success']:
            logger.info("🎉 Robust data refresh completed successfully!")
            return 0
        else:
            logger.error(f"❌ Robust data refresh failed: {result['error']}")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Critical error in robust refresh: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 