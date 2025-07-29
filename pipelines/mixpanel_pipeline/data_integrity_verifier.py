#!/usr/bin/env python3
"""
Data Integrity Verifier - Mixpanel Pipeline

This module provides "lock-step" verification capabilities to ensure data consistency
and integrity across different environments (local vs production) and after refresh operations.

Features:
- Compare data between local and production databases
- Verify refresh logic is working correctly
- Generate integrity reports with detailed discrepancies
- Provide confidence metrics for data consistency
"""

import os
import sys
import sqlite3
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from urllib.parse import urlparse

# Import timezone utilities
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from orchestrator.utils.timezone_utils import now_in_timezone

# Try to import psycopg2 for PostgreSQL support
try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    psycopg2 = None

# Add utils directory to path
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class IntegrityReport:
    """Data structure for integrity check results"""
    check_name: str
    status: str  # 'PASS', 'FAIL', 'WARNING'
    local_count: int
    remote_count: int
    difference: int
    difference_percentage: float
    details: Dict[str, Any]
    recommendations: List[str]

class DataIntegrityVerifier:
    """
    Main class for performing data integrity verification between environments
    """
    
    def __init__(self, local_db_path: str = None):
        """
        Initialize the verifier
        
        Args:
            local_db_path: Path to local SQLite database (auto-detected if None)
        """
        # Auto-detect local database path
        if local_db_path is None:
            local_db_path = get_database_path('mixpanel_data')
        self.local_db_path = local_db_path
        
        # Production database configuration
        self.production_db_url = os.environ.get('DATABASE_URL')
        self.use_postgres = self.production_db_url is not None and HAS_POSTGRES
        
        logger.info("DataIntegrityVerifier initialized")
        
    def get_local_connection(self):
        """Get connection to local SQLite database"""
        return sqlite3.connect(self.local_db_path)
    
    def get_production_connection(self):
        """Get connection to production database"""
        if self.use_postgres:
            url = urlparse(self.production_db_url)
            conn = psycopg2.connect(
                host=url.hostname,
                port=url.port,
                database=url.path[1:],
                user=url.username,
                password=url.password,
                sslmode='require'
            )
            return conn, 'postgres'
        else:
            # Fallback to local for testing
            logger.warning("No production database configured, using local database for both comparisons")
            return sqlite3.connect(self.local_db_path), 'sqlite'
    
    def verify_data_consistency(self, days_back: int = 7) -> List[IntegrityReport]:
        """
        Perform comprehensive data consistency verification
        
        Args:
            days_back: Number of days to verify (default 7 for recent data)
            
        Returns:
            List of integrity reports
        """
        logger.info(f"Starting data integrity verification for last {days_back} days")
        reports = []
        
        try:
            # Get database connections
            local_conn = self.get_local_connection()
            prod_conn, prod_type = self.get_production_connection()
            
            # Run all verification checks
            reports.extend(self._verify_user_counts(local_conn, prod_conn, prod_type))
            reports.extend(self._verify_event_counts_by_date(local_conn, prod_conn, prod_type, days_back))
            reports.extend(self._verify_value_calculations(local_conn, prod_conn, prod_type, days_back))
            reports.extend(self._verify_refresh_date_integrity(local_conn, prod_conn, prod_type))
            reports.extend(self._verify_data_freshness(local_conn, prod_conn, prod_type))
            
            local_conn.close()
            prod_conn.close()
            
            # Generate summary
            self._generate_verification_summary(reports)
            
            return reports
            
        except Exception as e:
            logger.error(f"Error during data integrity verification: {e}")
            return [IntegrityReport(
                check_name="Verification Error",
                status="FAIL",
                local_count=0,
                remote_count=0,
                difference=0,
                difference_percentage=0.0,
                details={"error": str(e)},
                recommendations=["Fix database connection issues and retry verification"]
            )]
    
    def _verify_user_counts(self, local_conn, prod_conn, prod_type) -> List[IntegrityReport]:
        """Verify user counts match between environments"""
        reports = []
        
        try:
            # Local user count
            local_cursor = local_conn.cursor()
            local_cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
            local_count = local_cursor.fetchone()[0]
            
            # Production user count
            prod_cursor = prod_conn.cursor()
            if prod_type == 'postgres':
                prod_cursor.execute("SELECT COUNT(*) FROM raw_user_data")
            else:
                prod_cursor.execute("SELECT COUNT(*) FROM raw_user_data")
            prod_count = prod_cursor.fetchone()[0]
            
            difference = abs(local_count - prod_count)
            diff_percentage = (difference / max(local_count, prod_count, 1)) * 100
            
            status = "PASS" if difference == 0 else ("WARNING" if diff_percentage < 5 else "FAIL")
            
            reports.append(IntegrityReport(
                check_name="User Count Consistency",
                status=status,
                local_count=local_count,
                remote_count=prod_count,
                difference=difference,
                difference_percentage=diff_percentage,
                details={
                    "local_users": local_count,
                    "production_users": prod_count,
                    "expected_match": True
                },
                recommendations=[] if status == "PASS" else [
                    "Check user ingestion process",
                    "Verify user filtering logic consistency",
                    "Consider re-running user refresh"
                ]
            ))
            
        except Exception as e:
            logger.error(f"Error verifying user counts: {e}")
            
        return reports
    
    def _verify_event_counts_by_date(self, local_conn, prod_conn, prod_type, days_back) -> List[IntegrityReport]:
        """Verify event counts by date match between environments"""
        reports = []
        
        try:
            # Get date range to check
            end_date = now_in_timezone().date()
            start_date = end_date - timedelta(days=days_back-1)
            
            # Local event counts by date
            local_cursor = local_conn.cursor()
            local_cursor.execute("""
                SELECT DATE(event_time) as event_date, COUNT(*) as event_count
                FROM mixpanel_event 
                WHERE DATE(event_time) BETWEEN ? AND ?
                GROUP BY DATE(event_time)
                ORDER BY event_date
            """, (start_date, end_date))
            local_events = {row[0]: row[1] for row in local_cursor.fetchall()}
            
            # Production event counts by date
            prod_cursor = prod_conn.cursor()
            if prod_type == 'postgres':
                prod_cursor.execute("""
                    SELECT date_day, events_downloaded
                    FROM downloaded_dates 
                    WHERE date_day BETWEEN %s AND %s
                    ORDER BY date_day
                """, (start_date, end_date))
            else:
                prod_cursor.execute("""
                    SELECT date_day, events_downloaded
                    FROM downloaded_dates 
                    WHERE date_day BETWEEN ? AND ?
                    ORDER BY date_day
                """, (start_date, end_date))
            prod_events = {str(row[0]): row[1] for row in prod_cursor.fetchall()}
            
            # Compare each date
            mismatches = []
            total_local = 0
            total_prod = 0
            
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                local_count = local_events.get(date_str, 0)
                prod_count = prod_events.get(date_str, 0)
                
                total_local += local_count
                total_prod += prod_count
                
                if local_count != prod_count:
                    mismatches.append({
                        'date': date_str,
                        'local': local_count,
                        'production': prod_count,
                        'difference': abs(local_count - prod_count)
                    })
                
                current_date += timedelta(days=1)
            
            total_difference = abs(total_local - total_prod)
            diff_percentage = (total_difference / max(total_local, total_prod, 1)) * 100
            
            status = "PASS" if len(mismatches) == 0 else ("WARNING" if len(mismatches) <= 2 else "FAIL")
            
            reports.append(IntegrityReport(
                check_name=f"Event Count Consistency ({days_back} days)",
                status=status,
                local_count=total_local,
                remote_count=total_prod,
                difference=total_difference,
                difference_percentage=diff_percentage,
                details={
                    "date_range": f"{start_date} to {end_date}",
                    "mismatched_dates": len(mismatches),
                    "mismatches": mismatches[:5]  # Show first 5 mismatches
                },
                recommendations=[] if status == "PASS" else [
                    "Check event ingestion for mismatched dates",
                    "Verify refresh logic is working correctly",
                    "Consider re-running data refresh for problem dates"
                ]
            ))
            
        except Exception as e:
            logger.error(f"Error verifying event counts by date: {e}")
            
        return reports
    
    def _verify_value_calculations(self, local_conn, prod_conn, prod_type, days_back) -> List[IntegrityReport]:
        """Verify value calculations are consistent"""
        reports = []
        
        try:
            # Check for users with zero values who shouldn't have them
            local_cursor = local_conn.cursor()
            local_cursor.execute("""
                SELECT COUNT(*) 
                FROM user_product_metrics 
                WHERE current_value = 0 
                AND current_status IN ('trial_pending', 'trial_converted', 'initial_purchase')
                AND value_status != 'final_value'
            """)
            zero_value_count = local_cursor.fetchone()[0]
            
            # Total users with these statuses
            local_cursor.execute("""
                SELECT COUNT(*) 
                FROM user_product_metrics 
                WHERE current_status IN ('trial_pending', 'trial_converted', 'initial_purchase')
                AND value_status != 'final_value'
            """)
            total_active_users = local_cursor.fetchone()[0]
            
            zero_percentage = (zero_value_count / max(total_active_users, 1)) * 100
            
            status = "PASS" if zero_percentage < 1 else ("WARNING" if zero_percentage < 5 else "FAIL")
            
            reports.append(IntegrityReport(
                check_name="Value Calculation Integrity",
                status=status,
                local_count=zero_value_count,
                remote_count=total_active_users,
                difference=zero_value_count,
                difference_percentage=zero_percentage,
                details={
                    "users_with_zero_values": zero_value_count,
                    "total_active_users": total_active_users,
                    "percentage_with_zero": zero_percentage
                },
                recommendations=[] if status == "PASS" else [
                    "Check value estimation logic for users with zero values",
                    "Verify price bucket and conversion rate assignments",
                    "Re-run value estimation for affected users"
                ]
            ))
            
        except Exception as e:
            logger.error(f"Error verifying value calculations: {e}")
            
        return reports
    
    def _verify_refresh_date_integrity(self, local_conn, prod_conn, prod_type) -> List[IntegrityReport]:
        """Verify that refresh dates are being processed correctly"""
        reports = []
        
        try:
            # Check if last 3 days have fresh processed timestamps
            today = now_in_timezone().date()
            refresh_start = today - timedelta(days=2)
            
            local_cursor = local_conn.cursor()
            local_cursor.execute("""
                SELECT date_day, processing_timestamp
                FROM processed_event_days 
                WHERE date_day BETWEEN ? AND ?
                ORDER BY date_day
            """, (refresh_start, today))
            
            processed_dates = local_cursor.fetchall()
            
            # Check if processing timestamps are recent (within last 24 hours)
            cutoff_time = now_in_timezone() - timedelta(hours=24)
            fresh_count = 0
            stale_dates = []
            
            for date_day, timestamp_str in processed_dates:
                if timestamp_str:
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        if timestamp > cutoff_time:
                            fresh_count += 1
                        else:
                            stale_dates.append(date_day)
                    except:
                        stale_dates.append(date_day)
                else:
                    stale_dates.append(date_day)
            
            expected_fresh = 3  # Last 3 days should be fresh
            status = "PASS" if fresh_count == expected_fresh else ("WARNING" if fresh_count >= 2 else "FAIL")
            
            reports.append(IntegrityReport(
                check_name="Refresh Date Integrity",
                status=status,
                local_count=fresh_count,
                remote_count=expected_fresh,
                difference=expected_fresh - fresh_count,
                difference_percentage=((expected_fresh - fresh_count) / expected_fresh) * 100,
                details={
                    "fresh_dates": fresh_count,
                    "expected_fresh": expected_fresh,
                    "stale_dates": stale_dates
                },
                recommendations=[] if status == "PASS" else [
                    "Check if refresh logic is running correctly",
                    "Verify pipeline scheduling",
                    "Consider manually triggering refresh for stale dates"
                ]
            ))
            
        except Exception as e:
            logger.error(f"Error verifying refresh date integrity: {e}")
            
        return reports
    
    def _verify_data_freshness(self, local_conn, prod_conn, prod_type) -> List[IntegrityReport]:
        """Verify data freshness and recency"""
        reports = []
        
        try:
            # Check latest event timestamp
            local_cursor = local_conn.cursor()
            local_cursor.execute("SELECT MAX(event_time) FROM mixpanel_event")
            latest_event_str = local_cursor.fetchone()[0]
            
            if latest_event_str:
                latest_event = datetime.fromisoformat(latest_event_str.replace('Z', '+00:00'))
                hours_since_latest = (now_in_timezone() - latest_event).total_seconds() / 3600
                
                status = "PASS" if hours_since_latest < 48 else ("WARNING" if hours_since_latest < 72 else "FAIL")
                
                reports.append(IntegrityReport(
                    check_name="Data Freshness",
                    status=status,
                    local_count=int(hours_since_latest),
                    remote_count=48,  # Expected max hours
                    difference=max(0, int(hours_since_latest) - 48),
                    difference_percentage=0.0,
                    details={
                        "latest_event_time": latest_event_str,
                        "hours_since_latest": hours_since_latest
                    },
                    recommendations=[] if status == "PASS" else [
                        "Check data pipeline scheduling",
                        "Verify S3 data availability",
                        "Consider running manual data refresh"
                    ]
                ))
            
        except Exception as e:
            logger.error(f"Error verifying data freshness: {e}")
            
        return reports
    
    def _generate_verification_summary(self, reports: List[IntegrityReport]):
        """Generate and log verification summary"""
        total_checks = len(reports)
        passed_checks = len([r for r in reports if r.status == "PASS"])
        warning_checks = len([r for r in reports if r.status == "WARNING"])
        failed_checks = len([r for r in reports if r.status == "FAIL"])
        
        logger.info("=== DATA INTEGRITY VERIFICATION SUMMARY ===")
        logger.info(f"Total Checks: {total_checks}")
        logger.info(f"✅ Passed: {passed_checks}")
        logger.info(f"⚠️  Warnings: {warning_checks}")
        logger.info(f"❌ Failed: {failed_checks}")
        
        if failed_checks > 0:
            logger.warning("❌ CRITICAL: Some integrity checks failed!")
            logger.info("Failed checks:")
            for report in reports:
                if report.status == "FAIL":
                    logger.info(f"  - {report.check_name}: {report.details}")
        
        if warning_checks > 0:
            logger.info("⚠️  Warning checks:")
            for report in reports:
                if report.status == "WARNING":
                    logger.info(f"  - {report.check_name}: {report.details}")
        
        confidence_score = (passed_checks / total_checks) * 100
        logger.info(f"📊 Overall Confidence Score: {confidence_score:.1f}%")
        
        if confidence_score >= 90:
            logger.info("🎉 High confidence in data integrity")
        elif confidence_score >= 70:
            logger.info("⚠️  Moderate confidence - some issues to investigate")
        else:
            logger.warning("❌ Low confidence - significant data integrity issues")


def main():
    """
    Main function to run data integrity verification
    """
    logger.info("Starting Data Integrity Verification")
    
    try:
        verifier = DataIntegrityVerifier()
        reports = verifier.verify_data_consistency(days_back=7)
        
        # Save results to file
        timestamp = now_in_timezone().strftime('%Y%m%d_%H%M%S')
        report_file = f"data_integrity_report_{timestamp}.json"
        
        report_data = []
        for report in reports:
            report_data.append({
                'check_name': report.check_name,
                'status': report.status,
                'local_count': report.local_count,
                'remote_count': report.remote_count,
                'difference': report.difference,
                'difference_percentage': report.difference_percentage,
                'details': report.details,
                'recommendations': report.recommendations
            })
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"Detailed report saved to: {report_file}")
        return True
        
    except Exception as e:
        logger.error(f"Data integrity verification failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 