"""
Self-Contained Meta Service for Dashboard

This service provides meta advertising data functionality specifically for the dashboard,
making it independent of external meta services.

Author: System Architecture Team  
Created: 2024
"""

import sqlite3
import json
import hashlib
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import sys
from pathlib import Path

# Add the project root to the Python path for database utilities
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.database_utils import get_database_path

logger = logging.getLogger(__name__)


@dataclass
class RequestConfig:
    """Configuration for a Meta API request"""
    fields: List[str]
    breakdowns: List[str]
    filtering: Optional[Dict] = None
    
    def __post_init__(self):
        # Ensure deterministic ordering
        self.fields = sorted(self.fields)
        self.breakdowns = sorted(self.breakdowns)
    
    def to_fields_string(self) -> str:
        """Convert fields list to comma-separated string"""
        return ','.join(self.fields)
    
    def to_breakdowns_string(self) -> str:
        """Convert breakdowns list to comma-separated string"""
        return ','.join(self.breakdowns)
    
    def get_hash(self) -> str:
        """Generate deterministic hash for this configuration"""
        # Create a canonical string representation
        fields_str = ','.join(self.fields)
        breakdowns_str = ','.join(self.breakdowns)
        filtering_str = json.dumps(self.filtering, sort_keys=True) if self.filtering else ''
        
        canonical_str = f"fields:{fields_str}|breakdowns:{breakdowns_str}|filtering:{filtering_str}"
        
        # Generate SHA256 hash
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()[:16]  # Use first 16 chars for brevity


class MetaHistoricalService:
    """
    Self-contained meta historical service for dashboard functionality.
    
    This service provides meta advertising data specifically for dashboard needs,
    without dependencies on external meta services.
    """
    
    def __init__(self):
        """Initialize the meta historical service."""
        self.db_lock = threading.Lock()
        
        # Use centralized database path discovery
        try:
            self.db_path = get_database_path('meta_historical_data')
        except Exception:
            # Fallback to a local database if meta_historical_data doesn't exist
            project_root = Path(__file__).parent.parent.parent.parent
            self.db_path = str(project_root / "database" / "meta_historical_data.db")
        
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Table for storing request configurations
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS request_configs (
                        config_hash TEXT PRIMARY KEY,
                        fields_list TEXT NOT NULL,
                        breakdowns_list TEXT NOT NULL,
                        filtering_json TEXT,
                        fields_string TEXT NOT NULL,
                        breakdowns_string TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Table for storing daily data
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS daily_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        config_hash TEXT NOT NULL,
                        request_key TEXT NOT NULL UNIQUE,
                        meta_response_json TEXT NOT NULL,
                        record_count INTEGER,
                        pages_fetched INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (config_hash) REFERENCES request_configs (config_hash)
                    )
                ''')
                
                # Table for tracking collection jobs
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS collection_jobs (
                        job_id TEXT PRIMARY KEY,
                        start_date TEXT NOT NULL,
                        end_date TEXT NOT NULL,
                        config_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        total_days INTEGER,
                        completed_days INTEGER DEFAULT 0,
                        failed_days INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        FOREIGN KEY (config_hash) REFERENCES request_configs (config_hash)
                    )
                ''')
                
                # Table for storing business metrics
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS daily_business_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        config_hash TEXT NOT NULL,
                        date TEXT NOT NULL,
                        entity_key TEXT NOT NULL,
                        business_metrics TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (config_hash) REFERENCES request_configs (config_hash),
                        UNIQUE(config_hash, date, entity_key)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_data_date ON daily_data (date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_data_config ON daily_data (config_hash)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_data_key ON daily_data (request_key)')
                
                conn.commit()
                
        except Exception as e:
            logger.warning(f"Could not initialize meta database at {self.db_path}: {e}")
            # Dashboard can still function without meta data
    
    def get_configurations(self) -> List[Dict[str, Any]]:
        """
        Get all available configurations with data summary.
        
        Returns:
            List of configuration dictionaries with metadata
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get configurations with data counts
                cursor.execute('''
                    SELECT 
                        rc.config_hash,
                        rc.fields_string,
                        rc.breakdowns_string,
                        COUNT(dd.id) as day_count,
                        MIN(dd.date) as earliest_date,
                        MAX(dd.date) as latest_date
                    FROM request_configs rc
                    LEFT JOIN daily_data dd ON rc.config_hash = dd.config_hash
                    GROUP BY rc.config_hash, rc.fields_string, rc.breakdowns_string
                    ORDER BY day_count DESC
                ''')
                
                results = []
                for row in cursor.fetchall():
                    config_hash, fields_str, breakdowns_str, day_count, earliest, latest = row
                    
                    results.append({
                        'config_hash': config_hash,
                        'fields': fields_str,
                        'breakdowns': breakdowns_str,
                        'day_count': day_count or 0,
                        'earliest_date': earliest,
                        'latest_date': latest
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting configurations: {e}")
            return []
    
    def export_data_for_config(self, start_date: str, end_date: str, fields: str, 
                             breakdowns: str = None, format: str = 'json', 
                             entity_type: str = None, entity_id: str = None) -> Dict[str, Any]:
        """
        Export data for a specific configuration and date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            fields: Comma-separated list of fields
            breakdowns: Comma-separated list of breakdowns (optional)
            format: Export format (default: 'json')
            entity_type: Filter by entity type (optional)
            entity_id: Filter by entity ID (optional)
            
        Returns:
            Dictionary containing the exported data
        """
        try:
            # Create config object
            fields_list = [f.strip() for f in fields.split(',') if f.strip()]
            breakdowns_list = [b.strip() for b in (breakdowns or '').split(',') if b.strip()]
            
            config = RequestConfig(
                fields=fields_list,
                breakdowns=breakdowns_list
            )
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get daily data for the configuration and date range
                cursor.execute('''
                    SELECT date, meta_response_json, record_count
                    FROM daily_data
                    WHERE config_hash = ? AND date BETWEEN ? AND ?
                    ORDER BY date ASC
                ''', (config.get_hash(), start_date, end_date))
                
                daily_data = []
                for row in cursor.fetchall():
                    date, response_json, record_count = row
                    
                    try:
                        response_data = json.loads(response_json)
                        
                        # Get business metrics for this date if available
                        cursor.execute('''
                            SELECT entity_key, business_metrics
                            FROM daily_business_metrics
                            WHERE config_hash = ? AND date = ?
                        ''', (config.get_hash(), date))
                        
                        business_metrics = {}
                        for bm_row in cursor.fetchall():
                            entity_key, metrics_json = bm_row
                            try:
                                business_metrics[entity_key] = json.loads(metrics_json)
                            except json.JSONDecodeError:
                                pass
                        
                        daily_data.append({
                            'date': date,
                            'data': response_data,
                            'record_count': record_count,
                            'business_metrics': business_metrics
                        })
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON for date {date}: {e}")
                        continue
                
                return {
                    'success': True,
                    'config': {
                        'fields': fields_list,
                        'breakdowns': breakdowns_list,
                        'hash': config.get_hash()
                    },
                    'date_range': {
                        'start': start_date,
                        'end': end_date
                    },
                    'data': daily_data,
                    'total_days': len(daily_data)
                }
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': []
            }
    
    def get_data_coverage_summary(self, config_hash: str = None) -> Dict[str, Any]:
        """
        Get a summary of data coverage.
        
        Args:
            config_hash: Optional configuration hash to filter by
            
        Returns:
            Dictionary with coverage summary
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if config_hash:
                    cursor.execute('''
                        SELECT 
                            COUNT(DISTINCT date) as total_days,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date,
                            SUM(record_count) as total_records
                        FROM daily_data
                        WHERE config_hash = ?
                    ''', (config_hash,))
                else:
                    cursor.execute('''
                        SELECT 
                            COUNT(DISTINCT date) as total_days,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date,
                            SUM(record_count) as total_records
                        FROM daily_data
                    ''')
                
                row = cursor.fetchone()
                if row:
                    total_days, earliest, latest, total_records = row
                    return {
                        'total_days': total_days or 0,
                        'earliest_date': earliest,
                        'latest_date': latest,
                        'total_records': total_records or 0
                    }
                else:
                    return {
                        'total_days': 0,
                        'earliest_date': None,
                        'latest_date': None,
                        'total_records': 0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting data coverage: {e}")
            return {
                'total_days': 0,
                'earliest_date': None,
                'latest_date': None,
                'total_records': 0
            }


# Create global instance
meta_historical_service = MetaHistoricalService()

# Export classes and instance
__all__ = [
    'RequestConfig',
    'MetaHistoricalService',
    'meta_historical_service'
] 