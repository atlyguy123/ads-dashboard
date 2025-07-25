import sqlite3
import json
import hashlib
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from . import meta_service
import sys
import os
# Add project root to path to import utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from utils.database_utils import get_database_path

# Import timezone utilities for consistent timezone handling
from ...utils.timezone_utils import now_in_timezone

# Set up logging
logger = logging.getLogger(__name__)

# Database file path - use the centralized database utilities
def get_db_path():
    """Get the Meta Analytics database path using the centralized database utilities"""
    return get_database_path('meta_analytics')

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

@dataclass
class DayRequest:
    """Represents a request for a single day"""
    date: str  # YYYY-MM-DD format
    config: RequestConfig
    
    def get_key(self) -> str:
        """Get unique key for this day request"""
        return f"{self.date}_{self.config.get_hash()}"

@dataclass
class CollectionProgress:
    """Tracks progress of a multi-day collection"""
    total_days: int
    completed_days: int
    failed_days: int
    current_date: Optional[str]
    status: str  # 'running', 'completed', 'failed', 'cancelled'
    start_time: datetime
    end_time: Optional[datetime] = None
    errors: List[Dict] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class MetaHistoricalService:
    """Service for managing historical Meta data collection"""
    
    def __init__(self):
        self.db_lock = threading.Lock()
        self.collection_progress = {}  # key -> CollectionProgress
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with required tables"""
        with sqlite3.connect(get_db_path()) as conn:
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
            
            # Table for tracking individual day job statuses
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS day_job_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    status TEXT NOT NULL,  -- 'pending', 'completed', 'failed', 'skipped'
                    attempt_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES collection_jobs (job_id)
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_day_job_status_job ON day_job_status (job_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_day_job_status_date ON day_job_status (date)')
            
            conn.commit()
    
    def save_request_config(self, config: RequestConfig) -> str:
        """Save a request configuration and return its hash"""
        config_hash = config.get_hash()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            
            # Insert or ignore (if already exists)
            cursor.execute('''
                INSERT OR IGNORE INTO request_configs 
                (config_hash, fields_list, breakdowns_list, filtering_json, fields_string, breakdowns_string)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                config_hash,
                json.dumps(config.fields),
                json.dumps(config.breakdowns),
                json.dumps(config.filtering) if config.filtering else None,
                config.to_fields_string(),
                config.to_breakdowns_string()
            ))
            
            conn.commit()
        
        return config_hash
    
    def has_day_data(self, date: str, config: RequestConfig) -> bool:
        """Check if we already have data for a specific day and configuration"""
        day_request = DayRequest(date, config)
        request_key = day_request.get_key()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM daily_data WHERE request_key = ?', (request_key,))
            result = cursor.fetchone()
            return result is not None
    
    def save_day_data(self, date: str, config: RequestConfig, meta_response: Dict) -> bool:
        """Save Meta API response data for a specific day"""
        try:
            day_request = DayRequest(date, config)
            request_key = day_request.get_key()
            config_hash = self.save_request_config(config)
            
            # Extract metadata from response
            record_count = len(meta_response.get('data', []))
            pages_fetched = meta_response.get('meta', {}).get('pages_fetched', 0)
            
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Insert or update
                cursor.execute('''
                    INSERT OR REPLACE INTO daily_data 
                    (date, config_hash, request_key, meta_response_json, record_count, pages_fetched, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    date,
                    config_hash,
                    request_key,
                    json.dumps(meta_response),
                    record_count,
                    pages_fetched
                ))
                
                # Apply action mappings and store processed business metrics
                self._store_processed_business_metrics(cursor, config_hash, date, meta_response)
                
                conn.commit()
            
            logger.info(f"Saved data for {date} with config {config_hash[:8]}... ({record_count} records)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save day data for {date}: {str(e)}")
            return False
    
    def get_day_data(self, date: str, config: RequestConfig) -> Optional[Dict]:
        """Retrieve stored data for a specific day and configuration"""
        day_request = DayRequest(date, config)
        request_key = day_request.get_key()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT meta_response_json, record_count, pages_fetched, created_at 
                FROM daily_data 
                WHERE request_key = ?
            ''', (request_key,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'data': json.loads(result[0]),
                    'record_count': result[1],
                    'pages_fetched': result[2],
                    'created_at': result[3]
                }
        
        return None
    
    def fetch_single_day_with_retry(self, date: str, config: RequestConfig, max_retries: int = 3) -> Tuple[bool, Optional[str]]:
        """Fetch data for a single day with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching data for {date} (attempt {attempt + 1}/{max_retries})")
                
                # Make the API request
                result, error = meta_service.fetch_meta_data(
                    start_date=date,
                    end_date=date,
                    time_increment=1,
                    fields=config.to_fields_string(),
                    breakdowns=config.to_breakdowns_string()
                )
                
                if error:
                    logger.warning(f"API error for {date} (attempt {attempt + 1}): {error}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return False, error
                
                # Save the data
                if self.save_day_data(date, config, result):
                    return True, None
                else:
                    return False, "Failed to save data to database"
                    
            except Exception as e:
                error_msg = f"Exception during fetch for {date}: {str(e)}"
                logger.error(error_msg)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return False, error_msg
        
        return False, f"Failed after {max_retries} attempts"
    
    def get_date_range_list(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end date (inclusive)"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
    
    def start_historical_collection(self, start_date: str, end_date: str, config: RequestConfig) -> str:
        """Start a historical data collection job"""
        import uuid
        job_id = str(uuid.uuid4())
        
        # Get list of all dates in range
        all_dates = self.get_date_range_list(start_date, end_date)
        
        # Check which dates we already have
        missing_dates = []
        for date in all_dates:
            if not self.has_day_data(date, config):
                missing_dates.append(date)
        
        config_hash = self.save_request_config(config)
        
        # Create collection job record
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO collection_jobs 
                (job_id, start_date, end_date, config_hash, status, total_days)
                VALUES (?, ?, ?, ?, 'running', ?)
            ''', (job_id, start_date, end_date, config_hash, len(all_dates)))
            conn.commit()
        
        # Initialize progress tracking
        progress = CollectionProgress(
            total_days=len(all_dates),
            completed_days=len(all_dates) - len(missing_dates),
            failed_days=0,
            current_date=None,
            status='running',
            start_time=now_in_timezone()
        )
        self.collection_progress[job_id] = progress
        
        # Start the collection in a separate thread
        thread = threading.Thread(
            target=self._run_collection_job,
            args=(job_id, missing_dates, config),
            daemon=True
        )
        thread.start()
        
        logger.info(f"Started collection job {job_id} for {len(missing_dates)} missing days out of {len(all_dates)} total days")
        
        return job_id
    
    def _run_collection_job(self, job_id: str, missing_dates: List[str], config: RequestConfig):
        """Run the actual collection job"""
        progress = self.collection_progress[job_id]
        
        try:
            for i, date in enumerate(missing_dates):
                # Check if job was cancelled
                if progress.status == 'cancelled':
                    break
                
                progress.current_date = date
                
                # Add rate limiting delay
                if i > 0:
                    time.sleep(1)  # 1 second delay between requests
                
                # Attempt to fetch data for this date
                success, error = self.fetch_single_day_with_retry(date, config)
                
                if success:
                    progress.completed_days += 1
                    self._update_day_job_status(job_id, date, 'completed')
                else:
                    progress.failed_days += 1
                    progress.errors.append({
                        'date': date,
                        'error': error,
                        'timestamp': now_in_timezone().isoformat()
                    })
                    self._update_day_job_status(job_id, date, 'failed', error_message=error)
                    
                    # If we have too many consecutive failures, stop
                    if len(progress.errors) >= 3:
                        last_errors = progress.errors[-3:]
                        if all(err['date'] in missing_dates[max(0, i-2):i+1] for err in last_errors):
                            logger.error(f"Too many consecutive failures in job {job_id}, stopping")
                            progress.status = 'failed'
                            break
                
                # Update job progress in database
                self._update_collection_job_progress(job_id, progress)
            
            # Finalize job status
            if progress.status == 'running':
                if progress.failed_days == 0:
                    progress.status = 'completed'
                elif progress.completed_days > 0:
                    progress.status = 'completed_with_errors'
                else:
                    progress.status = 'failed'
            
            progress.end_time = now_in_timezone()
            progress.current_date = None
            
            # Final database update
            self._update_collection_job_progress(job_id, progress)
            
            logger.info(f"Collection job {job_id} finished with status: {progress.status}")
            
        except Exception as e:
            logger.error(f"Unexpected error in collection job {job_id}: {str(e)}")
            progress.status = 'failed'
            progress.end_time = now_in_timezone()
            progress.errors.append({
                'error': f"Unexpected error: {str(e)}",
                'timestamp': now_in_timezone().isoformat()
            })
            self._update_collection_job_progress(job_id, progress)
    
    def _update_day_job_status(self, job_id: str, date: str, status: str, error_message: str = None):
        """Update status for a specific day in a job"""
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO day_job_status 
                (job_id, date, status, error_message, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (job_id, date, status, error_message))
            conn.commit()
    
    def _update_collection_job_progress(self, job_id: str, progress: CollectionProgress):
        """Update collection job progress in database"""
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE collection_jobs 
                SET status = ?, completed_days = ?, failed_days = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
            ''', (progress.status, progress.completed_days, progress.failed_days, job_id))
            conn.commit()
    
    def cancel_collection_job(self, job_id: str) -> bool:
        """Cancel a running collection job"""
        if job_id in self.collection_progress:
            progress = self.collection_progress[job_id]
            if progress.status == 'running':
                progress.status = 'cancelled'
                progress.end_time = now_in_timezone()
                self._update_collection_job_progress(job_id, progress)
                logger.info(f"Collection job {job_id} cancelled")
                return True
        return False
    
    def get_collection_job_status(self, job_id: str) -> Optional[Dict]:
        """Get status of a collection job"""
        if job_id not in self.collection_progress:
            return None
        
        progress = self.collection_progress[job_id]
        return {
            'job_id': job_id,
            'status': progress.status,
            'total_days': progress.total_days,
            'completed_days': progress.completed_days,
            'failed_days': progress.failed_days,
            'current_date': progress.current_date,
            'start_time': progress.start_time.isoformat(),
            'end_time': progress.end_time.isoformat() if progress.end_time else None,
            'errors': progress.errors,
            'progress_percentage': (progress.completed_days / progress.total_days * 100) if progress.total_days > 0 else 0
        }
    
    def get_data_coverage_summary(self, config: RequestConfig, start_date: str = None, end_date: str = None) -> Dict:
        """Get summary of data coverage for a configuration"""
        config_hash = config.get_hash()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            
            # Build query with optional date filtering
            query = '''
                SELECT 
                    COUNT(*) as total_days,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    SUM(record_count) as total_records
                FROM daily_data 
                WHERE config_hash = ?
            '''
            params = [config_hash]
            
            if start_date and end_date:
                query += ' AND date BETWEEN ? AND ?'
                params.extend([start_date, end_date])
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                return {
                    'config_hash': config_hash,
                    'total_days': result[0],
                    'earliest_date': result[1],
                    'latest_date': result[2],
                    'total_records': result[3] or 0,
                    'fields': config.to_fields_string(),
                    'breakdowns': config.to_breakdowns_string()
                }
            else:
                return {
                    'config_hash': config_hash,
                    'total_days': 0,
                    'earliest_date': None,
                    'latest_date': None,
                    'total_records': 0,
                    'fields': config.to_fields_string(),
                    'breakdowns': config.to_breakdowns_string()
                }
    
    def get_missing_dates(self, config: RequestConfig, start_date: str, end_date: str) -> List[str]:
        """Get list of dates missing for a configuration in a date range"""
        all_dates = set(self.get_date_range_list(start_date, end_date))
        
        config_hash = config.get_hash()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT date 
                FROM daily_data 
                WHERE config_hash = ? AND date BETWEEN ? AND ?
            ''', (config_hash, start_date, end_date))
            
            existing_dates = set(row[0] for row in cursor.fetchall())
        
        missing_dates = sorted(list(all_dates - existing_dates))
        return missing_dates
    
    def get_all_configurations(self) -> List[Dict]:
        """Get all stored request configurations"""
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    rc.config_hash,
                    rc.fields_string,
                    rc.breakdowns_string,
                    rc.created_at,
                    COUNT(dd.id) as day_count,
                    MIN(dd.date) as earliest_date,
                    MAX(dd.date) as latest_date
                FROM request_configs rc
                LEFT JOIN daily_data dd ON rc.config_hash = dd.config_hash
                GROUP BY rc.config_hash
                ORDER BY rc.created_at DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'config_hash': row[0],
                    'fields': row[1],
                    'breakdowns': row[2],
                    'created_at': row[3],
                    'day_count': row[4],
                    'earliest_date': row[5],
                    'latest_date': row[6]
                })
            
            return results
    
    def export_data(self, config: RequestConfig, start_date: str, end_date: str, format: str = 'json', entity_type: str = None, entity_id: str = None) -> Dict:
        """Export stored data for a configuration and date range, optionally filtered by entity"""
        config_hash = config.get_hash()
        
        with sqlite3.connect(get_db_path()) as conn:
            cursor = conn.cursor()
            
            # Build query based on whether entity filtering is requested
            if entity_type and entity_id:
                entity_key = f"{entity_type}:{entity_id}"
                cursor.execute('''
                    SELECT dd.date, dd.meta_response_json, dd.record_count, dd.created_at,
                           dbm.business_metrics
                    FROM daily_data dd
                    LEFT JOIN daily_business_metrics dbm ON dd.config_hash = dbm.config_hash 
                                                         AND dd.date = dbm.date 
                                                         AND dbm.entity_key = ?
                    WHERE dd.config_hash = ? AND dd.date BETWEEN ? AND ?
                    ORDER BY dd.date
                ''', (entity_key, config_hash, start_date, end_date))
            else:
                # No entity filtering - get all data and calculate aggregated business metrics
                cursor.execute('''
                    SELECT dd.date, dd.meta_response_json, dd.record_count, dd.created_at
                    FROM daily_data dd
                    WHERE dd.config_hash = ? AND dd.date BETWEEN ? AND ?
                    ORDER BY dd.date
                ''', (config_hash, start_date, end_date))
            
            results = []
            for row in cursor.fetchall():
                day_result = {
                    'date': row[0],
                    'data': json.loads(row[1]),
                    'record_count': row[2],
                    'stored_at': row[3]
                }
                
                if entity_type and entity_id:
                    # Add entity-specific business metrics if available
                    if row[4]:  # business_metrics column
                        day_result['business_metrics'] = json.loads(row[4])
                else:
                    # Calculate aggregated business metrics for this date
                    cursor.execute('''
                        SELECT business_metrics 
                        FROM daily_business_metrics 
                        WHERE config_hash = ? AND date = ?
                    ''', (config_hash, row[0]))
                    
                    metrics_rows = cursor.fetchall()
                    if metrics_rows:
                        # Aggregate all business metrics for this date
                        aggregated_metrics = {}
                        for metrics_row in metrics_rows:
                            entity_metrics = json.loads(metrics_row[0])
                            for concept_name, concept_data in entity_metrics.items():
                                if concept_name not in aggregated_metrics:
                                    aggregated_metrics[concept_name] = {
                                        'count': 0,
                                        'value': 0,
                                        'conversions': 0,
                                        'conversion_value': 0
                                    }
                                aggregated_metrics[concept_name]['count'] += concept_data.get('count', 0)
                                aggregated_metrics[concept_name]['value'] += concept_data.get('value', 0)
                                aggregated_metrics[concept_name]['conversions'] += concept_data.get('conversions', 0)
                                aggregated_metrics[concept_name]['conversion_value'] += concept_data.get('conversion_value', 0)
                        
                        if aggregated_metrics:
                            day_result['business_metrics'] = aggregated_metrics
                
                results.append(day_result)
            
            return {
                'config': {
                    'fields': config.to_fields_string(),
                    'breakdowns': config.to_breakdowns_string(),
                    'hash': config_hash
                },
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date
                },
                'entity_filter': {
                    'entity_type': entity_type,
                    'entity_id': entity_id
                } if entity_type and entity_id else None,
                'export_format': format,
                'exported_at': now_in_timezone().isoformat(),
                'total_days': len(results),
                'data': results
            }

    def delete_configuration_data(self, config_hash: str) -> bool:
        """Delete all data for a specific configuration"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Check if the configuration exists
                cursor.execute('SELECT config_hash FROM request_configs WHERE config_hash = ?', (config_hash,))
                if not cursor.fetchone():
                    logger.warning(f"Configuration {config_hash} not found")
                    return False
                
                # Delete daily data first (foreign key reference)
                cursor.execute('DELETE FROM daily_data WHERE config_hash = ?', (config_hash,))
                daily_deleted = cursor.rowcount
                
                # Delete any associated collection jobs
                cursor.execute('DELETE FROM collection_jobs WHERE config_hash = ?', (config_hash,))
                jobs_deleted = cursor.rowcount
                
                # Delete the configuration
                cursor.execute('DELETE FROM request_configs WHERE config_hash = ?', (config_hash,))
                config_deleted = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"Deleted configuration {config_hash}: {daily_deleted} daily records, {jobs_deleted} jobs, {config_deleted} config")
                return True
                
        except Exception as e:
            logger.error(f"Failed to delete configuration {config_hash}: {str(e)}")
            return False

    # =============================================
    # API Wrapper Methods
    # =============================================
    
    def start_collection_job(self, start_date: str, end_date: str, fields: str, breakdowns: str = None) -> str:
        """Wrapper method for API - converts string parameters to RequestConfig"""
        fields_list = [f.strip() for f in fields.split(',') if f.strip()]
        breakdowns_list = [b.strip() for b in breakdowns.split(',') if b.strip()] if breakdowns else []
        
        config = RequestConfig(
            fields=fields_list,
            breakdowns=breakdowns_list,
            filtering=None
        )
        
        return self.start_historical_collection(start_date, end_date, config)
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Wrapper method for API"""
        return self.get_collection_job_status(job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        """Wrapper method for API"""
        return self.cancel_collection_job(job_id)
    
    def list_jobs(self) -> List[Dict]:
        """Get list of all collection jobs"""
        with sqlite3.connect(get_db_path()) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    cj.*,
                    rc.fields_string,
                    rc.breakdowns_string
                FROM collection_jobs cj
                JOIN request_configs rc ON cj.config_hash = rc.config_hash
                ORDER BY cj.created_at DESC
            ''')
            jobs = [dict(row) for row in cursor.fetchall()]
            return jobs
    
    def get_configurations(self) -> List[Dict]:
        """Wrapper method for API"""
        return self.get_all_configurations()
    
    def get_data_coverage(self, fields: str, breakdowns: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Wrapper method for API - converts string parameters to RequestConfig"""
        fields_list = [f.strip() for f in fields.split(',') if f.strip()]
        breakdowns_list = [b.strip() for b in breakdowns.split(',') if b.strip()] if breakdowns else []
        
        config = RequestConfig(
            fields=fields_list,
            breakdowns=breakdowns_list,
            filtering=None
        )
        
        return self.get_data_coverage_summary(config, start_date, end_date)
    
    def get_missing_dates_for_config(self, start_date: str, end_date: str, fields: str, breakdowns: str = None) -> List[str]:
        """Wrapper method for API - converts string parameters to RequestConfig"""
        fields_list = [f.strip() for f in fields.split(',') if f.strip()]
        breakdowns_list = [b.strip() for b in breakdowns.split(',') if b.strip()] if breakdowns else []
        
        config = RequestConfig(
            fields=fields_list,
            breakdowns=breakdowns_list,
            filtering=None
        )
        
        return self.get_missing_dates(config, start_date, end_date)
    
    def export_data_for_config(self, start_date: str, end_date: str, fields: str, breakdowns: str = None, format: str = 'json', entity_type: str = None, entity_id: str = None) -> Dict:
        """Wrapper method for API - converts string parameters to RequestConfig"""
        fields_list = [f.strip() for f in fields.split(',') if f.strip()]
        breakdowns_list = [b.strip() for b in breakdowns.split(',') if b.strip()] if breakdowns else []
        
        config = RequestConfig(
            fields=fields_list,
            breakdowns=breakdowns_list,
            filtering=None
        )
        
        return self.export_data(config, start_date, end_date, format, entity_type, entity_id)

    def store_daily_data(self, config_hash: str, date: str, data: Dict) -> bool:
        """Store daily data for a configuration"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Get the configuration
                cursor.execute('SELECT config_hash FROM request_configs WHERE config_hash = ?', (config_hash,))
                if not cursor.fetchone():
                    logger.error(f"Configuration {config_hash} not found")
                    return False
                
                # Store the raw daily data
                cursor.execute('''
                    INSERT OR REPLACE INTO daily_data 
                    (config_hash, date, raw_data, record_count) 
                    VALUES (?, ?, ?, ?)
                ''', (
                    config_hash,
                    date,
                    json.dumps(data),
                    len(data.get('data', []))
                ))
                
                # Apply action mappings and store processed data
                self._store_processed_business_metrics(cursor, config_hash, date, data)
                
                conn.commit()
                logger.info(f"Stored daily data for {config_hash} on {date} ({len(data.get('data', []))} records)")
                return True
                
        except Exception as e:
            logger.error(f"Error storing daily data: {e}")
            return False

    def _store_processed_business_metrics(self, cursor, config_hash: str, date: str, data: Dict):
        """Apply action mappings and store business metrics per entity"""
        try:
            # Get action mappings for this account
            mappings = self._get_action_mappings(cursor)
            logger.info(f"Processing business metrics for {date} with mappings: {mappings}")
            if not mappings:
                logger.info("No action mappings found, skipping business metrics processing")
                return
            
            # Extract the actual records array from the data structure
            if isinstance(data.get('data'), dict) and 'data' in data['data']:
                # Structure: data.data.data (Facebook API response wrapped)
                records = data['data']['data']
            elif isinstance(data.get('data'), list):
                # Structure: data.data (direct list)
                records = data['data']
            else:
                logger.warning(f"Unexpected data structure: {type(data.get('data'))}")
                return
            
            if not records:
                logger.info("No records found for business metrics processing")
                return
            
            logger.info(f"Processing {len(records)} records for business metrics")
            
            # Process each record individually for entity-specific metrics
            entity_metrics = {}  # {entity_key: {mapping_name: metrics}}
            
            for record in records:
                # Ensure record is a dictionary
                if isinstance(record, str):
                    logger.warning(f"Record is a string, skipping: {record[:100]}...")
                    continue
                if not isinstance(record, dict):
                    logger.warning(f"Record is not a dict, type: {type(record)}, skipping")
                    continue
                
                # Extract entity IDs from record
                ad_id = record.get('ad_id')
                adset_id = record.get('adset_id') 
                campaign_id = record.get('campaign_id')
                
                # Generate entity keys for this record
                entity_keys = []
                if ad_id:
                    entity_keys.append(f"ad:{ad_id}")
                if adset_id:
                    entity_keys.append(f"adset:{adset_id}")
                if campaign_id:
                    entity_keys.append(f"campaign:{campaign_id}")
                
                # Process mappings for each entity level
                for entity_key in entity_keys:
                    if entity_key not in entity_metrics:
                        entity_metrics[entity_key] = {}
                    
                    # Apply each mapping to this record
                    for mapping_name, mapping_config in mappings.items():
                        if mapping_name not in entity_metrics[entity_key]:
                            entity_metrics[entity_key][mapping_name] = {
                                'count': 0,
                                'value': 0,
                                'conversions': 0,
                                'conversion_value': 0
                            }
                        
                        action_types = mapping_config.get('actionTypes', [])
                        
                        # Process actions (counts)
                        actions = record.get('actions', [])
                        if isinstance(actions, list):
                            for action in actions:
                                if action.get('action_type') in action_types:
                                    entity_metrics[entity_key][mapping_name]['count'] += float(action.get('value', 0))
                        
                        # Process action_values (monetary values)
                        action_values = record.get('action_values', [])
                        if isinstance(action_values, list):
                            for action in action_values:
                                if action.get('action_type') in action_types:
                                    entity_metrics[entity_key][mapping_name]['value'] += float(action.get('value', 0))
                        
                        # Process conversions
                        conversions = record.get('conversions', [])
                        if isinstance(conversions, list):
                            for conversion in conversions:
                                if conversion.get('action_type') in action_types:
                                    entity_metrics[entity_key][mapping_name]['conversions'] += float(conversion.get('value', 0))
                        
                        # Process conversion_values
                        conversion_values = record.get('conversion_values', [])
                        if isinstance(conversion_values, list):
                            for conversion in conversion_values:
                                if conversion.get('action_type') in action_types:
                                    entity_metrics[entity_key][mapping_name]['conversion_value'] += float(conversion.get('value', 0))
            
            # Store business metrics for each entity
            for entity_key, business_metrics in entity_metrics.items():
                if business_metrics:
                    logger.info(f"Storing business metrics for {entity_key} on {date}: {business_metrics}")
                    cursor.execute('''
                        INSERT OR REPLACE INTO daily_business_metrics 
                        (config_hash, date, entity_key, business_metrics) 
                        VALUES (?, ?, ?, ?)
                    ''', (
                        config_hash,
                        date,
                        entity_key,
                        json.dumps(business_metrics)
                    ))
                    logger.info(f"Successfully stored business metrics for {entity_key} on {date}")
                    
        except Exception as e:
            logger.error(f"Error processing business metrics: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

    def _get_action_mappings(self, cursor) -> Dict:
        """Get action mappings from database"""
        try:
            cursor.execute('SELECT mappings FROM action_mappings ORDER BY created_at DESC LIMIT 1')
            result = cursor.fetchone()
            if result:
                return json.loads(result[0])
            return {}
        except Exception as e:
            logger.error(f"Error getting action mappings: {e}")
            return {}

    def save_action_mappings(self, mappings: Dict) -> bool:
        """Save action mappings to database"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS action_mappings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mappings TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Insert new mappings
                cursor.execute('''
                    INSERT INTO action_mappings (mappings) 
                    VALUES (?)
                ''', (json.dumps(mappings),))
                
                conn.commit()
                logger.info("Saved action mappings to database")
                return True
                
        except Exception as e:
            logger.error(f"Error saving action mappings: {e}")
            return False

    def get_action_mappings(self) -> Dict:
        """Get the latest action mappings from database"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                return self._get_action_mappings(cursor)
        except Exception as e:
            logger.error(f"Error getting action mappings: {e}")
            return {}

    def get_tables_overview(self) -> Dict:
        """Get overview of all tables with row counts and recent activity"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                overview = {}
                
                # Get table info and row counts - discover all tables dynamically
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    # Get row count
                    cursor.execute(f'SELECT COUNT(*) FROM {table}')
                    row_count = cursor.fetchone()[0]
                    
                    # Get column info
                    cursor.execute(f'PRAGMA table_info({table})')
                    columns = [{'name': col[1], 'type': col[2]} for col in cursor.fetchall()]
                    
                    # Get most recent record if any exist
                    recent_record = None
                    if row_count > 0:
                        if table in ['request_configs', 'daily_data', 'collection_jobs', 'daily_business_metrics']:
                            if 'created_at' in [col['name'] for col in columns]:
                                cursor.execute(f'SELECT * FROM {table} ORDER BY created_at DESC LIMIT 1')
                            else:
                                cursor.execute(f'SELECT * FROM {table} LIMIT 1')
                            recent_record = cursor.fetchone()
                    
                    # Get date range for performance tables
                    date_range = None
                    available_days = 0
                    if table.startswith('ad_performance_') and row_count > 0:
                        try:
                            cursor.execute(f"SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM {table}")
                            result = cursor.fetchone()
                            if result and result[0] and result[1]:
                                date_range = {
                                    'start': result[0],
                                    'end': result[1]
                                }
                                available_days = result[2]
                        except Exception as e:
                            logger.warning(f"Could not get date range for {table}: {e}")
                    
                    overview[table] = {
                        'row_count': row_count,
                        'columns': columns,
                        'recent_record': recent_record,
                        'date_range': date_range,
                        'available_days': available_days
                    }
                
                return overview
                
        except Exception as e:
            logger.error(f"Error getting tables overview: {e}")
            return {}

    def get_table_data(self, table_name: str, limit: int = 100, offset: int = 0) -> Optional[Dict]:
        """Get data from a specific table"""
        # Security check - only allow tables that exist in the database
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    return None
        except:
            return None
            
        try:
            with sqlite3.connect(get_db_path()) as conn:
                conn.row_factory = sqlite3.Row  # Enable column access by name
                cursor = conn.cursor()
                
                # Get total count
                cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
                total_count = cursor.fetchone()[0]
                
                # Get paginated data
                cursor.execute(f'SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT ? OFFSET ?', (limit, offset))
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                data = [dict(row) for row in rows]
                
                # Get column info
                cursor.execute(f'PRAGMA table_info({table_name})')
                columns = [{'name': col[1], 'type': col[2]} for col in cursor.fetchall()]
                
                return {
                    'table_name': table_name,
                    'total_count': total_count,
                    'limit': limit,
                    'offset': offset,
                    'columns': columns,
                    'data': data
                }
                
        except Exception as e:
            logger.error(f"Error getting table data for {table_name}: {e}")
            return None

    def get_aggregated_daily_metrics(self, table_name: str, date_limit: int = None) -> Optional[Dict]:
        """Get aggregated daily metrics from a performance table"""
        # Only allow ad_performance tables
        if not table_name.startswith('ad_performance_'):
            return None
            
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # First check if table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    return None
                
                # Get ALL available dates
                cursor.execute(f"SELECT DISTINCT date FROM {table_name} ORDER BY date")
                dates = [row[0] for row in cursor.fetchall()]
                
                if not dates:
                    return {'dates': [], 'metrics': {}}
                
                # Determine breakdown dimension (if any)
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                
                breakdown_col = None
                if 'country' in columns:
                    breakdown_col = 'country'
                elif 'device' in columns:
                    breakdown_col = 'device'
                elif 'region' in columns:
                    breakdown_col = 'region'
                
                # Build aggregation query
                metrics_data = {}
                
                for date in dates:
                    if breakdown_col:
                        # For breakdown tables, aggregate across the breakdown dimension
                        query = f"""
                        SELECT 
                            COUNT(DISTINCT ad_id) as unique_ads,
                            COUNT(DISTINCT adset_id) as unique_adsets,
                            COUNT(DISTINCT campaign_id) as unique_campaigns,
                            COUNT(DISTINCT {breakdown_col}) as unique_{breakdown_col}s,
                            ROUND(SUM(spend), 2) as total_spend,
                            SUM(impressions) as total_impressions,
                            SUM(clicks) as total_clicks,
                            SUM(meta_trials) as total_meta_trials,
                            SUM(meta_purchases) as total_meta_purchases
                        FROM {table_name} 
                        WHERE date = ?
                        """
                    else:
                        # For base table, no breakdown aggregation needed
                        query = f"""
                        SELECT 
                            COUNT(DISTINCT ad_id) as unique_ads,
                            COUNT(DISTINCT adset_id) as unique_adsets,
                            COUNT(DISTINCT campaign_id) as unique_campaigns,
                            ROUND(SUM(spend), 2) as total_spend,
                            SUM(impressions) as total_impressions,
                            SUM(clicks) as total_clicks,
                            SUM(meta_trials) as total_meta_trials,
                            SUM(meta_purchases) as total_meta_purchases
                        FROM {table_name} 
                        WHERE date = ?
                        """
                    
                    cursor.execute(query, (date,))
                    result = cursor.fetchone()
                    
                    if result:
                        if breakdown_col:
                            metrics_data[date] = {
                                'unique_ads': result[0] or 0,
                                'unique_adsets': result[1] or 0,
                                'unique_campaigns': result[2] or 0,
                                f'unique_{breakdown_col}s': result[3] or 0,
                                'total_spend': result[4] or 0,
                                'total_impressions': result[5] or 0,
                                'total_clicks': result[6] or 0,
                                'total_meta_trials': result[7] or 0,
                                'total_meta_purchases': result[8] or 0
                            }
                        else:
                            metrics_data[date] = {
                                'unique_ads': result[0] or 0,
                                'unique_adsets': result[1] or 0,
                                'unique_campaigns': result[2] or 0,
                                'total_spend': result[3] or 0,
                                'total_impressions': result[4] or 0,
                                'total_clicks': result[5] or 0,
                                'total_meta_trials': result[6] or 0,
                                'total_meta_purchases': result[7] or 0
                            }
                
                # Analyze date range and detect issues
                date_analysis = self._analyze_date_range(dates, metrics_data)
                
                # Prepare metrics list for the UI
                if metrics_data:
                    sample_date = next(iter(metrics_data.values()))
                    metric_names = list(sample_date.keys())
                else:
                    metric_names = []
                
                return {
                    'table_name': table_name,
                    'breakdown_dimension': breakdown_col,
                    'dates': dates,
                    'metric_names': metric_names,
                    'metrics_data': metrics_data,
                    'date_analysis': date_analysis
                }
                
        except Exception as e:
            logger.error(f"Error getting aggregated metrics for {table_name}: {e}")
            return None

    def get_composite_validation_metrics(self) -> Optional[Dict]:
        """Get composite validation metrics across all performance tables"""
        try:
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Define all performance tables
                performance_tables = ['ad_performance_daily', 'ad_performance_daily_country', 
                                    'ad_performance_daily_device', 'ad_performance_daily_region']
                
                # Get all dates from base table (ad_performance_daily)
                cursor.execute("SELECT DISTINCT date FROM ad_performance_daily ORDER BY date")
                base_dates = [row[0] for row in cursor.fetchall()]
                
                if not base_dates:
                    return {'dates': [], 'metrics_data': {}, 'validation_errors': {}}
                
                # Collect data from all tables
                table_data = {}
                for table in performance_tables:
                    # Check if table exists and has data
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                    if not cursor.fetchone():
                        continue
                        
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    if cursor.fetchone()[0] == 0:
                        continue
                    
                    table_data[table] = {}
                    
                    # Get data for each date
                    for date in base_dates:
                        if table == 'ad_performance_daily':
                            # Base table - no aggregation needed
                            query = f"""
                            SELECT 
                                COUNT(DISTINCT ad_id) as unique_ads,
                                COUNT(DISTINCT adset_id) as unique_adsets,
                                COUNT(DISTINCT campaign_id) as unique_campaigns,
                                ROUND(SUM(spend), 2) as total_spend,
                                SUM(impressions) as total_impressions,
                                SUM(clicks) as total_clicks,
                                SUM(meta_trials) as total_meta_trials,
                                SUM(meta_purchases) as total_meta_purchases
                            FROM {table} WHERE date = ?
                            """
                        else:
                            # Breakdown tables - aggregate across breakdown dimension
                            query = f"""
                            SELECT 
                                COUNT(DISTINCT ad_id) as unique_ads,
                                COUNT(DISTINCT adset_id) as unique_adsets,
                                COUNT(DISTINCT campaign_id) as unique_campaigns,
                                ROUND(SUM(spend), 2) as total_spend,
                                SUM(impressions) as total_impressions,
                                SUM(clicks) as total_clicks,
                                SUM(meta_trials) as total_meta_trials,
                                SUM(meta_purchases) as total_meta_purchases
                            FROM {table} WHERE date = ?
                            """
                        
                        cursor.execute(query, (date,))
                        result = cursor.fetchone()
                        
                        if result and any(val is not None for val in result):
                            table_data[table][date] = {
                                'unique_ads': result[0] or 0,
                                'unique_adsets': result[1] or 0,
                                'unique_campaigns': result[2] or 0,
                                'total_spend': result[3] or 0,
                                'total_impressions': result[4] or 0,
                                'total_clicks': result[5] or 0,
                                'total_meta_trials': result[6] or 0,
                                'total_meta_purchases': result[7] or 0
                            }
                
                # Validate data consistency and build composite metrics
                composite_metrics = {}
                validation_errors = {}
                
                # Base metrics (shared across all tables)
                base_metric_names = ['unique_ads', 'unique_adsets', 'unique_campaigns', 'total_spend', 
                                   'total_impressions', 'total_clicks', 'total_meta_trials', 'total_meta_purchases']
                
                # Breakdown count metrics (unique to breakdown tables)
                breakdown_count_names = ['unique_countries', 'unique_devices', 'unique_regions']
                
                # All metrics for the composite view
                all_metric_names = base_metric_names + breakdown_count_names
                
                for date in base_dates:
                    composite_metrics[date] = {}
                    validation_errors[date] = {}
                    
                    # Get base table data for this date
                    base_data = table_data.get('ad_performance_daily', {}).get(date)
                    
                    # Process base metrics (shared across all tables)
                    for metric in base_metric_names:
                        if not base_data:
                            composite_metrics[date][metric] = 'MISSING_BASE'
                            validation_errors[date][metric] = 'Base table missing data'
                            continue
                            
                        base_value = base_data[metric]
                        composite_metrics[date][metric] = base_value
                        
                        # Check each breakdown table INDIVIDUALLY
                        missing_tables = []
                        discrepancies = []
                        
                        for breakdown in ['country', 'device', 'region']:
                            table_name = f'ad_performance_daily_{breakdown}'
                            
                            # Check if this specific table has data for this date
                            if table_name not in table_data or not table_data[table_name].get(date):
                                # This table is missing data - add to missing list
                                missing_tables.append(breakdown)
                            else:
                                # This table has data - check for discrepancy
                                table_date_data = table_data[table_name].get(date)
                                breakdown_value = table_date_data[metric]
                                
                                # Only count as discrepancy if values differ
                                if breakdown_value != base_value and base_value > 0:
                                    percentage_diff = abs(breakdown_value - base_value) / base_value * 100
                                    discrepancies.append({
                                        'table': breakdown,
                                        'value': breakdown_value,
                                        'percentage_diff': percentage_diff
                                    })
                        
                        # Set validation result based on what we found
                        # Each breakdown table is treated individually - show discrepancies for tables that have data
                        if discrepancies:
                            # Show discrepancies for tables that have data, regardless of missing tables
                            max_percentage = max(d['percentage_diff'] for d in discrepancies)
                            validation_errors[date][metric] = {
                                'type': 'mixed' if missing_tables else 'discrepancy',
                                'base_value': base_value,
                                'discrepancies': discrepancies,
                                'max_percentage': max_percentage,
                                'missing_tables': missing_tables if missing_tables else None
                            }
                        elif missing_tables:
                            # Only missing data, no discrepancies - show X only if ALL tables are missing
                            total_tables = ['country', 'device', 'region']
                            if len(missing_tables) == len(total_tables):
                                validation_errors[date][metric] = {
                                    'type': 'missing',
                                    'missing_tables': missing_tables
                                }
                            else:
                                # Some tables have data with no discrepancies - no error
                                validation_errors[date][metric] = None
                        else:
                            # No issues
                            validation_errors[date][metric] = None
                    
                    # Process breakdown count metrics
                    breakdown_mappings = {
                        'unique_countries': ('ad_performance_daily_country', 'country'),
                        'unique_devices': ('ad_performance_daily_device', 'device'), 
                        'unique_regions': ('ad_performance_daily_region', 'region')
                    }
                    
                    for count_metric, (table_name, breakdown_col) in breakdown_mappings.items():
                        if table_name not in table_data:
                            composite_metrics[date][count_metric] = 'MISSING_TABLE'
                            validation_errors[date][count_metric] = 'Missing table'
                            continue
                            
                        table_date_data = table_data[table_name].get(date)
                        if not table_date_data:
                            composite_metrics[date][count_metric] = 'MISSING_DATE'
                            validation_errors[date][count_metric] = 'Missing date'
                            continue
                        
                        # For breakdown count metrics, we need to query the actual unique count
                        try:
                            cursor.execute(f"SELECT COUNT(DISTINCT {breakdown_col}) FROM {table_name} WHERE date = ?", (date,))
                            unique_count = cursor.fetchone()[0] or 0
                            composite_metrics[date][count_metric] = unique_count
                            validation_errors[date][count_metric] = None
                        except Exception as e:
                            composite_metrics[date][count_metric] = 'ERROR'
                            validation_errors[date][count_metric] = f'Query error: {str(e)}'
                
                return {
                    'table_name': 'composite_validation',
                    'breakdown_dimension': 'validation',
                    'dates': base_dates,
                    'metric_names': all_metric_names,
                    'metrics_data': composite_metrics,
                    'validation_errors': validation_errors,
                    'tables_analyzed': list(table_data.keys())
                }
                
        except Exception as e:
            logger.error(f"Error getting composite validation metrics: {e}")
            return None

    def delete_date_data(self, table_name: str, date: str) -> Dict:
        """Delete all data for a specific date from a table"""
        try:
            # Validate table name
            valid_tables = ['ad_performance_daily', 'ad_performance_daily_country', 
                          'ad_performance_daily_region', 'ad_performance_daily_device']
            
            if table_name not in valid_tables:
                return {'success': False, 'error': f'Invalid table name: {table_name}'}
            
            # Validate date format
            from datetime import datetime
            try:
                datetime.strptime(date, '%Y-%m-%d')
            except ValueError:
                return {'success': False, 'error': f'Invalid date format: {date}. Use YYYY-MM-DD'}
            
            with sqlite3.connect(get_db_path()) as conn:
                cursor = conn.cursor()
                
                # Check if data exists for this date
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE date = ?", (date,))
                count_before = cursor.fetchone()[0]
                
                if count_before == 0:
                    return {'success': False, 'error': f'No data found for {date} in {table_name}'}
                
                # Delete the data
                cursor.execute(f"DELETE FROM {table_name} WHERE date = ?", (date,))
                rows_deleted = cursor.rowcount
                conn.commit()
                
                logger.info(f"Deleted {rows_deleted} rows for date {date} from {table_name}")
                
                return {
                    'success': True, 
                    'rows_deleted': rows_deleted,
                    'date': date,
                    'table_name': table_name
                }
                
        except Exception as e:
            logger.error(f"Error deleting date data: {e}")
            return {'success': False, 'error': str(e)}

    def _analyze_date_range(self, dates, metrics_data):
        """Analyze date range for missing dates and empty days"""
        if not dates:
            return {
                'total_days': 0,
                'date_range': None,
                'missing_dates': [],
                'empty_days': [],
                'warnings': []
            }
        
        from datetime import datetime, timedelta
        
        # Convert string dates to datetime objects for analysis
        date_objects = [datetime.strptime(date, '%Y-%m-%d') for date in dates]
        min_date = min(date_objects)
        max_date = max(date_objects)
        
        # Generate expected date range
        expected_dates = []
        current_date = min_date
        while current_date <= max_date:
            expected_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # Find missing dates
        missing_dates = [date for date in expected_dates if date not in dates]
        
        # Find empty days (days where all metrics are 0)
        empty_days = []
        for date in dates:
            if date in metrics_data:
                day_metrics = metrics_data[date]
                # Check if all numeric metrics are 0
                is_empty = all(
                    value == 0 
                    for key, value in day_metrics.items() 
                    if key.startswith(('total_', 'unique_')) and isinstance(value, (int, float))
                )
                if is_empty:
                    empty_days.append(date)
        
        # Generate warnings
        warnings = []
        if missing_dates:
            warnings.append(f"{len(missing_dates)} missing days in date range")
        if empty_days:
            warnings.append(f"{len(empty_days)} days with zero activity")
        
        return {
            'total_days': len(expected_dates),
            'available_days': len(dates),
            'date_range': {
                'start': min_date.strftime('%Y-%m-%d'),
                'end': max_date.strftime('%Y-%m-%d')
            },
            'missing_dates': missing_dates,
            'empty_days': empty_days,
            'warnings': warnings
        }

# Global service instance (TODO: Rename to meta_analytics_service when file is renamed)
meta_analytics_service = MetaHistoricalService()
# Backward compatibility alias
meta_historical_service = meta_analytics_service 