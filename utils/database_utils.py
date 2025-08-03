"""
Database Utilities Module

Production-grade database path discovery and connection utilities.
Provides centralized database path management for the entire project.

Author: System Architecture Team
Created: 2024
"""

import os
import sqlite3
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabasePathError(Exception):
    """Custom exception for database path related errors."""
    pass


class DatabaseManager:
    """
    Centralized database path discovery and management.
    
    This class provides robust database path discovery that works across
    different directory structures and deployment scenarios.
    """
    
    # Supported database configurations
    DATABASE_CONFIGS = {
        'mixpanel_data': {
            'filename': 'mixpanel_data.db',
            'description': 'Main Mixpanel user and event data'
        },
        'meta_analytics': {
            'filename': 'meta_analytics.db', 
            'description': 'Meta (Facebook) advertising analytics data'
        },
        'raw_data': {
            'filename': 'raw_data.db',
            'description': 'Raw downloaded data from S3'
        },
        'pipeline_runs': {
            'filename': 'pipeline_runs.db',
            'description': 'Pipeline execution tracking and status'
        }
    }
    
    def __init__(self, project_root: Optional[Union[str, Path]] = None):
        """
        Initialize the database manager.
        
        Args:
            project_root: Optional explicit project root path. If None, auto-detects.
        """
        self._project_root = None
        self._database_paths: Dict[str, Path] = {}
        
        if project_root:
            self._project_root = Path(project_root)
        else:
            self._project_root = self._find_project_root()
            
        self._discover_databases()
    
    def _find_project_root(self) -> Path:
        """
        Robustly find the project root directory.
        
        Uses multiple strategies to locate the project root that contains
        the database directory structure.
        
        Returns:
            Path to the project root directory
            
        Raises:
            DatabasePathError: If project root cannot be determined
        """
        # Start from the current file's location
        current_file = Path(__file__).resolve()
        
        # Strategy 1: Look for utils directory parent (normal case)
        if current_file.parent.name == 'utils':
            potential_root = current_file.parent.parent
            if self._validate_project_root(potential_root):
                return potential_root
        
        # Strategy 2: Walk up from current file location
        current = current_file.parent
        for _ in range(5):  # Limit search depth
            if self._validate_project_root(current):
                return current
            current = current.parent
            
        # Strategy 3: Use current working directory as reference
        cwd = Path.cwd()
        for _ in range(3):  # Limit search depth  
            if self._validate_project_root(cwd):
                return cwd
            cwd = cwd.parent
            
        # Strategy 4: Check common project patterns
        common_patterns = [
            Path.cwd(),
            Path.cwd().parent,
            current_file.parent.parent,
            current_file.parent.parent.parent
        ]
        
        for pattern in common_patterns:
            if pattern.exists() and self._validate_project_root(pattern):
                return pattern
                
        raise DatabasePathError(
            "Could not locate project root directory. "
            "Ensure you're running from within the project structure "
            "and that the 'database' directory exists."
        )
    
    def _validate_project_root(self, path: Path) -> bool:
        """
        Validate that a path is the correct project root.
        
        Args:
            path: Path to validate
            
        Returns:
            True if path appears to be valid project root
        """
        if not path.exists():
            return False
            
        # In production environments (like Heroku), database directory may not exist initially
        # Check for other project indicators instead
        required_indicators = [
            "orchestrator",  # Main app directory
            "utils",         # Utils directory
            "requirements.txt"  # Requirements file
        ]
        
        indicators_found = 0
        for indicator in required_indicators:
            if (path / indicator).exists():
                indicators_found += 1
        
        # If we find at least 2 indicators, consider it valid
        # This is more flexible than requiring the database directory
        return indicators_found >= 2
    
    def _discover_databases(self) -> None:
        """
        Discover and validate all database paths.
        
        Populates the internal database paths cache.
        In production, creates database directory if it doesn't exist.
        On Railway, uses RAILWAY_VOLUME_MOUNT_PATH for persistent storage.
        """
        # Check if we're running on Railway with a volume
        railway_volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
        
        if railway_volume_path:
            # Use Railway's persistent volume path
            database_dir = Path(railway_volume_path)
            logger.info(f"Using Railway volume for database storage: {database_dir}")
        else:
            # Use local project structure
            database_dir = self._project_root / "database"
            logger.info(f"Using local database directory: {database_dir}")
        
        # Create database directory if it doesn't exist (for production deployments)
        if not database_dir.exists():
            try:
                database_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created database directory at {database_dir}")
            except Exception as e:
                if railway_volume_path:
                    # On Railway, the volume will be mounted at runtime, so directory creation failure is expected
                    logger.info(f"Railway volume directory will be mounted at runtime: {database_dir}")
                else:
                    logger.warning(f"Could not create database directory: {e}")
                    # For local environments, this is more serious
                    return
        
        # Register database paths regardless of directory existence (Railway will mount at runtime)
        for db_key, config in self.DATABASE_CONFIGS.items():
            db_path = database_dir / config['filename']
            self._database_paths[db_key] = db_path
            if db_path.exists():
                logger.debug(f"Found existing database: {db_key} at {db_path}")
            else:
                logger.info(f"Database path registered for on-demand creation: {db_key} at {db_path}")
        
        # Store the database directory for future reference
        self._database_dir = database_dir
        
        # Health logging: Confirm the resolved database directory for debugging
        logger.info(f"✅ Database manager initialized with directory: {database_dir}")
        if railway_volume_path:
            logger.info(f"🚀 Using Railway persistent volume - data will survive restarts!")
        else:
            logger.info(f"💻 Using local development directory")
        logger.info(f"📊 Registered {len(self._database_paths)} database(s): {list(self._database_paths.keys())}")
    
    def get_database_path(self, database_key: str) -> Path:
        """
        Get the path to a specific database.
        
        Args:
            database_key: Key for the database (e.g., 'mixpanel_data')
            
        Returns:
            Path object to the database file
            
        Raises:
            DatabasePathError: If database key is invalid
        """
        if database_key not in self.DATABASE_CONFIGS:
            valid_keys = ", ".join(self.DATABASE_CONFIGS.keys())
            raise DatabasePathError(
                f"Invalid database key '{database_key}'. "
                f"Valid keys: {valid_keys}"
            )
            
        # Return the path even if the database doesn't exist yet
        # It will be created on first connection attempt
        if database_key in self._database_paths:
            return self._database_paths[database_key]
        else:
            # Fallback: construct path manually
            config = self.DATABASE_CONFIGS[database_key]
            db_path = self._project_root / "database" / config['filename']
            self._database_paths[database_key] = db_path
            return db_path
    
    def get_database_path_str(self, database_key: str) -> str:
        """
        Get the path to a database as a string.
        
        Args:
            database_key: Key for the database
            
        Returns:
            String path to the database file
        """
        return str(self.get_database_path(database_key))
    
    @contextmanager
    def get_connection(self, database_key: str, **kwargs):
        """
        Get a database connection with automatic cleanup.
        Creates database file if it doesn't exist.
        Optimized for Railway volume storage with WAL mode and thread safety.
        
        Args:
            database_key: Key for the database
            **kwargs: Additional arguments for sqlite3.connect()
            
        Yields:
            sqlite3.Connection object
            
        Example:
            with db_manager.get_connection('mixpanel_data') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users")
        """
        db_path = self.get_database_path(database_key)
        
        # Ensure database directory exists (safe: volume present at runtime)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = None
        
        try:
            # Create database file if it doesn't exist
            if not db_path.exists():
                logger.info(f"Creating new database: {database_key} at {db_path}")
                # Touch the file to create it
                db_path.touch()
            
            # Optimized connection settings for Railway volumes
            conn = sqlite3.connect(
                str(db_path),
                timeout=30,                    # 30 second timeout for network filesystems
                isolation_level=None,          # Enable autocommit mode
                check_same_thread=False,       # Safe for async/threaded environments
                **kwargs
            )
            
            # Enable critical SQLite optimizations for Railway volumes
            conn.execute("PRAGMA foreign_keys = ON")        # Enforce referential integrity
            conn.execute("PRAGMA journal_mode = WAL")       # Write-Ahead Logging for crash safety
            conn.execute("PRAGMA synchronous = NORMAL")     # Good balance of safety vs performance
            conn.execute("PRAGMA cache_size = -64000")      # 64MB cache for better performance
            conn.execute("PRAGMA temp_store = MEMORY")      # Store temp tables in memory
            
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabasePathError(f"Database connection error for {database_key}: {e}")
        finally:
            if conn:
                conn.close()
    
    def get_project_root(self) -> Path:
        """
        Get the project root directory.
        
        Returns:
            Path object to the project root
        """
        return self._project_root
    
    def list_available_databases(self) -> Dict[str, Dict[str, str]]:
        """
        List all available databases with their status.
        
        Returns:
            Dictionary mapping database keys to status information
        """
        result = {}
        for db_key, config in self.DATABASE_CONFIGS.items():
            status = "available" if db_key in self._database_paths else "not_found"
            path = str(self._database_paths.get(db_key, "N/A"))
            
            result[db_key] = {
                "description": config["description"],
                "filename": config["filename"],
                "status": status,
                "path": path
            }
            
        return result


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Returns:
        DatabaseManager instance (singleton)
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_database_path(database_key: str) -> str:
    """
    Get the path to a database as a string.
    
    This is the main entry point for getting database paths.
    Automatically detects Railway volume environment.
    
    Args:
        database_key: Key for the database (e.g., 'mixpanel_data')
        
    Returns:
        String path to the database file
        
    Raises:
        DatabasePathError: If database key is invalid
    """
    global _db_manager
    
    # Check if we're in Railway environment
    railway_volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
    
    # Force reinitialization if Railway environment detected and manager not using volume
    if railway_volume_path:
        if (_db_manager is None or 
            not hasattr(_db_manager, '_database_dir') or 
            str(_db_manager._database_dir) != railway_volume_path):
            logger.info(f"Initializing database manager for Railway volume: {railway_volume_path}")
            _db_manager = DatabaseManager()
    elif _db_manager is None:
        # First time initialization for local environment
        _db_manager = DatabaseManager()
    
    return str(_db_manager.get_database_path(database_key))


def get_database_connection(database_key: str, **kwargs):
    """
    Convenience function to get a database connection context manager.
    
    Args:
        database_key: Key for the database
        **kwargs: Additional arguments for sqlite3.connect()
        
    Returns:
        Context manager that yields sqlite3.Connection
        
    Example:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
    """
    return get_database_manager().get_connection(database_key, **kwargs)


def reset_database_manager():
    """
    Reset the global database manager instance.
    
    Useful for testing or when project structure changes.
    """
    global _db_manager
    _db_manager = None


# Export commonly used functions
__all__ = [
    'DatabaseManager',
    'DatabasePathError', 
    'get_database_manager',
    'get_database_path',
    'get_database_connection',
    'reset_database_manager'
] 