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
            
        # Check for database directory
        database_dir = path / "database"
        if not database_dir.exists():
            return False
            
        # Check for at least one expected database file
        for config in self.DATABASE_CONFIGS.values():
            db_path = database_dir / config['filename']
            if db_path.exists():
                return True
                
        return False
    
    def _discover_databases(self) -> None:
        """
        Discover and validate all database paths.
        
        Populates the internal database paths cache.
        """
        database_dir = self._project_root / "database"
        
        if not database_dir.exists():
            raise DatabasePathError(
                f"Database directory not found at {database_dir}. "
                f"Project root: {self._project_root}"
            )
        
        for db_key, config in self.DATABASE_CONFIGS.items():
            db_path = database_dir / config['filename']
            if db_path.exists():
                self._database_paths[db_key] = db_path
                logger.debug(f"Found database: {db_key} at {db_path}")
            else:
                logger.warning(f"Database not found: {db_key} at {db_path}")
    
    def get_database_path(self, database_key: str) -> Path:
        """
        Get the path to a specific database.
        
        Args:
            database_key: Key for the database (e.g., 'mixpanel_data')
            
        Returns:
            Path object to the database file
            
        Raises:
            DatabasePathError: If database is not found or key is invalid
        """
        if database_key not in self.DATABASE_CONFIGS:
            valid_keys = ", ".join(self.DATABASE_CONFIGS.keys())
            raise DatabasePathError(
                f"Invalid database key '{database_key}'. "
                f"Valid keys: {valid_keys}"
            )
            
        if database_key not in self._database_paths:
            config = self.DATABASE_CONFIGS[database_key]
            raise DatabasePathError(
                f"Database '{database_key}' ({config['description']}) "
                f"not found at expected location: "
                f"{self._project_root}/database/{config['filename']}"
            )
            
        return self._database_paths[database_key]
    
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
        
        Args:
            database_key: Key for the database
            **kwargs: Additional arguments for sqlite3.connect()
            
        Yields:
            sqlite3.Connection object
            
        Example:
            with db_manager.get_connection('mixpanel_data') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users")
                result = cursor.fetchone()
        """
        db_path = self.get_database_path(database_key)
        conn = None
        
        try:
            conn = sqlite3.connect(str(db_path), **kwargs)
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabasePathError(f"Database connection error: {e}")
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
    Convenience function to get a database path as string.
    
    Args:
        database_key: Key for the database (e.g., 'mixpanel_data')
        
    Returns:
        String path to the database file
        
    Example:
        db_path = get_database_path('mixpanel_data')
        conn = sqlite3.connect(db_path)
    """
    return get_database_manager().get_database_path_str(database_key)


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