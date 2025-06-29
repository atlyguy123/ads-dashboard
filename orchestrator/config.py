#!/usr/bin/env python3
"""
Configuration module for ATLY Analytics Dashboard
Reads from environment variables with fallbacks to .env file
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root (same fix as meta_service.py)
project_root = Path(__file__).resolve().parent.parent
env_file = project_root / '.env'
load_dotenv(env_file)

# Also try to load from orchestrator directory as fallback
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

class Config:
    """Configuration class that reads from environment variables"""
    
    # Authentication
    ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'change-this-password')
    TEAM_USERNAME = os.getenv('TEAM_USERNAME', 'team')
    TEAM_PASSWORD = os.getenv('TEAM_PASSWORD', 'change-this-password')
    
    # Flask Configuration
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    
    # Server Configuration - Fixed localhost/127.0.0.1 mismatch
    HOST = os.getenv('HOST', '0.0.0.0' if os.getenv('FLASK_ENV') == 'production' else 'localhost')
    PORT = int(os.getenv('PORT', '5001'))
    
    # Meta API Configuration
    META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN', '')
    META_ACCOUNT_ID = os.getenv('META_ACCOUNT_ID', '')
    META_API_VERSION = os.getenv('META_API_VERSION', 'v22.0')
    
    # Database Configuration
    META_ANALYTICS_DB_PATH = os.getenv('META_ANALYTICS_DB_PATH', '../database/meta_analytics.db')
    MIXPANEL_DB_PATH = os.getenv('MIXPANEL_DB_PATH', '../database/mixpanel_data.db')
    MIXPANEL_ANALYTICS_DB_PATH = os.getenv('MIXPANEL_ANALYTICS_DB_PATH', '../database/mixpanel_analytics.db')
    
    # Dashboard Configuration
    DASHBOARD_ENABLED = os.getenv('DASHBOARD_ENABLED', 'true').lower() == 'true'
    
    # Orchestrator Configuration
    ORCHESTRATOR_HOST = os.getenv('ORCHESTRATOR_HOST', HOST)
    ORCHESTRATOR_PORT = int(os.getenv('ORCHESTRATOR_PORT', PORT))
    
    # Allowed Origins for CORS - Fixed localhost/127.0.0.1 mismatch
    ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', 'http://localhost:3000,http://localhost:5001').split(',')
    
    @property
    def is_production(self):
        """Check if running in production environment"""
        return self.FLASK_ENV == 'production' or os.getenv('RAILWAY_ENVIRONMENT') is not None
    
    @property
    def is_development(self):
        return self.FLASK_ENV == 'development'
    
    def get_database_path(self, db_name):
        """Get database path, handling production vs development"""
        if self.is_production:
            return f'/app/database/{db_name}'
        else:
            return os.path.join(os.path.dirname(__file__), '..', 'database', db_name)

# Create singleton instance
config = Config() 