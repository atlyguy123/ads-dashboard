# Dashboard Configuration Manager
# 
# Manages predefined data configurations for the dashboard,
# including the basic ad data fields and any future configurations.

import json
from typing import Dict, List, Optional
from .meta_service import RequestConfig

class ConfigManager:
    """Manages dashboard data configurations"""
    
    # Basic Ad Data configuration (as shown in user's image)
    BASIC_AD_DATA_CONFIG = {
        'name': 'Basic Ad Data',
        'description': 'Core Meta fields: Ad ID, Ad Name, Adset ID, Adset Name, Campaign ID, Campaign Name, Impressions, Clicks, Spend',
        'fields': [
            'ad_id', 'ad_name', 
            'adset_id', 'adset_name', 
            'campaign_id', 'campaign_name', 
            'impressions', 'clicks', 'spend'
        ],
        'breakdowns': [],  # No breakdowns for basic config
        'filtering': None,
        'is_default': True
    }
    
    # Future configurations can be added here
    AVAILABLE_CONFIGS = {
        'basic_ad_data': BASIC_AD_DATA_CONFIG,
        # Example future config:
        # 'ad_data_with_geo': {
        #     'name': 'Ad Data with Geography',
        #     'description': 'Basic ad data broken down by country',
        #     'fields': ['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name', 'impressions', 'clicks', 'spend'],
        #     'breakdowns': ['country'],
        #     'filtering': None,
        #     'is_default': False
        # }
    }
    
    @classmethod
    def get_available_configs(cls) -> Dict[str, Dict]:
        """Get all available data configurations"""
        return cls.AVAILABLE_CONFIGS
    
    @classmethod
    def get_config(cls, config_key: str) -> Optional[Dict]:
        """Get a specific configuration by key"""
        return cls.AVAILABLE_CONFIGS.get(config_key)
    
    @classmethod
    def get_default_config(cls) -> Dict:
        """Get the default configuration"""
        for config in cls.AVAILABLE_CONFIGS.values():
            if config.get('is_default', False):
                return config
        # Fallback to basic_ad_data if no default is set
        return cls.BASIC_AD_DATA_CONFIG
    
    @classmethod
    def get_request_config(cls, config_key: str) -> RequestConfig:
        """Convert a dashboard config to a RequestConfig for the historical service"""
        config = cls.get_config(config_key)
        if not config:
            raise ValueError(f"Configuration '{config_key}' not found")
        
        return RequestConfig(
            fields=config['fields'],
            breakdowns=config['breakdowns'],
            filtering=config['filtering']
        )
    
    @classmethod
    def get_config_display_name(cls, config_key: str) -> str:
        """Get the display name for a configuration"""
        config = cls.get_config(config_key)
        return config['name'] if config else 'Unknown Configuration'
    
    @classmethod
    def validate_config(cls, config: Dict) -> bool:
        """Validate that a configuration has all required fields"""
        required_fields = ['name', 'description', 'fields', 'breakdowns']
        return all(field in config for field in required_fields)
    
    @classmethod
    def add_config(cls, config_key: str, config: Dict) -> bool:
        """Add a new configuration (for future extensibility)"""
        if not cls.validate_config(config):
            return False
        
        cls.AVAILABLE_CONFIGS[config_key] = config
        return True 