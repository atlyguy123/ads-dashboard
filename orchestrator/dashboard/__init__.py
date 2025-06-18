# Dashboard Module
# 
# This module provides dashboard functionality that integrates Meta API data
# with Mixpanel data to provide comprehensive campaign performance insights.
# 
# Migrated from standalone dashboard application to work within orchestrator.

from .api.dashboard_routes import dashboard_bp
from .services.dashboard_service import DashboardService
from .services.data_transformer import DataTransformer
from .services.config_manager import ConfigManager

__all__ = [
    'dashboard_bp',
    'DashboardService', 
    'DataTransformer',
    'ConfigManager'
] 