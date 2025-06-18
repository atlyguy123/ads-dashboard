"""
Debug Module Registry

Discovers and manages debug modules in the debug/modules directory.
Provides functionality to load, validate, and retrieve debug modules.
"""

import os
import glob
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class DebugModuleRegistry:
    """Registry for discovering and managing debug modules."""
    
    def __init__(self):
        self.modules: Dict[str, Dict[str, Any]] = {}
        self.modules_dir: Optional[str] = None
        self.load_modules()
    
    def load_modules(self) -> None:
        """Discover and load all debug modules from the modules directory."""
        try:
            # Get the directory where this registry file is located
            registry_dir = os.path.dirname(os.path.abspath(__file__))
            # Set modules directory path
            self.modules_dir = os.path.join(registry_dir, 'modules')
            
            # Look for module directories containing config.yaml files
            config_pattern = os.path.join(self.modules_dir, '*', 'config.yaml')
            config_files = glob.glob(config_pattern)
            
            logger.info(f"Looking for debug modules in: {config_pattern}")
            logger.info(f"Found config files: {config_files}")
            
            for config_file in config_files:
                try:
                    module_dir = os.path.dirname(config_file)
                    module_name = os.path.basename(module_dir)
                    
                    # Load module configuration
                    with open(config_file, 'r') as f:
                        module_config = yaml.safe_load(f)
                    
                    # Validate module structure
                    if self._validate_module_structure(module_dir, module_config):
                        module_config['dir'] = module_dir
                        module_config['name'] = module_name
                        self.modules[module_name] = module_config
                        logger.info(f"Loaded debug module: {module_name}")
                    else:
                        logger.warning(f"Invalid module structure for: {module_name}")
                        
                except Exception as e:
                    logger.error(f"Error loading debug module config {config_file}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading debug modules: {e}")
    
    def _validate_module_structure(self, module_dir: str, config: Dict[str, Any]) -> bool:
        """Validate that a module has the required files and structure."""
        try:
            # Check for required config fields
            required_config_fields = ['name', 'description', 'version']
            for field in required_config_fields:
                if field not in config:
                    logger.error(f"Missing required config field '{field}' in {module_dir}")
                    return False
            
            # Check for required files
            required_files = ['interface.html', 'handlers.py']
            for file_name in required_files:
                file_path = os.path.join(module_dir, file_name)
                if not os.path.exists(file_path):
                    logger.error(f"Missing required file '{file_name}' in {module_dir}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating module structure for {module_dir}: {e}")
            return False
    
    def get_available_modules(self) -> List[Dict[str, Any]]:
        """Get list of all available debug modules."""
        modules_list = []
        for module_name, module_config in self.modules.items():
            modules_list.append({
                'name': module_name,
                'display_name': module_config.get('display_name', module_name),
                'description': module_config.get('description', 'No description'),
                'version': module_config.get('version', '1.0.0'),
                'enabled': module_config.get('enabled', True)
            })
        return modules_list
    
    def get_module_config(self, module_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific module."""
        return self.modules.get(module_name)
    
    def get_module_interface_path(self, module_name: str) -> Optional[str]:
        """Get the path to a module's interface.html file."""
        module_config = self.get_module_config(module_name)
        if module_config:
            return os.path.join(module_config['dir'], 'interface.html')
        return None
    
    def get_module_handlers_path(self, module_name: str) -> Optional[str]:
        """Get the path to a module's handlers.py file."""
        module_config = self.get_module_config(module_name)
        if module_config:
            return os.path.join(module_config['dir'], 'handlers.py')
        return None
    
    def module_exists(self, module_name: str) -> bool:
        """Check if a module exists and is loaded."""
        return module_name in self.modules
    
    def refresh_modules(self) -> None:
        """Reload all modules from disk."""
        self.modules.clear()
        self.load_modules()
        logger.info("Debug modules refreshed")
    
    def get_module_count(self) -> int:
        """Get the total number of loaded modules."""
        return len(self.modules) 