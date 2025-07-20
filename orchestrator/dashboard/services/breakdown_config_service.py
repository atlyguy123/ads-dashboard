"""
Application-Layer Breakdown Mapping Service (Solution 2)

Handles Meta-to-Mixpanel mapping using JSON configuration files 
and in-memory processing during API requests.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import sqlite3
from utils.database_utils import get_database_path

# Import timezone utilities for consistent timezone handling
from ...utils.timezone_utils import now_in_timezone

logger = logging.getLogger(__name__)

class BreakdownConfigService:
    """Configuration-based breakdown mapping service"""
    
    def __init__(self, config_dir: str = "data/breakdown_mappings"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Mapping configuration files
        self.country_mapping_file = self.config_dir / "country_mappings.json"
        self.device_mapping_file = self.config_dir / "device_mappings.json"
        
        # Load or initialize mappings
        self.country_mappings = self._load_or_create_country_mappings()
        self.device_mappings = self._load_or_create_device_mappings()
    
    def _load_or_create_country_mappings(self) -> Dict[str, str]:
        """Load country mappings from JSON file or create defaults"""
        if self.country_mapping_file.exists():
            with open(self.country_mapping_file, 'r') as f:
                return json.load(f)
        
        # Default country mappings
        default_mappings = {
            "United States": "US",
            "United Kingdom": "GB",
            "Canada": "CA",
            "Australia": "AU",
            "Germany": "DE",
            "France": "FR",
            "Brazil": "BR",
            "India": "IN",
            "China": "CN",
            "Japan": "JP",
            "South Korea": "KR",
            "Mexico": "MX",
            "Italy": "IT",
            "Spain": "ES",
            "Netherlands": "NL",
            "Sweden": "SE",
            "Norway": "NO",
            "Denmark": "DK",
            "Switzerland": "CH",
            "Austria": "AT"
        }
        
        self._save_country_mappings(default_mappings)
        return default_mappings
    
    def _load_or_create_device_mappings(self) -> Dict[str, Dict[str, str]]:
        """Load device mappings from JSON file or create defaults"""
        if self.device_mapping_file.exists():
            with open(self.device_mapping_file, 'r') as f:
                return json.load(f)
        
        # Default device mappings
        default_mappings = {
            "iphone": {
                "store": "APP_STORE",
                "category": "mobile",
                "platform": "ios"
            },
            "ipad": {
                "store": "APP_STORE", 
                "category": "tablet",
                "platform": "ios"
            },
            "android_smartphone": {
                "store": "PLAY_STORE",
                "category": "mobile", 
                "platform": "android"
            },
            "android_tablet": {
                "store": "PLAY_STORE",
                "category": "tablet",
                "platform": "android"
            },
            "desktop": {
                "store": "STRIPE",
                "category": "desktop",
                "platform": "web"
            }
        }
        
        self._save_device_mappings(default_mappings)
        return default_mappings
    
    def _save_country_mappings(self, mappings: Dict[str, str]):
        """Save country mappings to JSON file"""
        with open(self.country_mapping_file, 'w') as f:
            json.dump(mappings, f, indent=2)
    
    def _save_device_mappings(self, mappings: Dict[str, Dict[str, str]]):
        """Save device mappings to JSON file"""
        with open(self.device_mapping_file, 'w') as f:
            json.dump(mappings, f, indent=2)
    
    def get_country_mapping(self, meta_country: str) -> Optional[str]:
        """Get Mixpanel country code for Meta country name"""
        return self.country_mappings.get(meta_country)
    
    def get_device_mapping(self, meta_device: str) -> Optional[Dict[str, str]]:
        """Get Mixpanel device info for Meta device type"""
        return self.device_mappings.get(meta_device)
    
    def add_country_mapping(self, meta_country: str, mixpanel_country: str):
        """Add new country mapping"""
        self.country_mappings[meta_country] = mixpanel_country
        self._save_country_mappings(self.country_mappings)
        logger.info(f"Added country mapping: {meta_country} → {mixpanel_country}")
    
    def add_device_mapping(self, meta_device: str, mixpanel_store: str, 
                          category: str = "unknown", platform: str = "unknown"):
        """Add new device mapping"""
        self.device_mappings[meta_device] = {
            "store": mixpanel_store,
            "category": category,
            "platform": platform
        }
        self._save_device_mappings(self.device_mappings)
        logger.info(f"Added device mapping: {meta_device} → {mixpanel_store}")
    
    def process_breakdown_data(self, breakdown_type: str, meta_data: List[Dict], 
                             mixpanel_db_path: str, start_date: str, end_date: str) -> List[Dict]:
        """
        Process breakdown data by applying mappings and joining with Mixpanel data
        """
        results = []
        
        for meta_record in meta_data:
            if breakdown_type == 'country':
                meta_country = meta_record.get('country')
                mixpanel_country = self.get_country_mapping(meta_country)
                
                if not mixpanel_country:
                    logger.warning(f"No mapping found for country: {meta_country}")
                    continue
                
                # Get corresponding Mixpanel data
                mixpanel_data = self._get_mixpanel_country_data(
                    mixpanel_db_path, mixpanel_country, 
                    meta_record.get('campaign_id'), start_date, end_date
                )
                
                results.append({
                    'breakdown_type': 'country',
                    'meta_value': meta_country,
                    'mixpanel_value': mixpanel_country,
                    'meta_data': meta_record,
                    'mixpanel_data': mixpanel_data,
                    'is_mapped': meta_country != mixpanel_country
                })
            
            elif breakdown_type == 'device':
                meta_device = meta_record.get('device')
                device_mapping = self.get_device_mapping(meta_device)
                
                if not device_mapping:
                    logger.warning(f"No mapping found for device: {meta_device}")
                    continue
                
                mixpanel_store = device_mapping['store']
                
                # Get corresponding Mixpanel data
                mixpanel_data = self._get_mixpanel_device_data(
                    mixpanel_db_path, mixpanel_store,
                    meta_record.get('campaign_id'), start_date, end_date
                )
                
                results.append({
                    'breakdown_type': 'device',
                    'meta_value': meta_device,
                    'mixpanel_value': mixpanel_store,
                    'device_info': device_mapping,
                    'meta_data': meta_record,
                    'mixpanel_data': mixpanel_data,
                    'is_mapped': True
                })
        
        return results
    
    def _get_mixpanel_country_data(self, db_path: str, country: str, 
                                 campaign_id: str, start_date: str, end_date: str) -> Dict:
        """Get Mixpanel data for specific country and campaign"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT u.distinct_id) as total_users,
                        SUM(CASE WHEN e.event_name = 'RC Trial started' 
                            AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as trials,
                        SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Trial converted') 
                            AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as purchases,
                        SUM(CASE WHEN e.event_name IN ('RC Initial purchase', 'RC Trial converted') 
                            AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as revenue
                    FROM mixpanel_user u
                    LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                    WHERE u.country = ? AND e.abi_campaign_id = ?
                """, (start_date, end_date, start_date, end_date, start_date, end_date, country, campaign_id))
                
                result = cursor.fetchone()
                return {
                    'total_users': result[0] or 0,
                    'trials': result[1] or 0,
                    'purchases': result[2] or 0,
                    'revenue': float(result[3] or 0)
                }
        except Exception as e:
            logger.error(f"Error getting Mixpanel country data: {e}")
            return {'total_users': 0, 'trials': 0, 'purchases': 0, 'revenue': 0}
    
    def _get_mixpanel_device_data(self, db_path: str, store: str,
                                campaign_id: str, start_date: str, end_date: str) -> Dict:
        """Get Mixpanel data for specific device/store and campaign"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT upm.distinct_id) as total_users,
                        SUM(CASE WHEN e.event_name = 'RC Trial started' 
                            AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as trials,
                        SUM(CASE WHEN e.event_name = 'RC Initial purchase' 
                            AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as purchases,
                        SUM(CASE WHEN e.event_name = 'RC Initial purchase' 
                            AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as revenue
                    FROM user_product_metrics upm
                    LEFT JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
                    WHERE upm.store = ? AND e.abi_campaign_id = ?
                """, (start_date, end_date, start_date, end_date, start_date, end_date, store, campaign_id))
                
                result = cursor.fetchone()
                return {
                    'total_users': result[0] or 0,
                    'trials': result[1] or 0,
                    'purchases': result[2] or 0,
                    'revenue': float(result[3] or 0)
                }
        except Exception as e:
            logger.error(f"Error getting Mixpanel device data: {e}")
            return {'total_users': 0, 'trials': 0, 'purchases': 0, 'revenue': 0}
    
    def discover_unmapped_values(self, db_path: str) -> Dict[str, List[str]]:
        """Discover unmapped breakdown values from Meta data"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # Find unmapped countries
                cursor.execute("SELECT DISTINCT country FROM ad_performance_daily_country")
                all_countries = [row[0] for row in cursor.fetchall()]
                unmapped_countries = [c for c in all_countries if c not in self.country_mappings]
                
                # Find unmapped devices
                cursor.execute("SELECT DISTINCT device FROM ad_performance_daily_device")
                all_devices = [row[0] for row in cursor.fetchall()]
                unmapped_devices = [d for d in all_devices if d not in self.device_mappings]
                
                return {
                    'unmapped_countries': unmapped_countries,
                    'unmapped_devices': unmapped_devices
                }
        except Exception as e:
            logger.error(f"Error discovering unmapped values: {e}")
            return {'unmapped_countries': [], 'unmapped_devices': []}
    
    def get_mapping_stats(self) -> Dict[str, Any]:
        """Get statistics about current mappings"""
        return {
            'country_mappings': len(self.country_mappings),
            'device_mappings': len(self.device_mappings),
            'last_updated': now_in_timezone().isoformat(),
            'config_files': {
                'country': str(self.country_mapping_file),
                'device': str(self.device_mapping_file)
            }
        } 