"""
Breakdown Mapping Service

Handles mapping between Meta and Mixpanel breakdown dimensions (country, device)
and provides unified breakdown data aggregation.
"""

import sqlite3
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from utils.database_utils import get_database_path

logger = logging.getLogger(__name__)

@dataclass
class BreakdownData:
    """Unified breakdown data structure"""
    breakdown_type: str
    breakdown_value: str
    meta_data: Dict[str, Any]
    mixpanel_data: Dict[str, Any]
    combined_metrics: Dict[str, Any]

class BreakdownMappingService:
    """Service for handling Meta-to-Mixpanel breakdown mapping and aggregation"""
    
    def __init__(self, mixpanel_db_path: str = None, meta_db_path: str = None):
        self.mixpanel_db_path = mixpanel_db_path or get_database_path('mixpanel_data')
        self.meta_db_path = meta_db_path or get_database_path('meta_analytics')
        
        # Initialize default mappings
        self._initialize_default_mappings()
    
    def _initialize_default_mappings(self):
        """Initialize default mapping tables with known mappings"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                
                # Country mappings (common Meta → Mixpanel mappings)
                country_mappings = [
                    ('United States', 'US'),
                    ('United Kingdom', 'GB'), 
                    ('Canada', 'CA'),
                    ('Australia', 'AU'),
                    ('Germany', 'DE'),
                    ('France', 'FR'),
                    ('Brazil', 'BR'),
                    ('India', 'IN'),
                    ('China', 'CN'),
                    ('Japan', 'JP'),
                    ('South Korea', 'KR'),
                    ('Mexico', 'MX'),
                    ('Italy', 'IT'),
                    ('Spain', 'ES'),
                    ('Netherlands', 'NL'),
                    ('Sweden', 'SE'),
                    ('Norway', 'NO'),
                    ('Denmark', 'DK'),
                    ('Switzerland', 'CH'),
                    ('Austria', 'AT'),
                    ('Poland', 'PL'),
                    ('Thailand', 'TH'),
                    ('Malaysia', 'MY'),
                    ('Singapore', 'SG'),
                    ('Hong Kong', 'HK'),
                    ('Taiwan', 'TW'),
                    ('South Africa', 'ZA'),
                    ('New Zealand', 'NZ'),
                    ('Argentina', 'AR'),
                    ('Chile', 'CL'),
                    ('Colombia', 'CO'),
                    ('Peru', 'PE'),
                    ('Venezuela', 'VE'),
                    ('Ecuador', 'EC'),
                    ('Russia', 'RU'),
                    ('Turkey', 'TR'),
                    ('Israel', 'IL'),
                    ('Saudi Arabia', 'SA'),
                    ('United Arab Emirates', 'AE'),
                    ('Egypt', 'EG'),
                    ('Morocco', 'MA'),
                    ('Nigeria', 'NG'),
                    ('Kenya', 'KE'),
                    ('Ghana', 'GH'),
                    ('Philippines', 'PH'),
                    ('Indonesia', 'ID'),
                    ('Vietnam', 'VN'),
                    ('Pakistan', 'PK'),
                    ('Bangladesh', 'BD'),
                    ('Sri Lanka', 'LK'),
                ]
                
                for meta_name, mixpanel_code in country_mappings:
                    cursor.execute("""
                        INSERT OR IGNORE INTO meta_country_mapping 
                        (meta_country_name, mixpanel_country_code, created_at, updated_at)
                        VALUES (?, ?, ?, ?)
                    """, (meta_name, mixpanel_code, datetime.now(), datetime.now()))
                
                # Device mappings (Meta impression_device → Mixpanel store)
                device_mappings = [
                    ('iphone', 'APP_STORE', 'mobile', 'ios'),
                    ('ipad', 'APP_STORE', 'tablet', 'ios'),
                    ('android_smartphone', 'PLAY_STORE', 'mobile', 'android'),
                    ('android_tablet', 'PLAY_STORE', 'tablet', 'android'),
                    ('desktop', 'STRIPE', 'desktop', 'web'),
                    ('unknown', 'STRIPE', 'unknown', 'unknown'),
                ]
                
                for meta_device, mixpanel_store, category, platform in device_mappings:
                    cursor.execute("""
                        INSERT OR IGNORE INTO meta_device_mapping 
                        (meta_device_type, mixpanel_store_category, device_category, platform, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (meta_device, mixpanel_store, category, platform, datetime.now(), datetime.now()))
                
                conn.commit()
                logger.info("✅ Initialized default breakdown mappings")
                
        except Exception as e:
            logger.error(f"Failed to initialize default mappings: {e}")

    def get_country_mapping(self, meta_country: str) -> Optional[str]:
        """Get Mixpanel country code for Meta country name"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT mixpanel_country_code 
                    FROM meta_country_mapping 
                    WHERE meta_country_name = ? AND is_active = TRUE
                """, (meta_country,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error getting country mapping for '{meta_country}': {e}")
            return None

    def get_device_mapping(self, meta_device: str) -> Optional[Dict[str, str]]:
        """Get Mixpanel store category mapping for Meta device type"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT mixpanel_store_category, device_category, platform
                    FROM meta_device_mapping 
                    WHERE meta_device_type = ? AND is_active = 1
                """, (meta_device,))
                
                result = cursor.fetchone()
                if result:
                    return {
                        'store': result[0],
                        'category': result[1],
                        'platform': result[2]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting device mapping for {meta_device}: {e}")
            return None

    def discover_unmapped_values(self) -> Dict[str, List[str]]:
        """Discover unmapped countries and devices from actual Meta breakdown data"""
        try:
            with sqlite3.connect(self.meta_db_path) as meta_conn:  # 🔥 CRITICAL FIX: Use Meta database
                meta_cursor = meta_conn.cursor()
                
                # Find unmapped countries from Meta data
                meta_cursor.execute("""
                    SELECT DISTINCT country
                    FROM ad_performance_daily_country
                    WHERE country IS NOT NULL
                    AND country != ''
                """)
                meta_countries = [row[0] for row in meta_cursor.fetchall()]
                
                # Find unmapped devices from Meta data  
                meta_cursor.execute("""
                    SELECT DISTINCT device
                    FROM ad_performance_daily_device
                    WHERE device IS NOT NULL
                    AND device != ''
                """)
                meta_devices = [row[0] for row in meta_cursor.fetchall()]
            
            # Check which ones are unmapped by querying the mapping tables in Mixpanel DB
            with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                mixpanel_cursor = mixpanel_conn.cursor()
                
                # Get existing country mappings
                mixpanel_cursor.execute("""
                    SELECT meta_country_name FROM meta_country_mapping WHERE is_active = 1
                """)
                mapped_countries = {row[0] for row in mixpanel_cursor.fetchall()}
                
                # Get existing device mappings
                mixpanel_cursor.execute("""
                    SELECT meta_device_type FROM meta_device_mapping WHERE is_active = 1
                """)
                mapped_devices = {row[0] for row in mixpanel_cursor.fetchall()}
                
                # Find unmapped values
                unmapped_countries = [c for c in meta_countries if c not in mapped_countries]
                unmapped_devices = [d for d in meta_devices if d not in mapped_devices]
                
                return {
                    'countries': unmapped_countries,
                    'devices': unmapped_devices
                }
                
        except Exception as e:
            logger.error(f"Error discovering unmapped values: {e}")
            return {'countries': [], 'devices': []}

    def discover_and_update_mappings(self):
        """Discover new breakdown values from Meta data and suggest mappings"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                
                # Discover new countries
                cursor.execute("""
                    SELECT DISTINCT country 
                    FROM ad_performance_daily_country 
                    WHERE country NOT IN (
                        SELECT meta_country_name FROM meta_country_mapping
                    )
                """)
                
                unmapped_countries = [row[0] for row in cursor.fetchall()]
                
                # Discover new devices
                cursor.execute("""
                    SELECT DISTINCT device 
                    FROM ad_performance_daily_device 
                    WHERE device NOT IN (
                        SELECT meta_device_type FROM meta_device_mapping
                    )
                """)
                
                unmapped_devices = [row[0] for row in cursor.fetchall()]
                
                if unmapped_countries:
                    logger.warning(f"Found {len(unmapped_countries)} unmapped countries: {unmapped_countries}")
                
                if unmapped_devices:
                    logger.warning(f"Found {len(unmapped_devices)} unmapped devices: {unmapped_devices}")
                
                return {
                    'unmapped_countries': unmapped_countries,
                    'unmapped_devices': unmapped_devices
                }
                
        except Exception as e:
            logger.error(f"Error discovering mappings: {e}")
            return {'unmapped_countries': [], 'unmapped_devices': []}

    def get_breakdown_data(self, breakdown_type: str, start_date: str, end_date: str, 
                          group_by: str = 'campaign') -> List[BreakdownData]:
        """
        Get unified breakdown data combining Meta and Mixpanel data
        
        Args:
            breakdown_type: 'country' or 'device'
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD) 
            group_by: 'campaign', 'adset', or 'ad'
            
        Returns:
            List of BreakdownData objects
        """
        cache_key = f"{breakdown_type}_{start_date}_{end_date}_{group_by}"
        
        # Check cache first
        cached_data = self._get_cached_breakdown(cache_key)
        if cached_data:
            return cached_data
        
        # Get fresh data
        if breakdown_type == 'country':
            breakdown_data = self._get_country_breakdown_data(start_date, end_date, group_by)
        elif breakdown_type == 'device':
            breakdown_data = self._get_device_breakdown_data(start_date, end_date, group_by)
        else:
            raise ValueError(f"Unsupported breakdown type: {breakdown_type}")
        
        # Cache the results
        self._cache_breakdown_data(cache_key, breakdown_type, start_date, end_date, breakdown_data)
        
        return breakdown_data

    def _get_country_breakdown_data(self, start_date: str, end_date: str, group_by: str) -> List[BreakdownData]:
        """Get country breakdown data with Meta-Mixpanel mapping"""
        try:
            # 🔥 CRITICAL FIX: Query Meta data from the correct database
            with sqlite3.connect(self.meta_db_path) as meta_conn:
                meta_cursor = meta_conn.cursor()
                
                # Get Meta data with country info
                if group_by == 'campaign':
                    meta_query = """
                        SELECT 
                            m.country as meta_country,
                            m.campaign_id,
                            m.campaign_name,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_country m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.country, m.campaign_id, m.campaign_name
                        ORDER BY SUM(m.spend) DESC
                    """
                    
                    meta_cursor.execute(meta_query, (start_date, end_date))
                    meta_results = meta_cursor.fetchall()
            
            # Query mapping tables and Mixpanel data from the correct database
            with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                mixpanel_cursor = mixpanel_conn.cursor()
                
                breakdown_data = []
                
                for row in meta_results:
                    meta_country, campaign_id, campaign_name, spend, impressions, clicks, meta_trials, meta_purchases = row
                    
                    # Get country mapping
                    mixpanel_cursor.execute("""
                        SELECT mixpanel_country_code FROM meta_country_mapping 
                        WHERE meta_country_name = ? AND is_active = 1
                    """, (meta_country,))
                    
                    mapping_result = mixpanel_cursor.fetchone()
                    if not mapping_result:
                        logger.warning(f"No mapping found for Meta country: {meta_country}")
                        continue
                    
                    mixpanel_country = mapping_result[0]
                    
                    # Get corresponding Mixpanel data
                    mixpanel_query = """
                        SELECT 
                            COUNT(DISTINCT u.distinct_id) as total_users,
                            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                        FROM mixpanel_user u
                        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                        WHERE u.country = ? AND u.abi_campaign_id = ?
                    """
                    
                    mixpanel_cursor.execute(mixpanel_query, (
                        start_date, end_date, start_date, end_date, start_date, end_date,
                        mixpanel_country, campaign_id
                    ))
                    
                    mixpanel_result = mixpanel_cursor.fetchone()
                    total_users, mixpanel_trials, mixpanel_purchases, mixpanel_revenue = mixpanel_result or (0, 0, 0, 0)
                    
                    # Create breakdown data object
                    breakdown_data.append(BreakdownData(
                        breakdown_type='country',
                        breakdown_value=mixpanel_country,
                        meta_data={
                            'country': meta_country,
                            'campaign_id': campaign_id,
                            'campaign_name': campaign_name,
                            'spend': float(spend or 0),
                            'impressions': int(impressions or 0),
                            'clicks': int(clicks or 0),
                            'meta_trials': int(meta_trials or 0),
                            'meta_purchases': int(meta_purchases or 0)
                        },
                        mixpanel_data={
                            'country': mixpanel_country,
                            'total_users': int(total_users or 0),
                            'mixpanel_trials': int(mixpanel_trials or 0),
                            'mixpanel_purchases': int(mixpanel_purchases or 0),
                            'mixpanel_revenue': float(mixpanel_revenue or 0)
                        },
                        combined_metrics={
                            'estimated_roas': (float(mixpanel_revenue or 0) / float(spend or 1)) if spend else 0,
                            'trial_accuracy_ratio': (float(mixpanel_trials or 0) / float(meta_trials or 1)) if meta_trials else 0,
                            'purchase_accuracy_ratio': (float(mixpanel_purchases or 0) / float(meta_purchases or 1)) if meta_purchases else 0
                        }
                    ))
                
                return breakdown_data
                
        except Exception as e:
            logger.error(f"Error getting country breakdown data: {e}")
            return []

    def _get_device_breakdown_data(self, start_date: str, end_date: str, group_by: str) -> List[BreakdownData]:
        """Get device breakdown data with Meta-Mixpanel mapping"""
        try:
            # 🔥 CRITICAL FIX: Query Meta data from the correct database
            with sqlite3.connect(self.meta_db_path) as meta_conn:
                meta_cursor = meta_conn.cursor()
                
                # Get Meta data with device info
                if group_by == 'campaign':
                    meta_query = """
                        SELECT 
                            m.device as meta_device,
                            m.campaign_id,
                            m.campaign_name,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_device m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.device, m.campaign_id, m.campaign_name
                        ORDER BY SUM(m.spend) DESC
                    """
                    
                    meta_cursor.execute(meta_query, (start_date, end_date))
                    meta_results = meta_cursor.fetchall()
            
            # Query mapping tables and Mixpanel data from the correct database
            with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                mixpanel_cursor = mixpanel_conn.cursor()
                
                breakdown_data = []
                
                for row in meta_results:
                    meta_device, campaign_id, campaign_name, spend, impressions, clicks, meta_trials, meta_purchases = row
                    
                    # Get device mapping
                    mixpanel_cursor.execute("""
                        SELECT mixpanel_store_category, device_category, platform
                        FROM meta_device_mapping 
                        WHERE meta_device_type = ? AND is_active = 1
                    """, (meta_device,))
                    
                    mapping_result = mixpanel_cursor.fetchone()
                    if not mapping_result:
                        logger.warning(f"No mapping found for Meta device: {meta_device}")
                        continue
                    
                    mixpanel_store, device_category, platform = mapping_result
                    
                    # Get corresponding Mixpanel data
                    mixpanel_query = """
                        SELECT 
                            COUNT(DISTINCT upm.distinct_id) as total_users,
                            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                        FROM user_product_metrics upm
                        LEFT JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                        LEFT JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
                        WHERE upm.store = ? AND u.abi_campaign_id = ?
                    """
                    
                    mixpanel_cursor.execute(mixpanel_query, (
                        start_date, end_date, start_date, end_date, start_date, end_date,
                        mixpanel_store, campaign_id
                    ))
                    
                    mixpanel_result = mixpanel_cursor.fetchone()
                    total_users, mixpanel_trials, mixpanel_purchases, mixpanel_revenue = mixpanel_result or (0, 0, 0, 0)
                    
                    # Create breakdown data object
                    breakdown_data.append(BreakdownData(
                        breakdown_type='device',
                        breakdown_value=mixpanel_store,
                        meta_data={
                            'device': meta_device,
                            'device_category': device_category,
                            'platform': platform,
                            'campaign_id': campaign_id,
                            'campaign_name': campaign_name,
                            'spend': float(spend or 0),
                            'impressions': int(impressions or 0),
                            'clicks': int(clicks or 0),
                            'meta_trials': int(meta_trials or 0),
                            'meta_purchases': int(meta_purchases or 0)
                        },
                        mixpanel_data={
                            'store': mixpanel_store,
                            'device_category': device_category,
                            'platform': platform,
                            'total_users': int(total_users or 0),
                            'mixpanel_trials': int(mixpanel_trials or 0),
                            'mixpanel_purchases': int(mixpanel_purchases or 0),
                            'mixpanel_revenue': float(mixpanel_revenue or 0)
                        },
                        combined_metrics={
                            'estimated_roas': (float(mixpanel_revenue or 0) / float(spend or 1)) if spend else 0,
                            'trial_accuracy_ratio': (float(mixpanel_trials or 0) / float(meta_trials or 1)) if meta_trials else 0,
                            'purchase_accuracy_ratio': (float(mixpanel_purchases or 0) / float(meta_purchases or 1)) if meta_purchases else 0
                        }
                    ))
                
                return breakdown_data
                
        except Exception as e:
            logger.error(f"Error getting device breakdown data: {e}")
            return []

    def _get_cached_breakdown(self, cache_key: str) -> Optional[List[BreakdownData]]:
        """Get cached breakdown data if valid"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT meta_data, mixpanel_data, computed_at, expires_at
                    FROM breakdown_cache 
                    WHERE cache_key = ? AND expires_at > ?
                """, (cache_key, datetime.now()))
                
                result = cursor.fetchone()
                if result:
                    logger.info(f"Using cached breakdown data for {cache_key}")
                    return None  # Simplified - implement proper deserialization
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting cached breakdown: {e}")
            return None

    def _cache_breakdown_data(self, cache_key: str, breakdown_type: str, start_date: str, 
                            end_date: str, breakdown_data: List[BreakdownData]):
        """Cache breakdown data for faster subsequent requests"""
        try:
            with sqlite3.connect(self.mixpanel_db_path) as conn:
                cursor = conn.cursor()
                
                # Cache for 1 hour
                expires_at = datetime.now() + timedelta(hours=1)
                
                # Serialize the data (simplified - you'd want proper serialization)
                meta_data = json.dumps([bd.meta_data for bd in breakdown_data])
                mixpanel_data = json.dumps([bd.mixpanel_data for bd in breakdown_data])
                
                cursor.execute("""
                    INSERT OR REPLACE INTO breakdown_cache
                    (cache_key, breakdown_type, start_date, end_date, meta_data, mixpanel_data, computed_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (cache_key, breakdown_type, start_date, end_date, meta_data, mixpanel_data, datetime.now(), expires_at))
                
                conn.commit()
                logger.info(f"Cached breakdown data for {cache_key}")
                
        except Exception as e:
            logger.error(f"Error caching breakdown data: {e}") 