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
                
                # CRITICAL FIX: Country mappings using ISO codes (Meta stores codes, not names)
                country_mappings = [
                    ('US', 'US'),    # United States
                    ('GB', 'GB'),    # United Kingdom (Meta uses GB, not UK)
                    ('CA', 'CA'),    # Canada
                    ('AU', 'AU'),    # Australia
                    ('DE', 'DE'),    # Germany
                    ('FR', 'FR'),    # France
                    ('BR', 'BR'),    # Brazil
                    ('IN', 'IN'),    # India
                    ('CN', 'CN'),    # China
                    ('JP', 'JP'),    # Japan
                    ('KR', 'KR'),    # South Korea
                    ('MX', 'MX'),    # Mexico
                    ('IT', 'IT'),    # Italy
                    ('ES', 'ES'),    # Spain
                    ('NL', 'NL'),    # Netherlands
                    ('SE', 'SE'),    # Sweden
                    ('NO', 'NO'),    # Norway
                    ('DK', 'DK'),    # Denmark
                    ('CH', 'CH'),    # Switzerland
                    ('AT', 'AT'),    # Austria
                    ('PL', 'PL'),    # Poland
                    ('TH', 'TH'),    # Thailand
                    ('MY', 'MY'),    # Malaysia
                    ('SG', 'SG'),    # Singapore
                    ('HK', 'HK'),    # Hong Kong
                    ('TW', 'TW'),    # Taiwan
                    ('ZA', 'ZA'),    # South Africa
                    ('NZ', 'NZ'),    # New Zealand
                    ('AR', 'AR'),    # Argentina
                    ('CL', 'CL'),    # Chile
                    ('CO', 'CO'),    # Colombia
                    ('PE', 'PE'),    # Peru
                    ('VE', 'VE'),    # Venezuela
                    ('EC', 'EC'),    # Ecuador
                    ('RU', 'RU'),    # Russia
                    ('TR', 'TR'),    # Turkey
                    ('IL', 'IL'),    # Israel
                    ('SA', 'SA'),    # Saudi Arabia
                    ('AE', 'AE'),    # United Arab Emirates
                    ('EG', 'EG'),    # Egypt
                    ('MA', 'MA'),    # Morocco
                    ('NG', 'NG'),    # Nigeria
                    ('KE', 'KE'),    # Kenya
                    ('GH', 'GH'),    # Ghana
                    ('PH', 'PH'),    # Philippines
                    ('ID', 'ID'),    # Indonesia
                    ('VN', 'VN'),    # Vietnam
                    ('PK', 'PK'),    # Pakistan
                    ('BD', 'BD'),    # Bangladesh
                    ('LK', 'LK'),    # Sri Lanka
                    ('IE', 'IE'),    # Ireland
                    ('PT', 'PT'),    # Portugal
                    ('FI', 'FI'),    # Finland
                    ('BE', 'BE'),    # Belgium
                    ('CZ', 'CZ'),    # Czech Republic
                    ('HU', 'HU'),    # Hungary
                    ('SK', 'SK'),    # Slovakia
                    ('GR', 'GR'),    # Greece
                    ('RO', 'RO'),    # Romania
                    ('BG', 'BG'),    # Bulgaria
                    ('HR', 'HR'),    # Croatia
                    ('SI', 'SI'),    # Slovenia
                    ('EE', 'EE'),    # Estonia
                    ('LV', 'LV'),    # Latvia
                    ('LT', 'LT'),    # Lithuania
                    ('MT', 'MT'),    # Malta
                    ('CY', 'CY'),    # Cyprus
                    ('LU', 'LU'),    # Luxembourg
                    ('IS', 'IS'),    # Iceland
                    ('UA', 'UA'),    # Ukraine
                    ('BY', 'BY'),    # Belarus
                    ('MD', 'MD'),    # Moldova
                    ('BA', 'BA'),    # Bosnia and Herzegovina
                    ('RS', 'RS'),    # Serbia
                    ('ME', 'ME'),    # Montenegro
                    ('MK', 'MK'),    # North Macedonia
                    ('AL', 'AL'),    # Albania
                    ('XK', 'XK'),    # Kosovo
                    ('CR', 'CR'),    # Costa Rica
                    ('PA', 'PA'),    # Panama
                    ('GT', 'GT'),    # Guatemala
                    ('HN', 'HN'),    # Honduras
                    ('SV', 'SV'),    # El Salvador
                    ('NI', 'NI'),    # Nicaragua
                    ('BZ', 'BZ'),    # Belize
                    ('JM', 'JM'),    # Jamaica
                    ('TT', 'TT'),    # Trinidad and Tobago
                    ('BB', 'BB'),    # Barbados
                    ('PR', 'PR'),    # Puerto Rico
                    ('DO', 'DO'),    # Dominican Republic
                    ('HT', 'HT'),    # Haiti
                    ('CU', 'CU'),    # Cuba
                    ('BS', 'BS'),    # Bahamas
                    ('UY', 'UY'),    # Uruguay
                    ('PY', 'PY'),    # Paraguay
                    ('BO', 'BO'),    # Bolivia
                    ('GY', 'GY'),    # Guyana
                    ('SR', 'SR'),    # Suriname
                    ('GF', 'GF'),    # French Guiana
                ]
                
                for meta_code, mixpanel_code in country_mappings:
                    cursor.execute("""
                        INSERT OR IGNORE INTO meta_country_mapping 
                        (meta_country_name, mixpanel_country_code, created_at, updated_at)
                        VALUES (?, ?, ?, ?)
                    """, (meta_code, mixpanel_code, datetime.now(), datetime.now()))
                
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
                logger.info("✅ Initialized default breakdown mappings with ISO country codes")
                
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
                          group_by: str = 'campaign', fetch_all_levels: bool = False) -> List[BreakdownData]:
        """
        Get unified breakdown data combining Meta and Mixpanel data
        
        Args:
            breakdown_type: 'country' or 'device'
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD) 
            group_by: 'campaign', 'adset', or 'ad'
            fetch_all_levels: If True, fetch breakdown data for all hierarchy levels (campaign, adset, ad)
            
        Returns:
            List of BreakdownData objects
        """
        if fetch_all_levels:
            # Fetch breakdown data for all hierarchy levels
            return self.get_breakdown_data_all_levels(breakdown_type, start_date, end_date)
        
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

    def get_breakdown_data_all_levels(self, breakdown_type: str, start_date: str, end_date: str) -> List[BreakdownData]:
        """
        Get breakdown data for ALL hierarchy levels (campaign, adset, ad) to enable 
        multi-level breakdown enrichment in hierarchical data structures
        
        Args:
            breakdown_type: 'country' or 'device'
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of BreakdownData objects for all levels
        """
        all_breakdown_data = []
        
        # Fetch breakdown data for each hierarchy level
        for level in ['campaign', 'adset', 'ad']:
            try:
                level_data = self.get_breakdown_data(
                    breakdown_type=breakdown_type,
                    start_date=start_date,
                    end_date=end_date,
                    group_by=level,
                    fetch_all_levels=False  # Prevent infinite recursion
                )
                all_breakdown_data.extend(level_data)
            except Exception as e:
                logger.warning(f"Failed to fetch {level}-level breakdown data for {breakdown_type}: {e}")
                continue
        
        logger.info(f"✅ Fetched breakdown data for all levels: {len(all_breakdown_data)} total records")
        return all_breakdown_data

    def _get_country_breakdown_data(self, start_date: str, end_date: str, group_by: str) -> List[BreakdownData]:
        """
        Get country breakdown data grouped by entity with all breakdown values
        
        CRITICAL RESTRUCTURE: Instead of creating separate entities for each country,
        this groups all countries under each campaign/adset/ad as required.
        """
        try:
            # Query Meta data grouped by entity, then collect all breakdown values
            with sqlite3.connect(self.meta_db_path) as meta_conn:
                meta_cursor = meta_conn.cursor()
                
                # Get all Meta breakdown data for the date range
                if group_by == 'campaign':
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.campaign_name,
                            m.country as meta_country,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_country m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.campaign_name, m.country
                        ORDER BY m.campaign_id, SUM(m.spend) DESC
                    """
                elif group_by == 'adset':
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.adset_id,
                            m.adset_name,
                            m.country as meta_country,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_country m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.adset_id, m.adset_name, m.country
                        ORDER BY m.campaign_id, m.adset_id, SUM(m.spend) DESC
                    """
                else:  # ad level
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.adset_id,
                            m.ad_id,
                            m.ad_name,
                            m.country as meta_country,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_country m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.adset_id, m.ad_id, m.ad_name, m.country
                        ORDER BY m.campaign_id, m.adset_id, m.ad_id, SUM(m.spend) DESC
                    """
                
                meta_cursor.execute(meta_query, (start_date, end_date))
                meta_results = meta_cursor.fetchall()
            
            # Group Meta data by entity and process with Mixpanel data
            with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                mixpanel_cursor = mixpanel_conn.cursor()
                
                # Group breakdown data by entity ID
                entity_breakdowns = {}  # {entity_id: [breakdown_data, ...]}
                
                for row in meta_results:
                    if group_by == 'campaign':
                        campaign_id, campaign_name, meta_country = row[:3]
                        entity_id = campaign_id
                        entity_name = campaign_name
                        meta_data_base = {'campaign_id': campaign_id, 'campaign_name': campaign_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[3:]
                    elif group_by == 'adset':
                        campaign_id, adset_id, adset_name, meta_country = row[:4]
                        entity_id = adset_id
                        entity_name = adset_name
                        meta_data_base = {'campaign_id': campaign_id, 'adset_id': adset_id, 'adset_name': adset_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[4:]
                    else:  # ad level
                        campaign_id, adset_id, ad_id, ad_name, meta_country = row[:5]
                        entity_id = ad_id
                        entity_name = ad_name
                        meta_data_base = {'campaign_id': campaign_id, 'adset_id': adset_id, 'ad_id': ad_id, 'ad_name': ad_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[5:]
                    
                    # CRITICAL SIMPLIFICATION: Use Meta country code directly (no mapping needed)
                    mixpanel_country = meta_country  # Both use same ISO codes!
                    
                    # Get corresponding Mixpanel data for this specific country and entity
                    if group_by == 'campaign':
                        mixpanel_query = """
                            SELECT 
                                COUNT(DISTINCT u.distinct_id) as total_users,
                                SUM(CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                            FROM mixpanel_user u
                            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                            WHERE u.country = ? AND u.abi_campaign_id = ?
                        """
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_country, campaign_id)
                    elif group_by == 'adset':
                        mixpanel_query = """
                            SELECT 
                                COUNT(DISTINCT u.distinct_id) as total_users,
                                SUM(CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                            FROM mixpanel_user u
                            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                            WHERE u.country = ? AND u.abi_ad_set_id = ?
                        """
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_country, adset_id)
                    else:  # ad level
                        mixpanel_query = """
                            SELECT 
                                COUNT(DISTINCT u.distinct_id) as total_users,
                                SUM(CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                            FROM mixpanel_user u
                            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                            WHERE u.country = ? AND u.abi_ad_id = ?
                        """
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_country, ad_id)
                    
                    mixpanel_cursor.execute(mixpanel_query, mixpanel_params)
                    mixpanel_result = mixpanel_cursor.fetchone()
                    total_users, mixpanel_trials, mixpanel_purchases, mixpanel_revenue = mixpanel_result or (0, 0, 0, 0)
                    
                    # CRITICAL FIX: Get estimated revenue from user_product_metrics (same as parent entities)
                    if group_by == 'campaign':
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_campaign_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, campaign_id, start_date, end_date)
                    elif group_by == 'adset':
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_set_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, adset_id, start_date, end_date)
                    else:  # ad level
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, ad_id, start_date, end_date)
                    
                    mixpanel_cursor.execute(estimated_revenue_query, estimated_revenue_params)
                    estimated_revenue_result = mixpanel_cursor.fetchone()
                    estimated_revenue = float(estimated_revenue_result[0] or 0) if estimated_revenue_result else 0.0
                    
                    # CRITICAL ADD: Get AVERAGE CONVERSION AND REFUND RATES from user_product_metrics (USER REQUESTED)
                    if group_by == 'campaign':
                        average_rates_query = """
                            SELECT 
                                AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                                AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                                AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                                COUNT(DISTINCT upm.distinct_id) as users_with_rates
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_campaign_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                              AND upm.trial_conversion_rate IS NOT NULL
                              AND upm.trial_converted_to_refund_rate IS NOT NULL  
                              AND upm.initial_purchase_to_refund_rate IS NOT NULL
                        """
                        average_rates_params = (mixpanel_country, campaign_id, start_date, end_date)
                    elif group_by == 'adset':
                        average_rates_query = """
                            SELECT 
                                AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                                AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                                AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                                COUNT(DISTINCT upm.distinct_id) as users_with_rates
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_set_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                              AND upm.trial_conversion_rate IS NOT NULL
                              AND upm.trial_converted_to_refund_rate IS NOT NULL  
                              AND upm.initial_purchase_to_refund_rate IS NOT NULL
                        """
                        average_rates_params = (mixpanel_country, adset_id, start_date, end_date)
                    else:  # ad level
                        average_rates_query = """
                            SELECT 
                                AVG(upm.trial_conversion_rate) as avg_trial_conversion_rate,
                                AVG(upm.trial_converted_to_refund_rate) as avg_trial_refund_rate,
                                AVG(upm.initial_purchase_to_refund_rate) as avg_purchase_refund_rate,
                                COUNT(DISTINCT upm.distinct_id) as users_with_rates
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                              AND upm.trial_conversion_rate IS NOT NULL
                              AND upm.trial_converted_to_refund_rate IS NOT NULL  
                              AND upm.initial_purchase_to_refund_rate IS NOT NULL
                        """
                        average_rates_params = (mixpanel_country, ad_id, start_date, end_date)
                    
                    mixpanel_cursor.execute(average_rates_query, average_rates_params)
                    average_rates_result = mixpanel_cursor.fetchone()
                    avg_trial_conversion_rate = float(average_rates_result[0] or 0) if average_rates_result and average_rates_result[0] else 0.0
                    avg_trial_refund_rate = float(average_rates_result[1] or 0) if average_rates_result and average_rates_result[1] else 0.0
                    avg_purchase_refund_rate = float(average_rates_result[2] or 0) if average_rates_result and average_rates_result[2] else 0.0
                    users_with_rates = int(average_rates_result[3] or 0) if average_rates_result and average_rates_result[3] else 0
                    
                    # Create breakdown data entry for this country
                    breakdown_entry = BreakdownData(
                        breakdown_type='country',
                        breakdown_value=mixpanel_country,
                        meta_data={
                            **meta_data_base,
                            'country': meta_country,
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
                            'mixpanel_revenue': float(mixpanel_revenue or 0),
                            'estimated_revenue': estimated_revenue,  # CRITICAL ADD: Include estimated revenue
                            # CRITICAL ADD: Include average rates (USER REQUESTED)
                            'avg_trial_conversion_rate': avg_trial_conversion_rate,
                            'avg_trial_refund_rate': avg_trial_refund_rate,
                            'avg_purchase_refund_rate': avg_purchase_refund_rate,
                            'users_with_rates': users_with_rates
                        },
                        combined_metrics={
                            'estimated_roas': (estimated_revenue / float(spend or 1)) if spend else 0,  # Use estimated revenue for ROAS
                            'trial_accuracy_ratio': (float(mixpanel_trials or 0) / float(meta_trials or 1)) if meta_trials else 0,
                            'purchase_accuracy_ratio': (float(mixpanel_purchases or 0) / float(meta_purchases or 1)) if meta_purchases else 0
                        }
                    )
                    
                    # Group by entity ID - this is the KEY RESTRUCTURE
                    if entity_id not in entity_breakdowns:
                        entity_breakdowns[entity_id] = []
                    entity_breakdowns[entity_id].append(breakdown_entry)
                
                # Flatten the grouped data back to a list
                # Each BreakdownData now represents one country for one entity
                breakdown_data = []
                for entity_id, breakdowns in entity_breakdowns.items():
                    breakdown_data.extend(breakdowns)
                
                logger.info(f"✅ Retrieved {len(breakdown_data)} country breakdown records for {len(entity_breakdowns)} entities")
                return breakdown_data
                
        except Exception as e:
            logger.error(f"Error getting country breakdown data: {e}", exc_info=True)
            return []

    def _get_device_breakdown_data(self, start_date: str, end_date: str, group_by: str) -> List[BreakdownData]:
        """
        Get device breakdown data grouped by entity with all breakdown values
        
        CRITICAL RESTRUCTURE: Groups all devices under each campaign/adset/ad as required.
        """
        try:
            # Query Meta data grouped by entity, then collect all breakdown values
            with sqlite3.connect(self.meta_db_path) as meta_conn:
                meta_cursor = meta_conn.cursor()
                
                # Get all Meta breakdown data for the date range
                if group_by == 'campaign':
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.campaign_name,
                            m.device as meta_device,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_device m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.campaign_name, m.device
                        ORDER BY m.campaign_id, SUM(m.spend) DESC
                    """
                elif group_by == 'adset':
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.adset_id,
                            m.adset_name,
                            m.device as meta_device,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_device m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.adset_id, m.adset_name, m.device
                        ORDER BY m.campaign_id, m.adset_id, SUM(m.spend) DESC
                    """
                else:  # ad level
                    meta_query = """
                        SELECT 
                            m.campaign_id,
                            m.adset_id,
                            m.ad_id,
                            m.ad_name,
                            m.device as meta_device,
                            SUM(m.spend) as spend,
                            SUM(m.impressions) as impressions,
                            SUM(m.clicks) as clicks,
                            SUM(m.meta_trials) as meta_trials,
                            SUM(m.meta_purchases) as meta_purchases
                        FROM ad_performance_daily_device m
                        WHERE m.date BETWEEN ? AND ?
                        GROUP BY m.campaign_id, m.adset_id, m.ad_id, m.ad_name, m.device
                        ORDER BY m.campaign_id, m.adset_id, m.ad_id, SUM(m.spend) DESC
                    """
                
                meta_cursor.execute(meta_query, (start_date, end_date))
                meta_results = meta_cursor.fetchall()
            
            # Group Meta data by entity and process with Mixpanel data
            with sqlite3.connect(self.mixpanel_db_path) as mixpanel_conn:
                mixpanel_cursor = mixpanel_conn.cursor()
                
                # Group breakdown data by entity ID
                entity_breakdowns = {}  # {entity_id: [breakdown_data, ...]}
                
                for row in meta_results:
                    if group_by == 'campaign':
                        campaign_id, campaign_name, meta_device = row[:3]
                        entity_id = campaign_id
                        entity_name = campaign_name
                        meta_data_base = {'campaign_id': campaign_id, 'campaign_name': campaign_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[3:]
                    elif group_by == 'adset':
                        campaign_id, adset_id, adset_name, meta_device = row[:4]
                        entity_id = adset_id
                        entity_name = adset_name
                        meta_data_base = {'campaign_id': campaign_id, 'adset_id': adset_id, 'adset_name': adset_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[4:]
                    else:  # ad level
                        campaign_id, adset_id, ad_id, ad_name, meta_device = row[:5]
                        entity_id = ad_id
                        entity_name = ad_name
                        meta_data_base = {'campaign_id': campaign_id, 'adset_id': adset_id, 'ad_id': ad_id, 'ad_name': ad_name}
                        spend, impressions, clicks, meta_trials, meta_purchases = row[5:]
                    
                    # Get device mapping
                    mixpanel_cursor.execute("""
                        SELECT mixpanel_store_category, device_category, platform
                        FROM meta_device_mapping 
                        WHERE meta_device_type = ? AND is_active = 1
                    """, (meta_device,))
                    
                    mapping_result = mixpanel_cursor.fetchone()
                    if not mapping_result:
                        logger.debug(f"No mapping found for Meta device: {meta_device}")
                        continue
                    
                    mixpanel_store, device_category, platform = mapping_result
                    
                    # Get corresponding Mixpanel data for this specific device and entity
                    if group_by == 'campaign':
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
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_store, campaign_id)
                    elif group_by == 'adset':
                        mixpanel_query = """
                            SELECT 
                                COUNT(DISTINCT upm.distinct_id) as total_users,
                                SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                            FROM user_product_metrics upm
                            LEFT JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            LEFT JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
                            WHERE upm.store = ? AND u.abi_ad_set_id = ?
                        """
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_store, adset_id)
                    else:  # ad level
                        mixpanel_query = """
                            SELECT 
                                COUNT(DISTINCT upm.distinct_id) as total_users,
                                SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases,
                                SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN COALESCE(e.revenue_usd, 0) ELSE 0 END) as mixpanel_revenue
                            FROM user_product_metrics upm
                            LEFT JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            LEFT JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
                            WHERE upm.store = ? AND u.abi_ad_id = ?
                        """
                        mixpanel_params = (start_date, end_date, start_date, end_date, start_date, end_date, mixpanel_store, ad_id)
                    
                    mixpanel_cursor.execute(mixpanel_query, mixpanel_params)
                    mixpanel_result = mixpanel_cursor.fetchone()
                    total_users, mixpanel_trials, mixpanel_purchases, mixpanel_revenue = mixpanel_result or (0, 0, 0, 0)
                    
                    # CRITICAL FIX: Get estimated revenue from user_product_metrics (same as parent entities)
                    if group_by == 'campaign':
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_campaign_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, campaign_id, start_date, end_date)
                    elif group_by == 'adset':
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_set_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, adset_id, start_date, end_date)
                    else:  # ad level
                        estimated_revenue_query = """
                            SELECT 
                                SUM(upm.current_value) as estimated_revenue
                            FROM user_product_metrics upm
                            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                            WHERE u.country = ? AND u.abi_ad_id = ?
                              AND upm.credited_date BETWEEN ? AND ?
                        """
                        estimated_revenue_params = (mixpanel_country, ad_id, start_date, end_date)
                    
                    mixpanel_cursor.execute(estimated_revenue_query, estimated_revenue_params)
                    estimated_revenue_result = mixpanel_cursor.fetchone()
                    estimated_revenue = float(estimated_revenue_result[0] or 0) if estimated_revenue_result else 0.0
                    
                    # Create breakdown data entry for this device
                    breakdown_entry = BreakdownData(
                        breakdown_type='device',
                        breakdown_value=mixpanel_store,
                        meta_data={
                            **meta_data_base,
                            'device': meta_device,
                            'device_category': device_category,
                            'platform': platform,
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
                            'mixpanel_revenue': float(mixpanel_revenue or 0),
                            'estimated_revenue': estimated_revenue  # CRITICAL ADD: Include estimated revenue
                        },
                        combined_metrics={
                            'estimated_roas': (estimated_revenue / float(spend or 1)) if spend else 0,  # Use estimated revenue for ROAS
                            'trial_accuracy_ratio': (float(mixpanel_trials or 0) / float(meta_trials or 1)) if meta_trials else 0,
                            'purchase_accuracy_ratio': (float(mixpanel_purchases or 0) / float(meta_purchases or 1)) if meta_purchases else 0
                        }
                    )
                    
                    # Group by entity ID - this is the KEY RESTRUCTURE
                    if entity_id not in entity_breakdowns:
                        entity_breakdowns[entity_id] = []
                    entity_breakdowns[entity_id].append(breakdown_entry)
                
                # Flatten the grouped data back to a list
                # Each BreakdownData now represents one device for one entity
                breakdown_data = []
                for entity_id, breakdowns in entity_breakdowns.items():
                    breakdown_data.extend(breakdowns)
                
                logger.info(f"✅ Retrieved {len(breakdown_data)} device breakdown records for {len(entity_breakdowns)} entities")
                return breakdown_data
                
        except Exception as e:
            logger.error(f"Error getting device breakdown data: {e}", exc_info=True)
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