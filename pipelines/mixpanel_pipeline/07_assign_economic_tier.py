#!/usr/bin/env python3
"""
Module 7: Assign Economic Tier

This module assigns economic tier classifications to users based on their country.
Economic tiers reflect purchasing power and economic development levels to enable
targeted pricing and analysis strategies.

Key Features:
- Comprehensive country-to-tier mapping based on economic indicators
- Robust validation and verification of assignments
- Efficient batch processing with proper indexing
- Detailed statistics and reporting
"""

import sqlite3
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configuration - Use centralized database path discovery
DATABASE_PATH = get_database_path('mixpanel_data')

# Economic Tier Mappings
# Based on discretionary income and purchasing power of average citizens
ECONOMIC_TIERS: Dict[str, List[str]] = {
    'tier1_high_income': [
        # North America & Oceania
        'US', 'CA', 'AU', 'NZ',
        # Western Europe
        'GB', 'IE', 'DE', 'FR', 'NL', 'BE', 'AT', 'CH', 'LU', 
        'DK', 'SE', 'NO', 'FI', 'IS',
        # Other high discretionary income
        'IL', 'QA', 'AE', 'KW', 'BN', 'MO',
        # Small wealthy territories & microstates
        'MC', 'LI', 'AD', 'MT', 'IM', 'JE', 'GG', 'GI', 'BM', 'SM',
        'KY', 'VI', 'AW', 'TC', 'VG', 'BQ', 'SX', 'MF', 'PM', 'AX',
        # Overseas territories of wealthy countries
        'MQ', 'GP', 'RE', 'FO', 'GL', 'BL'
    ],
    
    'tier2_upper_middle': [
        # East Asia
        'JP', 'KR', 'TW',
        # Southern Europe
        'IT', 'ES',
        # City-states
        'SG', 'HK',
        # Middle East
        'BH', 'OM',
        # Eastern Europe
        'PT', 'SI', 'CZ', 'SK', 'EE', 'LV', 'LT', 'HR', 'HU', 'PL',
        # Latin America
        'UY', 'CR', 'PA', 'PR', 'CL',
        # Caribbean & territories
        'CY', 'CW', 'BB', 'BS', 'TT', 'SC', 'KN', 'AG',
        # Africa
        'MU'
    ],
    
    'tier3_emerging': [
        # Europe
        'GR', 'RO', 'BG', 'RS', 'ME', 'MK', 'AL', 'BA', 'XK',
        # Middle East
        'SA',
        # Latin America
        'BR', 'MX', 'AR', 'CO', 'PE', 'EC', 'DO', 'JM', 'PY', 'GT',
        'SV', 'BZ', 'GY', 'SR',
        # Asia-Pacific
        'CN', 'TH', 'MY', 'RU', 'TR', 'KZ', 'AZ', 'GE', 'AM', 'BY',
        'MN', 'FJ', 'TO', 'MV',
        # Africa
        'ZA', 'NA', 'BW',
        # Caribbean
        'LC', 'GD', 'DM'
    ],
    
    'tier4_developing': [
        # South Asia
        'IN', 'PK', 'BD', 'LK', 'NP', 'BT', 'AF',
        # Southeast Asia
        'ID', 'PH', 'VN', 'MM', 'KH', 'LA',
        # Central Asia
        'UZ', 'KG', 'TJ',
        # Latin America
        'HN', 'NI', 'VE', 'BO',
        # Europe
        'MD', 'UA',
        # Pacific
        'VU', 'PG', 'TL', 'CK',
        # Middle East & North Africa
        'EG', 'MA', 'TN', 'DZ', 'LY', 'SD', 'LB', 'JO', 'PS', 'SY', 'IQ', 'IR', 'YE',
        # Sub-Saharan Africa
        'NG', 'KE', 'GH', 'TZ', 'UG', 'ET', 'RW', 'ZM', 'ZW', 'MW', 'MZ', 'AO',
        'CM', 'CI', 'SN', 'ML', 'BF', 'NE', 'TD', 'CF', 'CD', 'CG', 'GA', 'GQ',
        'ST', 'CV', 'GM', 'GW', 'SL', 'LR', 'BJ', 'TG', 'BI', 'DJ', 'SO',
        'ER', 'SS', 'MR', 'MG', 'KM', 'SZ', 'LS',
        # Caribbean & Pacific
        'HT', 'CU', 'NC', 'PF', 'AS', 'GU', 'FM', 'MH', 'PW', 'NR', 'TV', 'KI',
        'SB', 'WS'
    ],
    
    'unknown': [
        # Empty list - this tier is for users without country data
    ]
}

TIER_DISPLAY_NAMES: Dict[str, str] = {
    'tier1_high_income': 'Tier 1 (High Income)',
    'tier2_upper_middle': 'Tier 2 (Upper Middle)',
    'tier3_emerging': 'Tier 3 (Emerging)',
    'tier4_developing': 'Tier 4 (Developing)',
    'unknown': 'Unknown (No Country Data)'
}


class EconomicTierAssigner:
    """Handles economic tier assignment based on country mapping."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Build country-to-tier mapping once at initialization
        self.country_to_tier_cache = {}
        for tier, countries in ECONOMIC_TIERS.items():
            for country in countries:
                self.country_to_tier_cache[country] = tier
    
    def get_economic_tier(self, country: str) -> str:
        """
        Get economic tier for a country code.
        
        Args:
            country: 2-letter ISO country code
            
        Returns:
            Economic tier code (defaults to 'unknown' for empty/null, 'tier4_developing' for unrecognized)
        """
        if not country or not country.strip():
            return 'unknown'
        
        normalized_country = country.strip().upper()
        return self.country_to_tier_cache.get(normalized_country, 'tier4_developing')
    
    def validate_country_mappings(self, conn: sqlite3.Connection) -> bool:
        """Validate that all countries in DB have economic tier mappings."""
        print("Validating country mappings...")
        
        cursor = conn.cursor()
        
        # Get all countries with their user counts in a single query
        cursor.execute("""
            SELECT country, COUNT(*) as user_count
            FROM mixpanel_user 
            WHERE country IS NOT NULL 
              AND TRIM(country) != ''
            GROUP BY country
            ORDER BY country
        """)
        
        country_stats = cursor.fetchall()
        mapped_countries = set(self.country_to_tier_cache.keys())
        
        print(f"Found {len(country_stats)} unique countries in database")
        print(f"Have mappings for {len(mapped_countries)} countries")
        
        unmapped_countries = [(country, count) for country, count in country_stats 
                             if country not in mapped_countries]
        
        if unmapped_countries:
            print(f"\nWARNING: Found {len(unmapped_countries)} unmapped countries:")
            
            for country, count in unmapped_countries:
                print(f"  - {country}: {count} users")
            
            print("These countries will be assigned to 'tier4_developing' by default.")
            return False
        
        print("✅ All countries in database have economic tier mappings!")
        return True
    
    def assign_economic_tiers(self, conn: sqlite3.Connection) -> Dict[str, int]:
        """Assign economic tiers to all valid users based on their country."""
        print("Assigning economic tiers based on country...")
        
        cursor = conn.cursor()
        
        # Get total count for progress tracking
        cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
        total_users = cursor.fetchone()[0]
        print(f"Processing {total_users} users for tier assignment")
        
        tier_counts = {tier: 0 for tier in ECONOMIC_TIERS.keys()}
        chunk_size = 50000  # Process 50k users at a time
        processed = 0
        
        print("Processing users in chunks to optimize memory usage...")
        
        while processed < total_users:
            # Fetch chunk of users
            cursor.execute("""
                SELECT distinct_id, country
                FROM mixpanel_user
                ORDER BY distinct_id
                LIMIT ? OFFSET ?
            """, (chunk_size, processed))
            
            chunk_data = cursor.fetchall()
            if not chunk_data:
                break
                
            # Process tier assignments for this chunk
            chunk_updates = []
            for distinct_id, country in chunk_data:
                tier = self.get_economic_tier(country)
                chunk_updates.append((tier, distinct_id))
                tier_counts[tier] += 1
            
            # Write chunk updates to database
            cursor.executemany("""
                UPDATE mixpanel_user 
                SET economic_tier = ? 
                WHERE distinct_id = ?
            """, chunk_updates)
            
            processed += len(chunk_data)
            progress = (processed / total_users * 100) if total_users > 0 else 0
            print(f"  Processed {processed:,}/{total_users:,} users ({progress:.1f}%)")
        
        conn.commit()
        
        print("Economic tier assignment completed:")
        total_users = sum(tier_counts.values())
        for tier, count in tier_counts.items():
            percentage = (count / total_users * 100) if total_users > 0 else 0
            display_name = TIER_DISPLAY_NAMES[tier]
            print(f"  - {display_name}: {count} users ({percentage:.1f}%)")
        
        return tier_counts
    
    def verify_tier_assignment(self, conn: sqlite3.Connection) -> bool:
        """Verify the correctness of tier assignments."""
        print("Verifying economic tier assignment...")
        
        cursor = conn.cursor()
        
        # Get comprehensive stats in fewer queries
        cursor.execute("""
            SELECT 
                economic_tier,
                COUNT(*) as user_count,
                COUNT(CASE WHEN economic_tier IS NULL THEN 1 END) as null_count
            FROM mixpanel_user
            GROUP BY economic_tier
            HAVING economic_tier IS NOT NULL
            ORDER BY 
                CASE economic_tier 
                    WHEN 'tier1_high_income' THEN 1
                    WHEN 'tier2_upper_middle' THEN 2
                    WHEN 'tier3_emerging' THEN 3
                    WHEN 'tier4_developing' THEN 4
                    WHEN 'unknown' THEN 5
                    ELSE 6
                END
        """)
        
        tier_stats = cursor.fetchall()
        total_users = sum(row[1] for row in tier_stats)
        
        print(f"\nEconomic Tier Distribution:")
        for tier, count, _ in tier_stats:
            percentage = (count / total_users * 100) if total_users > 0 else 0
            display_name = TIER_DISPLAY_NAMES.get(tier, 'Unknown Tier')
            print(f"  - {display_name}: {count} users ({percentage:.1f}%)")
        
        # Check for missing tiers in a single query
        cursor.execute("""
            SELECT COUNT(*) 
            FROM mixpanel_user 
            WHERE economic_tier IS NULL
        """)
        
        missing_tiers = cursor.fetchone()[0]
        if missing_tiers > 0:
            print(f"\nWARNING: {missing_tiers} users have NULL economic_tier")
            
        # Get top countries per tier and validation data in combined queries
        print(f"\nTop countries by tier and validation:")
        validation_errors = 0
        
        for tier_code in ECONOMIC_TIERS.keys():
            expected_countries = set(ECONOMIC_TIERS[tier_code])
            
            # Get both top countries and all countries for validation in one query
            cursor.execute("""
                SELECT country, COUNT(*) as user_count
                FROM mixpanel_user
                WHERE economic_tier = ?
                  AND country IS NOT NULL
                  AND TRIM(country) != ''
                GROUP BY country
                ORDER BY user_count DESC
            """, (tier_code,))
            
            country_stats = cursor.fetchall()
            display_name = TIER_DISPLAY_NAMES[tier_code]
            
            if country_stats:
                # Show top 5 countries
                print(f"  {display_name} (top 5):")
                for country, count in country_stats[:5]:
                    print(f"    - {country}: {count} users")
                
                # Validate all countries in this tier
                actual_countries = [row[0] for row in country_stats]
                unexpected_countries = [c for c in actual_countries if c not in expected_countries]
                
                if unexpected_countries:
                    validation_errors += len(unexpected_countries)
                    print(f"    WARNING: Contains unexpected countries: {unexpected_countries[:10]}{'...' if len(unexpected_countries) > 10 else ''}")
            else:
                print(f"  {display_name}: No users in this tier")
        
        if validation_errors == 0 and missing_tiers == 0:
            print("\n✅ All tier assignments are valid!")
            return True
        else:
            print(f"\n❌ Found {validation_errors} validation errors and {missing_tiers} null assignments")
            return False
    
    def ensure_database_indexes(self, conn: sqlite3.Connection) -> None:
        """Create indexes to optimize country-based queries."""
        print("Ensuring database indexes for optimal performance...")
        
        cursor = conn.cursor()
        
        # Apply SQLite optimization settings for large batch operations
        optimization_settings = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL", 
            "PRAGMA cache_size = 1000000",  # 1GB cache
            "PRAGMA temp_store = MEMORY"
        ]
        
        for setting in optimization_settings:
            try:
                cursor.execute(setting)
                print(f"  ✅ Applied: {setting}")
            except sqlite3.Error as e:
                print(f"  ⚠️ Warning applying {setting}: {e}")
        
        indexes_to_create = [
            ("idx_mixpanel_user_country", "CREATE INDEX IF NOT EXISTS idx_mixpanel_user_country ON mixpanel_user(country)"),
            ("idx_mixpanel_user_economic_tier", "CREATE INDEX IF NOT EXISTS idx_mixpanel_user_economic_tier ON mixpanel_user(economic_tier)"),
            ("idx_mixpanel_user_distinct_id", "CREATE INDEX IF NOT EXISTS idx_mixpanel_user_distinct_id ON mixpanel_user(distinct_id)")
        ]
        
        for index_name, create_sql in indexes_to_create:
            try:
                cursor.execute(create_sql)
                print(f"  ✅ Created/verified index: {index_name}")
            except sqlite3.Error as e:
                print(f"  ⚠️ Warning creating index {index_name}: {e}")
        
        conn.commit()
        print("Index optimization completed")


def main() -> int:
    """Main execution function."""
    try:
        print("=== Module 7: Assign Economic Tier (Country-Based) ===")
        print("Starting economic tier assignment based on country mapping...")
        
        assigner = EconomicTierAssigner(str(DATABASE_PATH))
        
        with sqlite3.connect(str(DATABASE_PATH)) as conn:
            # Ensure optimal database indexes
            assigner.ensure_database_indexes(conn)
            
            # Validate country mappings
            assigner.validate_country_mappings(conn)
            
            # Assign economic tiers
            assigner.assign_economic_tiers(conn)
            
            # Verify results
            is_valid = assigner.verify_tier_assignment(conn)
        
        if is_valid:
            print("Economic tier assignment completed successfully")
            return 0
        else:
            print("Economic tier assignment completed with warnings")
            return 0
        
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Module 7 failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 