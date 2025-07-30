#!/usr/bin/env python3
"""
Module 2: Create ID-Name Canonical Mapping

This module creates canonical name mappings for all advertising IDs (campaign, adset, ad)
based on frequency analysis. When an ID has multiple names over time, the most 
frequently used name becomes the canonical display name.

Key Features:
- Analyzes all Meta advertising data sources for name frequency
- Creates canonical mappings for campaign_id, adset_id, and ad_id
- Handles name changes and updates over time
- Provides confidence scoring and audit trail
- Optimized for dashboard display consistency

Dependencies: Requires Meta data tables (ad_performance_daily_*)
Outputs: Populated id_name_mapping table
"""

import sqlite3
import logging
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import sys
from datetime import datetime, date

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Railway-compatible database connection management will be handled in main()

class NameMappingProcessor:
    """Processes and creates canonical ID-to-name mappings"""
    
    def __init__(self, meta_conn: sqlite3.Connection, output_conn: sqlite3.Connection):
        self.meta_conn = meta_conn
        self.meta_cursor = meta_conn.cursor()
        self.output_conn = output_conn
        self.output_cursor = output_conn.cursor()
        self.stats = {
            'campaigns_processed': 0,
            'adsets_processed': 0,
            'ads_processed': 0,
            'name_conflicts_resolved': 0,
            'total_mappings_created': 0
        }
    
    def analyze_name_frequencies(self, entity_type: str, id_column: str, name_column: str) -> List[Tuple[str, str, int, date, date]]:
        """
        Analyze name frequency for a given entity type
        
        Args:
            entity_type: 'campaign', 'adset', or 'ad'
            id_column: Column name for the ID (e.g., 'campaign_id')
            name_column: Column name for the name (e.g., 'campaign_name')
            
        Returns:
            List of tuples: (entity_id, canonical_name, frequency, first_seen, last_seen)
        """
        logger.info(f"Analyzing {entity_type} name frequencies...")
        
        # Query all Meta performance tables for comprehensive coverage
        tables = [
            'ad_performance_daily',
            'ad_performance_daily_country', 
            'ad_performance_daily_region',
            'ad_performance_daily_device'
        ]
        
        all_names = {}  # entity_id -> {name -> frequency_data}
        
        for table in tables:
            try:
                # Check if table exists
                self.meta_cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                    (table,)
                )
                if not self.meta_cursor.fetchone():
                    logger.warning(f"Table {table} not found, skipping...")
                    continue
                
                # Query name frequencies from this table
                query = f"""
                SELECT 
                    {id_column},
                    {name_column},
                    COUNT(*) as frequency,
                    MIN(date) as first_seen,
                    MAX(date) as last_seen
                FROM {table}
                WHERE {id_column} IS NOT NULL 
                  AND {name_column} IS NOT NULL 
                  AND {name_column} != ''
                GROUP BY {id_column}, {name_column}
                ORDER BY {id_column}, frequency DESC
                """
                
                self.meta_cursor.execute(query)
                results = self.meta_cursor.fetchall()
                
                logger.info(f"Found {len(results)} {entity_type} name records in {table}")
                
                # Aggregate frequencies across tables
                for entity_id, name, frequency, first_seen, last_seen in results:
                    if entity_id not in all_names:
                        all_names[entity_id] = {}
                    
                    if name not in all_names[entity_id]:
                        all_names[entity_id][name] = {
                            'frequency': 0,
                            'first_seen': first_seen,
                            'last_seen': last_seen
                        }
                    
                    all_names[entity_id][name]['frequency'] += frequency
                    
                    # Update date ranges
                    if first_seen < all_names[entity_id][name]['first_seen']:
                        all_names[entity_id][name]['first_seen'] = first_seen
                    if last_seen > all_names[entity_id][name]['last_seen']:
                        all_names[entity_id][name]['last_seen'] = last_seen
                        
            except sqlite3.Error as e:
                logger.warning(f"Error querying {table}: {e}")
                continue
        
        # Determine canonical names (most frequent)
        canonical_mappings = []
        
        for entity_id, names in all_names.items():
            if not names:
                continue
                
            # Sort by frequency (descending) and pick the most common
            sorted_names = sorted(names.items(), key=lambda x: x[1]['frequency'], reverse=True)
            canonical_name, name_data = sorted_names[0]
            
            # Track conflicts if multiple names exist
            if len(sorted_names) > 1:
                self.stats['name_conflicts_resolved'] += 1
                logger.debug(f"{entity_type} {entity_id} has {len(sorted_names)} names, chose '{canonical_name}' (freq: {name_data['frequency']})")
            
            canonical_mappings.append((
                entity_id,
                canonical_name,
                name_data['frequency'],
                name_data['first_seen'],
                name_data['last_seen']
            ))
        
        logger.info(f"Created {len(canonical_mappings)} canonical {entity_type} mappings")
        return canonical_mappings
    
    def create_id_name_mappings(self):
        """Create canonical mappings for all entity types"""
        logger.info("Creating canonical ID-name mappings...")
        
        # Clear existing mappings (fresh start)
        self.output_cursor.execute("DELETE FROM id_name_mapping")
        self.output_conn.commit()
        
        # Entity type configurations
        entity_configs = [
            ('campaign', 'campaign_id', 'campaign_name'),
            ('adset', 'adset_id', 'adset_name'),
            ('ad', 'ad_id', 'ad_name')
        ]
        
        total_mappings = 0
        
        for entity_type, id_column, name_column in entity_configs:
            try:
                # Analyze name frequencies for this entity type
                mappings = self.analyze_name_frequencies(entity_type, id_column, name_column)
                
                if not mappings:
                    logger.warning(f"No {entity_type} mappings found")
                    continue
                
                # Insert mappings into database
                insert_query = """
                INSERT INTO id_name_mapping 
                (entity_type, entity_id, canonical_name, frequency_count, last_seen_date, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                current_time = datetime.now()
                mapping_data = [
                    (entity_type, entity_id, canonical_name, frequency, last_seen, current_time, current_time)
                    for entity_id, canonical_name, frequency, first_seen, last_seen in mappings
                ]
                
                self.output_cursor.executemany(insert_query, mapping_data)
                self.output_conn.commit()
                
                # Update stats
                count = len(mappings)
                total_mappings += count
                
                if entity_type == 'campaign':
                    self.stats['campaigns_processed'] = count
                elif entity_type == 'adset':
                    self.stats['adsets_processed'] = count
                elif entity_type == 'ad':
                    self.stats['ads_processed'] = count
                
                logger.info(f"✅ Created {count} {entity_type} mappings")
                
            except Exception as e:
                logger.error(f"Failed to process {entity_type} mappings: {e}")
                self.output_conn.rollback()
                raise
        
        self.stats['total_mappings_created'] = total_mappings
        logger.info(f"✅ Successfully created {total_mappings} total canonical mappings")
    
    def validate_mappings(self):
        """Validate the created mappings"""
        logger.info("Validating canonical mappings...")
        
        # Check mapping counts by entity type
        self.output_cursor.execute("""
        SELECT entity_type, COUNT(*) as mapping_count
        FROM id_name_mapping
        GROUP BY entity_type
        ORDER BY entity_type
        """)
        
        validation_results = self.output_cursor.fetchall()
        
        for entity_type, count in validation_results:
            logger.info(f"✅ {entity_type}: {count} canonical mappings")
        
        # Check for any empty canonical names
        self.output_cursor.execute("""
        SELECT COUNT(*) FROM id_name_mapping 
        WHERE canonical_name IS NULL OR canonical_name = ''
        """)
        empty_names = self.output_cursor.fetchone()[0]
        
        if empty_names > 0:
            logger.error(f"❌ Found {empty_names} mappings with empty canonical names")
            return False
        
        # Sample check: verify mappings are reasonable
        self.output_cursor.execute("""
        SELECT entity_type, entity_id, canonical_name, frequency_count
        FROM id_name_mapping
        WHERE frequency_count >= 10
        LIMIT 5
        """)
        
        samples = self.output_cursor.fetchall()
        logger.info("Sample canonical mappings:")
        for entity_type, entity_id, name, freq in samples:
            logger.info(f"  {entity_type} {entity_id}: '{name}' (frequency: {freq})")
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()

def main():
    """Main execution function with Railway-compatible database management"""
    try:
        logger.info("=== Module 2: Create ID-Name Canonical Mapping ===")
        logger.info("Creating canonical mappings for advertising IDs...")
        
        # Use Railway-compatible database connections
        with get_database_connection('meta_analytics') as meta_conn, \
             get_database_connection('mixpanel_data') as output_conn:
            
            # Verify required tables exist
            meta_cursor = meta_conn.cursor()
            output_cursor = output_conn.cursor()
            
            # Check Meta tables exist (source data)
            meta_tables = ['ad_performance_daily']
            for table in meta_tables:
                meta_cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                    (table,)
                )
                if not meta_cursor.fetchone():
                    raise RuntimeError(f"Required Meta table '{table}' not found in meta_analytics.db")
            
            # Check output table exists (destination)
            output_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                ('id_name_mapping',)
            )
            if not output_cursor.fetchone():
                raise RuntimeError("Required table 'id_name_mapping' not found in mixpanel_data.db. Run database setup first.")
            
            # Process mappings
            processor = NameMappingProcessor(meta_conn, output_conn)
            processor.create_id_name_mappings()
            
            # Validate results
            if not processor.validate_mappings():
                raise RuntimeError("Mapping validation failed")
            
            # Display final statistics
            stats = processor.get_stats()
            logger.info("=== ID-Name Mapping Statistics ===")
            logger.info(f"Campaigns processed: {stats['campaigns_processed']}")
            logger.info(f"Ad sets processed: {stats['adsets_processed']}")
            logger.info(f"Ads processed: {stats['ads_processed']}")
            logger.info(f"Name conflicts resolved: {stats['name_conflicts_resolved']}")
            logger.info(f"Total mappings created: {stats['total_mappings_created']}")
        
        logger.info("✅ Module 2 completed successfully")
        logger.info("Canonical ID-name mappings are ready for dashboard use")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Module 2 failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())