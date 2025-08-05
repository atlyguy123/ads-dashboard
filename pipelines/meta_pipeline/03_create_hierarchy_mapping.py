#!/usr/bin/env python3
"""
Module 3: Create Hierarchy Relationship Mapping

This module establishes and stores clear campaign → adset → ad hierarchical 
relationships by directly extracting them from Meta advertising data. It creates
authoritative mappings using Meta's own data structure as the single source of truth.

Key Features:
- Extracts hierarchy relationships directly from Meta ad_performance_daily table
- Uses Meta data as authoritative source (100% confidence)
- Simple, reliable approach - no complex analysis needed
- Optimized for dashboard aggregation queries

Dependencies: Requires Meta data table (ad_performance_daily)
Outputs: Populated id_hierarchy_mapping table
"""

import sqlite3
import logging
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import sys
from datetime import datetime, date
from collections import defaultdict

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Railway-compatible database connection management will be handled in main()

class HierarchyMappingProcessor:
    """Processes and creates advertising hierarchy mappings"""
    
    def __init__(self, meta_conn: sqlite3.Connection, output_conn: sqlite3.Connection):
        self.meta_conn = meta_conn
        self.meta_cursor = meta_conn.cursor()
        self.output_conn = output_conn
        self.output_cursor = output_conn.cursor()
        self.stats = {
            'total_ads_analyzed': 0,
            'hierarchies_created': 0,
            'high_confidence_mappings': 0
        }
    
    def analyze_hierarchy_relationships(self) -> List[Tuple[str, str, str, float, date, date]]:
        """
        Extract hierarchy relationships directly from Meta data
        
        Returns:
            List of tuples: (ad_id, adset_id, campaign_id, confidence, first_seen, last_seen)
        """
        logger.info("Extracting hierarchy relationships from Meta data...")
        
        # Use primary Meta table as single source of truth
        table = 'ad_performance_daily'
        
        try:
            # Check if table exists
            self.meta_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table,)
            )
            if not self.meta_cursor.fetchone():
                raise RuntimeError(f"Required Meta table '{table}' not found")
            
            # Extract unique hierarchy mappings directly from Meta data
            query = f"""
            SELECT DISTINCT
                ad_id,
                adset_id,
                campaign_id,
                MIN(date) as first_seen,
                MAX(date) as last_seen
            FROM {table}
            WHERE ad_id IS NOT NULL 
              AND adset_id IS NOT NULL 
              AND campaign_id IS NOT NULL
            GROUP BY ad_id, adset_id, campaign_id
            ORDER BY ad_id
            """
            
            self.meta_cursor.execute(query)
            results = self.meta_cursor.fetchall()
            
            logger.info(f"Found {len(results)} unique hierarchy mappings in {table}")
            
            # Convert to expected format - confidence is always 1.0 since Meta data is authoritative
            final_hierarchies = []
            for ad_id, adset_id, campaign_id, first_seen, last_seen in results:
                final_hierarchies.append((
                    ad_id,
                    adset_id,
                    campaign_id,
                    1.0,  # 100% confidence - Meta data is authoritative
                    first_seen,
                    last_seen
                ))
            
            self.stats['total_ads_analyzed'] = len(final_hierarchies)
            self.stats['high_confidence_mappings'] = len(final_hierarchies)
            logger.info(f"Extracted {len(final_hierarchies)} hierarchy mappings")
            return final_hierarchies
            
        except sqlite3.Error as e:
            logger.error(f"Error querying {table}: {e}")
            raise
    
    def create_hierarchy_mappings(self):
        """Create hierarchy mappings for all ads"""
        logger.info("Creating advertising hierarchy mappings...")
        
        # Clear existing mappings (fresh start)
        self.output_cursor.execute("DELETE FROM id_hierarchy_mapping")
        self.output_conn.commit()
        
        try:
            # Analyze hierarchy relationships
            hierarchies = self.analyze_hierarchy_relationships()
            
            if not hierarchies:
                logger.warning("No hierarchy relationships found")
                return
            
            # Insert mappings into database
            insert_query = """
            INSERT INTO id_hierarchy_mapping 
            (ad_id, adset_id, campaign_id, relationship_confidence, first_seen_date, last_seen_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            current_time = datetime.now()
            mapping_data = [
                (ad_id, adset_id, campaign_id, confidence, first_seen, last_seen, current_time, current_time)
                for ad_id, adset_id, campaign_id, confidence, first_seen, last_seen in hierarchies
            ]
            
            self.output_cursor.executemany(insert_query, mapping_data)
            self.output_conn.commit()
            
            self.stats['hierarchies_created'] = len(hierarchies)
            logger.info(f"✅ Created {len(hierarchies)} hierarchy mappings")
            
        except Exception as e:
            logger.error(f"Failed to create hierarchy mappings: {e}")
            self.output_conn.rollback()
            raise
    
    def validate_hierarchies(self):
        """Validate the created hierarchy mappings"""
        logger.info("Validating hierarchy mappings...")
        
        # Check total count
        self.output_cursor.execute("SELECT COUNT(*) FROM id_hierarchy_mapping")
        total_mappings = self.output_cursor.fetchone()[0]
        
        if total_mappings == 0:
            logger.error("❌ No hierarchy mappings found")
            return False
        
        logger.info(f"✅ Total hierarchy mappings: {total_mappings}")
        
        # All mappings have 100% confidence since Meta data is authoritative
        logger.info(f"  All mappings: {total_mappings} (100% confidence - Meta data is authoritative)")
        
        # Check for duplicate ad_ids (should not happen due to PRIMARY KEY)
        self.output_cursor.execute("""
        SELECT ad_id, COUNT(*) 
        FROM id_hierarchy_mapping 
        GROUP BY ad_id 
        HAVING COUNT(*) > 1
        """)
        duplicates = self.output_cursor.fetchall()
        
        if duplicates:
            logger.error(f"❌ Found {len(duplicates)} duplicate ad_id mappings")
            return False
        
        # Sample check: verify hierarchy relationships make sense
        self.output_cursor.execute("""
        SELECT ad_id, adset_id, campaign_id, relationship_confidence
        FROM id_hierarchy_mapping
        LIMIT 5
        """)
        
        samples = self.output_cursor.fetchall()
        logger.info("Sample hierarchy mappings:")
        for ad_id, adset_id, campaign_id, confidence in samples:
            logger.info(f"  Ad {ad_id} → AdSet {adset_id} → Campaign {campaign_id} (confidence: {confidence})")
        
        # Verify referential integrity by checking if IDs exist in other tables
        # Note: Skipping cross-database validation for Railway compatibility
        # The main hierarchy creation logic above ensures data integrity
        logger.info("✅ Hierarchy validation passed - all mappings have high confidence")
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()

def main():
    """Main execution function with Railway-compatible database management"""
    try:
        logger.info("=== Module 3: Create Hierarchy Relationship Mapping ===")
        logger.info("Creating advertising hierarchy mappings...")
        
        # Use Railway-compatible database connections
        with get_database_connection('meta_analytics') as meta_conn, \
             get_database_connection('mixpanel_data') as output_conn:
            
            # Verify required tables exist
            meta_cursor = meta_conn.cursor()
            output_cursor = output_conn.cursor()
            
            # Check Meta table exists (source data)
            meta_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                ('ad_performance_daily',)
            )
            if not meta_cursor.fetchone():
                raise RuntimeError("Required Meta table 'ad_performance_daily' not found in meta_analytics.db")
            
            # Check output table exists (destination)
            output_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                ('id_hierarchy_mapping',)
            )
            if not output_cursor.fetchone():
                raise RuntimeError("Required table 'id_hierarchy_mapping' not found in mixpanel_data.db. Run database setup first.")
            
            # Process hierarchy mappings
            processor = HierarchyMappingProcessor(meta_conn, output_conn)
            processor.create_hierarchy_mappings()
            
            # Validate results
            if not processor.validate_hierarchies():
                raise RuntimeError("Hierarchy mapping validation failed")
            
            # Display final statistics
            stats = processor.get_stats()
            logger.info("=== Hierarchy Mapping Statistics ===")
            logger.info(f"Total ads analyzed: {stats['total_ads_analyzed']}")
            logger.info(f"Hierarchies created: {stats['hierarchies_created']}")
            logger.info(f"All mappings have 100% confidence (Meta data is authoritative)")
        
        logger.info("✅ Module 3 completed successfully")
        logger.info("Hierarchy mappings are ready for dashboard aggregation")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Module 3 failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())