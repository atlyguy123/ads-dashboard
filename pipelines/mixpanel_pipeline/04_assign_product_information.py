#!/usr/bin/env python3
"""
Module 4: User-Product Relationship Assignment
Analyzes all user events to discover which products each user has interacted with.
Creates user-product relationships by examining event history for product_id information.

For each user, identifies all products they've engaged with and captures:
- Which store/platform they used (iOS App Store, Google Play, etc.)
- When they first encountered each product
- Geographic and attribution data for marketing analysis
- Complete relationship mapping for revenue and engagement analytics

Processes the entire user base to ensure comprehensive product relationship data
for downstream analytics and reporting.
"""

import sqlite3
import json
import logging
import argparse
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, Tuple, Optional, Any

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def normalize_store_value(store_value: Any) -> Optional[str]:
    """
    Normalize store values to standard format
    
    Args:
        store_value: Raw store value from event data
        
    Returns:
        Normalized store string or None
    """
    if not store_value:
        return None
    
    # Convert to string and normalize
    store_str = str(store_value).strip().upper()
    
    # Handle empty or whitespace-only strings
    if not store_str:
        return None
    
    # Direct mappings for known store values
    store_mappings = {
        'APP_STORE': 'APP_STORE',
        'APPSTORE': 'APP_STORE', 
        'IOS': 'APP_STORE',
        'APPLE': 'APP_STORE',
        'PLAY_STORE': 'PLAY_STORE',
        'PLAYSTORE': 'PLAY_STORE',
        'GOOGLE_PLAY': 'PLAY_STORE',
        'GOOGLE': 'PLAY_STORE',
        'ANDROID': 'PLAY_STORE'
    }
    
    return store_mappings.get(store_str, store_str)

def prioritize_store(stores: Set[str]) -> str:
    """
    Choose the best store when multiple stores are found for the same product
    Priority: APP_STORE > PLAY_STORE > others
    
    Args:
        stores: Set of store values
        
    Returns:
        The highest priority store
    """
    if not stores:
        return None
    
    # Priority order
    if 'APP_STORE' in stores:
        return 'APP_STORE'
    elif 'PLAY_STORE' in stores:
        return 'PLAY_STORE'
    else:
        # Return the first one alphabetically for consistency
        return sorted(stores)[0]

def extract_metadata_from_event(event_json: str) -> Dict[str, Any]:
    """
    Extract metadata from a single event's JSON
    
    Args:
        event_json: JSON string from event
        
    Returns:
        Dictionary of extracted metadata
    """
    # Handle edge cases for malformed or empty JSON
    if not event_json or not isinstance(event_json, str):
        return {}
    
    # Handle whitespace-only strings
    if not event_json.strip():
        return {}
    
    try:
        event_data = json.loads(event_json)
        
        # Handle case where event_data is not a dict
        if not isinstance(event_data, dict):
            return {}
            
        properties = event_data.get('properties', {})
        
        # Handle case where properties is not a dict
        if not isinstance(properties, dict):
            return {}
        
        # Extract key metadata
        metadata = {
            'store': normalize_store_value(properties.get('store')),
            'device': properties.get('device'),
            'revenue': properties.get('revenue'),
            'currency': properties.get('currency'),
        }
        
        # Clean up None values but keep the keys
        return metadata
        
    except (json.JSONDecodeError, TypeError, AttributeError) as e:
        logger.debug(f"Failed to parse event JSON: {e}")
        return {}

def discover_all_user_products_efficiently(cursor: sqlite3.Cursor) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    EFFICIENTLY discover all user-product relationships by processing ALL events ONCE
    This is much faster than the old approach of querying each user individually.
    
    Args:
        cursor: Database cursor
        
    Returns:
        Dictionary mapping user_id -> product_id -> metadata
    """
    logger.info("🚀 Starting EFFICIENT discovery: processing all events once...")
    
    # Single query to get ALL events with product information
    # This replaces 466,692 individual user queries with ONE query
    cursor.execute("""
        SELECT 
            e.distinct_id,
            e.event_time,
            e.country,
            e.region,
            e.abi_ad_id,
            e.abi_campaign_id,
            e.abi_ad_set_id,
            e.event_json
        FROM mixpanel_event e
        WHERE json_extract(e.event_json, '$.properties.product_id') IS NOT NULL
        ORDER BY e.event_time ASC
    """)
    
    # Build user-product mappings in memory
    user_products = defaultdict(lambda: defaultdict(lambda: {
        'stores_seen': set(),
        'first_seen': None,
        'country': None,
        'region': None,
        'abi_ad_id': None,
        'abi_campaign_id': None,
        'abi_ad_set_id': None,
        'device': None,
        'store': None
    }))
    
    events_processed = 0
    users_discovered = set()
    
    # Process ALL events in one pass
    for row in cursor.fetchall():
        distinct_id, event_time, country, region, abi_ad_id, abi_campaign_id, abi_ad_set_id, event_json = row
        
        # Track progress
        events_processed += 1
        users_discovered.add(distinct_id)
        
        if events_processed % 10000 == 0:
            logger.info(f"📊 Processed {events_processed:,} events, discovered {len(users_discovered):,} users with products...")
        
        try:
            # Extract product_id from event JSON
            event_data = json.loads(event_json)
            product_id = event_data.get('properties', {}).get('product_id')
            
            if not product_id:
                continue
                
            # Get/create metadata for this user-product combination
            metadata = user_products[distinct_id][product_id]
            
            # Update first_seen timestamp (earliest event for this product)
            if not metadata['first_seen'] or event_time < metadata['first_seen']:
                metadata['first_seen'] = event_time
            
            # Update location info (prefer non-null values from events)
            if country and not metadata['country']:
                metadata['country'] = country
            if region and not metadata['region']:
                metadata['region'] = region
                
            # Update attribution info (prefer non-null values from events)
            if abi_ad_id and not metadata['abi_ad_id']:
                metadata['abi_ad_id'] = abi_ad_id
            if abi_campaign_id and not metadata['abi_campaign_id']:
                metadata['abi_campaign_id'] = abi_campaign_id
            if abi_ad_set_id and not metadata['abi_ad_set_id']:
                metadata['abi_ad_set_id'] = abi_ad_set_id
                
            # Extract and process store/device metadata from event
            event_metadata = extract_metadata_from_event(event_json)
            
            # Track all stores seen for this product (for prioritization)
            if event_metadata.get('store'):
                metadata['stores_seen'].add(event_metadata['store'])
                
            # Update device info (prefer non-null values)
            if event_metadata.get('device') and not metadata['device']:
                metadata['device'] = event_metadata['device']
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"⚠️  Skipping event with invalid JSON: {e}")
            continue
    
    logger.info(f"✅ Processed {events_processed:,} events total")
    logger.info(f"🎯 Discovered {len(users_discovered):,} users with products")
    
    # Finalize metadata for each user-product combination
    total_relationships = 0
    for user_id in user_products:
        for product_id in user_products[user_id]:
            total_relationships += 1
            metadata = user_products[user_id][product_id]
            
            # Choose the best store from all stores seen (APP_STORE > PLAY_STORE > others)
            if metadata['stores_seen']:
                metadata['store'] = prioritize_store(metadata['stores_seen'])
            
            # Convert sets to lists for JSON serialization (if needed)
            metadata['stores_seen'] = list(metadata['stores_seen'])
    
    logger.info(f"📦 Total user-product relationships discovered: {total_relationships:,}")
    
    # Convert to regular dict for return
    return {user_id: dict(products) for user_id, products in user_products.items()}

# Keep the old function for backward compatibility (used by tests and demonstrations)
def discover_user_products(cursor: sqlite3.Cursor, user_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Discover all products for a specific user by analyzing their events
    (This is the old approach - kept for compatibility and testing)
    
    Args:
        cursor: Database cursor
        user_id: User's distinct_id
        
    Returns:
        Dictionary mapping product_id to metadata
    """
    # Get all events for this user that have product_id
    cursor.execute("""
        SELECT 
            event_time,
            country,
            region,
            abi_ad_id,
            abi_campaign_id,
            abi_ad_set_id,
            event_json
        FROM mixpanel_event
        WHERE distinct_id = ?
        AND json_extract(event_json, '$.properties.product_id') IS NOT NULL
        ORDER BY event_time ASC
    """, (user_id,))
    
    events = cursor.fetchall()
    if not events:
        return {}
    
    # Track products and their metadata
    products = defaultdict(lambda: {
        'stores_seen': set(),
        'first_seen': None,
        'country': None,
        'region': None,
        'abi_ad_id': None,
        'abi_campaign_id': None,
        'abi_ad_set_id': None,
        'device': None,
        'store': None
    })
    
    for event_time, country, region, abi_ad_id, abi_campaign_id, abi_ad_set_id, event_json in events:
        try:
            # Extract product_id
            event_data = json.loads(event_json)
            product_id = event_data.get('properties', {}).get('product_id')
            
            if not product_id:
                continue
        
            # Get existing metadata for this product
            metadata = products[product_id]
            
            # Update first_seen if this is earlier or first time
            if not metadata['first_seen'] or event_time < metadata['first_seen']:
                metadata['first_seen'] = event_time
            
            # Update location info (prefer non-null values)
            if country and not metadata['country']:
                metadata['country'] = country
            if region and not metadata['region']:
                metadata['region'] = region
                
            # Update attribution info (prefer non-null values)
            if abi_ad_id and not metadata['abi_ad_id']:
                metadata['abi_ad_id'] = abi_ad_id
            if abi_campaign_id and not metadata['abi_campaign_id']:
                metadata['abi_campaign_id'] = abi_campaign_id
            if abi_ad_set_id and not metadata['abi_ad_set_id']:
                metadata['abi_ad_set_id'] = abi_ad_set_id
                
            # Extract and process event metadata
            event_metadata = extract_metadata_from_event(event_json)
            
            # Track stores seen
            if event_metadata.get('store'):
                metadata['stores_seen'].add(event_metadata['store'])
                
            # Update device info (prefer non-null values)  
            if event_metadata.get('device') and not metadata['device']:
                metadata['device'] = event_metadata['device']
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Skipping event with invalid JSON: {e}")
            continue
    
    # Finalize metadata for each product
    for product_id in products:
        metadata = products[product_id]
        
        # Choose the best store from all stores seen
        if metadata['stores_seen']:
            metadata['store'] = prioritize_store(metadata['stores_seen'])
        
        # Convert sets to lists for JSON serialization (if needed)
        metadata['stores_seen'] = list(metadata['stores_seen'])
    
    return dict(products)

def create_user_product_relationships(conn: sqlite3.Connection, limit: Optional[int] = None):
    """
    Main function to create user-product relationships using the EFFICIENT approach
    
    Args:
        conn: Database connection
        limit: Optional limit on number of users to process (for testing)
    """
    cursor = conn.cursor()
    
    logger.info("=== Module 4: Create User-Product Relationships ===")
    logger.info("🚀 Starting EFFICIENT user-product relationship creation...")
    logger.info("💡 NEW APPROACH: Process all events once instead of querying each user individually")
    
    # Handle edge case: limit=0 should process nothing
    if limit is not None and limit <= 0:
        logger.info("🔬 Limit is 0 or negative, processing no users")
        return
    
    # Validate database schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    
    required_tables = {'mixpanel_user', 'mixpanel_event', 'user_product_metrics'}
    missing_tables = required_tables - tables
    
    if missing_tables:
        raise Exception(f"Missing required tables: {missing_tables}")
    
    logger.info("✅ All required tables found")
    
    # Use the NEW EFFICIENT approach to discover all user-product relationships
    user_products = discover_all_user_products_efficiently(cursor)
    
    logger.info(f"🎯 Found {len(user_products):,} users with product relationships")
    
    # If limit is specified, only process that many users (for testing)
    if limit:
        user_ids = list(user_products.keys())[:limit]
        user_products = {user_id: user_products[user_id] for user_id in user_ids}
        logger.info(f"🔬 Limited to {len(user_products):,} users for testing")
    
    # Prepare data for batch operations
    relationships_to_create = []
    relationships_to_update = []
    current_date = datetime.now().date()
    current_timestamp = datetime.now()
    
    # Check which relationships already exist (to determine create vs update)
    logger.info("🔍 Checking for existing relationships...")
    cursor.execute("SELECT distinct_id, product_id FROM user_product_metrics")
    existing_relationships = set(cursor.fetchall())
    logger.info(f"📊 Found {len(existing_relationships):,} existing relationships")
    
    # Process each user-product combination discovered
    total_combinations = sum(len(products) for products in user_products.values())
    logger.info(f"⚙️  Processing {total_combinations:,} user-product combinations...")
    
    for user_id, products in user_products.items():
        for product_id, metadata in products.items():
            relationship_key = (user_id, product_id)
            
            # Prepare the relationship data
            relationship_data = {
                'distinct_id': user_id,
                'product_id': product_id,
                'credited_date': current_date,
                'country': metadata.get('country'),
                'region': metadata.get('region'),
                'device': metadata.get('device') or metadata.get('store'),  # Use store as device fallback
                'abi_ad_id': metadata.get('abi_ad_id'),
                'abi_campaign_id': metadata.get('abi_campaign_id'),
                'abi_ad_set_id': metadata.get('abi_ad_set_id'),
                'store': metadata.get('store'),
                # Required fields with defaults
                'current_status': 'active',
                'current_value': 0.0,
                'value_status': 'unknown',
                'last_updated_ts': current_timestamp,
                'valid_lifecycle': False
            }
            
            if relationship_key in existing_relationships:
                relationships_to_update.append(relationship_data)
            else:
                relationships_to_create.append(relationship_data)
    
    # Execute batch operations (much faster than individual inserts)
    created_count = 0
    updated_count = 0
    
    if relationships_to_create:
        logger.info(f"📝 Creating {len(relationships_to_create):,} new relationships...")
        
        cursor.executemany("""
            INSERT INTO user_product_metrics (
                distinct_id, product_id, credited_date, country, region, device,
                abi_ad_id, abi_campaign_id, abi_ad_set_id, current_status,
                current_value, value_status, last_updated_ts, valid_lifecycle, store
            ) VALUES (
                :distinct_id, :product_id, :credited_date, :country, :region, :device,
                :abi_ad_id, :abi_campaign_id, :abi_ad_set_id, :current_status,
                :current_value, :value_status, :last_updated_ts, :valid_lifecycle, :store
            )
        """, relationships_to_create)
        
        created_count = len(relationships_to_create)
    
    if relationships_to_update:
        logger.info(f"🔄 Updating {len(relationships_to_update):,} existing relationships...")
        
        cursor.executemany("""
            UPDATE user_product_metrics 
            SET country = :country,
                region = :region,
                device = :device,
                abi_ad_id = :abi_ad_id,
                abi_campaign_id = :abi_campaign_id,
                abi_ad_set_id = :abi_ad_set_id,
                store = :store,
                last_updated_ts = :last_updated_ts
            WHERE distinct_id = :distinct_id AND product_id = :product_id
        """, relationships_to_update)
        
        updated_count = len(relationships_to_update)
    
    # Commit all changes
    conn.commit()
    
    logger.info(f"🎉 Relationship creation complete!")
    logger.info(f"📊 Results: {created_count:,} relationships created, {updated_count:,} relationships updated")
    logger.info(f"⚡ EFFICIENCY GAIN: Processed {total_combinations:,} relationships with 1 query instead of 466,692 queries!")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Create user-product relationships by analyzing events (EFFICIENT approach)"
    )
    
    # Use centralized database path discovery
    default_db_path = get_database_path('mixpanel_data')
    
    parser.add_argument(
        '--database', 
        default=str(default_db_path),
        help='Path to the database file'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit processing to N users (for testing)'
    )
    
    args = parser.parse_args()
    
    # Ensure database exists
    db_path = Path(args.database)
    if not db_path.exists():
        logger.error(f"❌ Database not found: {db_path}")
        return 1
    
    try:
        # Connect to database
        conn = sqlite3.connect(str(db_path))
        
        # Run the EFFICIENT relationship creation
        create_user_product_relationships(conn, limit=args.limit)
        
        logger.info("✅ Module 4 completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    exit(main()) 