#!/usr/bin/env python3
"""
Example script showing how to use the selective Meta data update functionality

This script demonstrates how to:
1. Update only specific breakdown tables (country, region, device)
2. Update custom date ranges (e.g., April 1-30)
3. Skip dates that already have data
4. Or overwrite existing data if needed

Usage examples:
- Update country breakdown for April 2024, skipping existing dates
- Update device breakdown for a specific week, overwriting all data
- Fill gaps in region breakdown data

Author: Analytics Pipeline Team
Created: 2025
"""

import sys
from pathlib import Path
from datetime import datetime

# Add the meta pipeline directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Import the MetaDataUpdater class from the main module
import importlib.util
spec = importlib.util.spec_from_file_location("meta_updater", current_dir / "01_update_meta_data.py")
meta_updater_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(meta_updater_module)
MetaDataUpdater = meta_updater_module.MetaDataUpdater

def example_update_country_breakdown_april():
    """
    Example: Update country breakdown table for April 2024,
    skipping dates that already have data
    """
    print("🌍 Example: Update country breakdown for April 2024 (skip existing)")
    print("=" * 70)
    
    updater = MetaDataUpdater()
    
    success = updater.update_specific_breakdown_table(
        table_name='ad_performance_daily_country',
        breakdown_type='country',
        start_date='2024-04-01',
        end_date='2024-04-30',
        skip_existing=True  # Skip dates that already have data
    )
    
    if success:
        print("✅ Country breakdown update completed successfully!")
    else:
        print("❌ Country breakdown update failed!")
    
    return success

def example_update_device_breakdown_overwrite():
    """
    Example: Update device breakdown table for a specific week,
    overwriting any existing data
    """
    print("📱 Example: Update device breakdown for one week (overwrite mode)")
    print("=" * 70)
    
    updater = MetaDataUpdater()
    
    success = updater.update_specific_breakdown_table(
        table_name='ad_performance_daily_device',
        breakdown_type='device',
        start_date='2024-04-15',
        end_date='2024-04-21',
        skip_existing=False  # Overwrite existing data
    )
    
    if success:
        print("✅ Device breakdown update completed successfully!")
    else:
        print("❌ Device breakdown update failed!")
    
    return success

def example_fill_gaps_region_breakdown():
    """
    Example: Fill gaps in region breakdown data for March 2024
    """
    print("🗺️  Example: Fill gaps in region breakdown for March 2024")
    print("=" * 70)
    
    updater = MetaDataUpdater()
    
    success = updater.update_specific_breakdown_table(
        table_name='ad_performance_daily_region',
        breakdown_type='region',
        start_date='2024-03-01',
        end_date='2024-03-31',
        skip_existing=True  # Only fill missing dates
    )
    
    if success:
        print("✅ Region breakdown gap filling completed successfully!")
    else:
        print("❌ Region breakdown gap filling failed!")
    
    return success

def example_update_main_table_custom_range():
    """
    Example: Update the main ad performance table (no breakdown)
    for a custom date range
    """
    print("📊 Example: Update main table for custom date range")
    print("=" * 70)
    
    updater = MetaDataUpdater()
    
    success = updater.update_specific_breakdown_table(
        table_name='ad_performance_daily',
        breakdown_type=None,  # No breakdown
        start_date='2024-04-01',
        end_date='2024-04-07',
        skip_existing=True
    )
    
    if success:
        print("✅ Main table update completed successfully!")
    else:
        print("❌ Main table update failed!")
    
    return success

def interactive_update():
    """
    Interactive function to let user choose what to update
    """
    print("🎛️  Interactive Meta Data Update")
    print("=" * 50)
    
    # Get table choice
    print("\nChoose table to update:")
    print("1. ad_performance_daily (no breakdown)")
    print("2. ad_performance_daily_country (country breakdown)")
    print("3. ad_performance_daily_region (region breakdown)")
    print("4. ad_performance_daily_device (device breakdown)")
    
    table_choice = input("Enter choice (1-4): ").strip()
    
    table_map = {
        '1': ('ad_performance_daily', None),
        '2': ('ad_performance_daily_country', 'country'),
        '3': ('ad_performance_daily_region', 'region'),
        '4': ('ad_performance_daily_device', 'device')
    }
    
    if table_choice not in table_map:
        print("❌ Invalid choice!")
        return False
    
    table_name, breakdown_type = table_map[table_choice]
    
    # Get date range
    print(f"\nSelected: {table_name}")
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()
    
    # Validate dates
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("❌ Invalid date format! Use YYYY-MM-DD")
        return False
    
    # Get skip existing choice
    skip_choice = input("Skip existing dates? (y/n): ").strip().lower()
    skip_existing = skip_choice in ['y', 'yes']
    
    print(f"\n🚀 Starting update...")
    print(f"   Table: {table_name}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Skip existing: {skip_existing}")
    
    # Execute update
    updater = MetaDataUpdater()
    success = updater.update_specific_breakdown_table(
        table_name=table_name,
        breakdown_type=breakdown_type,
        start_date=start_date,
        end_date=end_date,
        skip_existing=skip_existing
    )
    
    if success:
        print("✅ Update completed successfully!")
    else:
        print("❌ Update failed!")
    
    return success

def main():
    """
    Main function - run examples or interactive mode
    """
    print("🔮 Meta Data Selective Update Examples")
    print("=" * 50)
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'country':
            example_update_country_breakdown_april()
        elif mode == 'device':
            example_update_device_breakdown_overwrite()
        elif mode == 'region':
            example_fill_gaps_region_breakdown()
        elif mode == 'main':
            example_update_main_table_custom_range()
        elif mode == 'interactive':
            interactive_update()
        else:
            print("Usage: python example_selective_update.py [country|device|region|main|interactive]")
    else:
        # Run all examples
        print("Running all examples...\n")
        
        examples = [
            example_update_main_table_custom_range,
            example_update_country_breakdown_april,
            example_fill_gaps_region_breakdown,
            example_update_device_breakdown_overwrite
        ]
        
        for i, example_func in enumerate(examples, 1):
            print(f"\n--- Example {i} ---")
            try:
                example_func()
            except Exception as e:
                print(f"❌ Example {i} failed: {e}")
            print()

if __name__ == "__main__":
    main() 