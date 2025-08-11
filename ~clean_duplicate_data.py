#!/usr/bin/env python3
"""
Clean Duplicate Data Script - Phase 1

This script cleans existing duplicates by deleting the last 2 weeks of data
from the raw database. The master pipeline will automatically backfill this data.
"""

import sqlite3
import sys
from pathlib import Path

# Add utils directory to path for database utilities
sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from database_utils import get_database_path

def main():
    """Clean duplicate data by deleting last 2 weeks"""
    print("🧹 === CLEANING DUPLICATE DATA ===\n")
    
    # Connect to raw database
    raw_db_path = Path(get_database_path('raw_data'))
    if not raw_db_path.exists():
        print(f"❌ Raw database not found at {raw_db_path}")
        return 1
    
    conn = sqlite3.connect(str(raw_db_path))
    cursor = conn.cursor()
    
    try:
        # Show current state
        print("1. CURRENT STATE")
        cursor.execute("SELECT COUNT(*) FROM raw_event_data")
        total_before = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT date_day || '-' || file_sequence || '-' || event_data) FROM raw_event_data")
        unique_before = cursor.fetchone()[0]
        
        duplicates_before = total_before - unique_before
        print(f"   - Total raw events: {total_before:,}")
        print(f"   - Unique events: {unique_before:,}")
        print(f"   - Duplicates: {duplicates_before:,}")
        print()
        
        # Show what will be deleted
        print("2. DELETION SCOPE")
        cursor.execute("SELECT COUNT(*) FROM raw_event_data WHERE date_day >= date('now', '-14 days')")
        events_to_delete = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT date_day) FROM raw_event_data WHERE date_day >= date('now', '-14 days')")
        dates_to_delete = cursor.fetchone()[0]
        
        print(f"   - Events in last 14 days: {events_to_delete:,}")
        print(f"   - Dates in last 14 days: {dates_to_delete}")
        print()
        
        # Confirm deletion
        response = input("🤔 Proceed with deletion? The master pipeline will backfill this data. (y/N): ")
        if response.lower() != 'y':
            print("❌ Deletion cancelled")
            return 0
        
        print("3. DELETING DATA")
        cursor.execute("BEGIN TRANSACTION")
        
        # Delete last 14 days from raw_event_data
        cursor.execute("DELETE FROM raw_event_data WHERE date_day >= date('now', '-14 days')")
        events_deleted = cursor.rowcount
        
        # Delete last 14 days from downloaded_dates
        cursor.execute("DELETE FROM downloaded_dates WHERE date_day >= date('now', '-14 days')")
        dates_deleted = cursor.rowcount
        
        cursor.execute("COMMIT")
        
        print(f"   - ✅ Deleted {events_deleted:,} events")
        print(f"   - ✅ Deleted {dates_deleted} date records")
        print()
        
        # Show final state
        print("4. FINAL STATE")
        cursor.execute("SELECT COUNT(*) FROM raw_event_data")
        total_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT date_day || '-' || file_sequence || '-' || event_data) FROM raw_event_data")
        unique_after = cursor.fetchone()[0]
        
        duplicates_after = total_after - unique_after
        print(f"   - Total raw events: {total_after:,}")
        print(f"   - Unique events: {unique_after:,}")
        print(f"   - Duplicates: {duplicates_after:,}")
        print()
        
        # Summary
        duplicates_removed = duplicates_before - duplicates_after
        print("5. SUMMARY")
        print(f"   - 🗑️  Removed {events_deleted:,} events from last 14 days")
        print(f"   - 🎯 Reduced duplicates by {duplicates_removed:,}")
        print(f"   - 📈 Remaining duplicates: {duplicates_after:,}")
        print()
        print("✅ Phase 1 complete! Run the master pipeline to backfill clean data.")
        
        return 0
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"❌ Error during cleanup: {e}")
        return 1
    
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
