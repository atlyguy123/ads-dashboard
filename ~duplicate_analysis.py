#!/usr/bin/env python3
"""
Comprehensive Duplicate Data Analysis Script

This script analyzes duplicate data issues in the Mixpanel database
to identify the root cause of data corruption that gets worse over time.
"""

import sqlite3
import sys
from pathlib import Path

# Add utils directory to path for database utilities
sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from database_utils import get_database_path

def main():
    """Main analysis function"""
    print("🔍 === DUPLICATE DATA ANALYSIS ===\n")
    
    # Connect to main database
    db_path = Path(get_database_path('mixpanel_data'))
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # 1. Summary of duplicate events
        print("1. DUPLICATE EVENTS SUMMARY")
        cursor.execute("""
            SELECT COUNT(*) as total_events, 
                   COUNT(DISTINCT event_uuid) as unique_uuids,
                   COUNT(DISTINCT distinct_id || '-' || event_name || '-' || event_time) as unique_logical_events
            FROM mixpanel_event
        """)
        total, unique_uuids, unique_logical = cursor.fetchone()
        duplicates = total - unique_logical
        print(f"   - Total events: {total:,}")
        print(f"   - Unique UUIDs: {unique_uuids:,}")
        print(f"   - Unique logical events: {unique_logical:,}")
        print(f"   - 🚨 DUPLICATE EVENTS: {duplicates:,}")
        print()
        
        # 2. Duplicate events by date
        print("2. DUPLICATE EVENTS BY DATE")
        cursor.execute("""
            SELECT DATE(event_time) as event_date, 
                   COUNT(*) as total_events,
                   COUNT(DISTINCT distinct_id || '-' || event_name || '-' || event_time) as unique_events,
                   COUNT(*) - COUNT(DISTINCT distinct_id || '-' || event_name || '-' || event_time) as duplicates
            FROM mixpanel_event 
            GROUP BY DATE(event_time) 
            HAVING duplicates > 0
            ORDER BY event_date DESC
            LIMIT 15
        """)
        duplicate_dates = cursor.fetchall()
        for date, total, unique, dups in duplicate_dates:
            print(f"   - {date}: {dups} duplicates ({total} total, {unique} unique)")
        print()
        
        # 3. Most duplicated events
        print("3. MOST DUPLICATED EVENTS")
        cursor.execute("""
            SELECT distinct_id, event_name, event_time, COUNT(*) as duplicate_count
            FROM mixpanel_event 
            GROUP BY distinct_id, event_name, event_time 
            HAVING COUNT(*) > 1 
            ORDER BY duplicate_count DESC, event_time DESC
            LIMIT 10
        """)
        top_duplicates = cursor.fetchall()
        for distinct_id, event_name, event_time, count in top_duplicates:
            print(f"   - {event_name} | {distinct_id[:30]}... | {event_time} | {count} copies")
        print()
        
        # 4. Check UUID patterns for duplicates
        print("4. UUID ANALYSIS FOR DUPLICATES")
        cursor.execute("""
            SELECT COUNT(*) as event_count, COUNT(DISTINCT event_uuid) as unique_uuids
            FROM mixpanel_event 
            WHERE (distinct_id || '-' || event_name || '-' || event_time) IN (
                SELECT distinct_id || '-' || event_name || '-' || event_time 
                FROM mixpanel_event 
                GROUP BY distinct_id, event_name, event_time 
                HAVING COUNT(*) > 1
            )
        """)
        dup_events, dup_uuids = cursor.fetchone()
        print(f"   - Duplicate logical events: {dup_events}")
        print(f"   - Unique UUIDs in duplicates: {dup_uuids}")
        if dup_events == dup_uuids:
            print("   - ✅ Each duplicate has a unique UUID (problem is with INSERT OR REPLACE logic)")
        else:
            print("   - ❌ Some duplicates share UUIDs (problem is with UUID generation)")
        print()
        
        # 5. Check pipeline processing history
        print("5. PIPELINE PROCESSING HISTORY")
        cursor.execute("""
            SELECT date_day, processing_timestamp, status, events_processed
            FROM processed_event_days 
            ORDER BY date_day DESC 
            LIMIT 10
        """)
        processing_history = cursor.fetchall()
        for date, timestamp, status, events in processing_history:
            print(f"   - {date}: {events} events, processed {timestamp}, status: {status}")
        print()
        
        # 6. Analysis of raw data
        print("6. RAW DATA ANALYSIS")
        raw_db_path = Path(get_database_path('raw_data'))
        if raw_db_path.exists():
            raw_conn = sqlite3.connect(str(raw_db_path))
            raw_cursor = raw_conn.cursor()
            
            # Check raw events by date
            raw_cursor.execute("""
                SELECT date_day, COUNT(*) as raw_events
                FROM raw_event_data 
                GROUP BY date_day 
                ORDER BY date_day DESC 
                LIMIT 10
            """)
            raw_dates = raw_cursor.fetchall()
            print("   Raw events by date:")
            for date, count in raw_dates:
                print(f"     - {date}: {count} raw events")
            
            # Check for duplicates in raw data
            raw_cursor.execute("""
                SELECT COUNT(*) as total_raw, 
                       COUNT(DISTINCT date_day || '-' || file_sequence) as unique_entries
                FROM raw_event_data
            """)
            total_raw, unique_raw = raw_cursor.fetchone()
            if total_raw != unique_raw:
                print(f"   🚨 RAW DATA DUPLICATES: {total_raw - unique_raw} duplicate entries in raw_event_data")
            else:
                print("   ✅ No duplicates in raw_event_data")
            
            raw_conn.close()
        else:
            print("   ❌ Raw data database not found")
        print()
        
        # 7. Recommendations
        print("7. ROOT CAUSE ANALYSIS & RECOMMENDATIONS")
        print()
        print("🔍 FINDINGS:")
        if duplicates > 0:
            print(f"   - {duplicates:,} duplicate events found in processed database")
            print(f"   - Events have unique UUIDs but identical logical content")
            print(f"   - This suggests the issue is in the refresh/reprocessing logic")
        
        print()
        print("🛠️  RECOMMENDATIONS:")
        print("   1. The 'INSERT OR REPLACE' logic for refresh dates is not working correctly")
        print("   2. Events are being re-inserted instead of replaced during refresh")
        print("   3. This happens because:")
        print("      - UUID generation creates new UUIDs for same events during refresh")
        print("      - OR REPLACE uses PRIMARY KEY (event_uuid) for deduplication")
        print("      - But UUIDs are different, so new rows are created")
        print()
        print("🔧 SOLUTION:")
        print("   1. Modify the refresh logic to use proper deduplication")
        print("   2. Either:")
        print("      a) Use a composite UNIQUE constraint on (distinct_id, event_name, event_time)")
        print("      b) Delete existing events for the date before inserting new ones")
        print("      c) Use deterministic UUID generation based on event content")
        print()
        print("📈 IMPACT:")
        print("   - Data corruption increases with each pipeline run")
        print("   - Week-old data gets worse because it's included in refresh window")
        print("   - Analytics become inaccurate due to inflated event counts")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
