#!/usr/bin/env python3
"""
🔍 Trial Accuracy Drop Analysis Script

This script systematically analyzes the trial accuracy drop that occurred after the 13th,
coinciding with the migration from daily to hourly pipeline with different JSON field structures.

The goal is to identify why trial accuracy = (Mixpanel Trials / Meta Trials) * 100 dropped significantly.
"""

import sqlite3
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def analyze_trial_accuracy_drop():
    """Comprehensive analysis of trial accuracy drop around the 13th"""
    
    print("🔍 TRIAL ACCURACY DROP ANALYSIS")
    print("=" * 80)
    print("Analyzing the drop in trial accuracy after the 13th...")
    print("This coincides with migration from daily to hourly pipeline with new JSON format")
    print()
    
    # Connect to mixpanel database
    db_path = get_database_path('mixpanel_data')
    print(f"📁 Database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. ANALYZE EVENT PROCESSING BY DATE
    print("1. EVENT PROCESSING BY DATE ANALYSIS")
    print("-" * 50)
    
    # Get processed dates and event counts
    cursor.execute("""
        SELECT 
            date_day,
            events_processed,
            processing_timestamp,
            status
        FROM processed_event_days 
        ORDER BY date_day DESC 
        LIMIT 30
    """)
    
    processed_dates = cursor.fetchall()
    print(f"📊 Last 30 processed dates:")
    print("Date       | Events | Status   | Processing Time")
    print("-" * 55)
    
    thirteenth_found = False
    for date_day, events, timestamp, status in processed_dates:
        marker = " ⭐" if "13" in str(date_day) else ""
        if "13" in str(date_day):
            thirteenth_found = True
        print(f"{date_day} | {events:6d} | {status:8s} | {timestamp}{marker}")
    
    if not thirteenth_found:
        print("⚠️  No dates containing '13' found in recent processing")
    
    print()
    
    # 2. ANALYZE RAW EVENT DATA AROUND THE 13TH
    print("2. RAW EVENT DATA ANALYSIS AROUND THE 13TH")
    print("-" * 50)
    
    # Check for the specific 13th dates (likely 2025-06-13, 2025-07-13, etc.)
    cursor.execute("""
        SELECT 
            DATE(event_time) as event_date,
            COUNT(*) as total_events,
            COUNT(DISTINCT CASE WHEN event_name = 'RC Trial started' THEN distinct_id END) as trial_started,
            COUNT(DISTINCT event_name) as unique_event_types,
            MIN(event_time) as earliest_event,
            MAX(event_time) as latest_event
        FROM mixpanel_event 
        WHERE DATE(event_time) LIKE '%13'
           OR DATE(event_time) BETWEEN '2025-06-10' AND '2025-06-20'
           OR DATE(event_time) BETWEEN '2025-07-10' AND '2025-07-20'
        GROUP BY DATE(event_time)
        ORDER BY event_date
    """)
    
    event_dates = cursor.fetchall()
    print("Date       | Total | Trials | Event Types | Time Range")
    print("-" * 65)
    
    for event_date, total, trials, types, earliest, latest in event_dates:
        marker = " ⭐" if "13" in event_date else ""
        print(f"{event_date} | {total:5d} | {trials:6d} | {types:11d} | {earliest[:10]} to {latest[:10]}{marker}")
    
    print()
    
    # 3. ANALYZE EVENT STRUCTURE CHANGES
    print("3. EVENT STRUCTURE ANALYSIS - OLD VS NEW FORMAT")
    print("-" * 55)
    
    # Sample events before and after the 13th to check field structure
    cursor.execute("""
        SELECT 
            event_time,
            event_name,
            event_json
        FROM mixpanel_event 
        WHERE event_name = 'RC Trial started'
          AND (DATE(event_time) = '2025-06-12' OR DATE(event_time) = '2025-06-14'
               OR DATE(event_time) LIKE '%12' OR DATE(event_time) LIKE '%14')
        ORDER BY event_time
        LIMIT 6
    """)
    
    sample_events = cursor.fetchall()
    
    for i, (event_time, event_name, event_json) in enumerate(sample_events):
        date_str = event_time[:10]
        marker = "BEFORE" if any(x in date_str for x in ['12', '11', '10']) else "AFTER"
        print(f"\n📅 Event {i+1} ({marker} 13th): {date_str}")
        
        try:
            event_data = json.loads(event_json)
            
            # Check field locations for key fields
            print("  Field Location Analysis:")
            
            # Event name location
            event_top = event_data.get('event')
            event_name_top = event_data.get('event_name')
            event_props = event_data.get('properties', {}).get('event')
            print(f"    event (top): {event_top}")
            print(f"    event_name (top): {event_name_top}")
            print(f"    event (props): {event_props}")
            
            # Distinct ID location
            distinct_top = event_data.get('distinct_id')
            distinct_props = event_data.get('properties', {}).get('distinct_id')
            print(f"    distinct_id (top): {distinct_top}")
            print(f"    distinct_id (props): {distinct_props}")
            
            # Insert ID location
            insert_top = event_data.get('insert_id')
            insert_props = event_data.get('properties', {}).get('$insert_id')
            print(f"    insert_id (top): {insert_top}")
            print(f"    $insert_id (props): {insert_props}")
            
            # Time location
            time_top = event_data.get('time')
            time_props = event_data.get('properties', {}).get('time')
            print(f"    time (top): {time_top}")
            print(f"    time (props): {time_props}")
            
        except json.JSONDecodeError:
            print("  ❌ Invalid JSON structure")
    
    print()
    
    # 4. ANALYZE TRIAL ACCURACY CALCULATION INPUTS
    print("4. TRIAL ACCURACY CALCULATION INPUTS")
    print("-" * 40)
    
    # Check Meta vs Mixpanel trial counts around the boundary
    # First check if we have Meta data
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%ad_performance%'")
    meta_tables = cursor.fetchall()
    
    if meta_tables:
        print("📊 Meta vs Mixpanel Trial Comparison:")
        cursor.execute("""
            SELECT 
                ap.date,
                SUM(ap.meta_trials) as meta_trials_total,
                COUNT(DISTINCT me.distinct_id) as mixpanel_trials_total,
                CASE 
                    WHEN SUM(ap.meta_trials) > 0 
                    THEN ROUND((COUNT(DISTINCT me.distinct_id) * 100.0 / SUM(ap.meta_trials)), 2)
                    ELSE 0 
                END as trial_accuracy_pct
            FROM ad_performance_daily ap
            LEFT JOIN mixpanel_user mu ON ap.ad_id = mu.abi_ad_id
            LEFT JOIN mixpanel_event me ON mu.distinct_id = me.distinct_id 
                AND me.event_name = 'RC Trial started'
                AND DATE(me.event_time) = ap.date
            WHERE ap.date BETWEEN '2025-06-10' AND '2025-06-20'
               OR ap.date BETWEEN '2025-07-10' AND '2025-07-20'
               OR ap.date LIKE '%13'
            GROUP BY ap.date
            ORDER BY ap.date
        """)
        
        accuracy_data = cursor.fetchall()
        
        print("Date       | Meta | Mixpanel | Accuracy%")
        print("-" * 42)
        
        for date, meta_trials, mp_trials, accuracy in accuracy_data:
            marker = " ⭐" if "13" in date else ""
            print(f"{date} | {meta_trials:4d} | {mp_trials:8d} | {accuracy:8.2f}%{marker}")
    
    else:
        print("⚠️  No Meta performance tables found - checking Mixpanel data only")
        
        # Just show Mixpanel trial counts by date
        cursor.execute("""
            SELECT 
                DATE(event_time) as date,
                COUNT(DISTINCT distinct_id) as mixpanel_trials
            FROM mixpanel_event 
            WHERE event_name = 'RC Trial started'
              AND (DATE(event_time) BETWEEN '2025-06-10' AND '2025-06-20'
                   OR DATE(event_time) BETWEEN '2025-07-10' AND '2025-07-20'
                   OR DATE(event_time) LIKE '%13')
            GROUP BY DATE(event_time)
            ORDER BY date
        """)
        
        trial_data = cursor.fetchall()
        
        print("Date       | Mixpanel Trials")
        print("-" * 25)
        
        for date, trials in trial_data:
            marker = " ⭐" if "13" in date else ""
            print(f"{date} | {trials:13d}{marker}")
    
    print()
    
    # 5. ANALYZE EVENT EXTRACTION FAILURES
    print("5. EVENT EXTRACTION FAILURE ANALYSIS")
    print("-" * 38)
    
    # Check for potential extraction issues by looking at event structure
    cursor.execute("""
        SELECT 
            DATE(event_time) as date,
            COUNT(*) as total_events,
            COUNT(CASE WHEN event_name IS NULL THEN 1 END) as null_event_names,
            COUNT(CASE WHEN distinct_id IS NULL THEN 1 END) as null_distinct_ids,
            COUNT(CASE WHEN event_uuid IS NULL THEN 1 END) as null_uuids
        FROM mixpanel_event 
        WHERE DATE(event_time) BETWEEN '2025-06-10' AND '2025-06-20'
           OR DATE(event_time) BETWEEN '2025-07-10' AND '2025-07-20'
           OR DATE(event_time) LIKE '%13'
        GROUP BY DATE(event_time)
        ORDER BY date
    """)
    
    extraction_data = cursor.fetchall()
    
    print("Date       | Total | Null Names | Null IDs | Null UUIDs")
    print("-" * 55)
    
    for date, total, null_names, null_ids, null_uuids in extraction_data:
        marker = " ⭐" if "13" in date else ""
        issues = "⚠️" if (null_names > 0 or null_ids > 0 or null_uuids > 0) else "✅"
        print(f"{date} | {total:5d} | {null_names:10d} | {null_ids:8d} | {null_uuids:10d} {issues}{marker}")
    
    print()
    
    # 6. ANALYZE PROCESSING TIMESTAMPS AND POTENTIAL TIMEZONE ISSUES  
    print("6. PROCESSING TIMESTAMPS ANALYSIS")
    print("-" * 35)
    
    # Check for any timezone or timing issues in event processing
    cursor.execute("""
        SELECT 
            DATE(event_time) as event_date,
            MIN(event_time) as earliest_time,
            MAX(event_time) as latest_time,
            COUNT(DISTINCT strftime('%H', event_time)) as unique_hours,
            COUNT(*) as total_events
        FROM mixpanel_event 
        WHERE DATE(event_time) LIKE '%13'
           OR DATE(event_time) BETWEEN '2025-06-12' AND '2025-06-14'
           OR DATE(event_time) BETWEEN '2025-07-12' AND '2025-07-14'
        GROUP BY DATE(event_time)
        ORDER BY event_date
    """)
    
    timing_data = cursor.fetchall()
    
    print("Date       | Earliest Time       | Latest Time         | Hours | Events")
    print("-" * 75)
    
    for date, earliest, latest, hours, events in timing_data:
        marker = " ⭐" if "13" in date else ""
        print(f"{date} | {earliest} | {latest} | {hours:5d} | {events:6d}{marker}")
    
    print()
    
    # 7. SUMMARY AND RECOMMENDATIONS
    print("7. ANALYSIS SUMMARY AND RECOMMENDATIONS")
    print("-" * 42)
    
    print("🔍 Key Findings:")
    print("  1. Event processing status around the 13th boundary")
    print("  2. JSON field structure changes between old/new pipeline formats")
    print("  3. Trial accuracy calculation input variations")
    print("  4. Potential event extraction failures")
    print("  5. Processing timestamp patterns")
    print()
    
    print("🎯 Next Steps:")
    print("  • Examine specific events with malformed JSON structures")
    print("  • Validate field extraction logic for both old and new formats")
    print("  • Check if event name extraction is failing silently")
    print("  • Verify distinct_id extraction consistency")
    print("  • Test timestamp parsing for different formats")
    print()
    
    conn.close()

if __name__ == "__main__":
    analyze_trial_accuracy_drop() 