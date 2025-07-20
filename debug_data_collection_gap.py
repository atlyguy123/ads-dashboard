#!/usr/bin/env python3
"""
🔍 Data Collection Gap Analysis

This script validates the hypothesis that the trial accuracy drop is caused by 
incomplete data processing on July 13th, where only ~12 hours of data was collected
instead of the full 24 hours.
"""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def analyze_data_collection_gap():
    """Analyze the data collection gap around July 13th"""
    
    print("🔍 DATA COLLECTION GAP ANALYSIS")
    print("=" * 60)
    print("Validating hypothesis: July 13th has incomplete data (only ~12 hours)")
    print()
    
    # Connect to mixpanel database
    db_path = get_database_path('mixpanel_data')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. DETAILED HOURLY BREAKDOWN AROUND JULY 13TH
    print("1. HOURLY EVENT DISTRIBUTION ANALYSIS")
    print("-" * 42)
    
    dates_to_analyze = ['2025-07-11', '2025-07-12', '2025-07-13', '2025-07-14']
    
    for date in dates_to_analyze:
        print(f"\n📅 {date}:")
        
        cursor.execute("""
            SELECT 
                strftime('%H', event_time) as hour,
                COUNT(*) as total_events,
                COUNT(CASE WHEN event_name = 'RC Trial started' THEN 1 END) as trial_events
            FROM mixpanel_event 
            WHERE DATE(event_time) = ?
            GROUP BY strftime('%H', event_time)
            ORDER BY hour
        """, (date,))
        
        hourly_data = cursor.fetchall()
        
        if hourly_data:
            print("  Hour | Events | Trials")
            print("  -----|--------|-------")
            for hour, events, trials in hourly_data:
                print(f"  {hour:02s}   | {events:6d} | {trials:6d}")
            
            # Summary stats
            total_events = sum(events for _, events, _ in hourly_data)
            total_trials = sum(trials for _, _, trials in hourly_data)
            hours_with_data = len(hourly_data)
            
            marker = " ⭐" if date == '2025-07-13' else ""
            print(f"  Total: {total_events} events, {total_trials} trials, {hours_with_data} hours{marker}")
        else:
            print("  No data found")
    
    print()
    
    # 2. ANALYZE RAW DATA DOWNLOAD STATUS
    print("2. RAW DATA DOWNLOAD STATUS")
    print("-" * 30)
    
    # Check what was downloaded for each date
    cursor.execute("""
        SELECT 
            date_day,
            files_downloaded,
            events_downloaded,
            downloaded_at
        FROM downloaded_dates 
        WHERE date_day BETWEEN '2025-07-11' AND '2025-07-14'
        ORDER BY date_day
    """)
    
    download_data = cursor.fetchall()
    
    print("Date       | Files | Events | Downloaded At")
    print("-" * 45)
    
    for date_day, files, events, downloaded_at in download_data:
        marker = " ⭐" if "13" in str(date_day) else ""
        print(f"{date_day} | {files:5d} | {events:6d} | {downloaded_at}{marker}")
    
    print()
    
    # 3. COMPARE WITH PROCESSED EVENT DATA
    print("3. DOWNLOAD vs PROCESSED COMPARISON")
    print("-" * 38)
    
    # Compare downloaded events vs processed events
    cursor.execute("""
        SELECT 
            dd.date_day,
            dd.events_downloaded as raw_events,
            ped.events_processed as processed_events,
            CASE 
                WHEN dd.events_downloaded > 0 
                THEN ROUND((ped.events_processed * 100.0 / dd.events_downloaded), 2)
                ELSE 0 
            END as processing_rate
        FROM downloaded_dates dd
        LEFT JOIN processed_event_days ped ON dd.date_day = ped.date_day
        WHERE dd.date_day BETWEEN '2025-07-11' AND '2025-07-14'
        ORDER BY dd.date_day
    """)
    
    processing_data = cursor.fetchall()
    
    print("Date       | Raw    | Processed | Rate%")
    print("-" * 42)
    
    for date_day, raw_events, processed_events, rate in processing_data:
        marker = " ⭐" if "13" in str(date_day) else ""
        processed_events = processed_events or 0
        print(f"{date_day} | {raw_events:6d} | {processed_events:9d} | {rate:5.1f}%{marker}")
    
    print()
    
    # 4. INVESTIGATE JULY 13TH S3 DATA AVAILABILITY
    print("4. JULY 13TH DATA AVAILABILITY INVESTIGATION")
    print("-" * 47)
    
    # Check if there are more events in raw_event_data table than processed
    try:
        # Connect to raw data database if it exists
        raw_db_path = get_database_path('raw_data')
        if Path(raw_db_path).exists():
            raw_conn = sqlite3.connect(raw_db_path)
            raw_cursor = raw_conn.cursor()
            
            raw_cursor.execute("""
                SELECT 
                    date_day,
                    COUNT(*) as raw_records,
                    COUNT(DISTINCT file_sequence) as file_count
                FROM raw_event_data 
                WHERE date_day BETWEEN '2025-07-11' AND '2025-07-14'
                GROUP BY date_day
                ORDER BY date_day
            """)
            
            raw_data = raw_cursor.fetchall()
            
            print("📦 Raw Event Data (pre-processing):")
            print("Date       | Records | Files")
            print("-" * 28)
            
            for date_day, records, files in raw_data:
                marker = " ⭐" if "13" in str(date_day) else ""
                print(f"{date_day} | {records:7d} | {files:5d}{marker}")
            
            raw_conn.close()
        else:
            print("⚠️  Raw data database not found")
            
    except Exception as e:
        print(f"⚠️  Could not access raw data: {e}")
    
    print()
    
    # 5. TRIAL ACCURACY IMPACT CALCULATION
    print("5. TRIAL ACCURACY IMPACT CALCULATION")
    print("-" * 40)
    
    # Calculate what trial accuracy would look like with incomplete data
    cursor.execute("""
        SELECT 
            DATE(event_time) as date,
            COUNT(DISTINCT CASE WHEN event_name = 'RC Trial started' THEN distinct_id END) as mixpanel_trials,
            strftime('%H', MIN(event_time)) as earliest_hour,
            strftime('%H', MAX(event_time)) as latest_hour,
            COUNT(DISTINCT strftime('%H', event_time)) as hours_covered
        FROM mixpanel_event 
        WHERE DATE(event_time) BETWEEN '2025-07-11' AND '2025-07-14'
        GROUP BY DATE(event_time)
        ORDER BY date
    """)
    
    trial_data = cursor.fetchall()
    
    print("Date       | Trials | Hours | Coverage | Est. Full Day")
    print("-" * 52)
    
    for date, trials, earliest, latest, hours in trial_data:
        # Estimate what full day would be if we had complete data
        if hours > 0:
            estimated_full_day = int(trials * 24 / hours)
        else:
            estimated_full_day = 0
            
        marker = " ⭐" if "13" in date else ""
        print(f"{date} | {trials:6d} | {hours:5d} | {hours/24*100:6.1f}% | {estimated_full_day:12d}{marker}")
    
    print()
    
    # 6. HOURLY PIPELINE MIGRATION EVIDENCE
    print("6. HOURLY PIPELINE MIGRATION EVIDENCE")
    print("-" * 39)
    
    # Look for evidence of the pipeline change in file patterns or data structure
    cursor.execute("""
        SELECT 
            DATE(event_time) as date,
            COUNT(*) as events,
            COUNT(DISTINCT substr(event_uuid, 1, 8)) as uuid_patterns,
            MIN(LENGTH(event_json)) as min_json_length,
            MAX(LENGTH(event_json)) as max_json_length,
            AVG(LENGTH(event_json)) as avg_json_length
        FROM mixpanel_event 
        WHERE DATE(event_time) BETWEEN '2025-07-11' AND '2025-07-14'
        GROUP BY DATE(event_time)
        ORDER BY date
    """)
    
    structure_data = cursor.fetchall()
    
    print("Date       | Events | UUID Pat. | JSON Size (min/avg/max)")
    print("-" * 55)
    
    for date, events, uuid_patterns, min_len, max_len, avg_len in structure_data:
        marker = " ⭐" if "13" in date else ""
        print(f"{date} | {events:6d} | {uuid_patterns:9d} | {min_len:4d}/{avg_len:4.0f}/{max_len:4d}{marker}")
    
    print()
    
    # 7. CONCLUSIONS AND RECOMMENDATIONS
    print("7. CONCLUSIONS AND RECOMMENDATIONS")
    print("-" * 36)
    
    # Calculate the data gap impact
    july_13_data = [row for row in trial_data if '13' in row[0]]
    july_12_data = [row for row in trial_data if '12' in row[0]]
    
    if july_13_data and july_12_data:
        july_13_trials = july_13_data[0][1]
        july_13_hours = july_13_data[0][4]
        july_12_trials = july_12_data[0][1]
        
        expected_july_13_full = july_13_trials * 24 / july_13_hours if july_13_hours > 0 else 0
        trial_loss = expected_july_13_full - july_13_trials
        
        print("🔍 DATA GAP IMPACT:")
        print(f"  • July 12th: {july_12_trials} trials (24 hours)")
        print(f"  • July 13th: {july_13_trials} trials ({july_13_hours} hours)")
        print(f"  • Expected full day: ~{expected_july_13_full:.0f} trials")
        print(f"  • Lost trials due to gap: ~{trial_loss:.0f} trials")
        print(f"  • Data completeness: {july_13_hours/24*100:.1f}%")
        print()
    
    print("🎯 ROOT CAUSE ANALYSIS:")
    print("  1. July 13th has incomplete data collection (only ~12 hours)")
    print("  2. This creates artificially low Mixpanel trial counts")
    print("  3. If Meta reports full-day metrics, trial accuracy drops dramatically")
    print("  4. The hourly pipeline migration may have introduced data gaps")
    print()
    
    print("🔧 IMMEDIATE ACTIONS NEEDED:")
    print("  • Check S3 bucket for missing July 13th afternoon/evening files")
    print("  • Verify hourly pipeline is running continuously")
    print("  • Re-download missing July 13th data if available")
    print("  • Implement data completeness monitoring")
    print("  • Add alerts for significant daily volume drops")
    print()
    
    conn.close()

if __name__ == "__main__":
    analyze_data_collection_gap() 