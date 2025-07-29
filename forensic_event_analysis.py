#!/usr/bin/env python3
"""
🕵️ FORENSIC EVENT ANALYSIS - DETECTIVE MODE
Critical business issue: 31.7% data loss in pipeline

This script performs deep forensic analysis to identify patterns in:
- ✅ 28 successful events (found in pipeline)  
- ❌ 13 missing events (not in pipeline)

Goal: Find ROOT CAUSE of data gaps
"""

import sqlite3
import csv
from pathlib import Path
import sys
from datetime import datetime, timedelta
import json
from collections import defaultdict, Counter
import re

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Main forensic analysis process"""
    
    print("🕵️ FORENSIC EVENT ANALYSIS - DETECTIVE MODE")
    print("=" * 70)
    print("🚨 CRITICAL BUSINESS ISSUE: 31.7% pipeline data loss")
    print("📅 July 16-29, 2025 | Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25")
    print()
    
    # Load the evidence (CSV events and pipeline results)
    csv_events = read_csv_events()
    if not csv_events:
        return 1
    
    # Get pipeline status for each event
    pipeline_results = get_pipeline_status(csv_events)
    
    # Separate successful vs missing events
    successful_events = [e for e in pipeline_results if e['in_pipeline']]
    missing_events = [e for e in pipeline_results if not e['in_pipeline']]
    
    print(f"📊 EVIDENCE SUMMARY:")
    print(f"   🎯 Total Events: {len(csv_events)}")
    print(f"   ✅ Successful: {len(successful_events)} ({len(successful_events)/len(csv_events)*100:.1f}%)")
    print(f"   ❌ Missing: {len(missing_events)} ({len(missing_events)/len(csv_events)*100:.1f}%)")
    print()
    
    # FORENSIC ANALYSIS SECTIONS
    analyze_temporal_patterns(successful_events, missing_events)
    analyze_device_patterns(successful_events, missing_events)
    analyze_user_patterns(successful_events, missing_events)
    analyze_geographic_patterns(successful_events, missing_events)
    analyze_technical_patterns(successful_events, missing_events)
    identify_critical_gaps(missing_events)
    generate_actionable_recommendations(missing_events, successful_events)
    
    return 0

def read_csv_events():
    """Read events from CSV with full metadata"""
    events = []
    
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if all(key in row for key in ['Insert ID', 'Time', 'Distinct ID']):
                    events.append({
                        'insert_id': row['Insert ID'].strip(),
                        'timestamp': row['Time'].strip(),
                        'distinct_id': row['Distinct ID'].strip(),
                        'campaign_id': row['abi_~campaign_id'].strip(),
                        'campaign_name': row['abi_~campaign'].strip(),
                        'user_id': row.get('User ID', '').strip()
                    })
        return events
    except Exception as e:
        print(f"❌ CRITICAL: Cannot load evidence CSV: {e}")
        return []

def get_pipeline_status(csv_events):
    """Determine which events made it through pipeline"""
    results = []
    
    try:
        with sqlite3.connect(get_database_path('raw_data')) as raw_conn:
            raw_cursor = raw_conn.cursor()
            
            with sqlite3.connect(get_database_path('mixpanel_data')) as processed_conn:
                processed_cursor = processed_conn.cursor()
                
                for event in csv_events:
                    # Check raw database
                    in_raw = check_event_in_raw(raw_cursor, event)
                    
                    # Check processed database
                    in_processed = check_event_in_processed(processed_cursor, event)
                    
                    results.append({
                        **event,
                        'in_raw': bool(in_raw),
                        'in_processed': bool(in_processed),
                        'in_pipeline': bool(in_processed),  # Final success metric
                        'raw_data': in_raw,
                        'processed_data': in_processed
                    })
        
        return results
        
    except Exception as e:
        print(f"❌ CRITICAL: Pipeline status check failed: {e}")
        return []

def check_event_in_raw(cursor, event):
    """Check if event exists in raw database"""
    try:
        cursor.execute("""
            SELECT event_data FROM raw_event_data 
            WHERE event_data LIKE ? 
            LIMIT 1
        """, [f'%{event["insert_id"]}%'])
        
        result = cursor.fetchone()
        if result:
            try:
                event_data = json.loads(result[0])
                if (event_data.get('properties', {}).get('$insert_id') == event['insert_id']):
                    return event_data
            except json.JSONDecodeError:
                pass
        return None
    except Exception:
        return None

def check_event_in_processed(cursor, event):
    """Check if event exists in processed database"""
    try:
        cursor.execute("""
            SELECT * FROM mixpanel_event 
            WHERE distinct_id = ? 
            AND event_name = 'RC Trial started'
            AND event_json LIKE ?
            LIMIT 1
        """, [event['distinct_id'], f'%{event["insert_id"]}%'])
        
        result = cursor.fetchone()
        return result if result else None
    except Exception:
        return None

def analyze_temporal_patterns(successful, missing):
    """🕐 TEMPORAL FORENSICS - When do failures occur?"""
    print("🕐 TEMPORAL FORENSICS ANALYSIS")
    print("-" * 50)
    
    # Parse timestamps and analyze patterns
    successful_times = []
    missing_times = []
    
    for event in successful:
        try:
            dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            successful_times.append(dt)
        except:
            continue
    
    for event in missing:
        try:
            dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            missing_times.append(dt)
        except:
            continue
    
    # Date analysis
    successful_dates = Counter([dt.date() for dt in successful_times])
    missing_dates = Counter([dt.date() for dt in missing_times])
    
    print("📅 DATE DISTRIBUTION:")
    all_dates = sorted(set(list(successful_dates.keys()) + list(missing_dates.keys())))
    
    for date in all_dates:
        s_count = successful_dates.get(date, 0)
        m_count = missing_dates.get(date, 0)
        total = s_count + m_count
        success_rate = (s_count / total * 100) if total > 0 else 0
        
        status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
        print(f"   {status} {date}: {s_count}✅ {m_count}❌ ({success_rate:.1f}% success)")
    
    # Hour analysis
    successful_hours = Counter([dt.hour for dt in successful_times])
    missing_hours = Counter([dt.hour for dt in missing_times])
    
    print("\n🕐 HOURLY DISTRIBUTION:")
    problem_hours = []
    
    for hour in range(24):
        s_count = successful_hours.get(hour, 0)
        m_count = missing_hours.get(hour, 0)
        total = s_count + m_count
        
        if total > 0:
            success_rate = (s_count / total * 100)
            status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
            
            if success_rate < 60:
                problem_hours.append(hour)
            
            print(f"   {status} {hour:02d}:00: {s_count}✅ {m_count}❌ ({success_rate:.1f}%)")
    
    # Critical findings
    if problem_hours:
        print(f"\n🚨 CRITICAL: Problem hours detected: {problem_hours}")
        print(f"   💡 HYPOTHESIS: Pipeline may have gaps during these hours")
    
    print()

def analyze_device_patterns(successful, missing):
    """📱 DEVICE FORENSICS - Device type correlation"""
    print("📱 DEVICE FORENSICS ANALYSIS")
    print("-" * 50)
    
    def extract_device_type(distinct_id):
        """Extract device information from distinct_id"""
        if distinct_id.startswith('$device:'):
            return 'iOS_Device'
        elif '$device:' in distinct_id:
            return 'iOS_Device'
        elif re.match(r'^[a-zA-Z0-9]{10,}$', distinct_id):
            return 'Android_UserID'
        elif '@' in distinct_id:
            return 'Email_Based'
        else:
            return 'Other'
    
    successful_devices = Counter([extract_device_type(e['distinct_id']) for e in successful])
    missing_devices = Counter([extract_device_type(e['distinct_id']) for e in missing])
    
    print("📊 DEVICE TYPE ANALYSIS:")
    all_device_types = set(list(successful_devices.keys()) + list(missing_devices.keys()))
    
    critical_devices = []
    
    for device_type in sorted(all_device_types):
        s_count = successful_devices.get(device_type, 0)
        m_count = missing_devices.get(device_type, 0)
        total = s_count + m_count
        success_rate = (s_count / total * 100) if total > 0 else 0
        
        status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
        
        if success_rate < 60:
            critical_devices.append(device_type)
        
        print(f"   {status} {device_type}: {s_count}✅ {m_count}❌ ({success_rate:.1f}% success)")
    
    if critical_devices:
        print(f"\n🚨 CRITICAL: Device types with poor success rates: {critical_devices}")
        print(f"   💡 HYPOTHESIS: Pipeline may have device-specific issues")
    
    print()

def analyze_user_patterns(successful, missing):
    """👤 USER FORENSICS - User characteristic patterns"""
    print("👤 USER FORENSICS ANALYSIS")  
    print("-" * 50)
    
    # Analyze User ID vs Distinct ID patterns
    successful_has_user_id = sum(1 for e in successful if e['user_id'])
    missing_has_user_id = sum(1 for e in missing if e['user_id'])
    
    print("🆔 USER ID ANALYSIS:")
    print(f"   ✅ Successful with User ID: {successful_has_user_id}/{len(successful)} ({successful_has_user_id/len(successful)*100:.1f}%)")
    print(f"   ❌ Missing with User ID: {missing_has_user_id}/{len(missing)} ({missing_has_user_id/len(missing)*100:.1f}%)")
    
    # ID length analysis (could indicate different ID schemes)
    successful_id_lengths = Counter([len(e['distinct_id']) for e in successful])
    missing_id_lengths = Counter([len(e['distinct_id']) for e in missing])
    
    print("\n📏 DISTINCT ID LENGTH ANALYSIS:")
    all_lengths = sorted(set(list(successful_id_lengths.keys()) + list(missing_id_lengths.keys())))
    
    for length in all_lengths:
        s_count = successful_id_lengths.get(length, 0)
        m_count = missing_id_lengths.get(length, 0)
        total = s_count + m_count
        success_rate = (s_count / total * 100) if total > 0 else 0
        
        status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
        print(f"   {status} Length {length}: {s_count}✅ {m_count}❌ ({success_rate:.1f}%)")
    
    print()

def analyze_geographic_patterns(successful, missing):
    """🌍 GEOGRAPHIC FORENSICS - Regional patterns"""
    print("🌍 GEOGRAPHIC FORENSICS ANALYSIS")
    print("-" * 50)
    
    # This would require looking up the events in processed DB to get country/region
    # For now, we'll analyze timestamp patterns that might indicate timezone/region
    
    successful_times = []
    missing_times = []
    
    for event in successful:
        try:
            dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            successful_times.append(dt.hour)
        except:
            continue
    
    for event in missing:
        try:
            dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            missing_times.append(dt.hour)
        except:
            continue
    
    # Timezone inference based on activity patterns
    print("🕐 TIMEZONE ACTIVITY INFERENCE:")
    print("   (Based on event timing patterns)")
    
    # US business hours: 9-17 EST (14-22 UTC)
    # Europe business hours: 9-17 CET (8-16 UTC)  
    # Asia business hours: 9-17 JST (0-8 UTC)
    
    us_hours = list(range(14, 23))  # 9 AM - 5 PM EST in UTC
    eu_hours = list(range(8, 17))   # 9 AM - 5 PM CET in UTC
    asia_hours = list(range(0, 9))  # 9 AM - 5 PM JST in UTC
    
    successful_us = sum(1 for h in successful_times if h in us_hours)
    successful_eu = sum(1 for h in successful_times if h in eu_hours)  
    successful_asia = sum(1 for h in successful_times if h in asia_hours)
    
    missing_us = sum(1 for h in missing_times if h in us_hours)
    missing_eu = sum(1 for h in missing_times if h in eu_hours)
    missing_asia = sum(1 for h in missing_times if h in asia_hours)
    
    regions = [
        ("US (EST)", successful_us, missing_us),
        ("Europe (CET)", successful_eu, missing_eu), 
        ("Asia (JST)", successful_asia, missing_asia)
    ]
    
    for region, s_count, m_count in regions:
        total = s_count + m_count
        if total > 0:
            success_rate = (s_count / total * 100)
            status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
            print(f"   {status} {region}: {s_count}✅ {m_count}❌ ({success_rate:.1f}%)")
    
    print()

def analyze_technical_patterns(successful, missing):
    """⚙️ TECHNICAL FORENSICS - Technical characteristics"""
    print("⚙️ TECHNICAL FORENSICS ANALYSIS")
    print("-" * 50)
    
    # Insert ID pattern analysis
    print("🔗 INSERT ID PATTERN ANALYSIS:")
    
    successful_insert_patterns = []
    missing_insert_patterns = []
    
    for event in successful:
        insert_id = event['insert_id']
        # Analyze UUID format and characteristics
        if len(insert_id) == 36 and insert_id.count('-') == 4:
            successful_insert_patterns.append('standard_uuid')
        else:
            successful_insert_patterns.append('non_standard')
    
    for event in missing:
        insert_id = event['insert_id']
        if len(insert_id) == 36 and insert_id.count('-') == 4:
            missing_insert_patterns.append('standard_uuid')
        else:
            missing_insert_patterns.append('non_standard')
    
    successful_patterns = Counter(successful_insert_patterns)
    missing_patterns = Counter(missing_insert_patterns)
    
    for pattern in set(list(successful_patterns.keys()) + list(missing_patterns.keys())):
        s_count = successful_patterns.get(pattern, 0)
        m_count = missing_patterns.get(pattern, 0)
        total = s_count + m_count
        success_rate = (s_count / total * 100) if total > 0 else 0
        
        status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
        print(f"   {status} {pattern}: {s_count}✅ {m_count}❌ ({success_rate:.1f}%)")
    
    # Timestamp precision analysis
    print("\n⏰ TIMESTAMP PRECISION ANALYSIS:")
    
    successful_precisions = []
    missing_precisions = []
    
    for event in successful:
        ts = event['timestamp']
        if '.' in ts:
            successful_precisions.append('microseconds')
        else:
            successful_precisions.append('seconds')
    
    for event in missing:
        ts = event['timestamp']
        if '.' in ts:
            missing_precisions.append('microseconds')
        else:
            missing_precisions.append('seconds')
    
    s_precision_counts = Counter(successful_precisions)
    m_precision_counts = Counter(missing_precisions)
    
    for precision in set(list(s_precision_counts.keys()) + list(m_precision_counts.keys())):
        s_count = s_precision_counts.get(precision, 0)
        m_count = m_precision_counts.get(precision, 0)
        total = s_count + m_count
        success_rate = (s_count / total * 100) if total > 0 else 0
        
        status = "🟢" if success_rate >= 80 else "🟡" if success_rate >= 60 else "🔴"
        print(f"   {status} {precision}: {s_count}✅ {m_count}❌ ({success_rate:.1f}%)")
    
    print()

def identify_critical_gaps(missing_events):
    """🔍 CRITICAL GAP IDENTIFICATION"""
    print("🔍 CRITICAL GAP IDENTIFICATION")
    print("-" * 50)
    
    print("❌ DETAILED MISSING EVENT ANALYSIS:")
    
    for i, event in enumerate(missing_events, 1):
        dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        device_type = 'iOS' if '$device:' in event['distinct_id'] else 'Android/Other'
        
        print(f"\n   {i:2d}. Missing Event:")
        print(f"       📅 Date/Time: {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"       🔗 Insert ID: {event['insert_id']}")
        print(f"       📱 Device: {device_type}")
        print(f"       🆔 Distinct ID: {event['distinct_id'][:40]}...")
        print(f"       👤 User ID: {event['user_id'][:20]}..." if event['user_id'] else "       👤 User ID: None")
    
    # Find patterns in missing events
    missing_dates = [datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).date() 
                    for e in missing_events]
    missing_date_counts = Counter(missing_dates)
    
    print(f"\n📊 MISSING EVENT DATE CLUSTERS:")
    for date, count in missing_date_counts.most_common():
        print(f"   🔴 {date}: {count} missing events")
    
    print()

def generate_actionable_recommendations(missing_events, successful_events):
    """💡 ACTIONABLE RECOMMENDATIONS"""
    print("💡 ACTIONABLE RECOMMENDATIONS")
    print("=" * 50)
    
    print("🎯 IMMEDIATE ACTION ITEMS:")
    
    # Date-based recommendations
    missing_dates = [datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).date() 
                    for e in missing_events]
    missing_date_counts = Counter(missing_dates)
    
    if missing_date_counts:
        worst_date = missing_date_counts.most_common(1)[0]
        print(f"   1. 🔍 INVESTIGATE {worst_date[0]} - {worst_date[1]} missing events")
        print(f"      → Check S3 data availability for this date")
        print(f"      → Verify download pipeline ran successfully")
        print(f"      → Review logs for errors on this date")
    
    # Device-based recommendations  
    ios_missing = sum(1 for e in missing_events if '$device:' in e['distinct_id'])
    android_missing = len(missing_events) - ios_missing
    
    if ios_missing > android_missing:
        print(f"   2. 📱 iOS DEVICE ISSUE - {ios_missing} iOS events missing")
        print(f"      → Check iOS-specific data collection")
        print(f"      → Verify device ID handling for $device: format")
    elif android_missing > ios_missing:
        print(f"   2. 📱 ANDROID DEVICE ISSUE - {android_missing} Android events missing")
        print(f"      → Check Android user ID collection")
        print(f"      → Verify non-device ID handling")
    
    # Technical recommendations
    print(f"   3. 🔧 TECHNICAL VALIDATION")
    print(f"      → Run pipeline for missing event dates")
    print(f"      → Check raw S3 files manually for missing Insert IDs")
    print(f"      → Verify event filtering logic in ingestion")
    
    print(f"\n🚨 CRITICAL BUSINESS IMPACT:")
    print(f"   📊 Data Loss: {len(missing_events)}/{len(missing_events) + len(successful_events)} events ({len(missing_events)/(len(missing_events) + len(successful_events))*100:.1f}%)")
    print(f"   💰 Revenue Impact: Potential revenue attribution missing")
    print(f"   📈 Analytics Impact: Dashboards showing {len(successful_events)} instead of {len(missing_events) + len(successful_events)}")
    
    print(f"\n✅ NEXT STEPS:")
    print(f"   1. Fix data gaps for dates with missing events")
    print(f"   2. Implement monitoring for real-time data completeness")
    print(f"   3. Add event-level reconciliation alerts")
    print(f"   4. Backfill missing events once root cause is fixed")

if __name__ == "__main__":
    exit(main()) 