#!/usr/bin/env python3
"""
10x Detective Data Verification Script V2
Mission-critical verification using modified download pipeline with debug mode.

This script will:
1. Enable debug mode in download pipeline
2. Run full 90-day download with JSON file saving
3. Compare raw database with saved JSON files
4. Ensure 100% alignment using SAME S3 source
"""

import json
import sqlite3
import hashlib
import os
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import time
import subprocess
import sys

@dataclass
class EventRecord:
    """Standardized event record for comparison"""
    insert_id: str
    distinct_id: str
    event_name: str
    timestamp: int
    event_json: str
    source: str  # 'json', 'raw'
    file_path: Optional[str] = None
    db_id: Optional[str] = None

def clear_existing_data():
    """Clear existing data to start fresh"""
    print("🧹 Clearing existing data...")
    
    # Clear raw database
    raw_db_path = Path("database/raw_data.db")
    if raw_db_path.exists():
        print(f"Removing raw database: {raw_db_path}")
        raw_db_path.unlink()
    
    # Clear mixpanel database  
    mixpanel_db_path = Path("database/mixpanel_data.db")
    if mixpanel_db_path.exists():
        print(f"Removing mixpanel database: {mixpanel_db_path}")
        mixpanel_db_path.unlink()
    
    # Clear events folder
    events_dir = Path("data/events")
    if events_dir.exists():
        print(f"Removing events directory: {events_dir}")
        shutil.rmtree(events_dir)
    
    print("✅ All existing data cleared")

def enable_debug_mode():
    """Enable debug mode by setting environment variable"""
    print("🔍 Enabling debug mode for JSON file saving...")
    os.environ['DEBUG_SAVE_JSON_FILES'] = 'true'
    print("✅ Debug mode enabled - JSON files will be saved during download")

def run_full_download():
    """Run the full 90-day download with debug mode enabled"""
    print("⬇️  Running full 90-day S3 download with debug mode...")
    
    # Run the download pipeline
    download_script = "pipelines/mixpanel_pipeline/01_download_update_data.py"
    if not Path(download_script).exists():
        raise FileNotFoundError(f"Download script not found: {download_script}")
    
    print(f"Running: {download_script}")
    print("⏱️  This will take 30-60 minutes for 90 days of data...")
    
    # Run with real-time output
    process = subprocess.Popen(
        [sys.executable, download_script], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # Stream output in real-time
    for line in iter(process.stdout.readline, ''):
        if line:
            print(f"📥 {line.rstrip()}")
    
    process.wait()
    
    if process.returncode != 0:
        raise RuntimeError("S3 download failed")
    
    print("✅ Full S3 download completed successfully")

def load_json_events() -> Dict[str, EventRecord]:
    """Load all events from JSON files saved during download"""
    print("📖 Loading JSON events from debug files...")
    
    events = {}
    events_dir = Path("data/events")
    
    if not events_dir.exists():
        raise FileNotFoundError("Events directory not found. Download failed.")
    
    for json_file in events_dir.rglob("*.json"):
        print(f"Reading {json_file}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event_data = json.loads(line)
                    insert_id = event_data.get('insert_id')
                    if not insert_id:
                        continue
                    
                    events[insert_id] = EventRecord(
                        insert_id=insert_id,
                        distinct_id=event_data.get('distinct_id', ''),
                        event_name=event_data.get('event_name') or event_data.get('event', ''),
                        timestamp=event_data.get('time', 0),
                        event_json=line,
                        source='json',
                        file_path=str(json_file)
                    )
                except json.JSONDecodeError:
                    print(f"Warning: Invalid JSON at {json_file}:{line_num}")
                    continue
    
    print(f"✅ Loaded {len(events)} events from JSON files")
    return events

def load_raw_database_events() -> Dict[str, EventRecord]:
    """Load all RC events from raw database"""
    print("📖 Loading raw database events...")
    
    raw_db_path = Path("database/raw_data.db")
    if not raw_db_path.exists():
        raise FileNotFoundError("Raw database not found. Download failed.")
    
    events = {}
    conn = sqlite3.connect(str(raw_db_path))
    cursor = conn.cursor()
    
    # Get all RC events from raw database
    cursor.execute("""
        SELECT id, date_day, event_data 
        FROM raw_event_data 
        WHERE json_extract(event_data, '$.event_name') LIKE 'RC%'
           OR json_extract(event_data, '$.event') LIKE 'RC%'
    """)
    
    for row in cursor.fetchall():
        db_id, date_day, event_json = row
        try:
            event_data = json.loads(event_json)
            insert_id = event_data.get('insert_id')
            if not insert_id:
                continue
            
            events[insert_id] = EventRecord(
                insert_id=insert_id,
                distinct_id=event_data.get('distinct_id', ''),
                event_name=event_data.get('event_name') or event_data.get('event', ''),
                timestamp=event_data.get('time', 0),
                event_json=event_json,
                source='raw',
                db_id=str(db_id)
            )
        except json.JSONDecodeError:
            continue
    
    conn.close()
    print(f"✅ Loaded {len(events)} events from raw database")
    return events

def compare_events(json_events: Dict[str, EventRecord], 
                  raw_events: Dict[str, EventRecord]) -> Dict[str, any]:
    """Compare events between JSON files and raw database"""
    print("🔍 Comparing events...")
    
    json_ids = set(json_events.keys())
    raw_ids = set(raw_events.keys())
    
    # Find differences
    only_in_json = json_ids - raw_ids
    only_in_raw = raw_ids - json_ids
    in_both = json_ids & raw_ids
    
    # Check for data corruption in matching events
    corrupted_events = []
    for insert_id in in_both:
        json_event = json_events[insert_id]
        raw_event = raw_events[insert_id]
        
        # Compare JSON hashes
        json_hash = hashlib.md5(json_event.event_json.encode()).hexdigest()
        raw_hash = hashlib.md5(raw_event.event_json.encode()).hexdigest()
        
        if json_hash != raw_hash:
            corrupted_events.append({
                'insert_id': insert_id,
                'json_hash': json_hash,
                'raw_hash': raw_hash,
                'json_event': json_event,
                'raw_event': raw_event
            })
    
    return {
        'total_json': len(json_events),
        'total_raw': len(raw_events),
        'only_in_json': len(only_in_json),
        'only_in_raw': len(only_in_raw),
        'in_both': len(in_both),
        'corrupted_events': corrupted_events,
        'corruption_count': len(corrupted_events),
        'perfect_match': len(in_both) == len(json_events) == len(raw_events) and len(corrupted_events) == 0,
        'only_in_json_list': list(only_in_json)[:10],  # First 10 for analysis
        'only_in_raw_list': list(only_in_raw)[:10]     # First 10 for analysis
    }

def analyze_discrepancies(json_events: Dict[str, EventRecord], 
                         raw_events: Dict[str, EventRecord],
                         comparison: Dict[str, any]) -> Dict[str, any]:
    """Analyze potential causes of discrepancies"""
    print("🔬 Analyzing discrepancies...")
    
    analysis = {
        'date_distribution_json': defaultdict(int),
        'date_distribution_raw': defaultdict(int),
        'event_type_distribution_json': defaultdict(int),
        'event_type_distribution_raw': defaultdict(int)
    }
    
    # Analyze JSON events
    for event in json_events.values():
        dt = datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
        date_str = dt.strftime('%Y-%m-%d')
        analysis['date_distribution_json'][date_str] += 1
        analysis['event_type_distribution_json'][event.event_name] += 1
    
    # Analyze raw events
    for event in raw_events.values():
        dt = datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
        date_str = dt.strftime('%Y-%m-%d')
        analysis['date_distribution_raw'][date_str] += 1
        analysis['event_type_distribution_raw'][event.event_name] += 1
    
    return analysis

def main():
    """Main detective verification process"""
    print("🕵️  === 10x Detective Data Verification V2 ===")
    print("Mission: Verify 100% alignment using SAME S3 source")
    print("Strategy: Enable debug mode for JSON saving during download")
    print("=" * 60)
    
    try:
        # Step 1: Clear existing data
        clear_existing_data()
        
        # Step 2: Enable debug mode
        enable_debug_mode()
        
        # Step 3: Run full 90-day download
        run_full_download()
        
        # Step 4: Load events from both sources
        json_events = load_json_events()
        raw_events = load_raw_database_events()
        
        # Step 5: Compare events
        comparison = compare_events(json_events, raw_events)
        
        # Step 6: Analyze discrepancies
        analysis = analyze_discrepancies(json_events, raw_events, comparison)
        
        # Step 7: Report findings
        print("\n" + "=" * 60)
        print("🔍 === DETECTIVE FINDINGS ===")
        print("=" * 60)
        
        print(f"\n📊 Event Counts:")
        print(f"  JSON files: {comparison['total_json']:,} events")
        print(f"  Raw database: {comparison['total_raw']:,} events")
        print(f"  Events in both: {comparison['in_both']:,} events")
        print(f"  Only in JSON: {comparison['only_in_json']:,} events")
        print(f"  Only in raw: {comparison['only_in_raw']:,} events")
        
        print(f"\n🔍 Data Integrity:")
        print(f"  Corrupted events: {comparison['corruption_count']:,} events")
        print(f"  Perfect match: {'✅ YES' if comparison['perfect_match'] else '❌ NO'}")
        
        if comparison['corruption_count'] > 0:
            print(f"\n⚠️  CORRUPTION DETAILS (first 5):")
            for i, corruption in enumerate(comparison['corrupted_events'][:5]):
                print(f"  {i+1}. Insert ID: {corruption['insert_id']}")
                print(f"     JSON Hash: {corruption['json_hash'][:8]}...")
                print(f"     Raw Hash:  {corruption['raw_hash'][:8]}...")
        
        print(f"\n📅 Date Distribution Analysis:")
        json_dates = sorted(analysis['date_distribution_json'].items())
        raw_dates = sorted(analysis['date_distribution_raw'].items())
        
        print(f"  JSON files date range: {json_dates[0][0] if json_dates else 'None'} to {json_dates[-1][0] if json_dates else 'None'}")
        print(f"  Raw database date range: {raw_dates[0][0] if raw_dates else 'None'} to {raw_dates[-1][0] if raw_dates else 'None'}")
        
        # Final verdict
        print(f"\n🎯 === FINAL VERDICT ===")
        if comparison['perfect_match']:
            print("✅ PERFECT ALIGNMENT: JSON files and raw database are 100% identical")
            print("🎉 Data pipeline integrity VERIFIED!")
        else:
            print("❌ ALIGNMENT ISSUES DETECTED:")
            if comparison['only_in_json'] > 0:
                print(f"  - {comparison['only_in_json']} events missing from raw database")
                print(f"  - Sample missing IDs: {comparison['only_in_json_list']}")
            if comparison['only_in_raw'] > 0:
                print(f"  - {comparison['only_in_raw']} extra events in raw database")
                print(f"  - Sample extra IDs: {comparison['only_in_raw_list']}")
            if comparison['corruption_count'] > 0:
                print(f"  - {comparison['corruption_count']} events have data corruption")
        
        return comparison['perfect_match']
        
    except Exception as e:
        print(f"❌ Detective verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 