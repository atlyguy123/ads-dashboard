#!/usr/bin/env python3
"""
🔍 VALIDATE RAW DATABASE
Check CSV users and events in Raw Database and update the markdown report
"""

import json
import csv
import sqlite3
import sys
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def read_csv_data():
    """Read CSV data"""
    csv_data = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data.append({
                'insert_id': row['Insert ID'],
                'distinct_id': row['Distinct ID'], 
                'user_id': row['User ID'],
                'time': row['Time'],
                'date': row['Time'][:10]
            })
    return csv_data

def check_users_in_raw_db(csv_data):
    """Check users in raw database"""
    print("🔍 CHECKING USERS IN RAW DATABASE")
    print("=" * 60)
    
    raw_db_path = get_database_path("raw_data")
    conn = sqlite3.connect(raw_db_path)
    cursor = conn.cursor()
    
    # Get all users from raw database
    cursor.execute("SELECT DISTINCT distinct_id FROM raw_user_data")
    raw_users = {row[0] for row in cursor.fetchall()}
    
    print(f"📊 Total users in raw DB: {len(raw_users)}")
    
    # Check each CSV user
    csv_users_in_raw = []
    csv_users_missing_raw = []
    
    for item in csv_data:
        distinct_id = item['distinct_id']
        if distinct_id in raw_users:
            csv_users_in_raw.append(item)
            print(f"✅ FOUND: {distinct_id}")
        else:
            csv_users_missing_raw.append(item)
            print(f"❌ MISSING: {distinct_id}")
    
    print(f"\n📊 CSV users found in raw DB: {len(csv_users_in_raw)}/41")
    print(f"📊 CSV users missing from raw DB: {len(csv_users_missing_raw)}/41")
    
    conn.close()
    return csv_users_in_raw, csv_users_missing_raw

def check_events_in_raw_db(csv_data):
    """Check events in raw database"""
    print("\n🔍 CHECKING EVENTS IN RAW DATABASE")
    print("=" * 60)
    
    raw_db_path = get_database_path("raw_data")
    conn = sqlite3.connect(raw_db_path)
    cursor = conn.cursor()
    
    # Get all insert_ids from raw database
    raw_events = set()
    cursor.execute("SELECT event_data FROM raw_event_data")
    for row in cursor.fetchall():
        try:
            event_data = json.loads(row[0])
            insert_id = event_data.get('insert_id')
            if insert_id:
                raw_events.add(insert_id)
        except json.JSONDecodeError:
            continue
    
    print(f"📊 Total events in raw DB: {len(raw_events)}")
    
    # Check each CSV event
    csv_events_in_raw = []
    csv_events_missing_raw = []
    
    for item in csv_data:
        insert_id = item['insert_id']
        if insert_id in raw_events:
            csv_events_in_raw.append(item)
            print(f"✅ FOUND: {insert_id} → {item['distinct_id']}")
        else:
            csv_events_missing_raw.append(item)
            print(f"❌ MISSING: {insert_id} → {item['distinct_id']}")
    
    print(f"\n📊 CSV events found in raw DB: {len(csv_events_in_raw)}/41")
    print(f"📊 CSV events missing from raw DB: {len(csv_events_missing_raw)}/41")
    
    conn.close()
    return csv_events_in_raw, csv_events_missing_raw

def check_complete_pairs_raw(csv_users_in_raw, csv_events_in_raw):
    """Check complete pairs in raw database"""
    print("\n🔍 CHECKING COMPLETE PAIRS IN RAW DATABASE")
    print("=" * 60)
    
    # Create sets for lookup
    users_in_raw_ids = {item['distinct_id'] for item in csv_users_in_raw}
    events_in_raw_ids = {item['insert_id'] for item in csv_events_in_raw}
    
    csv_data = read_csv_data()
    complete_pairs_raw = []
    incomplete_pairs_raw = []
    
    for item in csv_data:
        user_in_raw = item['distinct_id'] in users_in_raw_ids
        event_in_raw = item['insert_id'] in events_in_raw_ids
        
        if user_in_raw and event_in_raw:
            complete_pairs_raw.append(item)
            print(f"✅ COMPLETE: {item['distinct_id']} ↔ {item['insert_id']}")
        else:
            incomplete_pairs_raw.append(item)
            status = []
            if not user_in_raw:
                status.append("❌USER")
            if not event_in_raw:
                status.append("❌EVENT")
            print(f"❌ INCOMPLETE: {item['distinct_id']} ↔ {item['insert_id']} ({' + '.join(status)})")
    
    print(f"\n📊 Complete pairs in raw DB: {len(complete_pairs_raw)}/41")
    print(f"📊 Incomplete pairs in raw DB: {len(incomplete_pairs_raw)}/41")
    
    return complete_pairs_raw, incomplete_pairs_raw

def update_markdown_report(users_in_raw, users_missing_raw, events_in_raw, events_missing_raw, 
                          complete_pairs_raw, incomplete_pairs_raw):
    """Update the markdown report with raw database results"""
    
    # Read the current report
    with open('COMPLETE_DATA_VALIDATION_REPORT.md', 'r') as f:
        content = f.read()
    
    # Create raw database section
    raw_db_section = f"""
### **USERS IN RAW DATABASE ({len(users_in_raw)}/41)**
The following {len(users_in_raw)} users from the CSV were found in Raw Database:

"""
    
    for i, user in enumerate(users_in_raw, 1):
        raw_db_section += f"{i}. `{user['distinct_id']}`\n"
    
    if users_missing_raw:
        raw_db_section += f"\n#### **❌ USERS MISSING FROM RAW DATABASE ({len(users_missing_raw)}/41)**\n"
        for i, user in enumerate(users_missing_raw, 1):
            raw_db_section += f"{i}. `{user['distinct_id']}`\n"
    
    raw_db_section += f"""

### **EVENTS IN RAW DATABASE ({len(events_in_raw)}/41)**
The following {len(events_in_raw)} events from the CSV were found in Raw Database:

"""
    
    for i, event in enumerate(events_in_raw, 1):
        raw_db_section += f"{i}. `{event['insert_id']}` → `{event['distinct_id']}`\n"
    
    if events_missing_raw:
        raw_db_section += f"\n#### **❌ EVENTS MISSING FROM RAW DATABASE ({len(events_missing_raw)}/41)**\n"
        for i, event in enumerate(events_missing_raw, 1):
            raw_db_section += f"{i}. `{event['insert_id']}` → `{event['distinct_id']}`\n"
    
    raw_db_section += f"""

### **COMPLETE PAIRS IN RAW DATABASE ({len(complete_pairs_raw)}/41)**
The following {len(complete_pairs_raw)} complete pairs (both user AND event) were found in Raw Database:

"""
    
    for i, pair in enumerate(complete_pairs_raw, 1):
        raw_db_section += f"{i}. `{pair['distinct_id']}` ↔ `{pair['insert_id']}`\n"
    
    if incomplete_pairs_raw:
        raw_db_section += f"\n#### **❌ INCOMPLETE PAIRS IN RAW DATABASE ({len(incomplete_pairs_raw)}/41)**\n"
        for i, pair in enumerate(incomplete_pairs_raw, 1):
            user_status = "✅USER" if pair['distinct_id'] in {u['distinct_id'] for u in users_in_raw} else "❌USER"
            event_status = "✅EVENT" if pair['insert_id'] in {e['insert_id'] for e in events_in_raw} else "❌EVENT"
            raw_db_section += f"{i}. `{pair['distinct_id']}` ↔ `{pair['insert_id']}` *({user_status} + {event_status})*\n"
    
    # Replace the placeholder section
    updated_content = content.replace(
        "*[TO BE COMPLETED AFTER VALIDATION SCRIPT RUNS]*\n\n### **USERS IN RAW DATABASE**\n*[Results pending...]*\n\n### **EVENTS IN RAW DATABASE** \n*[Results pending...]*\n\n### **COMPLETE PAIRS IN RAW DATABASE**\n*[Results pending...]*",
        raw_db_section.strip()
    )
    
    # Add comparison section
    comparison_section = f"""
### **IDENTICAL DATA VERIFICATION**
- {'✅' if len(users_in_raw) == 40 else '❌'} Users match between S3 and Raw DB ({len(users_in_raw)}/40 S3 users found in Raw DB)
- {'✅' if len(events_in_raw) == 40 else '❌'} Events match between S3 and Raw DB ({len(events_in_raw)}/40 S3 events found in Raw DB)
- {'✅' if len(complete_pairs_raw) == 40 else '❌'} Complete pairs match between S3 and Raw DB ({len(complete_pairs_raw)}/40 S3 pairs found in Raw DB)

### **DISCREPANCIES IDENTIFIED**
"""
    
    if len(users_in_raw) != 40:
        comparison_section += f"- **USER DISCREPANCY**: {40 - len(users_in_raw)} users present in S3 but missing from Raw DB\n"
    
    if len(events_in_raw) != 40:
        comparison_section += f"- **EVENT DISCREPANCY**: {40 - len(events_in_raw)} events present in S3 but missing from Raw DB\n"
    
    if len(complete_pairs_raw) != 40:
        comparison_section += f"- **COMPLETE PAIRS DISCREPANCY**: {40 - len(complete_pairs_raw)} complete pairs present in S3 but missing from Raw DB\n"
    
    if len(users_in_raw) == 40 and len(events_in_raw) == 40 and len(complete_pairs_raw) == 40:
        comparison_section += "**NO DISCREPANCIES FOUND** - Perfect data consistency between S3 and Raw Database!\n"
    
    # Update pipeline health section
    pipeline_section = f"""
- {'✅' if len(users_in_raw) == 40 else '❌'} S3 → Raw DB user transfer integrity (**{len(users_in_raw)}/40 transferred**)
- {'✅' if len(events_in_raw) == 40 else '❌'} S3 → Raw DB event transfer integrity (**{len(events_in_raw)}/40 transferred**)
- {'✅' if len(users_in_raw) == 40 and len(events_in_raw) == 40 else '❌'} Data consistency verification
- {'✅' if len(complete_pairs_raw) == 40 else '❌'} Bug location identification: **{'No bugs found' if len(complete_pairs_raw) == 40 else 'S3 → Raw DB pipeline issue'}**"""
    
    # Replace placeholders
    updated_content = updated_content.replace(
        "*[TO BE COMPLETED AFTER RAW DB VALIDATION]*\n\n### **IDENTICAL DATA VERIFICATION**\n- [ ] Users match exactly between S3 and Raw DB\n- [ ] Events match exactly between S3 and Raw DB  \n- [ ] Complete pairs match exactly between S3 and Raw DB\n\n### **DISCREPANCIES IDENTIFIED**\n*[If any discrepancies found, they will be documented here]*",
        comparison_section.strip()
    )
    
    updated_content = updated_content.replace(
        "*[TO BE UPDATED AFTER RAW DB VALIDATION]*\n- [ ] S3 → Raw DB transfer integrity\n- [ ] Data consistency verification\n- [ ] Bug location identification",
        pipeline_section.strip()
    )
    
    # Write updated report
    with open('COMPLETE_DATA_VALIDATION_REPORT.md', 'w') as f:
        f.write(updated_content)
    
    print(f"\n✅ MARKDOWN REPORT UPDATED")

def main():
    print("🔍 RAW DATABASE VALIDATION")
    print("=" * 60)
    
    csv_data = read_csv_data()
    print(f"📊 Total CSV entries: {len(csv_data)}")
    print()
    
    # Check raw database
    users_in_raw, users_missing_raw = check_users_in_raw_db(csv_data)
    events_in_raw, events_missing_raw = check_events_in_raw_db(csv_data)
    complete_pairs_raw, incomplete_pairs_raw = check_complete_pairs_raw(users_in_raw, events_in_raw)
    
    print(f"\n🎯 RAW DATABASE RESULTS")
    print("=" * 60)
    print(f"1️⃣ Users in Raw DB: {len(users_in_raw)}/41")
    print(f"2️⃣ Events in Raw DB: {len(events_in_raw)}/41")
    print(f"3️⃣ Complete pairs in Raw DB: {len(complete_pairs_raw)}/41")
    
    # Update markdown report
    update_markdown_report(users_in_raw, users_missing_raw, events_in_raw, events_missing_raw,
                          complete_pairs_raw, incomplete_pairs_raw)

if __name__ == "__main__":
    main() 