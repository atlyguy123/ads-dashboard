#!/usr/bin/env python3
"""
Compare CSV vs JSON Data

Compare all 40 Mixpanel users from CSV export against the raw JSON file
to identify exactly where data loss occurs in the pipeline.
"""

import csv
import json
from pathlib import Path

def main():
    """Compare CSV users against raw JSON file"""
    
    print("🔍 CSV vs JSON COMPARISON")
    print("=" * 50)
    print("📊 Analyzing where users are lost in the pipeline...")
    print()
    
    # Read all users from CSV
    csv_users = read_csv_users()
    print(f"📋 Total users from Mixpanel CSV: {len(csv_users)}")
    
    # Check JSON file - using LATEST downloaded data (July 29th)
    json_file = "data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json"
    
    if not Path(json_file).exists():
        print(f"❌ JSON file not found: {json_file}")
        return 1
    
    print(f"📄 Checking JSON file: {json_file}")
    print()
    
    # Search for each user in JSON
    found_users, missing_users = search_users_in_json(csv_users, json_file)
    
    # Report results
    print("📊 RESULTS SUMMARY")
    print("-" * 30)
    print(f"✅ Found in JSON: {len(found_users)}/{len(csv_users)} users")
    print(f"❌ Missing from JSON: {len(missing_users)}/{len(csv_users)} users")
    print()
    
    if found_users:
        print("✅ USERS FOUND IN JSON:")
        print("-" * 25)
        for i, user in enumerate(found_users, 1):
            print(f"   {i:2d}. {user['user_id']} - {user['campaign_name']}")
        print()
    
    if missing_users:
        print("❌ USERS MISSING FROM JSON:")
        print("-" * 28)
        for i, user in enumerate(missing_users, 1):
            print(f"   {i:2d}. {user['user_id']} - {user['campaign_name']}")
        print()
        
        print("💡 NEXT STEPS:")
        print("   • Missing users may be from more recent Mixpanel data")
        print("   • Check if we need to download newer data from Mixpanel")
        print("   • Verify data sync frequency and timing")
    
    # Calculate percentages
    if len(csv_users) > 0:
        found_pct = (len(found_users) / len(csv_users)) * 100
        missing_pct = (len(missing_users) / len(csv_users)) * 100
        
        print(f"📈 COVERAGE: {found_pct:.1f}% of Mixpanel users found in our JSON")
        print(f"📉 GAP: {missing_pct:.1f}% of Mixpanel users missing from our JSON")
    else:
        print("❌ No CSV users loaded - cannot calculate percentages")
    
    return 0

def read_csv_users():
    """Read all users from the Mixpanel CSV export"""
    users = []
    
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                users.append({
                    'user_id': row['User ID'],
                    'campaign_name': row['abi_~campaign'],
                    'campaign_id': row['abi_~campaign_id']
                })
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        
    return users

def search_users_in_json(csv_users, json_file):
    """Search for each CSV user in the JSON file"""
    
    found_users = []
    missing_users = []
    
    print("🔍 Searching for users in JSON file...")
    print("   (This may take a moment for large files)")
    print()
    
    try:
        # Read JSON file in chunks to handle large files
        with open(json_file, 'r') as f:
            file_content = f.read()
        
        # Check each user
        for i, user in enumerate(csv_users, 1):
            user_id = user['user_id']
            
            # Search for user ID in file content
            if user_id in file_content:
                print(f"   ✅ {i:2d}/40: Found {user_id}")
                found_users.append(user)
                
                # Extract the full user record for verification
                try:
                    # Find the line containing this user
                    lines = file_content.split('\n')
                    for line in lines:
                        if user_id in line:
                            user_data = json.loads(line)
                            properties = user_data.get('properties', {})
                            
                            # Verify campaign ID matches
                            abi_campaign_id = properties.get('abi_~campaign_id')
                            if abi_campaign_id == user['campaign_id']:
                                print(f"       ✓ Campaign ID matches: {abi_campaign_id}")
                            else:
                                print(f"       ⚠️  Campaign ID mismatch: JSON={abi_campaign_id} vs CSV={user['campaign_id']}")
                            break
                except Exception as e:
                    print(f"       ⚠️  Error parsing user data: {e}")
            else:
                print(f"   ❌ {i:2d}/40: Missing {user_id}")
                missing_users.append(user)
        
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return [], csv_users
    
    return found_users, missing_users

if __name__ == "__main__":
    exit(main()) 