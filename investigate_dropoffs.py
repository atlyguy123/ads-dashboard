#!/usr/bin/env python3
"""
Investigate Drop-offs

Check why:
1. "undefined" user is missing from raw database
2. "pe60vc5po2b" user failed processing from raw to main database
"""

import sqlite3
import json
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Investigate the two drop-offs"""
    
    print("🔍 INVESTIGATING DROP-OFFS")
    print("=" * 40)
    
    # Check undefined user
    check_undefined_user()
    
    print()
    
    # Check pe60vc5po2b user
    check_processing_failure()
    
    return 0

def check_undefined_user():
    """Check why 'undefined' user is missing from raw DB"""
    print("1️⃣ CHECKING 'undefined' USER...")
    
    try:
        with sqlite3.connect(get_database_path('raw_data')) as conn:
            cursor = conn.cursor()
            
            # Search for 'undefined' in any form
            cursor.execute("SELECT COUNT(*) FROM raw_user_data WHERE user_data LIKE ?", ['%undefined%'])
            count = cursor.fetchone()[0]
            print(f"   🔍 Found {count} records containing 'undefined'")
            
            # Look for actual undefined user_id
            cursor.execute("SELECT user_data FROM raw_user_data WHERE user_data LIKE ? LIMIT 1", ['%"$user_id":"undefined"%'])
            result = cursor.fetchone()
            
            if result:
                user_data = json.loads(result[0])
                properties = user_data.get('properties', {})
                distinct_id = user_data.get('distinct_id')
                campaign_id = properties.get('abi_~campaign_id')
                print(f"   ✅ Found undefined user:")
                print(f"      Distinct ID: {distinct_id}")
                print(f"      Campaign: {campaign_id}")
            else:
                print("   ❌ 'undefined' user truly missing from raw DB")
                
                # Check if it might be stored as distinct_id
                cursor.execute("SELECT user_data FROM raw_user_data WHERE distinct_id = ?", ['undefined'])
                result2 = cursor.fetchone()
                if result2:
                    print("   💡 Found as distinct_id='undefined'")
                    user_data = json.loads(result2[0])
                    properties = user_data.get('properties', {})
                    user_id = properties.get('$user_id')
                    campaign_id = properties.get('abi_~campaign_id')
                    print(f"      User ID: {user_id}")
                    print(f"      Campaign: {campaign_id}")
                else:
                    print("   💡 Completely absent - likely invalid user ID in Mixpanel export")
                    
    except Exception as e:
        print(f"   ❌ Error checking undefined user: {e}")

def check_processing_failure():
    """Check why pe60vc5po2b failed processing"""
    print("2️⃣ CHECKING 'pe60vc5po2b' PROCESSING FAILURE...")
    
    try:
        # Get raw data
        with sqlite3.connect(get_database_path('raw_data')) as raw_conn:
            raw_cursor = raw_conn.cursor()
            
            raw_cursor.execute("SELECT user_data FROM raw_user_data WHERE distinct_id = ?", ['pe60vc5po2b'])
            raw_result = raw_cursor.fetchone()
            
            if not raw_result:
                print("   ❌ Not found in raw DB either!")
                return
            
            raw_data = json.loads(raw_result[0])
            raw_properties = raw_data.get('properties', {})
            
            print("   ✅ Found in raw DB:")
            print(f"      User ID: {raw_properties.get('$user_id')}")
            print(f"      Campaign: {raw_properties.get('abi_~campaign_id')}")
            print(f"      Email: {raw_properties.get('$email', 'N/A')}")
            print(f"      First seen: {raw_properties.get('first_install_date', 'N/A')}")
            
            # Check processed DB
            with sqlite3.connect(get_database_path('mixpanel_data')) as main_conn:
                main_cursor = main_conn.cursor()
                
                main_cursor.execute("SELECT distinct_id FROM mixpanel_user WHERE distinct_id = ?", ['pe60vc5po2b'])
                main_result = main_cursor.fetchone()
                
                if main_result:
                    print("   ✅ Actually found in processed DB (false alarm)")
                else:
                    print("   ❌ Confirmed missing from processed DB")
                    print("   🔍 Checking for processing issues...")
                    
                    # Check if user might be filtered out
                    email = raw_properties.get('$email', '')
                    if email:
                        email_lower = email.lower()
                        if '@atly.com' in email_lower:
                            print(f"      🚫 FILTERED: Internal email ({email})")
                        elif 'test' in email_lower:
                            print(f"      🚫 FILTERED: Test email ({email})")
                        elif '@steps.me' in email_lower:
                            print(f"      🚫 FILTERED: Steps email ({email})")
                        else:
                            print(f"      ✅ Email OK: {email}")
                            print("      🤔 User should have been processed - potential pipeline bug")
                    else:
                        print("      ⚠️  No email found - user should still be processed")
                        print("      🤔 Potential pipeline issue")
                        
    except Exception as e:
        print(f"   ❌ Error checking processing failure: {e}")

if __name__ == "__main__":
    exit(main()) 