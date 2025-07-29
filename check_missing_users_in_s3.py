#!/usr/bin/env python3
"""
🔍 CHECK MISSING USERS IN S3 DATA
Check if the 4 missing users exist in our downloaded S3 user data
"""

import json
from pathlib import Path

def main():
    print("🔍 CHECKING IF MISSING USERS EXIST IN S3 USER DATA")
    print("=" * 60)
    
    missing_users = ["t9UtN9Zdkzm", "C9GeaFRjpfa", "_a1qrFYs55X", "WhCxnzxApfY"]
    
    # Check if user file exists - use the decompressed one
    user_file = Path("data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json")
    
    if not user_file.exists():
        print("❌ User JSON file not found!")
        return
    
    print(f"📄 Checking file: {user_file}")
    print()
    
    # Check each missing user
    for user_id in missing_users:
        print(f"👤 CHECKING USER: {user_id}")
        print("-" * 40)
        
        found = False
        user_data = None
        
        try:
            with open(user_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        user = json.loads(line.strip())
                        if user.get('distinct_id') == user_id:
                            found = True
                            user_data = user
                            print(f"✅ FOUND in S3 user data (line {line_num})")
                            break
                    except json.JSONDecodeError:
                        continue
            
            if found and user_data:
                email = user_data.get('$email', 'No email')
                print(f"📧 Email: {email}")
                
                # Check for filtering reasons
                if '@atly.com' in email:
                    print("🚫 LIKELY FILTERED: @atly.com email (internal user)")
                elif 'test' in email.lower():
                    print("🚫 LIKELY FILTERED: Contains 'test' (test user)")  
                elif '@steps.me' in email:
                    print("🚫 LIKELY FILTERED: @steps.me email (internal user)")
                else:
                    print("🤔 SHOULD NOT BE FILTERED: Regular user email")
                    
                # Show some properties
                properties = user_data.get('properties', {})
                if properties:
                    prop_keys = list(properties.keys())[:5]
                    print(f"🏷️  Properties: {prop_keys}")
                    
            else:
                print("❌ NOT FOUND in S3 user data")
                print("🚨 CRITICAL: User exists in events but not in user profiles!")
            
        except Exception as e:
            print(f"❌ Error checking user: {e}")
        
        print()

if __name__ == "__main__":
    main() 