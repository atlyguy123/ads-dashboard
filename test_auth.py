#!/usr/bin/env python3
"""
Test script to verify authentication is working
"""

import requests
import os
from requests.auth import HTTPBasicAuth

def test_auth():
    """Test the authentication setup"""
    base_url = "http://localhost:5001"
    
    # Test credentials
    admin_user = os.getenv('ADMIN_USERNAME', 'admin')
    admin_pass = os.getenv('ADMIN_PASSWORD', 'secure-password')
    team_user = os.getenv('TEAM_USERNAME', 'team')
    team_pass = os.getenv('TEAM_PASSWORD', 'team-password')
    
    print("🔐 Testing Dashboard Authentication")
    print("=" * 50)
    
    # Test 1: Access without credentials (should fail)
    print("\n1. Testing access without credentials...")
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 401:
            print("✅ PASS: Unauthorized access correctly blocked")
        else:
            print(f"❌ FAIL: Expected 401, got {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("❌ Server not running. Start with: cd orchestrator && python3 app.py")
        return False
    
    # Test 2: Access with admin credentials (should work)
    print("\n2. Testing admin access...")
    try:
        response = requests.get(
            f"{base_url}/", 
            auth=HTTPBasicAuth(admin_user, admin_pass),
            timeout=5
        )
        if response.status_code == 200:
            print("✅ PASS: Admin access successful")
        else:
            print(f"❌ FAIL: Admin access failed with status {response.status_code}")
    except Exception as e:
        print(f"❌ FAIL: Admin access error: {e}")
    
    # Test 3: Access with team credentials (should work)
    print("\n3. Testing team access...")
    try:
        response = requests.get(
            f"{base_url}/", 
            auth=HTTPBasicAuth(team_user, team_pass),
            timeout=5
        )
        if response.status_code == 200:
            print("✅ PASS: Team access successful")
        else:
            print(f"❌ FAIL: Team access failed with status {response.status_code}")
    except Exception as e:
        print(f"❌ FAIL: Team access error: {e}")
    
    # Test 4: Access with wrong credentials (should fail)
    print("\n4. Testing access with wrong credentials...")
    try:
        response = requests.get(
            f"{base_url}/", 
            auth=HTTPBasicAuth("wrong", "credentials"),
            timeout=5
        )
        if response.status_code == 401:
            print("✅ PASS: Wrong credentials correctly blocked")
        else:
            print(f"❌ FAIL: Expected 401, got {response.status_code}")
    except Exception as e:
        print(f"❌ FAIL: Wrong credentials test error: {e}")
    
    # Test 5: API endpoints
    print("\n5. Testing API endpoint access...")
    try:
        response = requests.get(
            f"{base_url}/api/analytics-pipeline/health", 
            auth=HTTPBasicAuth(admin_user, admin_pass),
            timeout=5
        )
        if response.status_code == 200:
            print("✅ PASS: API access successful")
        else:
            print(f"❌ FAIL: API access failed with status {response.status_code}")
    except Exception as e:
        print(f"❌ FAIL: API access error: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Authentication test completed!")
    print("\nCredentials for your team:")
    print(f"Admin: {admin_user} / {admin_pass}")
    print(f"Team:  {team_user} / {team_pass}")
    print("\n⚠️  Remember to change these passwords in production!")
    
    return True

if __name__ == "__main__":
    test_auth() 