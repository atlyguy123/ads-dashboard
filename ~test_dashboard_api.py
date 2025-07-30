#!/usr/bin/env python3
"""
Call the dashboard API directly to see what it actually returns for our ad set
"""

import requests
import json

# Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"
DASHBOARD_URL = "http://localhost:5000"  # Adjust if needed

def test_analytics_api():
    """Test the /analytics/data endpoint"""
    print("=== TESTING DASHBOARD API DIRECTLY ===")
    
    url = f"{DASHBOARD_URL}/dashboard/analytics/data"
    
    payload = {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "breakdown": "all",
        "group_by": "adset",
        "include_mixpanel": True
    }
    
    try:
        print(f"🌐 Calling: {url}")
        print(f"📄 Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, json=payload, timeout=30)
        
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success'):
                records = data.get('data', [])
                print(f"✅ Success! Found {len(records)} records")
                
                # Find our specific ad set
                our_adset = None
                for record in records:
                    if record.get('adset_id') == AD_SET_ID or record.get('id') == AD_SET_ID:
                        our_adset = record
                        break
                
                if our_adset:
                    print(f"\n🎯 FOUND OUR AD SET:")
                    print(f"   Ad Set ID: {our_adset.get('adset_id', our_adset.get('id'))}")
                    print(f"   Ad Set Name: {our_adset.get('adset_name', our_adset.get('name', 'Unknown'))}")
                    print(f"   Mixpanel Trials: {our_adset.get('mixpanel_trials_started', 'N/A')}")
                    print(f"   Total Users: {our_adset.get('total_attributed_users', 'N/A')}")
                    print(f"   Purchases: {our_adset.get('mixpanel_purchases', 'N/A')}")
                    print(f"   Revenue: {our_adset.get('estimated_revenue_usd', 'N/A')}")
                    
                    # Show all fields for debugging
                    print(f"\n📋 ALL FIELDS:")
                    for key, value in our_adset.items():
                        print(f"   {key}: {value}")
                        
                else:
                    print(f"\n❌ AD SET {AD_SET_ID} NOT FOUND in {len(records)} records")
                    if records:
                        print(f"📋 Available ad set IDs:")
                        for record in records[:10]:  # Show first 10
                            print(f"   - {record.get('adset_id', record.get('id', 'No ID'))}")
                        if len(records) > 10:
                            print(f"   ... and {len(records) - 10} more")
            else:
                print(f"❌ API returned error: {data.get('error')}")
                
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed. Is the dashboard server running?")
        print("💡 Try running: python orchestrator/app.py")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
    
    return True

def test_chart_data_api():
    """Test the chart data API for specific ad set"""
    print("\n=== TESTING CHART DATA API ===")
    
    url = f"{DASHBOARD_URL}/dashboard/analytics/chart-data"
    
    params = {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "breakdown": "all",
        "entity_type": "adset",
        "entity_id": AD_SET_ID
    }
    
    try:
        print(f"🌐 Calling: {url}")
        print(f"📄 Params: {json.dumps(params, indent=2)}")
        
        response = requests.get(url, params=params, timeout=30)
        
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Chart data response: {json.dumps(data, indent=2)[:500]}...")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed for chart data API")
    except Exception as e:
        print(f"❌ Chart data error: {str(e)}")

def main():
    print("🔍 TESTING DASHBOARD API DIRECTLY")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Expected: 47 users (CSV/DB), Dashboard claims: 42 users")
    
    success = test_analytics_api()
    if success:
        test_chart_data_api()

if __name__ == "__main__":
    main() 