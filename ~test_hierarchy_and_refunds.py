#!/usr/bin/env python3
"""
Test script to verify hierarchy and refund rate fixes
"""

import sys
import os
sys.path.append('.')

import requests
import json

def test_dashboard_data():
    """Test that hierarchy children have data and refund rates are calculated"""
    
    # Test API call
    url = "http://localhost:5050/api/dashboard/analytics/data"
    data = {
        'start_date': '2025-07-10', 
        'end_date': '2025-07-19', 
        'breakdown': 'all', 
        'group_by': 'campaign',  # Start with campaigns to see adset children
        'enable_breakdown_mapping': True
    }
    
    print("🧪 Testing Dashboard API with Campaign Data...")
    try:
        response = requests.post(url, json=data)
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            return
            
        result = response.json()
        
        if not result.get('success'):
            print(f"❌ API Failed: {result.get('error')}")
            return
            
        entities = result.get('data', [])
        print(f"✅ Got {len(entities)} campaigns")
        
        # Check for children with data
        campaigns_with_children = 0
        children_with_data = 0
        entities_with_refunds = 0
        
        for entity in entities[:3]:  # Check first 3 campaigns
            entity_id = entity.get('entity_id')
            children = entity.get('children', [])
            
            print(f"\n📊 Campaign {entity_id}:")
            print(f"   - Children: {len(children)}")
            print(f"   - Trial Refund Rate (actual): {entity.get('avg_trial_refund_rate', 0)*100:.2f}%")
            print(f"   - Trial Refund Rate (estimated): {entity.get('trial_refund_rate_estimated', 0)*100:.2f}%")
            print(f"   - Purchase Refund Rate: {entity.get('purchase_refund_rate', 0)*100:.2f}%")
            
            if len(children) > 0:
                campaigns_with_children += 1
                
                # Check first child adset
                child = children[0]
                print(f"   - First Adset ID: {child.get('entity_id')}")
                print(f"   - Adset Spend: ${child.get('spend', 0):,.2f}")
                print(f"   - Adset Trials: {child.get('mixpanel_trials_started', 0)}")
                print(f"   - Adset Trial Refund: {child.get('avg_trial_refund_rate', 0)*100:.2f}%")
                
                # Check if adset has children (ads)
                adset_children = child.get('children', [])
                print(f"   - Adset Children (ads): {len(adset_children)}")
                
                if len(adset_children) > 0:
                    ad = adset_children[0]
                    print(f"   - First Ad ID: {ad.get('entity_id')}")
                    print(f"   - Ad Spend: ${ad.get('spend', 0):,.2f}")
                    print(f"   - Ad Trials: {ad.get('mixpanel_trials_started', 0)}")
                    print("   ✅ 3-level hierarchy working!")
                
                if child.get('spend', 0) > 0 or child.get('mixpanel_trials_started', 0) > 0:
                    children_with_data += 1
                    print("   ✅ Child has data!")
                else:
                    print("   ❌ Child has no data")
            
            # Check refund rates (both actual and estimated)
            trial_refund_actual = entity.get('avg_trial_refund_rate', 0)
            trial_refund_estimated = entity.get('trial_refund_rate_estimated', 0)
            purchase_refund = entity.get('purchase_refund_rate', 0)
            
            if trial_refund_actual > 0 or trial_refund_estimated > 0 or purchase_refund > 0:
                entities_with_refunds += 1
                print(f"   ✅ Has refund rates: Trial(actual)={trial_refund_actual*100:.2f}%, Trial(est)={trial_refund_estimated*100:.2f}%, Purchase={purchase_refund*100:.2f}%")
        
        print(f"\n📈 SUMMARY:")
        print(f"   - Campaigns with children: {campaigns_with_children}")
        print(f"   - Children with data: {children_with_data}")
        print(f"   - Entities with refund rates > 0: {entities_with_refunds}")
        
        # Success criteria
        if campaigns_with_children > 0 and children_with_data > 0:
            print("✅ HIERARCHY FIX: WORKING!")
        else:
            print("❌ HIERARCHY FIX: FAILED!")
            
        if entities_with_refunds > 0:
            print("✅ REFUND RATES FIX: WORKING!")
        else:
            print("❌ REFUND RATES FIX: FAILED!")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_dashboard_data()
