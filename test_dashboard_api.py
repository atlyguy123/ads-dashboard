#!/usr/bin/env python3

import sys
import os
sys.path.append('orchestrator')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService
from orchestrator.dashboard.services.analytics_query_service import QueryConfig

def test_dashboard_api():
    """
    Test what the dashboard API actually returns for the campaign
    """
    
    print("🔍 TESTING DASHBOARD API RESPONSE")
    print("=" * 80)
    
    campaign_id = '120215772671800178'
    start_date = '2025-06-18'
    end_date = '2025-06-24'
    
    analytics_service = AnalyticsQueryService()
    
    # Create query config like the dashboard would
    config = QueryConfig(
        breakdown='all',
        start_date=start_date,
        end_date=end_date,
        group_by='campaign',
        include_mixpanel=True,
        enable_breakdown_mapping=True
    )
    
    try:
        # Execute the same query the dashboard uses
        result = analytics_service.execute_analytics_query(config)
        
        if result.get('success'):
            data = result.get('data', [])
            print(f"✅ Dashboard API Success")
            print(f"  Records returned: {len(data)}")
            
            # Find our specific campaign
            campaign_record = None
            for record in data:
                if record.get('entity_id') == f'campaign_{campaign_id}':
                    campaign_record = record
                    break
            
            if campaign_record:
                print(f"\n📊 Campaign Record Found:")
                print(f"  Campaign ID: {campaign_record.get('entity_id')}")
                print(f"  Campaign Name: {campaign_record.get('campaign_name', 'N/A')}")
                print(f"  Trials (Meta): {campaign_record.get('trials_meta', 0)}")
                print(f"  Trials (Mixpanel): {campaign_record.get('trials_mixpanel', 0)}")
                print(f"  Purchases (Meta): {campaign_record.get('purchases_meta', 0)}")
                print(f"  Purchases (Mixpanel): {campaign_record.get('purchases_mixpanel', 0)}")
                print(f"  Trial Conversion Rate: {campaign_record.get('avg_trial_conversion_rate', 0):.2f}%")
                print(f"  Trial Refund Rate: {campaign_record.get('avg_trial_refund_rate', 0):.2f}%")
                print(f"  Purchase Refund Rate: {campaign_record.get('purchase_refund_rate', 0):.2f}%")
                
                print(f"\n🔍 Comparison:")
                print(f"  Dashboard Mixpanel Trials: {campaign_record.get('trials_mixpanel', 0)}")
                print(f"  Tooltip Users: 87")
                print(f"  Dashboard Mixpanel Purchases: {campaign_record.get('purchases_mixpanel', 0)}")
                print(f"  Actual DB Purchases: 9")
                
            else:
                print(f"❌ Campaign {campaign_id} not found in dashboard data")
                print(f"Available campaigns:")
                for record in data[:5]:
                    print(f"  {record.get('entity_id')}: {record.get('campaign_name', 'N/A')}")
        else:
            print(f"❌ Dashboard API failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error testing dashboard API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dashboard_api() 