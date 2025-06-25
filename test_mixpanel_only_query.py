#!/usr/bin/env python3

import sys
import os
sys.path.append('orchestrator')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig

def test_mixpanel_only_query():
    """
    Test the Mixpanel-only query to see if it picks up our campaign
    """
    
    print("🔍 TESTING MIXPANEL-ONLY QUERY")
    print("=" * 80)
    
    start_date = '2025-06-18'
    end_date = '2025-06-24'
    
    analytics_service = AnalyticsQueryService()
    
    # Create query config for Mixpanel-only mode
    config = QueryConfig(
        breakdown='all',
        start_date=start_date,
        end_date=end_date,
        group_by='campaign',
        include_mixpanel=True,
        enable_breakdown_mapping=True
    )
    
    try:
        # Force Mixpanel-only mode
        result = analytics_service._execute_mixpanel_only_query(config)
        
        if result.get('success'):
            data = result.get('data', [])
            print(f"✅ Mixpanel-only query success")
            print(f"  Records returned: {len(data)}")
            
            # Find our specific campaign
            campaign_120215772671800178 = None
            for record in data:
                if record.get('campaign_id') == '120215772671800178':
                    campaign_120215772671800178 = record
                    break
            
            if campaign_120215772671800178:
                print(f"\n🎯 FOUND OUR CAMPAIGN!")
                print(f"  Campaign ID: {campaign_120215772671800178.get('campaign_id')}")
                print(f"  Campaign Name: {campaign_120215772671800178.get('campaign_name')}")
                print(f"  Entity ID: {campaign_120215772671800178.get('id')}")
                print(f"  Mixpanel Trials: {campaign_120215772671800178.get('mixpanel_trials_started', 0)}")
                print(f"  Mixpanel Purchases: {campaign_120215772671800178.get('mixpanel_purchases', 0)}")
                print(f"  Estimated Revenue: ${campaign_120215772671800178.get('estimated_revenue_usd', 0):.2f}")
                print(f"  Total Users: {campaign_120215772671800178.get('total_users', 0)}")
                
                # Now test the full analytics query to see if it uses Mixpanel-only mode
                print(f"\n🔍 TESTING FULL ANALYTICS QUERY:")
                print("-" * 60)
                
                full_result = analytics_service.execute_analytics_query(config)
                
                if full_result.get('success'):
                    full_data = full_result.get('data', [])
                    print(f"  Full query returned: {len(full_data)} records")
                    print(f"  Data source: {full_result.get('metadata', {}).get('data_source', 'unknown')}")
                    
                    # Find our campaign in full results
                    found_in_full = False
                    for record in full_data:
                        if record.get('campaign_id') == '120215772671800178':
                            found_in_full = True
                            print(f"  ✅ Found campaign in full query results!")
                            break
                    
                    if not found_in_full:
                        print(f"  ❌ Campaign NOT found in full query results")
                        print(f"  Available campaigns:")
                        for record in full_data[:5]:
                            print(f"    {record.get('campaign_id', 'N/A')}: {record.get('campaign_name', 'N/A')}")
                else:
                    print(f"  ❌ Full query failed: {full_result.get('error', 'Unknown error')}")
                    
            else:
                print(f"\n❌ Campaign 120215772671800178 NOT found in Mixpanel-only results")
                print(f"Available campaigns:")
                for record in data[:10]:
                    print(f"  {record.get('campaign_id', 'N/A')}: {record.get('campaign_name', 'N/A')} (trials: {record.get('mixpanel_trials_started', 0)})")
        else:
            print(f"❌ Mixpanel-only query failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error testing Mixpanel-only query: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_mixpanel_only_query() 