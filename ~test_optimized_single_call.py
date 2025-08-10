#!/usr/bin/env python3
"""
Test script to verify the optimized analytics API uses minimal database connections
"""

import sys
import os
import subprocess
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig

def count_open_db_connections():
    """Count open database connections to mixpanel_data.db"""
    try:
        result = subprocess.run(['lsof'], capture_output=True, text=True)
        connections = [line for line in result.stdout.split('\n') if 'mixpanel_data.db' in line.lower()]
        return len(connections)
    except Exception:
        return -1

def test_optimized_analytics():
    """Test the optimized analytics service"""
    print("🚀 Testing Optimized Analytics Service")
    print("=" * 50)
    
    # Count connections before
    connections_before = count_open_db_connections()
    print(f"📊 Database connections before: {connections_before}")
    
    try:
        # Create service
        service = AnalyticsQueryService()
        
        # Create test config
        config = QueryConfig(
            breakdown='all',
            start_date='2024-07-15',
            end_date='2024-07-25',
            group_by='campaign'
        )
        
        print(f"🔍 Testing query with config: {config.__dict__}")
        
        # Execute optimized query
        result = service.execute_analytics_query_optimized(config)
        
        if result.get('success'):
            data = result.get('data', [])
            print(f"✅ Query succeeded!")
            print(f"📈 Retrieved {len(data)} entities")
            
            # Check if sparkline data is included
            if data:
                first_entity = data[0]
                sparkline_data = first_entity.get('sparkline_data', [])
                print(f"🎯 Sparkline data points: {len(sparkline_data)}")
                print(f"📋 Entity fields: {list(first_entity.keys())[:10]}...")  # Show first 10 fields
                
                # Check for user details
                user_details = first_entity.get('user_details', {})
                if user_details:
                    print(f"👥 User details included: {list(user_details.keys())}")
                else:
                    print("⚠️  No user details found")
            
            metadata = result.get('metadata', {})
            print(f"⚡ Query time: {metadata.get('query_time_ms', 'unknown')}")
            print(f"📊 Data source: {metadata.get('data_source', 'unknown')}")
            
        else:
            print(f"❌ Query failed: {result.get('error')}")
            
    except Exception as e:
        print(f"💥 Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    
    # Count connections after
    connections_after = count_open_db_connections()
    print(f"📊 Database connections after: {connections_after}")
    
    # Calculate difference
    if connections_before >= 0 and connections_after >= 0:
        diff = connections_after - connections_before
        if diff <= 2:  # Allow for 1-2 new connections from our test
            print(f"✅ Connection management: Good! (+{diff} connections)")
        else:
            print(f"⚠️  Connection management: Potential issue (+{diff} connections)")
    
    print("\n🎯 Summary:")
    print("- Single optimized query should retrieve all data")
    print("- Sparkline data should be included in response")
    print("- Database connections should remain minimal")

if __name__ == "__main__":
    test_optimized_analytics()
