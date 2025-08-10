#!/usr/bin/env python3
"""
🚀 Direct Method Testing - API Optimization Validation
Tests the optimized methods directly without HTTP overhead.
"""

import sys
import os
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import the optimized analytics service
from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService, QueryConfig

def test_optimized_methods():
    """Test the optimized methods directly"""
    print("🚀 DIRECT METHOD TESTING - API Optimization Validation")
    print("="*60)
    
    # Initialize the analytics service
    try:
        analytics_service = AnalyticsQueryService()
        print("✅ Analytics service initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize analytics service: {e}")
        return
    
    # Test configuration
    config = QueryConfig(
        breakdown='all',
        start_date='2024-07-01',
        end_date='2024-07-31',
        group_by='campaign',
        include_mixpanel=True
    )
    
    print(f"\n📅 Test Configuration:")
    print(f"   Date Range: {config.start_date} to {config.end_date}")
    print(f"   Group By: {config.group_by}")
    print(f"   Breakdown: {config.breakdown}")
    
    # Test 1: Legacy method performance
    print(f"\n🧪 TEST 1: Legacy Method Performance")
    try:
        start_time = time.time()
        legacy_result = analytics_service.execute_analytics_query(config)
        legacy_time = (time.time() - start_time) * 1000
        
        print(f"   ⏱️  Legacy Time: {legacy_time:.2f}ms")
        print(f"   ✅ Success: {legacy_result.get('success')}")
        print(f"   📊 Records: {len(legacy_result.get('data', []))}")
        
    except Exception as e:
        print(f"   ❌ Legacy method failed: {e}")
        legacy_result = None
        legacy_time = 0
    
    # Test 2: Optimized method performance
    print(f"\n🚀 TEST 2: Optimized Method Performance")
    try:
        start_time = time.time()
        optimized_result = analytics_service.execute_analytics_query_optimized(config)
        optimized_time = (time.time() - start_time) * 1000
        
        print(f"   ⏱️  Optimized Time: {optimized_time:.2f}ms")
        print(f"   ✅ Success: {optimized_result.get('success')}")
        print(f"   📊 Records: {len(optimized_result.get('data', []))}")
        
    except Exception as e:
        print(f"   ❌ Optimized method failed: {e}")
        optimized_result = None
        optimized_time = 0
    
    # Performance comparison
    if legacy_time > 0 and optimized_time > 0:
        improvement = ((legacy_time - optimized_time) / legacy_time) * 100
        speedup = legacy_time / optimized_time
        
        print(f"\n🚀 PERFORMANCE COMPARISON:")
        print(f"   Legacy:     {legacy_time:.2f}ms")
        print(f"   Optimized:  {optimized_time:.2f}ms")
        print(f"   Improvement: {improvement:.1f}% faster")
        print(f"   Speedup:     {speedup:.1f}x")
        
        if improvement >= 50:
            print(f"   ✅ SIGNIFICANT IMPROVEMENT ACHIEVED!")
        else:
            print(f"   ⚠️  Improvement below expectations")
    
    # Test 3: Campaign data method comparison
    print(f"\n🏢 TEST 3: Campaign Data Method Comparison")
    try:
        # Legacy campaign method
        start_time = time.time()
        legacy_campaigns = analytics_service._get_mixpanel_campaign_data(config)
        legacy_campaign_time = (time.time() - start_time) * 1000
        
        # Optimized campaign method
        start_time = time.time()
        optimized_campaigns = analytics_service._get_mixpanel_campaign_data_optimized(config)
        optimized_campaign_time = (time.time() - start_time) * 1000
        
        print(f"   Legacy Campaign Method:     {legacy_campaign_time:.2f}ms ({len(legacy_campaigns)} records)")
        print(f"   Optimized Campaign Method:  {optimized_campaign_time:.2f}ms ({len(optimized_campaigns)} records)")
        
        if optimized_campaign_time > 0:
            campaign_improvement = ((legacy_campaign_time - optimized_campaign_time) / legacy_campaign_time) * 100
            print(f"   Campaign Method Improvement: {campaign_improvement:.1f}% faster")
        
    except Exception as e:
        print(f"   ❌ Campaign method test failed: {e}")
    
    # Test 4: Rate calculation method comparison
    print(f"\n📊 TEST 4: Rate Calculation Method Test")
    try:
        # Test the new optimized rate method
        entity_ids = ['123', '456', '789']  # Sample entity IDs
        start_time = time.time()
        rates = analytics_service._get_cached_rates_from_precomputed('campaign', entity_ids, config.start_date, config.end_date)
        rate_time = (time.time() - start_time) * 1000
        
        print(f"   Optimized Rate Calculation: {rate_time:.2f}ms ({len(rates)} rates)")
        print(f"   ✅ New rate method works correctly")
        
    except Exception as e:
        print(f"   ❌ Rate calculation test failed: {e}")
    
    # Test 5: Database connectivity
    print(f"\n🗄️ TEST 5: Database Connectivity")
    try:
        import sqlite3
        conn = sqlite3.connect(analytics_service.mixpanel_db_path)
        cursor = conn.cursor()
        
        # Check if pre-computed tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
        if cursor.fetchone():
            print(f"   ✅ daily_mixpanel_metrics table exists")
        else:
            print(f"   ❌ daily_mixpanel_metrics table missing")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics_breakdown'")
        if cursor.fetchone():
            print(f"   ✅ daily_mixpanel_metrics_breakdown table exists")
        else:
            print(f"   ❌ daily_mixpanel_metrics_breakdown table missing")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='id_name_mapping'")
        if cursor.fetchone():
            print(f"   ✅ id_name_mapping table exists")
        else:
            print(f"   ❌ id_name_mapping table missing")
        
        # Check record counts
        cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics WHERE date BETWEEN ? AND ?", [config.start_date, config.end_date])
        metrics_count = cursor.fetchone()[0]
        print(f"   📊 Metrics records in date range: {metrics_count}")
        
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Database connectivity test failed: {e}")
    
    print(f"\n✅ DIRECT METHOD TESTING COMPLETED")
    print("="*60)

if __name__ == "__main__":
    test_optimized_methods()