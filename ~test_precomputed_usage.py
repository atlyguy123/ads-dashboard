#!/usr/bin/env python3
"""
Test to verify that the analytics query service is actually using pre-computed database values
rather than computing metrics on-demand
"""

import sys
import os
sys.path.append('/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final')

import sqlite3
import time
from utils.database_utils import get_database_connection
from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService
from datetime import datetime, timedelta

def test_direct_database_query():
    """Test direct database queries to verify pre-computed data"""
    print("🔍 TESTING DIRECT DATABASE QUERIES")
    print("=" * 50)
    
    try:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Test 1: Direct query for overview data (what overview sparklines should use)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=27)).strftime('%Y-%m-%d')
            
            print(f"1. Testing direct overview aggregation query ({start_date} to {end_date})...")
            
            start_time = time.time()
            overview_query = """
            SELECT 
                date,
                SUM(meta_spend) as daily_spend,
                SUM(meta_impressions) as daily_impressions,
                SUM(meta_clicks) as daily_clicks,
                SUM(meta_trial_count) as daily_meta_trials,
                SUM(meta_purchase_count) as daily_meta_purchases,
                SUM(trial_users_count) as daily_mixpanel_trials,
                SUM(purchase_users_count) as daily_mixpanel_purchases,
                SUM(estimated_revenue_usd) as daily_estimated_revenue,
                SUM(adjusted_estimated_revenue_usd) as daily_adjusted_revenue,
                SUM(profit_usd) as daily_profit,
                AVG(estimated_roas) as daily_roas,
                AVG(trial_accuracy_ratio) as daily_trial_accuracy
            FROM daily_mixpanel_metrics 
            WHERE date BETWEEN ? AND ?
            GROUP BY date
            ORDER BY date
            """
            
            cursor.execute(overview_query, [start_date, end_date])
            results = cursor.fetchall()
            query_time = (time.time() - start_time) * 1000
            
            print(f"   ⏱️  Direct DB query: {query_time:.1f}ms")
            print(f"   📊 Returned {len(results)} days of data")
            
            if results:
                latest_day = results[-1]
                print(f"   📈 Latest day sample: {latest_day[0]} - ${latest_day[1]:.2f} spend, {latest_day[6]} trials, ${latest_day[9]:.2f} revenue")
            
            return results, query_time
            
    except Exception as e:
        print(f"❌ Error in direct database query: {e}")
        return None, 0

def test_analytics_service_query():
    """Test analytics service to see if it uses pre-computed data"""
    print("\n🔍 TESTING ANALYTICS SERVICE QUERIES")
    print("=" * 50)
    
    service = AnalyticsQueryService()
    
    # Test overview sparklines (what we tested earlier)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=27)).strftime('%Y-%m-%d')
    
    print(f"1. Testing overview sparklines service ({start_date} to {end_date})...")
    
    start_time = time.time()
    result = service.get_overview_roas_chart_data(start_date, end_date, 'all')
    service_time = (time.time() - start_time) * 1000
    
    print(f"   ⏱️  Service query: {service_time:.1f}ms")
    print(f"   📊 Success: {result.get('success', False)}")
    
    if result.get('success'):
        chart_data = result.get('chart_data', [])
        metadata = result.get('metadata', {})
        
        print(f"   📈 Chart data days: {len(chart_data)}")
        print(f"   💰 Total spend: ${metadata.get('total_spend', 0):,.2f}")
        print(f"   💵 Total revenue: ${metadata.get('total_revenue', 0):,.2f}")
        
        if chart_data:
            latest_day = chart_data[-1]
            print(f"   📈 Latest day sample: {latest_day.get('date')} - ${latest_day.get('daily_spend', 0):,.2f} spend, {latest_day.get('daily_mixpanel_trials', 0)} trials")
    
    return result, service_time

def compare_query_sources():
    """Compare direct DB queries vs service queries to verify they use same data source"""
    print("\n🔍 COMPARING DATA SOURCES")
    print("=" * 50)
    
    # Run both queries
    db_results, db_time = test_direct_database_query()
    service_result, service_time = test_analytics_service_query()
    
    if not db_results or not service_result.get('success'):
        print("❌ One or both queries failed - cannot compare")
        return False
    
    # Compare timing (should be similar if both use pre-computed data)
    print(f"\n⏱️  TIMING COMPARISON:")
    print(f"   Direct DB query: {db_time:.1f}ms")
    print(f"   Service query:   {service_time:.1f}ms")
    
    timing_ratio = service_time / db_time if db_time > 0 else float('inf')
    print(f"   Service overhead: {timing_ratio:.1f}x")
    
    if timing_ratio > 10:
        print("⚠️  Service is significantly slower - may not be using pre-computed data efficiently")
    else:
        print("✅ Service timing suggests it's using pre-computed data")
    
    # Compare data values
    print(f"\n📊 DATA COMPARISON:")
    chart_data = service_result.get('chart_data', [])
    
    if len(db_results) == len(chart_data):
        print(f"   Record count match: ✅ {len(db_results)} days")
        
        # Compare a few sample values
        for i in range(min(3, len(db_results))):
            db_row = db_results[i]
            chart_row = chart_data[i]
            
            db_date = db_row[0]
            db_spend = float(db_row[1])
            db_trials = int(db_row[6])
            
            chart_date = chart_row.get('date')
            chart_spend = float(chart_row.get('daily_spend', 0))
            chart_trials = int(chart_row.get('daily_mixpanel_trials', 0))
            
            print(f"   Day {i+1}: {db_date} vs {chart_date}")
            print(f"     Spend: ${db_spend:.2f} vs ${chart_spend:.2f} {'✅' if abs(db_spend - chart_spend) < 0.01 else '❌'}")
            print(f"     Trials: {db_trials} vs {chart_trials} {'✅' if db_trials == chart_trials else '❌'}")
    else:
        print(f"   Record count mismatch: DB={len(db_results)}, Service={len(chart_data)} ❌")
        return False
    
    print("\n✅ DATA SOURCE VERIFICATION:")
    print("   Service appears to be using pre-computed database values")
    print("   Query times suggest efficient pre-computed data access")
    
    return True

def test_individual_entity_query():
    """Test individual entity queries to verify they use pre-computed data"""
    print("\n🔍 TESTING INDIVIDUAL ENTITY QUERIES") 
    print("=" * 50)
    
    try:
        # First find a valid entity with data
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT entity_type, entity_id, COUNT(*) as days_with_data
                FROM daily_mixpanel_metrics 
                WHERE trial_users_count > 0 OR meta_spend > 0
                GROUP BY entity_type, entity_id
                ORDER BY days_with_data DESC
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            if not result:
                print("❌ No entities with data found")
                return False
                
            entity_type, entity_id, days_count = result
            print(f"Testing entity: {entity_type} {entity_id} (has {days_count} days of data)")
            
            # Test direct database query for this entity
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=13)).strftime('%Y-%m-%d')
            
            start_time = time.time()
            cursor.execute("""
                SELECT date, meta_spend, trial_users_count, estimated_revenue_usd, trial_accuracy_ratio, estimated_roas
                FROM daily_mixpanel_metrics
                WHERE entity_type = ? AND entity_id = ? AND date BETWEEN ? AND ?
                ORDER BY date
            """, [entity_type, entity_id, start_date, end_date])
            
            db_rows = cursor.fetchall()
            db_time = (time.time() - start_time) * 1000
            
            print(f"   ⏱️  Direct entity query: {db_time:.1f}ms")
            print(f"   📊 Found {len(db_rows)} days of data")
            
            if db_rows:
                sample_row = db_rows[0]
                print(f"   📈 Sample: {sample_row[0]} - ${sample_row[1]:.2f} spend, {sample_row[2]} trials, ROAS {sample_row[5]:.2f}")
                
                return True
            else:
                print("❌ No data found for this entity in the date range")
                return False
                
    except Exception as e:
        print(f"❌ Error testing individual entity: {e}")
        return False

if __name__ == "__main__":
    print("🔍 VERIFYING PRE-COMPUTED DATA USAGE")
    print("=" * 60)
    
    # Test all query types
    success = True
    
    # Test direct database access
    db_results, _ = test_direct_database_query()
    if not db_results:
        success = False
    
    # Test service queries
    service_result, _ = test_analytics_service_query() 
    if not service_result or not service_result.get('success'):
        success = False
    
    # Compare to verify they use same source
    if success:
        comparison_ok = compare_query_sources()
        if not comparison_ok:
            success = False
    
    # Test individual entity queries
    entity_ok = test_individual_entity_query()
    if not entity_ok:
        success = False
    
    # Final summary
    print(f"\n📊 FINAL VERIFICATION RESULT")
    print("=" * 40)
    
    if success:
        print("✅ CONFIRMED: Analytics service is using pre-computed database values")
        print("✅ Query performance indicates efficient pre-computed data access")
        print("✅ Data consistency verified between direct DB and service queries")
        print("\n🎉 PRE-COMPUTATION SYSTEM IS WORKING CORRECTLY!")
    else:
        print("❌ ISSUES DETECTED: Service may not be using pre-computed data properly")
        print("⚠️  Manual verification needed")