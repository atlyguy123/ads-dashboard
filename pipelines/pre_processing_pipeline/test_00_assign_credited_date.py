#!/usr/bin/env python3
"""
Comprehensive Test Suite for 00_assign_credited_date.py

This test suite verifies the credited date assignment module works correctly
with various scenarios including edge cases.

Test Categories:
1. Unit tests for individual functions
2. Integration tests with database
3. Edge case tests (missing data, multiple events, etc.)
4. Performance tests
5. Verification tests
"""

import os
import sys
import sqlite3
import tempfile
import unittest
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Add the module path so we can import our module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the module we're testing
import importlib.util
spec = importlib.util.spec_from_file_location("credited_date_module", "00_assign_credited_date.py")
credited_date_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(credited_date_module)


class TestCreditedDateAssignment(unittest.TestCase):
    """Test suite for credited date assignment functionality"""
    
    def setUp(self):
        """Set up test database and sample data"""
        # Create temporary database for testing
        self.test_db_fd, self.test_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.test_db_fd)  # Close the file descriptor
        
        # Update module's DB_PATH for testing
        credited_date_module.DB_PATH = self.test_db_path
        
        # Create tables and sample data
        self.create_test_database()
        self.insert_test_data()
    
    def tearDown(self):
        """Clean up test database"""
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def create_test_database(self):
        """Create test database with required schema"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create mixpanel_event table
        cursor.execute("""
            CREATE TABLE mixpanel_event (
                event_uuid TEXT PRIMARY KEY,
                event_name TEXT NOT NULL,
                distinct_id TEXT NOT NULL,
                event_time DATETIME NOT NULL,
                event_json TEXT
            )
        """)
        
        # Create user_product_metrics table
        cursor.execute("""
            CREATE TABLE user_product_metrics (
                user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                distinct_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                credited_date DATE,
                current_status TEXT NOT NULL DEFAULT 'pending',
                current_value DECIMAL(10,2) NOT NULL DEFAULT 0.0,
                value_status TEXT NOT NULL DEFAULT 'pending',
                last_updated_ts DATETIME NOT NULL,
                UNIQUE (distinct_id, product_id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def insert_test_data(self):
        """Insert test data for various scenarios"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Test events data
        test_events = [
            # User1 + Product1: Trial started first, then initial purchase
            ('evt_001', 'RC Trial started', 'user1', '2024-01-01 10:00:00', '{"properties": {"product_id": "product1"}}'),
            ('evt_002', 'RC Initial purchase', 'user1', '2024-01-02 11:00:00', '{"properties": {"product_id": "product1"}}'),
            
            # User1 + Product2: Initial purchase only
            ('evt_003', 'RC Initial purchase', 'user1', '2024-01-03 12:00:00', '{"properties": {"product_id": "product2"}}'),
            
            # User2 + Product1: Multiple trial starts (should use earliest)
            ('evt_004', 'RC Trial started', 'user2', '2024-01-05 09:00:00', '{"properties": {"product_id": "product1"}}'),
            ('evt_005', 'RC Trial started', 'user2', '2024-01-04 08:00:00', '{"properties": {"product_id": "product1"}}'),  # Earlier
            
            # User3 + Product1: Initial purchase first, then trial start
            ('evt_006', 'RC Initial purchase', 'user3', '2024-01-06 14:00:00', '{"properties": {"product_id": "product1"}}'),
            ('evt_007', 'RC Trial started', 'user3', '2024-01-07 15:00:00', '{"properties": {"product_id": "product1"}}'),
            
            # User4 + Product3: No product_id (should be ignored)
            ('evt_008', 'RC Trial started', 'user4', '2024-01-08 16:00:00', '{"properties": {}}'),
            
            # User5 + Product4: Empty product_id (should be ignored)
            ('evt_009', 'RC Trial started', 'user5', '2024-01-09 17:00:00', '{"properties": {"product_id": ""}}'),
            
            # User6 + Product5: NULL product_id (should be ignored)
            ('evt_010', 'RC Trial started', 'user6', '2024-01-10 18:00:00', '{"properties": {"product_id": null}}'),
        ]
        
        cursor.executemany("""
            INSERT INTO mixpanel_event (event_uuid, event_name, distinct_id, event_time, event_json)
            VALUES (?, ?, ?, ?, ?)
        """, test_events)
        
        # Test user_product_metrics data
        test_metrics = [
            ('user1', 'product1', '2024-01-01 00:00:00'),  # Should get 2024-01-01 (trial start)
            ('user1', 'product2', '2024-01-03 00:00:00'),  # Should get 2024-01-03 (initial purchase)
            ('user2', 'product1', '2024-01-04 00:00:00'),  # Should get 2024-01-04 (earliest trial)
            ('user3', 'product1', '2024-01-06 00:00:00'),  # Should get 2024-01-06 (initial purchase first)
            ('user7', 'product7', '2024-01-11 00:00:00'),  # No events (should remain NULL)
        ]
        
        cursor.executemany("""
            INSERT INTO user_product_metrics (distinct_id, product_id, last_updated_ts)
            VALUES (?, ?, ?)
        """, test_metrics)
        
        conn.commit()
        conn.close()


class TestGetStarterEvents(TestCreditedDateAssignment):
    """Test the get_all_starter_events function"""
    
    def test_get_starter_events_basic(self):
        """Test basic starter event retrieval"""
        df = credited_date_module.get_all_starter_events()
        
        # Should return 7 valid events (excluding the ones without proper product_id)
        self.assertEqual(len(df), 7)
        
        # Check event types
        event_counts = df['event_name'].value_counts()
        self.assertEqual(event_counts.get('RC Trial started', 0), 4)
        self.assertEqual(event_counts.get('RC Initial purchase', 0), 3)
        
        # Check required columns exist
        required_columns = ['distinct_id', 'product_id', 'event_time', 'event_name', 'event_date']
        for col in required_columns:
            self.assertIn(col, df.columns)
    
    def test_starter_events_filtering(self):
        """Test that events without product_id are filtered out"""
        df = credited_date_module.get_all_starter_events()
        
        # Should not contain events with missing/empty/null product_id
        invalid_users = ['user4', 'user5', 'user6']
        for user in invalid_users:
            user_events = df[df['distinct_id'] == user]
            self.assertEqual(len(user_events), 0, f"User {user} should have no events due to invalid product_id")
    
    def test_event_date_extraction(self):
        """Test that event dates are extracted correctly"""
        df = credited_date_module.get_all_starter_events()
        
        # Check specific event date extraction
        user1_product1 = df[(df['distinct_id'] == 'user1') & (df['product_id'] == 'product1')]
        self.assertEqual(len(user1_product1), 2)  # Should have 2 events
        
        # Check dates are in YYYY-MM-DD format
        for _, row in user1_product1.iterrows():
            self.assertRegex(row['event_date'], r'^\d{4}-\d{2}-\d{2}$')


class TestCalculateCreditedDates(TestCreditedDateAssignment):
    """Test the calculate_credited_dates function"""
    
    def test_calculate_earliest_event(self):
        """Test that earliest event is selected for each user-product"""
        df = credited_date_module.get_all_starter_events()
        credited_dates = credited_date_module.calculate_credited_dates(df)
        
        # User1 + Product1: Should use trial start (2024-01-01) over initial purchase (2024-01-02)
        self.assertEqual(credited_dates[('user1', 'product1')], '2024-01-01')
        
        # User1 + Product2: Should use initial purchase (2024-01-03)
        self.assertEqual(credited_dates[('user1', 'product2')], '2024-01-03')
        
        # User2 + Product1: Should use earlier trial start (2024-01-04 vs 2024-01-05)
        self.assertEqual(credited_dates[('user2', 'product1')], '2024-01-04')
        
        # User3 + Product1: Should use initial purchase (2024-01-06) over trial start (2024-01-07)
        self.assertEqual(credited_dates[('user3', 'product1')], '2024-01-06')
    
    def test_unique_user_product_combinations(self):
        """Test that each user-product combination appears exactly once"""
        df = credited_date_module.get_all_starter_events()
        credited_dates = credited_date_module.calculate_credited_dates(df)
        
        # Should have 4 unique combinations based on our test data
        expected_combinations = {
            ('user1', 'product1'),
            ('user1', 'product2'),
            ('user2', 'product1'),
            ('user3', 'product1')
        }
        
        self.assertEqual(set(credited_dates.keys()), expected_combinations)


class TestDatabaseUpdate(TestCreditedDateAssignment):
    """Test the database update functionality"""
    
    def test_update_credited_dates_success(self):
        """Test successful database update"""
        # Get test data
        df = credited_date_module.get_all_starter_events()
        credited_dates = credited_date_module.calculate_credited_dates(df)
        
        # Update database
        success = credited_date_module.update_credited_dates_in_db(credited_dates)
        self.assertTrue(success)
        
        # Verify updates
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Check specific updates
        cursor.execute("""
            SELECT credited_date FROM user_product_metrics 
            WHERE distinct_id = ? AND product_id = ?
        """, ('user1', 'product1'))
        result = cursor.fetchone()
        self.assertEqual(result[0], '2024-01-01')
        
        cursor.execute("""
            SELECT credited_date FROM user_product_metrics 
            WHERE distinct_id = ? AND product_id = ?
        """, ('user1', 'product2'))
        result = cursor.fetchone()
        self.assertEqual(result[0], '2024-01-03')
        
        conn.close()
    
    def test_no_matching_records(self):
        """Test behavior when no matching user_product_metrics records exist"""
        # Create credited_dates for non-existent user-product combinations
        credited_dates = {
            ('nonexistent_user', 'nonexistent_product'): '2024-01-01'
        }
        
        success = credited_date_module.update_credited_dates_in_db(credited_dates)
        self.assertTrue(success)  # Should still succeed, just no updates made
    
    def test_null_credited_dates_remain_null(self):
        """Test that records without matching events keep NULL credited_date"""
        # Run the full process
        df = credited_date_module.get_all_starter_events()
        credited_dates = credited_date_module.calculate_credited_dates(df)
        credited_date_module.update_credited_dates_in_db(credited_dates)
        
        # Check that user7/product7 still has NULL credited_date
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT credited_date FROM user_product_metrics 
            WHERE distinct_id = ? AND product_id = ?
        """, ('user7', 'product7'))
        result = cursor.fetchone()
        self.assertIsNone(result[0])
        
        conn.close()


class TestEdgeCases(TestCreditedDateAssignment):
    """Test edge cases and error conditions"""
    
    def test_empty_events_table(self):
        """Test behavior with no starter events"""
        # Clear events table
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM mixpanel_event")
        conn.commit()
        conn.close()
        
        # Should handle gracefully
        df = credited_date_module.get_all_starter_events()
        self.assertTrue(df.empty)
        
        credited_dates = credited_date_module.calculate_credited_dates(df)
        self.assertEqual(len(credited_dates), 0)
    
    def test_empty_user_product_metrics(self):
        """Test behavior with no user_product_metrics records"""
        # Clear user_product_metrics table
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM user_product_metrics")
        conn.commit()
        conn.close()
        
        # Should handle gracefully
        df = credited_date_module.get_all_starter_events()
        credited_dates = credited_date_module.calculate_credited_dates(df)
        success = credited_date_module.update_credited_dates_in_db(credited_dates)
        self.assertTrue(success)
    
    def test_malformed_event_json(self):
        """Test handling of malformed JSON in event_json"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Insert event with malformed JSON
        cursor.execute("""
            INSERT INTO mixpanel_event (event_uuid, event_name, distinct_id, event_time, event_json)
            VALUES (?, ?, ?, ?, ?)
        """, ('evt_bad', 'RC Trial started', 'user_bad', '2024-01-01 10:00:00', 'invalid json'))
        
        conn.commit()
        conn.close()
        
        # Should handle gracefully (SQLite JSON functions return NULL for invalid JSON)
        df = credited_date_module.get_all_starter_events()
        # The malformed event should be filtered out due to NULL product_id
        bad_events = df[df['distinct_id'] == 'user_bad']
        self.assertEqual(len(bad_events), 0)


class TestIntegration(TestCreditedDateAssignment):
    """Integration tests for the full module"""
    
    def test_main_function_success(self):
        """Test the main function executes successfully"""
        success = credited_date_module.main()
        self.assertTrue(success)
        
        # Verify that credited dates were assigned
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM user_product_metrics 
            WHERE credited_date IS NOT NULL
        """)
        count_with_dates = cursor.fetchone()[0]
        
        # Should have 4 records with credited dates based on our test data
        self.assertEqual(count_with_dates, 4)
        
        conn.close()
    
    def test_verification_function(self):
        """Test the verification function"""
        # Run main first
        credited_date_module.main()
        
        # Then verify
        success = credited_date_module.verify_credited_dates()
        self.assertTrue(success)


class TestPerformance(TestCreditedDateAssignment):
    """Performance tests"""
    
    def test_large_dataset_simulation(self):
        """Test with a larger simulated dataset"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM mixpanel_event")
        cursor.execute("DELETE FROM user_product_metrics")
        
        # Insert larger dataset
        large_events = []
        large_metrics = []
        
        for i in range(1000):
            user_id = f"user_{i}"
            product_id = f"product_{i % 10}"  # 10 products total
            event_date = datetime(2024, 1, 1) + timedelta(days=i % 365)
            
            # Add starter event
            large_events.append((
                f"evt_{i}",
                'RC Trial started',
                user_id,
                event_date.strftime('%Y-%m-%d %H:%M:%S'),
                f'{{"properties": {{"product_id": "{product_id}"}}}}'
            ))
            
            # Add user_product_metrics record
            large_metrics.append((
                user_id,
                product_id,
                event_date.strftime('%Y-%m-%d %H:%M:%S')
            ))
        
        cursor.executemany("""
            INSERT INTO mixpanel_event (event_uuid, event_name, distinct_id, event_time, event_json)
            VALUES (?, ?, ?, ?, ?)
        """, large_events)
        
        cursor.executemany("""
            INSERT INTO user_product_metrics (distinct_id, product_id, last_updated_ts)
            VALUES (?, ?, ?)
        """, large_metrics)
        
        conn.commit()
        conn.close()
        
        # Test performance
        start_time = datetime.now()
        success = credited_date_module.main()
        end_time = datetime.now()
        
        self.assertTrue(success)
        
        # Should complete within reasonable time (less than 10 seconds for 1000 records)
        execution_time = (end_time - start_time).total_seconds()
        self.assertLess(execution_time, 10, f"Execution took {execution_time:.2f} seconds, which is too slow")
        
        # Verify all records were processed
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM user_product_metrics 
            WHERE credited_date IS NOT NULL
        """)
        count_with_dates = cursor.fetchone()[0]
        
        # Should have 1000 records with credited dates
        self.assertEqual(count_with_dates, 1000)
        
        conn.close()


def run_comprehensive_tests():
    """Run all tests and provide detailed report"""
    print("🧪 RUNNING COMPREHENSIVE TESTS FOR CREDITED DATE ASSIGNMENT MODULE")
    print("=" * 80)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestGetStarterEvents,
        TestCalculateCreditedDates,
        TestDatabaseUpdate,
        TestEdgeCases,
        TestIntegration,
        TestPerformance
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 80)
    print("🎯 TEST SUMMARY")
    print(f"   Tests run: {result.testsRun}")
    print(f"   Failures: {len(result.failures)}")
    print(f"   Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"   {test}: {traceback}")
    
    if result.errors:
        print("\n💥 ERRORS:")
        for test, traceback in result.errors:
            print(f"   {test}: {traceback}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        print("\n✅ ALL TESTS PASSED! Module is ready for production use.")
    else:
        print("\n❌ SOME TESTS FAILED! Please fix issues before deployment.")
    
    return success


if __name__ == "__main__":
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1) 