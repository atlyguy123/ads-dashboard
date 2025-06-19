# Value Estimation Debug Handlers
# This module contains handlers for debugging value estimation calculations

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

logger = logging.getLogger(__name__)

class ValueEstimationDebugger:
    """Main class for debugging value estimation calculations"""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = get_database_path('mixpanel_data')
        self.db_path = db_path
    
    def get_overview_statistics(self) -> Dict[str, Any]:
        """Get comprehensive overview statistics for value estimation debug"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Basic statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_pairs,
                    COUNT(CASE WHEN upm.current_status != 'PLACEHOLDER_STATUS' AND upm.current_value != -999.99 THEN 1 END) as processed_pairs,
                    COUNT(CASE WHEN upm.current_value > 0 THEN 1 END) as pairs_with_values,
                    AVG(CASE WHEN upm.current_value > 0 THEN upm.current_value END) as avg_value,
                    SUM(CASE WHEN upm.current_value > 0 THEN upm.current_value ELSE 0 END) as total_value
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
            """)
            
            stats = cursor.fetchone()
            total_pairs, processed_pairs, pairs_with_values, avg_value, total_value = stats
            
            # Current status breakdown (ordered chronologically)
            cursor.execute("""
                SELECT upm.current_status, COUNT(*) as count
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.current_status IS NOT NULL AND upm.current_status != 'PLACEHOLDER_STATUS'
                GROUP BY upm.current_status
                ORDER BY 
                    CASE upm.current_status
                        WHEN 'pending_trial' THEN 1
                        WHEN 'trial_cancelled' THEN 2
                        WHEN 'trial_converted' THEN 3
                        WHEN 'trial_converted_cancelled' THEN 4
                        WHEN 'trial_converted_refunded' THEN 5
                        WHEN 'initial_purchase' THEN 6
                        WHEN 'purchase_cancelled' THEN 7
                        WHEN 'purchase_refunded' THEN 8
                        ELSE 9
                    END
            """)
            status_breakdown = []
            for row in cursor.fetchall():
                status, count = row
                status_breakdown.append({
                    'status': status,
                    'count': count,
                    'percentage': round((count / processed_pairs * 100) if processed_pairs > 0 else 0, 1)
                })
            
            # Value status breakdown (ordered logically)
            cursor.execute("""
                SELECT upm.value_status, COUNT(*) as count
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.value_status IS NOT NULL AND upm.value_status != 'PLACEHOLDER_VALUE_STATUS'
                GROUP BY upm.value_status
                ORDER BY 
                    CASE upm.value_status
                        WHEN 'pending_trial' THEN 1
                        WHEN 'final_value' THEN 2
                        WHEN 'post_conversion_pre_refund' THEN 3
                        WHEN 'post_purchase_pre_refund' THEN 4
                        ELSE 5
                    END
            """)
            value_status_breakdown = []
            for row in cursor.fetchall():
                status, count = row
                value_status_breakdown.append({
                    'status': status,
                    'count': count,
                    'percentage': round((count / processed_pairs * 100) if processed_pairs > 0 else 0, 1)
                })
            
            # Enhanced value distribution with $20 buckets and negative/zero values
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN upm.current_value < 0 AND upm.current_value != -999.99 THEN 1 END) as negative_value,
                    COUNT(CASE WHEN upm.current_value = 0 THEN 1 END) as zero_value,
                    COUNT(CASE WHEN upm.current_value > 0 AND upm.current_value <= 20 THEN 1 END) as bucket_0_20,
                    COUNT(CASE WHEN upm.current_value > 20 AND upm.current_value <= 40 THEN 1 END) as bucket_20_40,
                    COUNT(CASE WHEN upm.current_value > 40 AND upm.current_value <= 60 THEN 1 END) as bucket_40_60,
                    COUNT(CASE WHEN upm.current_value > 60 AND upm.current_value <= 80 THEN 1 END) as bucket_60_80,
                    COUNT(CASE WHEN upm.current_value > 80 AND upm.current_value <= 100 THEN 1 END) as bucket_80_100,
                    COUNT(CASE WHEN upm.current_value > 100 AND upm.current_value <= 150 THEN 1 END) as bucket_100_150,
                    COUNT(CASE WHEN upm.current_value > 150 AND upm.current_value <= 200 THEN 1 END) as bucket_150_200,
                    COUNT(CASE WHEN upm.current_value > 200 THEN 1 END) as bucket_200_plus,
                    COUNT(*) as total_with_values
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.current_value IS NOT NULL AND upm.current_value != -999.99
            """)
            
            value_dist = cursor.fetchone()
            neg_val, zero_val, b0_20, b20_40, b40_60, b60_80, b80_100, b100_150, b150_200, b200_plus, total_with_values = value_dist
            
            value_distribution = [
                {'range': 'Below $0', 'count': neg_val or 0, 'percentage': round(((neg_val or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$0.00', 'count': zero_val or 0, 'percentage': round(((zero_val or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$0.01 - $20.00', 'count': b0_20 or 0, 'percentage': round(((b0_20 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$20.01 - $40.00', 'count': b20_40 or 0, 'percentage': round(((b20_40 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$40.01 - $60.00', 'count': b40_60 or 0, 'percentage': round(((b40_60 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$60.01 - $80.00', 'count': b60_80 or 0, 'percentage': round(((b60_80 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$80.01 - $100.00', 'count': b80_100 or 0, 'percentage': round(((b80_100 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$100.01 - $150.00', 'count': b100_150 or 0, 'percentage': round(((b100_150 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$150.01 - $200.00', 'count': b150_200 or 0, 'percentage': round(((b150_200 or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
                {'range': '$200.01+', 'count': b200_plus or 0, 'percentage': round(((b200_plus or 0) / total_with_values * 100) if total_with_values > 0 else 0, 1)},
            ]
            
            # Phase distribution for trial users
            cursor.execute("""
                SELECT 
                    e.distinct_id,
                    JSON_EXTRACT(e.event_json, '$.properties.product_id') as product_id,
                    e.event_time,
                    upm.value_status,
                    CASE 
                        WHEN JULIANDAY('now') - JULIANDAY(DATE(e.event_time)) <= 7 THEN 'phase_1_0_7_days'
                        WHEN JULIANDAY('now') - JULIANDAY(DATE(e.event_time)) <= 37 THEN 'phase_2_8_37_days'
                        ELSE 'phase_3_38_plus_days'
                    END as phase
                FROM mixpanel_event e
                JOIN user_product_metrics upm ON e.distinct_id = upm.distinct_id 
                    AND JSON_EXTRACT(e.event_json, '$.properties.product_id') = upm.product_id
                JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
                WHERE e.event_name = 'RC Trial started'
                  AND upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.value_status IS NOT NULL AND upm.value_status != 'PLACEHOLDER_VALUE_STATUS'
            """)
            
            trial_phases = {}
            for row in cursor.fetchall():
                phase = row[4]
                trial_phases[phase] = trial_phases.get(phase, 0) + 1
            
            conn.close()
            
            return {
                'success': True,
                'statistics': {
                    'total_user_product_pairs': total_pairs or 0,
                    'processed_pairs': processed_pairs or 0,
                    'pairs_with_values': pairs_with_values or 0,
                    'avg_value': round(avg_value or 0, 2),
                    'total_estimated_value': round(total_value or 0, 2),
                    'processing_rate': round((processed_pairs / total_pairs * 100) if total_pairs > 0 else 0, 1)
                },
                'status_breakdown': status_breakdown,
                'value_status_breakdown': value_status_breakdown,
                'value_distribution': value_distribution,
                'trial_phases': trial_phases
            }
            
        except Exception as e:
            logger.error(f"Error getting overview statistics: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_status_examples(self) -> Dict[str, Any]:
        """Get 2 examples for each current_status and value_status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all distinct current_status values
            cursor.execute("""
                SELECT DISTINCT upm.current_status
                FROM user_product_metrics upm
                WHERE upm.current_status IS NOT NULL 
                  AND upm.current_status != 'PLACEHOLDER_STATUS'
                ORDER BY upm.current_status
            """)
            current_statuses = [row[0] for row in cursor.fetchall()]
            
            # Get all distinct value_status values
            cursor.execute("""
                SELECT DISTINCT upm.value_status
                FROM user_product_metrics upm
                WHERE upm.value_status IS NOT NULL 
                  AND upm.value_status != 'PLACEHOLDER_VALUE_STATUS'
                ORDER BY upm.value_status
            """)
            value_statuses = [row[0] for row in cursor.fetchall()]
            
            current_status_examples = {}
            value_status_examples = {}
            
            # Get 2 examples for each current_status
            for status in current_statuses:
                cursor.execute("""
                    SELECT 
                        upm.distinct_id, upm.product_id, upm.current_status, upm.current_value,
                        upm.value_status, upm.price_bucket, upm.trial_conversion_rate,
                        upm.trial_converted_to_refund_rate, upm.initial_purchase_to_refund_rate,
                        upm.accuracy_score, u.region, u.profile_json
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE upm.current_status = ?
                    ORDER BY upm.current_value DESC
                    LIMIT 2
                """, (status,))
                
                examples = []
                for row in cursor.fetchall():
                    user_data = self._build_user_data(cursor, row)
                    if user_data:
                        examples.append(user_data)
                
                current_status_examples[status] = examples
            
            # Get 2 examples for each value_status
            for status in value_statuses:
                cursor.execute("""
                    SELECT 
                        upm.distinct_id, upm.product_id, upm.current_status, upm.current_value,
                        upm.value_status, upm.price_bucket, upm.trial_conversion_rate,
                        upm.trial_converted_to_refund_rate, upm.initial_purchase_to_refund_rate,
                        upm.accuracy_score, u.region, u.profile_json
                    FROM user_product_metrics upm
                    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                    WHERE upm.value_status = ?
                    ORDER BY upm.current_value DESC
                    LIMIT 2
                """, (status,))
                
                examples = []
                for row in cursor.fetchall():
                    user_data = self._build_user_data(cursor, row)
                    if user_data:
                        examples.append(user_data)
                
                value_status_examples[status] = examples
            
            conn.close()
            
            return {
                'success': True,
                'current_status_examples': current_status_examples,
                'value_status_examples': value_status_examples
            }
            
        except Exception as e:
            logger.error(f"Error getting status examples: {e}")
            return {'success': False, 'error': str(e)}
    
    def _build_user_data(self, cursor, row) -> Optional[Dict]:
        """Build user data object from database row"""
        try:
            distinct_id, product_id, current_status, current_value, value_status, price_bucket, trial_conv_rate, trial_refund_rate, purchase_refund_rate, accuracy_score, region, profile_json = row
            
            # Parse profile
            try:
                profile = json.loads(profile_json) if profile_json else {}
            except json.JSONDecodeError:
                profile = {}
            
            # Get user's subscription events for this product
            events = self._get_user_events(cursor, distinct_id, product_id)
            
            # Calculate additional metrics
            start_event = self._find_start_event(events)
            credited_date = start_event.get('event_time', '')[:10] if start_event else None
            days_since_start = None
            
            if credited_date:
                try:
                    start_date = datetime.strptime(credited_date, '%Y-%m-%d').date()
                    days_since_start = (datetime.utcnow().date() - start_date).days
                except:
                    pass
            
            return {
                'distinct_id': distinct_id,
                'product_id': product_id,
                'current_status': current_status,
                'current_value': float(current_value) if current_value and current_value != -999.99 else 0.0,
                'value_status': value_status,
                'price_bucket': float(price_bucket) if price_bucket else 0.0,
                'trial_conversion_rate': float(trial_conv_rate) if trial_conv_rate else 0.0,
                'trial_refund_rate': float(trial_refund_rate) if trial_refund_rate else 0.0,
                'purchase_refund_rate': float(purchase_refund_rate) if purchase_refund_rate else 0.0,
                'accuracy_score': accuracy_score if accuracy_score else 'unknown',
                'country': profile.get('mp_country_code', ''),
                'region': region if region else 'unknown',
                'credited_date': credited_date,
                'days_since_start': days_since_start,
                'events': events,
                'start_event_type': start_event.get('event_name', '') if start_event else '',
                'total_events': len(events)
            }
            
        except Exception as e:
            logger.error(f"Error building user data: {e}")
            return None
    
    def _get_user_events(self, cursor, distinct_id: str, product_id: str) -> List[Dict]:
        """Get subscription events for a specific user-product pair"""
        try:
            cursor.execute("""
                SELECT event_name, event_time, revenue_usd, refund_flag
                FROM mixpanel_event
                WHERE distinct_id = ?
                  AND JSON_EXTRACT(event_json, '$.properties.product_id') = ?
                  AND event_name IN ('RC Trial started', 'RC Trial cancelled', 'RC Trial converted', 'RC Initial purchase', 'RC Cancellation')
                ORDER BY event_time
            """, (distinct_id, product_id))
            
            events = []
            for row in cursor.fetchall():
                event_name, event_time, revenue_usd, refund_flag = row
                events.append({
                    'event_name': event_name,
                    'event_time': event_time,
                    'revenue_usd': float(revenue_usd) if revenue_usd else 0.0,
                    'refund_flag': bool(refund_flag)
                })
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting events for {distinct_id}, {product_id}: {e}")
            return []
    
    def _find_start_event(self, events: List[Dict]) -> Optional[Dict]:
        """Find the subscription start event (trial start or initial purchase)"""
        for event in events:
            if event['event_name'] in ['RC Trial started', 'RC Initial purchase']:
                return event
        return None
    
    def validate_value_calculations(self) -> Dict[str, Any]:
        """Validate value calculations and identify potential issues"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            validation_issues = []
            
            # Check for users with placeholder values
            cursor.execute("""
                SELECT COUNT(*) 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND (upm.current_status = 'PLACEHOLDER_STATUS' OR upm.current_value = -999.99 OR upm.value_status = 'PLACEHOLDER_VALUE_STATUS')
            """)
            
            placeholder_count = cursor.fetchone()[0]
            if placeholder_count > 0:
                validation_issues.append({
                    'type': 'placeholder_values',
                    'count': placeholder_count,
                    'description': f'{placeholder_count} user-product pairs still have placeholder values'
                })
            
            # Check for negative values (should only be 0 or positive)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.current_value < 0 AND upm.current_value != -999.99
            """)
            
            negative_count = cursor.fetchone()[0]
            if negative_count > 0:
                validation_issues.append({
                    'type': 'negative_values',
                    'count': negative_count,
                    'description': f'{negative_count} user-product pairs have negative values'
                })
            
            # Check for inconsistent status combinations
            cursor.execute("""
                SELECT COUNT(*) 
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.valid_lifecycle = 1 AND u.valid_user = 1
                  AND upm.current_status IN ('trial_converted', 'initial_purchase')
                  AND upm.current_value = 0
            """)
            
            inconsistent_count = cursor.fetchone()[0]
            if inconsistent_count > 0:
                validation_issues.append({
                    'type': 'inconsistent_status_value',
                    'count': inconsistent_count,
                    'description': f'{inconsistent_count} converted users have $0 value'
                })
            
            conn.close()
            
            return {
                'success': True,
                'validation_issues': validation_issues,
                'total_issues': len(validation_issues)
            }
            
        except Exception as e:
            logger.error(f"Error validating calculations: {e}")
            return {'success': False, 'error': str(e)}


# Handler functions for the debug API
def load_overview():
    """Load overview statistics for value estimation debug"""
    debugger = ValueEstimationDebugger()
    return debugger.get_overview_statistics()

def load_status_examples():
    """Load examples for all statuses"""
    debugger = ValueEstimationDebugger()
    return debugger.get_status_examples()

def validate_calculations():
    """Validate value calculations"""
    debugger = ValueEstimationDebugger()
    return debugger.validate_value_calculations()

def load_single_status_examples(status_type: str, status_value: str):
    """Load examples for a single status"""
    try:
        debugger = ValueEstimationDebugger()
        conn = sqlite3.connect(debugger.db_path)
        cursor = conn.cursor()
        
        if status_type == 'current_status':
            cursor.execute("""
                SELECT 
                    upm.distinct_id, upm.product_id, upm.current_status, upm.current_value,
                    upm.value_status, upm.price_bucket, upm.trial_conversion_rate,
                    upm.trial_converted_to_refund_rate, upm.initial_purchase_to_refund_rate,
                    upm.accuracy_score, u.region, u.profile_json
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.current_status = ?
                ORDER BY upm.current_value DESC
                LIMIT 2
            """, (status_value,))
        elif status_type == 'value_status':
            cursor.execute("""
                SELECT 
                    upm.distinct_id, upm.product_id, upm.current_status, upm.current_value,
                    upm.value_status, upm.price_bucket, upm.trial_conversion_rate,
                    upm.trial_converted_to_refund_rate, upm.initial_purchase_to_refund_rate,
                    upm.accuracy_score, u.region, u.profile_json
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE upm.value_status = ?
                ORDER BY upm.current_value DESC
                LIMIT 2
            """, (status_value,))
        else:
            return {'success': False, 'error': 'Invalid status_type'}
        
        examples = []
        for row in cursor.fetchall():
            user_data = debugger._build_user_data(cursor, row)
            if user_data:
                examples.append(user_data)
        
        conn.close()
        
        return {
            'success': True,
            'examples': examples
        }
        
    except Exception as e:
        logger.error(f"Error loading single status examples: {e}")
        return {'success': False, 'error': str(e)} 