#!/usr/bin/env python3
"""
Dynamic Lifecycle Pattern Analyzer

This script analyzes all user-product event sequences in the database without any 
preconceived notions of what constitutes "valid" vs "invalid" lifecycles.

It provides comprehensive statistics on:
- Event sequence patterns and their frequencies
- Event transition patterns (what comes after what)  
- Revenue patterns within sequences
- Timeline patterns and gaps
- Outlier detection and common pattern identification

The goal is to let the data tell us what's happening, then we can decide 
which patterns need attention.
"""

import sqlite3
import json
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Any
from pathlib import Path
import sys

# Add utils directory to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LifecyclePatternAnalyzer:
    """
    Analyzes lifecycle patterns dynamically without preconceptions about validity.
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_database_path('mixpanel_data')
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Core data structures for analysis
        self.user_lifecycles = {}  # {(user_id, product_id): [events]}
        self.sequence_patterns = Counter()  # Event sequence → frequency
        self.transition_patterns = defaultdict(Counter)  # event → {next_event: count}
        self.revenue_patterns = defaultdict(list)  # sequence → [revenue_amounts]
        self.timeline_stats = defaultdict(list)  # pattern → [time_gaps]
        
        # Analysis results
        self.pattern_analysis = {}
        self.outlier_analysis = {}
        self.transition_analysis = {}
        
    def analyze_all_lifecycles(self):
        """Main analysis function - runs complete lifecycle analysis."""
        logger.info("🔍 Starting Dynamic Lifecycle Pattern Analysis")
        logger.info("=" * 60)
        
        try:
            # Step 1: Load all user-product lifecycles
            self._load_all_lifecycles()
            
            # Step 2: Analyze sequence patterns
            self._analyze_sequence_patterns()
            
            # Step 3: Analyze transition patterns  
            self._analyze_transition_patterns()
            
            # Step 4: Analyze revenue patterns
            self._analyze_revenue_patterns()
            
            # Step 5: Analyze timeline patterns
            self._analyze_timeline_patterns()
            
            # Step 6: Generate comprehensive report
            self._generate_comprehensive_report()
            
            logger.info("✅ Dynamic Lifecycle Analysis Complete")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in lifecycle analysis: {e}")
            return False
        finally:
            self.conn.close()
    
    def _load_all_lifecycles(self):
        """Load complete event history for all user-product pairs."""
        logger.info("📊 Loading all user-product lifecycles...")
        
        cursor = self.conn.cursor()
        
        # Get all user-product pairs from user_product_metrics
        cursor.execute("""
            SELECT DISTINCT distinct_id, product_id 
            FROM user_product_metrics
        """)
        user_product_pairs = cursor.fetchall()
        
        logger.info(f"   Found {len(user_product_pairs):,} user-product pairs to analyze")
        
        # Get all relevant events for these pairs
        cursor.execute("""
            SELECT 
                distinct_id,
                event_name,
                event_time,
                revenue_usd,
                event_json
            FROM mixpanel_event 
            WHERE event_name IN (
                'RC Trial started', 'RC Initial purchase', 'RC Trial converted', 
                'RC Trial cancelled', 'RC Cancellation', 'RC Renewal'
            )
            ORDER BY distinct_id, event_time
        """)
        
        all_events = cursor.fetchall()
        logger.info(f"   Loaded {len(all_events):,} relevant events")
        
        # Group events by user-product pair
        for event in all_events:
            try:
                event_data = json.loads(event['event_json']) if event['event_json'] else {}
                product_id = event_data.get('properties', {}).get('product_id', '')
                
                if product_id:  # Only process events with product_id
                    key = (event['distinct_id'], product_id)
                    
                    if key not in self.user_lifecycles:
                        self.user_lifecycles[key] = []
                    
                    self.user_lifecycles[key].append({
                        'event_name': event['event_name'],
                        'event_time': event['event_time'],
                        'revenue_usd': event['revenue_usd'] or 0,
                        'days_since_start': None  # Will calculate later
                    })
            except (json.JSONDecodeError, KeyError):
                continue
        
        # Sort events chronologically and calculate time gaps
        for key in self.user_lifecycles:
            events = self.user_lifecycles[key]
            events.sort(key=lambda x: x['event_time'])
            
            # Calculate days since first event
            if events:
                start_time = datetime.fromisoformat(events[0]['event_time'].replace('Z', '+00:00'))
                for event in events:
                    event_time = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
                    event['days_since_start'] = (event_time - start_time).days
        
        logger.info(f"✅ Loaded {len(self.user_lifecycles):,} complete user-product lifecycles")
    
    def _analyze_sequence_patterns(self):
        """Analyze complete event sequence patterns."""
        logger.info("🔍 Analyzing event sequence patterns...")
        
        for (user_id, product_id), events in self.user_lifecycles.items():
            if not events:
                continue
                
            # Create event sequence
            event_sequence = tuple(event['event_name'] for event in events)
            self.sequence_patterns[event_sequence] += 1
            
            # Also track subsequences (sliding windows)
            for i in range(len(events)):
                for j in range(i + 2, min(i + 6, len(events) + 1)):  # Max 5-event subsequences
                    subseq = tuple(event['event_name'] for event in events[i:j])
                    if len(subseq) >= 2:  # At least 2 events
                        self.sequence_patterns[subseq] += 1
        
        logger.info(f"   Found {len(self.sequence_patterns):,} unique sequence patterns")
    
    def _analyze_transition_patterns(self):
        """Analyze what events typically follow other events."""
        logger.info("🔍 Analyzing event transition patterns...")
        
        for (user_id, product_id), events in self.user_lifecycles.items():
            for i in range(len(events) - 1):
                current_event = events[i]['event_name']
                next_event = events[i + 1]['event_name']
                
                self.transition_patterns[current_event][next_event] += 1
        
        logger.info(f"   Analyzed transitions for {len(self.transition_patterns)} event types")
    
    def _analyze_revenue_patterns(self):
        """Analyze revenue patterns within different sequences."""
        logger.info("🔍 Analyzing revenue patterns...")
        
        for (user_id, product_id), events in self.user_lifecycles.items():
            if not events:
                continue
            
            # Get total revenue for this lifecycle
            total_revenue = sum(event['revenue_usd'] for event in events)
            
            # Track revenue by sequence pattern
            event_sequence = tuple(event['event_name'] for event in events)
            self.revenue_patterns[event_sequence].append(total_revenue)
            
            # Track revenue by individual events
            for event in events:
                if event['revenue_usd'] != 0:
                    event_name = event['event_name']
                    self.revenue_patterns[event_name].append(event['revenue_usd'])
    
    def _analyze_timeline_patterns(self):
        """Analyze timing patterns and gaps between events."""
        logger.info("🔍 Analyzing timeline patterns...")
        
        for (user_id, product_id), events in self.user_lifecycles.items():
            if len(events) < 2:
                continue
            
            event_sequence = tuple(event['event_name'] for event in events)
            
            # Calculate time gaps between consecutive events
            time_gaps = []
            for i in range(len(events) - 1):
                current_time = datetime.fromisoformat(events[i]['event_time'].replace('Z', '+00:00'))
                next_time = datetime.fromisoformat(events[i + 1]['event_time'].replace('Z', '+00:00'))
                gap_days = (next_time - current_time).days
                time_gaps.append(gap_days)
            
            self.timeline_stats[event_sequence].extend(time_gaps)
    
    def _generate_comprehensive_report(self):
        """Generate comprehensive analysis report."""
        logger.info("📊 Generating comprehensive analysis report...")
        
        print("\n" + "=" * 80)
        print("DYNAMIC LIFECYCLE PATTERN ANALYSIS REPORT")
        print("=" * 80)
        
        self._report_basic_statistics()
        self._report_sequence_patterns()
        self._report_transition_patterns()
        self._report_revenue_patterns()
        self._report_timeline_patterns()
        self._report_outlier_analysis()
        
        print("=" * 80)
    
    def _report_basic_statistics(self):
        """Report basic statistics about the dataset."""
        print(f"\n📊 BASIC STATISTICS:")
        print(f"   Total user-product pairs analyzed: {len(self.user_lifecycles):,}")
        
        # Lifecycle length distribution
        lengths = [len(events) for events in self.user_lifecycles.values() if events]
        if lengths:
            print(f"   Average events per lifecycle: {np.mean(lengths):.1f}")
            print(f"   Event count distribution:")
            length_counts = Counter(lengths)
            for length, count in sorted(length_counts.items())[:10]:  # Top 10
                print(f"     {length} events: {count:,} lifecycles ({count/len(lengths)*100:.1f}%)")
        
        # Revenue distribution
        total_revenues = []
        for events in self.user_lifecycles.values():
            total_revenue = sum(event['revenue_usd'] for event in events)
            total_revenues.append(total_revenue)
        
        if total_revenues:
            positive_revenue = [r for r in total_revenues if r > 0]
            negative_revenue = [r for r in total_revenues if r < 0]
            zero_revenue = [r for r in total_revenues if r == 0]
            
            print(f"   Revenue patterns:")
            print(f"     Positive revenue lifecycles: {len(positive_revenue):,} ({len(positive_revenue)/len(total_revenues)*100:.1f}%)")
            print(f"     Zero revenue lifecycles: {len(zero_revenue):,} ({len(zero_revenue)/len(total_revenues)*100:.1f}%)")
            print(f"     Negative revenue lifecycles: {len(negative_revenue):,} ({len(negative_revenue)/len(total_revenues)*100:.1f}%)")
    
    def _report_sequence_patterns(self):
        """Report the most common sequence patterns."""
        print(f"\n🔄 SEQUENCE PATTERNS (Top 20):")
        
        # Filter to only full lifecycle sequences (not subsequences)
        full_sequences = {seq: count for seq, count in self.sequence_patterns.items() 
                         if self._is_likely_full_sequence(seq)}
        
        for i, (sequence, count) in enumerate(sorted(full_sequences.items(), 
                                                   key=lambda x: x[1], reverse=True)[:20]):
            percentage = count / len(self.user_lifecycles) * 100
            print(f"   {i+1:2d}. {' → '.join(sequence)}")
            print(f"       Count: {count:,} ({percentage:.1f}% of all lifecycles)")
            
            # Show revenue stats for this pattern
            revenues = self.revenue_patterns.get(sequence, [])
            if revenues:
                avg_revenue = np.mean(revenues)
                print(f"       Avg Revenue: ${avg_revenue:.2f}")
            print()
    
    def _report_transition_patterns(self):
        """Report transition patterns - what typically follows each event."""
        print(f"\n➡️  EVENT TRANSITION PATTERNS:")
        
        for event_name in sorted(self.transition_patterns.keys()):
            transitions = self.transition_patterns[event_name]
            total_transitions = sum(transitions.values())
            
            print(f"\n   After '{event_name}' ({total_transitions:,} times):")
            for next_event, count in sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:5]:
                percentage = count / total_transitions * 100
                print(f"     → {next_event}: {count:,} times ({percentage:.1f}%)")
    
    def _report_revenue_patterns(self):
        """Report revenue patterns by event type and sequence."""
        print(f"\n💰 REVENUE PATTERNS:")
        
        print(f"\n   By Event Type:")
        for event_name in ['RC Trial started', 'RC Initial purchase', 'RC Trial converted', 'RC Cancellation']:
            revenues = self.revenue_patterns.get(event_name, [])
            if revenues:
                positive = [r for r in revenues if r > 0]
                negative = [r for r in revenues if r < 0]
                zero = [r for r in revenues if r == 0]
                
                print(f"     {event_name}:")
                if positive:
                    print(f"       Positive: {len(positive):,} events, avg ${np.mean(positive):.2f}")
                if negative:
                    print(f"       Negative: {len(negative):,} events, avg ${np.mean(negative):.2f}")
                if zero:
                    print(f"       Zero: {len(zero):,} events")
    
    def _report_timeline_patterns(self):
        """Report timing patterns between events."""
        print(f"\n⏰ TIMELINE PATTERNS:")
        
        # Analyze common time gaps
        all_gaps = []
        for gaps in self.timeline_stats.values():
            all_gaps.extend(gaps)
        
        if all_gaps:
            print(f"   Time gaps between events:")
            print(f"     Average: {np.mean(all_gaps):.1f} days")
            print(f"     Median: {np.median(all_gaps):.1f} days")
            print(f"     Same day events: {sum(1 for gap in all_gaps if gap == 0):,} ({sum(1 for gap in all_gaps if gap == 0)/len(all_gaps)*100:.1f}%)")
            print(f"     Within 7 days: {sum(1 for gap in all_gaps if 0 <= gap <= 7):,} ({sum(1 for gap in all_gaps if 0 <= gap <= 7)/len(all_gaps)*100:.1f}%)")
            print(f"     Within 30 days: {sum(1 for gap in all_gaps if 0 <= gap <= 30):,} ({sum(1 for gap in all_gaps if 0 <= gap <= 30)/len(all_gaps)*100:.1f}%)")
    
    def _report_outlier_analysis(self):
        """Identify and report outlier patterns."""
        print(f"\n🚨 OUTLIER ANALYSIS:")
        
        # Find very rare patterns (less than 0.1% of lifecycles)
        total_lifecycles = len(self.user_lifecycles)
        rare_threshold = total_lifecycles * 0.001  # 0.1%
        
        rare_patterns = {seq: count for seq, count in self.sequence_patterns.items() 
                        if count <= rare_threshold and self._is_likely_full_sequence(seq)}
        
        print(f"   Rare patterns (≤{rare_threshold:.0f} occurrences):")
        print(f"     Found {len(rare_patterns):,} rare sequence patterns")
        
        # Show some examples
        for i, (sequence, count) in enumerate(sorted(rare_patterns.items(), 
                                                   key=lambda x: len(x[0]), reverse=True)[:10]):
            print(f"     {i+1}. {' → '.join(sequence)} ({count} times)")
        
        # Find patterns with unusual revenue
        print(f"\n   Revenue outliers:")
        for sequence, revenues in self.revenue_patterns.items():
            if len(revenues) >= 10 and self._is_likely_full_sequence(sequence):  # Only analyze common patterns
                if any(r < -100 for r in revenues):  # Large negative revenue
                    large_refunds = [r for r in revenues if r < -100]
                    print(f"     Large refunds in '{' → '.join(sequence)}': {len(large_refunds)} cases, avg ${np.mean(large_refunds):.2f}")
                
                if any(r > 500 for r in revenues):  # Large positive revenue
                    large_revenues = [r for r in revenues if r > 500]
                    print(f"     Large revenues in '{' → '.join(sequence)}': {len(large_revenues)} cases, avg ${np.mean(large_revenues):.2f}")
    
    def _is_likely_full_sequence(self, sequence):
        """Heuristic to determine if a sequence is likely a complete lifecycle vs subsequence."""
        # A full sequence likely starts with a start event and doesn't have too many repetitions
        start_events = ['RC Trial started', 'RC Initial purchase']
        
        if not sequence:
            return False
            
        # Should start with a start event
        if sequence[0] not in start_events:
            return False
            
        # Shouldn't be too repetitive (more than 70% same event)
        most_common = Counter(sequence).most_common(1)[0][1]
        if most_common / len(sequence) > 0.7:
            return False
            
        return True

def main():
    """Main function to run the dynamic lifecycle analysis."""
    logger.info("Starting Dynamic Lifecycle Pattern Analysis")
    
    try:
        analyzer = LifecyclePatternAnalyzer()
        success = analyzer.analyze_all_lifecycles()
        
        if success:
            logger.info("Analysis completed successfully")
            return True
        else:
            logger.error("Analysis failed")
            return False
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 