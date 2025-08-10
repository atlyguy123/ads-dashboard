# Pre-Computation Specification for Dashboard Optimization

## Overview

This specification outlines the implementation of a pre-computation system to optimize dashboard performance by calculating and storing all metrics in advance. Instead of computing metrics on-demand during dashboard requests, all calculations will be performed during the master pipeline execution and stored in optimized data structures for fast retrieval.

## 🚨 **CRITICAL IMPLEMENTATION NOTES**

### **Database Schema Validation Requirements**
**URGENT**: The following tables exist in `database/schema.sql` but are **missing from EXPECTED_TABLES validation** in `02_setup_database.py` and must be added before implementation:
- `daily_mixpanel_metrics` (currently missing from validation)
- `id_name_mapping` (currently missing from validation) 
- `id_hierarchy_mapping` (currently missing from validation)

### **Database Schema Updates Required**
**CRITICAL**: The following schema modifications must be applied to `database/schema.sql`:
1. **Add 20+ new columns** to existing `daily_mixpanel_metrics` table (lines 47-90)
2. **Create new `daily_mixpanel_metrics_breakdown` table** (lines 94-158) 
3. **Add corresponding indexes** for optimal query performance (lines 162-174)
4. **Update database validation** in `02_setup_database.py` EXPECTED_TABLES (lines 386-440)

**Note**: Schema changes are **backward-compatible** - existing data preserved during extension.

### **Meta Database Integration**
Meta Analytics database integration is **required** for pre-computation. If `meta_analytics.db` is unavailable, the module should **fail with clear error message** rather than attempting graceful degradation.

### **Memory Management Reality Check**
While specification estimates 3-4GB peak usage, loading "entire event tables" for processing could easily exceed 8GB. Implementation should include memory monitoring and batch processing safeguards.

## Current State Analysis

### Current Dashboard Flow
1. **Real-time Computation**: Dashboard currently calculates metrics on-demand using various calculators:
   - Revenue calculators (actual vs estimated revenue)
   - ROAS calculators (estimated ROAS with accuracy adjustments)
   - Accuracy calculators (trial/purchase accuracy ratios)
   - Cost calculators (cost per trial/purchase for both Mixpanel and Meta)
   - Rate calculators (conversion rates, refund rates)

2. **Performance Bottlenecks**:
   - Complex JOIN queries across multiple databases
   - Real-time aggregations for different date ranges
   - On-demand breakdown calculations (country, device, region)
   - Hierarchical rollups (campaign → adset → ad)

## Proposed Pre-Computation Architecture

### Core Concept
Pre-compute **all dashboard metrics** for **every advertising entity** (campaign, adset, ad) for **every date** with **complete breakdown support** (all countries, overall) during the master pipeline execution.

### Data Structure Design

#### Table 1: Core Daily Metrics (Overall Data) - EXTENDS EXISTING TABLE
```sql
-- EXTEND existing daily_mixpanel_metrics table with additional columns
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_spend DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_impressions INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_clicks INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_trial_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_purchase_count INTEGER NOT NULL DEFAULT 0;

-- Current schema already has:
-- entity_type, entity_id, date
-- trial_users_count (as mixpanel_trial_count), trial_users_list (JSON array)
-- purchase_users_count (as mixpanel_purchase_count), purchase_users_list (JSON array)
-- estimated_revenue_usd, computed_at

-- ADD the following additional columns:
ALTER TABLE daily_mixpanel_metrics ADD COLUMN post_trial_user_ids TEXT; -- JSON array for users in post-trial phase
ALTER TABLE daily_mixpanel_metrics ADD COLUMN converted_user_ids TEXT;   -- JSON array for trial->purchase conversions  
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_refund_user_ids TEXT; -- JSON array for trial refunds
ALTER TABLE daily_mixpanel_metrics ADD COLUMN purchase_refund_user_ids TEXT; -- JSON array for purchase refunds

-- ADD calculated metrics (using existing calculator logic)
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_conversion_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_conversion_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN purchase_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN purchase_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000;

-- ADD revenue metrics (using existing revenue calculator logic)
ALTER TABLE daily_mixpanel_metrics ADD COLUMN actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN actual_refunds_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN net_actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN adjusted_estimated_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00;

-- ADD performance metrics (using existing accuracy and ROAS calculators)
ALTER TABLE daily_mixpanel_metrics ADD COLUMN profit_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN estimated_roas DECIMAL(8,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN trial_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN purchase_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000;

-- ADD cost metrics (using existing cost calculators)
ALTER TABLE daily_mixpanel_metrics ADD COLUMN mixpanel_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN mixpanel_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN meta_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00;
ALTER TABLE daily_mixpanel_metrics ADD COLUMN click_to_trial_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000;
```

#### Table 2: Country Breakdown Metrics - NEW TABLE
```sql
CREATE TABLE daily_mixpanel_metrics_breakdown (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,          -- 'campaign', 'adset', 'ad'  
    entity_id TEXT NOT NULL,            -- The actual ID
    date DATE NOT NULL,                 -- Daily granularity
    breakdown_type TEXT NOT NULL,       -- 'country', 'region', 'device'
    breakdown_value TEXT NOT NULL,      -- 'US', 'mobile', etc.
    -- NOTE: Consider using specific typed fields (country_code, device_type, region_code) for better type safety
    
    -- Meta Advertising Metrics (country-specific)
    meta_spend DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_impressions INTEGER NOT NULL DEFAULT 0,
    meta_clicks INTEGER NOT NULL DEFAULT 0,
    meta_trial_count INTEGER NOT NULL DEFAULT 0,
    meta_purchase_count INTEGER NOT NULL DEFAULT 0,
    
    -- Mixpanel Metrics (country-specific)
    mixpanel_trial_count INTEGER NOT NULL DEFAULT 0,
    mixpanel_purchase_count INTEGER NOT NULL DEFAULT 0,
    
    -- User Lists (comma-separated distinct_ids for this country)
    trial_user_ids TEXT,
    post_trial_user_ids TEXT,
    converted_user_ids TEXT,
    trial_refund_user_ids TEXT,
    purchase_user_ids TEXT,
    purchase_refund_user_ids TEXT,
    
    -- Conversion Rate Metrics (country-specific)
    trial_conversion_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_conversion_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    purchase_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    purchase_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    
    -- Revenue Metrics (country-specific, USD)
    actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    actual_refunds_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    net_actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    estimated_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    adjusted_estimated_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    
    -- Performance Metrics (country-specific)
    profit_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    estimated_roas DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    trial_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    purchase_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    
    -- Cost Metrics (country-specific, USD)
    mixpanel_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    mixpanel_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    click_to_trial_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    
    -- Metadata
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (entity_type, entity_id, date, breakdown_type, breakdown_value),
    FOREIGN KEY (entity_type, entity_id, date) 
        REFERENCES daily_mixpanel_metrics(entity_type, entity_id, date)
);
```

#### Optimized Indexes for Fast Retrieval
```sql
-- Core table indexes
CREATE INDEX idx_entity_daily_entity_date ON entity_daily_metrics(entity_type, entity_id, date);
CREATE INDEX idx_entity_daily_date_range ON entity_daily_metrics(date, entity_type);

-- Country breakdown indexes
CREATE INDEX idx_country_breakdown_entity ON entity_country_breakdown(entity_type, entity_id, date);
CREATE INDEX idx_country_breakdown_country ON entity_country_breakdown(country_code, date);

-- Sparkline optimization
CREATE INDEX idx_sparkline_lookup ON entity_daily_metrics(entity_type, entity_id, date);
CREATE INDEX idx_country_sparkline ON entity_country_breakdown(entity_type, entity_id, country_code, date);
```

### Breakdown Structure Design

#### Country Breakdown Implementation
- **Overall Data**: Stored in `entity_daily_metrics` table (aggregated across all countries)
- **Country-Specific Data**: Stored in `entity_country_breakdown` table with `country_code` field
- **Mapping**: Uses existing `breakdown_mapping_country` table for Meta country → ISO code conversion

#### Data Organization
- **Main Metrics**: Always in `entity_daily_metrics` (no breakdown)
- **Country Breakdown**: Only populated in `entity_country_breakdown` when country-specific data exists
- **User Lists**: Comma-separated strings (e.g., "user1,user2,user3") instead of JSON arrays

#### Hierarchical Structure
- Each entity level (campaign, adset, ad) gets its own complete set of pre-computed records
- Rollup aggregations computed separately for each level
- Parent-child relationships maintained through existing `id_hierarchy_mapping` table

## Detailed Metric Definitions

### User Lists Structure
All user lists stored as JSON arrays (preserving existing format). Count fields must equal the number of users in their corresponding user lists.

### Meta Advertising Metrics

#### **Meta Spend**
- **Definition**: Total advertising spend on Meta platform for the given entity and date
- **Source**: `ad_performance_daily.spend` (aggregated for country breakdowns)
- **Country Breakdown**: Uses `ad_performance_daily_country.spend` summed by country
- **Formula**: Simple SUM aggregation across the date range

#### **Meta Trial Count** 
- **Definition**: Number of trial conversions reported by Meta advertising platform
- **Source**: `ad_performance_daily.meta_trials` 
- **Country Breakdown**: Uses `ad_performance_daily_country.meta_trials` summed by country
- **Field Alignment**: Maps to `meta_trial_count` (matches existing `meta_trials` field)

#### **Meta Purchase Count**
- **Definition**: Number of purchase conversions reported by Meta advertising platform  
- **Source**: `ad_performance_daily.meta_purchases`
- **Country Breakdown**: Uses `ad_performance_daily_country.meta_purchases` summed by country
- **Field Alignment**: Maps to `meta_purchase_count` (matches existing `meta_purchases` field)

### Mixpanel Trial Metrics

#### **Mixpanel Trial Count**
- **Definition**: Count of unique users who started trials, tracked through Mixpanel events
- **Source**: COUNT DISTINCT users with `event_name = 'RC Trial started'` for specific product_id
- **Attribution Filter**: `WHERE u.has_abi_attribution = TRUE` AND entity matches (campaign/adset/ad)
- **Country Breakdown**: Additional filter `WHERE u.country = ?` for specific countries
- **Formula**: `COUNT(DISTINCT distinct_id)` with proper attribution and date filters
- **Deduplication**: Users counted only once per entity-date-country combination

#### **Mixpanel Trial User List**
- **Definition**: JSON array of `distinct_id` values for users who started trials
- **Source**: Same query as trial count but returns `GROUP_CONCAT(DISTINCT distinct_id)` as JSON array
- **Length Constraint**: `len(mixpanel_trial_user_list) = mixpanel_trial_count`
- **Purpose**: Enables drill-down analysis and user-level debugging

#### **Mixpanel Post-Trial User List**  
- **Definition**: Users whose trials started at least 7 days ago (post-trial phase)
- **Source**: Users from trial user list where `DATE(trial_event_time) <= CURRENT_DATE - 7`
- **Business Logic**: Trial period is 7 days, so users are "post-trial" after day 7
- **Length Constraint**: `len(mixpanel_post_trial_user_list) <= len(mixpanel_trial_user_list)`
- **Code Reference**: Similar to logic in `03_estimate_values.py` line 714-715 for days_since calculation

#### **Mixpanel Converted User List**
- **Definition**: Users from post-trial list who have 'RC Trial converted' events for the specific product_id
- **Source**: Intersection of post-trial users and users with conversion events
- **Query Logic**: 
  ```sql
  SELECT DISTINCT e.distinct_id 
  FROM mixpanel_event e
  WHERE e.distinct_id IN (post_trial_user_list)
    AND e.event_name = 'RC Trial converted'
    AND e.product_id = target_product_id
  ```
- **Length Constraint**: `len(mixpanel_converted_user_list) <= len(mixpanel_post_trial_user_list)`

#### **Mixpanel Trial Refund User List**
- **Definition**: Users from converted list who have refund events (RC cancellation with negative revenue)
- **Source**: Users with `event_name LIKE '%cancel%'` AND `revenue_usd < 0` after their conversion
- **Query Logic**: Find cancellation events that occur after trial conversion events
- **Length Constraint**: `len(mixpanel_trial_refund_user_list) <= len(mixpanel_converted_user_list)`

### Trial Conversion Rate Calculations

#### **Trial Conversion Rate (Estimated)**
- **Definition**: Cohort-based estimated conversion rate using similar user segments
- **Source**: Existing logic in `02_assign_conversion_rates.py` (lines 8-25)
- **Method**: 
  1. For each user in trial list, find their assigned estimated conversion rate from `user_product_metrics.trial_conversion_rate`
  2. Average all individual estimated rates across the cohort
- **Formula**: `AVG(user_product_metrics.trial_conversion_rate)` for all users in trial list
- **Code Reference**: Database pass-through in `database_calculators.py` line 20-40

#### **Trial Conversion Rate (Actual)**
- **Definition**: Real observed conversion rate from this specific cohort
- **Formula**: `(len(mixpanel_converted_user_list) / len(mixpanel_post_trial_user_list)) * 100`
- **Business Logic**: Only count users who have had enough time to convert (post-trial users)
- **Example**: 50 trial users → 25 post-trial users → 5 converted = 5/25 = 20% actual conversion
- **Code Reference**: Similar logic in `analytics_query_service.py` lines 3279-3280

### Purchase Metrics

#### **Mixpanel Purchase Count**
- **Definition**: Count of unique users with direct purchase events ('RC Initial purchase')
- **Source**: COUNT DISTINCT users with `event_name = 'RC Initial purchase'` for specific product_id
- **Note**: Excludes trial conversions - only direct purchases
- **Attribution**: Same attribution filters as trial metrics

#### **Mixpanel Purchase User List**
- **Definition**: JSON array of distinct_id values for users who made direct purchases
- **Length Constraint**: `len(mixpanel_purchase_user_list) = mixpanel_purchase_count`

#### **Mixpanel Purchase Refund User List**
- **Definition**: Users from purchase list who have refund events after their purchase
- **Source**: Users with cancellation events and negative revenue after 'RC Initial purchase'
- **Length Constraint**: `len(mixpanel_purchase_refund_user_list) <= len(mixpanel_purchase_user_list)`

### Refund Rate Calculations

#### **Trial Refund Rate (Estimated)**
- **Definition**: Estimated refund rate for trial conversions based on cohort analysis
- **Method**: Average of estimated refund rates from user segments
- **Formula**: `AVG(user_product_metrics.trial_converted_to_refund_rate)` for all users in converted list
- **Code Reference**: `03_estimate_values.py` line 729 for rate retrieval

#### **Trial Refund Rate (Actual)**
- **Definition**: Real observed refund rate for trial conversions
- **Formula**: `(len(mixpanel_trial_refund_user_list) / len(mixpanel_converted_user_list)) * 100`
- **Code Reference**: Similar to `analytics_query_service.py` lines 3284-3285

#### **Purchase Refund Rate (Estimated)**
- **Definition**: Estimated refund rate for direct purchases based on cohort analysis
- **Formula**: `AVG(user_product_metrics.initial_purchase_to_refund_rate)` for all users in purchase list
- **Code Reference**: `03_estimate_values.py` line 730

#### **Purchase Refund Rate (Actual)**
- **Definition**: Real observed refund rate for direct purchases
- **Formula**: `(len(mixpanel_purchase_refund_user_list) / len(mixpanel_purchase_user_list)) * 100`

### Revenue Metrics

#### **Actual Revenue (USD)**
- **Definition**: Sum of real revenue from completed purchase events
- **Source**: SUM of `revenue_usd` from events where `event_name IN ('RC Initial purchase', 'RC Trial converted')`
- **Attribution**: Only includes users with proper attribution to the entity
- **Code Reference**: `revenue_calculators.py` lines for actual revenue calculation

#### **Actual Refunds (USD)**
- **Definition**: Sum of real refunds from cancellation events
- **Source**: SUM of `ABS(revenue_usd)` from events where `event_name LIKE '%cancel%'` AND `revenue_usd < 0`
- **Note**: Refund amounts are stored as negative values, so we use ABS() for positive refund totals

#### **Net Actual Revenue (USD)**
- **Definition**: Net revenue after refunds
- **Formula**: `actual_revenue_usd - actual_refunds_usd`

#### **Estimated Revenue (USD)**
- **Definition**: Sum of predicted future value for all users in the cohort
- **Source**: SUM of `user_product_metrics.current_value` for all users in trial and purchase lists
- **Method**: 
  1. Get all users from trial_user_list and purchase_user_list
  2. Sum their individual `current_value` from user_product_metrics table
- **Code Reference**: `03_estimate_values.py` for individual value calculation logic

#### **Adjusted Estimated Revenue (USD)**
- **Definition**: Estimated revenue adjusted for Meta/Mixpanel tracking accuracy differences
- **Purpose**: Account for users that Meta sees but Mixpanel misses
- **Method**:
  1. Determine which accuracy ratio to use based on event priority:
     - If `mixpanel_trial_count > mixpanel_purchase_count`: Use trial accuracy ratio
     - If `mixpanel_purchase_count > mixpanel_trial_count`: Use purchase accuracy ratio  
     - If equal or both zero: Default to trial accuracy ratio
  2. Apply adjustment: `estimated_revenue_usd / (accuracy_ratio / 100)`
- **Example**: If estimated revenue = $50, trial accuracy = 50%, adjusted = $50 / 0.5 = $100
- **Rationale**: "If we only see 50% of users, the real revenue potential is double what we can measure"
- **Code Reference**: `revenue_calculators.py` lines 149-204 for accuracy adjustment logic

### Accuracy Metrics

#### **Trial Accuracy Ratio**
- **Definition**: Measures how well Mixpanel trial tracking matches Meta trial tracking
- **Formula**: `(mixpanel_trial_count / meta_trial_count) * 100`
- **Special Case**: If `meta_trial_count = 0` AND `mixpanel_trial_count > 0`, return 100% (perfect tracking)
- **Purpose**: Identifies attribution gaps and tracking discrepancies
- **Code Reference**: `accuracy_calculators.py` lines 17-47

#### **Purchase Accuracy Ratio**
- **Definition**: Measures how well Mixpanel purchase tracking matches Meta purchase tracking
- **Formula**: `(mixpanel_purchase_count / meta_purchase_count) * 100`
- **Code Reference**: `accuracy_calculators.py` lines 49-73

### Performance Metrics

#### **Profit (USD)**
- **Definition**: Net profit after advertising spend
- **Formula**: `adjusted_estimated_revenue_usd - meta_spend`
- **Note**: Uses adjusted revenue to account for tracking gaps

#### **Estimated ROAS (Return on Ad Spend)**
- **Definition**: Revenue return per dollar spent on advertising
- **Formula**: `adjusted_estimated_revenue_usd / meta_spend`
- **Note**: Uses adjusted revenue for more accurate ROAS calculation
- **Code Reference**: Dashboard calculation logic for ROAS with accuracy adjustment

## Pipeline Integration

### Database Schema Updates Required

**CRITICAL MODULE 2 INTEGRATION**: `02_setup_database.py` must be updated to include validation for existing tables that are currently missing from EXPECTED_TABLES:

```python
# URGENT: ADD missing table validations to EXPECTED_TABLES in 02_setup_database.py:

# 1. daily_mixpanel_metrics (exists in schema.sql but missing from validation)
'daily_mixpanel_metrics': {
    'metric_id': 'INTEGER',
    'date': 'DATE', 
    'entity_type': 'TEXT',
    'entity_id': 'TEXT',
    'trial_users_count': 'INTEGER',
    'trial_users_list': 'TEXT',
    'purchase_users_count': 'INTEGER', 
    'purchase_users_list': 'TEXT',
    'estimated_revenue_usd': 'DECIMAL',
    'computed_at': 'DATETIME',

    # NEW COLUMNS TO ADD: [all new metric columns]
},

# 2. id_name_mapping (exists in schema.sql but missing from validation)  
'id_name_mapping': {
    'entity_type': 'TEXT',
    'entity_id': 'TEXT',
    'canonical_name': 'TEXT',
    'frequency_count': 'INTEGER',
    'last_seen_date': 'DATE',
    'created_at': 'DATETIME',
    'updated_at': 'DATETIME'
},

# 3. id_hierarchy_mapping (exists in schema.sql but missing from validation)
'id_hierarchy_mapping': {
    'ad_id': 'TEXT',
    'adset_id': 'TEXT', 
    'campaign_id': 'TEXT',
    'relationship_confidence': 'DECIMAL',
    'first_seen_date': 'DATE',
    'last_seen_date': 'DATE',
    'created_at': 'DATETIME',
    'updated_at': 'DATETIME'
},

# 4. NEW breakdown table:
'daily_mixpanel_metrics_breakdown': {
    'metric_id': 'INTEGER',
    'entity_type': 'TEXT',
    'entity_id': 'TEXT', 
    'date': 'DATE',
    'breakdown_type': 'TEXT',
    'breakdown_value': 'TEXT',
    # ... (same metric columns as daily_mixpanel_metrics)
}
```

**URGENT DATABASE SETUP FIX**: Add missing table validations to `02_setup_database.py`:

```python
# ADD to EXPECTED_TABLES dictionary (currently missing):
'daily_mixpanel_metrics': {
    'metric_id': 'INTEGER',
    'date': 'DATE', 
    'entity_type': 'TEXT',
    'entity_id': 'TEXT',
    'trial_users_count': 'INTEGER',
    'trial_users_list': 'TEXT',
    'purchase_users_count': 'INTEGER', 
    'purchase_users_list': 'TEXT',
    'estimated_revenue_usd': 'DECIMAL',
    'computed_at': 'DATETIME',
    # NEW COLUMNS TO ADD: [all additional metric columns from spec]
},

'id_name_mapping': {
    'entity_type': 'TEXT',
    'entity_id': 'TEXT',
    'canonical_name': 'TEXT',
    'frequency_count': 'INTEGER',
    'last_seen_date': 'DATE',
    'created_at': 'DATETIME',
    'updated_at': 'DATETIME'
},

'id_hierarchy_mapping': {
    'ad_id': 'TEXT',
    'adset_id': 'TEXT', 
    'campaign_id': 'TEXT',
    'relationship_confidence': 'DECIMAL',
    'first_seen_date': 'DATE',
    'last_seen_date': 'DATE',
    'created_at': 'DATETIME',
    'updated_at': 'DATETIME'
}
```

### Master Pipeline Integration
**EXTEND EXISTING STEP 8** instead of creating new step:
```yaml
# Current step 8: Compute Daily Metrics (EXTEND THIS)
- description: Pre-compute daily metrics for all advertising entities and dates WITH COMPLETE DASHBOARD METRICS
  file: ../mixpanel_pipeline/08_compute_daily_metrics.py  
  id: "📊 Mixpanel - Compute Daily Metrics (EXTENDED)"
  tested: false
```

**Module 8 Implementation Strategy** (Single File Approach):
```
pipelines/mixpanel_pipeline/08_compute_daily_metrics.py (EXTENDED - single file)
```

> **📝 Note**: We're starting with a single file approach following existing team conventions (like `03_estimate_values.py` at 1,088 lines). If the module becomes overly cumbersome during implementation (>1,200 lines or difficult to maintain), we can refactor into a `module_8_sub_modules/` directory with importable sub-modules for better organization. Start simple, refactor if needed.

### Implementation Structure

**Extended `08_compute_daily_metrics.py`** (~900-1,100 lines total):

```python
#!/usr/bin/env python3
"""
Module 8: Compute Daily Mixpanel Metrics (EXTENDED)
Pre-computes ALL dashboard metrics for lightning-fast queries
"""

# === IMPORTS ===
# Existing imports + new calculator imports
from orchestrator.dashboard.calculators.revenue_calculators import RevenueCalculators
from orchestrator.dashboard.calculators.accuracy_calculators import AccuracyCalculators
from orchestrator.dashboard.calculators.roas_calculators import ROASCalculators
# ... etc

# === META DATA COLLECTION SECTION ===
def collect_meta_advertising_data(start_date, end_date):
    """Query Meta Analytics database for spend, impressions, clicks, etc."""
    # Connect to meta_analytics.db
    # Query ad_performance_daily* tables
    # Group by entity_type, entity_id, date
    # Return structured Meta data

def collect_meta_breakdown_data(start_date, end_date, breakdown_type):
    """Query Meta breakdown tables for country/device/region data"""
    # Query ad_performance_daily_country, etc.
    # Use existing BreakdownMappingService
    # Return breakdown-specific Meta data

# === MIXPANEL DATA PROCESSING SECTION ===
def collect_mixpanel_user_data(start_date, end_date):
    """Query Mixpanel users with attribution filters"""
    # Query mixpanel_user with has_abi_attribution = TRUE
    # Apply valid_lifecycle filtering
    # Return attributed user data

def generate_user_lists_by_entity_date(entity_type, entity_id, date):
    """Generate trial, purchase, conversion, refund user lists"""
    # Query mixpanel_event for trials, conversions, refunds
    # Apply 8-day trial logic and attribution matching
    # Return JSON user lists (preserve existing format)

# === METRICS CALCULATION SECTION ===
def calculate_overall_metrics(meta_data, mixpanel_data):
    """Calculate metrics for daily_mixpanel_metrics table"""
    # Use existing calculator classes
    # Apply RevenueCalculators, AccuracyCalculators, etc.
    # Handle all edge cases with existing safe_divide logic
    # Return computed metrics for main table

def calculate_breakdown_metrics(meta_breakdown_data, mixpanel_breakdown_data):
    """Calculate metrics for daily_mixpanel_metrics_breakdown table"""
    # Same calculation logic as overall but grouped by breakdown
    # Handle country/device/region breakdowns
    # Return computed metrics for breakdown table

# === DATABASE OPERATIONS SECTION ===
def truncate_and_repopulate_tables():
    """Efficiently update both pre-computed tables"""
    # Truncate daily_mixpanel_metrics and daily_mixpanel_metrics_breakdown
    # Bulk INSERT operations for performance
    # Transaction management with rollback on errors

def validate_computed_metrics():
    """Apply existing data quality validation"""
    # Validate user list counts match metrics
    # Apply existing data quality scoring
    # Cross-check against current calculations

# === MAIN EXECUTION ===
def main():
    """Extended main function coordinating all operations"""
    # Initialize and validate dependencies
    # Collect Meta and Mixpanel data
    # Calculate metrics for overall and breakdown
    # Store results with validation
    # Report completion statistics
```

## **🚀 PERFORMANCE OPTIMIZATION STRATEGY**

### **Memory & Batch Processing** (8GB RAM Available)
- **Load All Meta Data**: Read entire `ad_performance_daily*` tables into memory dictionaries
- **Load All Mixpanel Data**: Read users and events with attribution into memory structures  
- **Batch Size**: Process entities in batches of 10,000-50,000 for optimal memory usage
- **Bulk Operations**: Use `executemany()` for INSERTs (1000+ records per transaction)
- **Memory Efficiency**: Use generators and dictionary lookups instead of repeated database queries



### **Performance Expectations & Implementation Notes**
- **Realistic Timeline**: 15-25 minutes for 100,000+ entity-date combinations (conservative estimate)
- **Memory Management**: Implement progressive batch processing with memory monitoring to stay within 6GB usage
- **Error Recovery**: Include transaction rollback and checkpoint recovery for large dataset processing
- **Meta Database Fallback**: Handle scenarios where `meta_analytics.db` is unavailable with graceful degradation

### Error Handling & Data Quality
**LEVERAGE EXISTING IMPLEMENTATIONS**:
- **Missing Data**: Use existing error handling from current calculators (safe_divide, safe_percentage)
- **Attribution Gaps**: Use existing attribution filtering (`has_abi_attribution = TRUE`)
- **Product ID Mismatches**: Use existing lifecycle validation (`valid_lifecycle = TRUE`)
- **Division by Zero**: Use existing BaseCalculator.safe_divide() methods
- **Invalid Revenue**: Use existing revenue validation from RevenueCalculators
- **Date Consistency**: Use existing date range validation from current pipeline
- **Validation**: Compare pre-computed totals against current dashboard for accuracy

## Dashboard API Optimization

### New Query Patterns
Instead of complex real-time calculations, dashboard will use simple SELECT queries:

```sql
-- Overall data for single entity, single date  
SELECT * FROM daily_mixpanel_metrics 
WHERE entity_type = ? AND entity_id = ? AND date = ?;

-- Overall data for date range aggregation
SELECT 
  SUM(meta_spend) as total_spend,
  SUM(trial_users_count) as total_trials,
  AVG(trial_accuracy_ratio) as avg_accuracy
FROM daily_mixpanel_metrics 
WHERE entity_type = ? AND entity_id = ? 
  AND date BETWEEN ? AND ?;

-- Country breakdown for single date
SELECT breakdown_value as country_code, meta_spend, trial_users_count, adjusted_estimated_revenue_usd
FROM daily_mixpanel_metrics_breakdown
WHERE entity_type = ? AND entity_id = ? AND date = ? AND breakdown_type = 'country';
```

### Optimized Country Breakdown + Date Range Query
**Critical Use Case**: Get country breakdown for 14-day date range

```sql
-- Get both overall AND country breakdown data for 14-day range
SELECT 
    'overall' as breakdown_type,
    'ALL' as country_code,
    SUM(meta_spend) as total_spend,
    SUM(mixpanel_trial_count) as total_trials,
    SUM(mixpanel_purchase_count) as total_purchases,
    SUM(adjusted_estimated_revenue_usd) as total_revenue,
    SUM(profit_usd) as total_profit,
    AVG(trial_accuracy_ratio) as avg_trial_accuracy
FROM daily_mixpanel_metrics 
WHERE entity_type = 'campaign' 
  AND entity_id = 'your_campaign_id'
  AND date BETWEEN '2025-01-01' AND '2025-01-14'

UNION ALL

SELECT 
    'country' as breakdown_type,
    breakdown_value as country_code,
    SUM(meta_spend) as total_spend,
    SUM(trial_users_count) as total_trials,
    SUM(purchase_users_count) as total_purchases,
    SUM(adjusted_estimated_revenue_usd) as total_revenue,
    SUM(profit_usd) as total_profit,
    AVG(trial_accuracy_ratio) as avg_trial_accuracy
FROM daily_mixpanel_metrics_breakdown 
WHERE entity_type = 'campaign' 
  AND entity_id = 'your_campaign_id'
  AND date BETWEEN '2025-01-01' AND '2025-01-14'
GROUP BY country_code
ORDER BY breakdown_type, total_spend DESC;
```

**Query Performance Characteristics**:
- ✅ **Single Query**: Gets both general data and country breakdown in one request
- ✅ **Index Optimized**: Uses `idx_precomputed_chart_data` index for optimal performance
- ✅ **Pre-Aggregated**: No real-time calculations needed
- ✅ **Scalable**: Performance is identical for 1 day or 365 days
- ✅ **Memory Efficient**: Simple SUM operations, no complex JOINs

**Results Structure**:
```json
[
  {
    "breakdown_type": "all",
    "country": "ALL",
    "total_spend": 5000.00,
    "total_trials": 150,
    "total_purchases": 45,
    "total_revenue": 4500.00
  },
  {
    "breakdown_type": "country", 
    "country": "US",
    "total_spend": 3000.00,
    "total_trials": 90,
    "total_purchases": 30,
    "total_revenue": 2700.00
  },
  {
    "breakdown_type": "country",
    "country": "CA", 
    "total_spend": 1500.00,
    "total_trials": 45,
    "total_purchases": 12,
    "total_revenue": 1350.00
  }
]
```

### Sparkline Data Strategy

#### **Optimized Approach: Use Daily Records for Sparklines**
Instead of storing duplicate sparkline data, we leverage the fact that all daily metrics are already pre-computed. This provides a **single source of truth** with maximum flexibility.

#### **Row-Level Sparklines (14 days)**
**Query Pattern for Any Entity Row**:
```sql
-- Get 14 days of sparkline data for any entity and date
SELECT 
    date,
    meta_spend,
    adjusted_estimated_revenue_usd,
    profit_usd,
    mixpanel_purchase_count,  -- For ROAS coloring logic
    mixpanel_trial_count,
    trial_accuracy_ratio,
    purchase_accuracy_ratio
FROM daily_mixpanel_metrics 
WHERE entity_type = ? 
  AND entity_id = ? 
  AND date BETWEEN (target_date - 13) AND target_date
ORDER BY date;
```

**Dashboard Sparkline Generation**:
1. Dashboard requests 14-day range from current date
2. Calculates ROAS per day: `adjusted_estimated_revenue_usd / meta_spend`
3. Applies performance coloring based on conversion counts
4. Renders sparkline from the 14 daily data points

**Benefits of This Approach**:
- ✅ **Single Source of Truth**: No duplicate data storage
- ✅ **Dynamic Date Ranges**: Can generate sparklines for ANY 14-day period
- ✅ **Storage Efficiency**: No additional storage overhead
- ✅ **Flexibility**: Dashboard controls exact date ranges needed
- ✅ **Maintainability**: Only one calculation pipeline to maintain
- ✅ **Ultra-Fast Queries**: < 20ms with proper indexing

#### **Overview Sparklines (28 days)**
**Similar approach for overview metrics**:
```sql
-- Get 28 days of overview sparkline data
SELECT 
    date,
    SUM(meta_spend) as daily_spend,
    SUM(adjusted_estimated_revenue_usd) as daily_revenue,
    SUM(profit_usd) as daily_profit
FROM daily_mixpanel_metrics 
WHERE date BETWEEN (target_date - 27) AND target_date
GROUP BY date
ORDER BY date;
```

#### **Optimized Index for Sparkline Queries**
```sql
-- LEVERAGE EXISTING INDEXES from daily_mixpanel_metrics:
-- idx_daily_metrics_date_type_id ON daily_mixpanel_metrics(date, entity_type, entity_id)
-- idx_daily_metrics_date_range ON daily_mixpanel_metrics(date)

-- ADD breakdown table indexes:
CREATE INDEX idx_breakdown_entity_lookup ON daily_mixpanel_metrics_breakdown(entity_type, entity_id, date, breakdown_type);
CREATE INDEX idx_breakdown_type_value ON daily_mixpanel_metrics_breakdown(breakdown_type, breakdown_value, date);
```

**Performance Characteristics**:
- ⚡ **Row Sparklines**: < 20ms (14 records with index lookup)
- ⚡ **Overview Sparklines**: < 30ms (28 records with aggregation)  
- 📊 **No JSON Parsing**: Direct numeric data access
- 🎯 **Flexible Date Ranges**: Any start/end dates supported
- 🚀 **Linear Scaling**: Performance independent of historical data size

### Performance Benefits
- **Query Speed**: Simple SELECT instead of complex JOINs and calculations
- **Consistency**: All calculations performed once during pipeline
- **Scalability**: Linear performance regardless of date range complexity
- **Reliability**: Pre-validated data with quality scores
- **Sparkline Speed**: 28-day sparklines retrieved in single query with no calculations

## Implementation Phases

### **Phase 1: Database Schema Setup**
- Update `02_setup_database.py` EXPECTED_TABLES validation with missing tables
- Extend `daily_mixpanel_metrics` table with additional columns
- Create `daily_mixpanel_metrics_breakdown` table
- Test database schema migration and validation

### **Phase 2: Extend Module 8**
- Extend existing `08_compute_daily_metrics.py` with meta data collection, breakdown processing, and calculator integration
- Import existing calculator classes (RevenueCalculators, AccuracyCalculators, etc.)
- Preserve existing user list generation and validation logic

### **Phase 3: Integration & Validation**
- Integrate all existing dashboard calculators into Module 8
- Leverage existing BreakdownMappingService for country/device mappings
- Validate pre-computed metrics match current dashboard output exactly

### **Phase 4: Dashboard API Migration**
- Modify `analytics_query_service.py` to use pre-computed tables
- Simplify sparkline queries to basic daily record retrieval
- Maintain existing API response formats for frontend compatibility

### **Phase 5: Production Deployment**
- Deploy complete pre-computation system to production
- Monitor pipeline execution health and performance
- Document new system architecture and maintenance procedures

## 📋 **WHAT NEEDS TO BE DONE - CHECKLIST**

### **Database Changes**:
- [ ] Add 20+ new columns to `daily_mixpanel_metrics` table
- [ ] Create new `daily_mixpanel_metrics_breakdown` table
- [ ] Update `02_setup_database.py` validation
- [ ] Create appropriate indexes for fast querying

### **Module 8 Extensions**:
- [ ] Add Meta Analytics database connection and queries
- [ ] Add breakdown processing using existing BreakdownMappingService
- [ ] Integrate RevenueCalculators, AccuracyCalculators, ROASCalculators
- [ ] Add bulk INSERT operations for both tables
- [ ] Extend validation and quality scoring

### **Dashboard API Changes**:
- [ ] Replace complex queries in `analytics_query_service.py` with simple SELECTs
- [ ] Simplify sparkline data retrieval to daily record queries
- [ ] Update breakdown entity processing to use new breakdown table
- [ ] Remove deprecated caching and complex calculation logic

### **Testing & Validation**:
- [ ] Validate pre-computed metrics match current dashboard 100%
- [ ] Performance test query response times (target: <1 second)
- [ ] Load test with full data volumes
- [ ] Test breakdown functionality across all dimensions

## Frontend & API Integration Updates

### **API Endpoint Modifications Required**

#### **1. Analytics Query Service Updates**
**File**: `orchestrator/dashboard/services/analytics_query_service.py`
- **Replace complex JOIN queries** with simple SELECT statements from pre-computed tables
- **Update `_execute_mixpanel_only_query()`** to use `daily_mixpanel_metrics` table
- **Modify breakdown handling** to use `daily_mixpanel_metrics_breakdown` table
- **Remove calculator integration** from query layer (move to pipeline)

#### **2. Dashboard API Route Updates**  
**File**: `orchestrator/dashboard/api/dashboard_routes.py`
- **Update `/analytics/data` endpoint** to use simplified query patterns
- **Maintain existing response format** for frontend compatibility
- **Add breakdown aggregation logic** for date range queries
- **No frontend API contract changes required**

#### **3. Additional Dashboard Endpoints**
**File**: `orchestrator/dashboard/api/dashboard_routes.py`
- **`/configurations` endpoint** - Update if configuration logic changes due to pre-computed data structure
- **`/data` endpoint (legacy)** - Verify compatibility with new pre-computed data sources
- **`/collection/trigger`** - Ensure manual data collection triggers work with extended pipeline
- **`/health`** - Update health checks to validate pre-computed table availability and data freshness

#### **4. Pipeline Integration APIs**  
**Files**: Various analytics and pipeline services
- **Analytics pipeline status endpoints** - Update to reflect new pre-computation step in Module 8
- **Meta data endpoints** (`/api/meta/`) - Ensure breakdown aggregation alignment with new breakdown tables
- **Pipeline monitoring APIs** - Include pre-computation step tracking and error reporting
- **WebSocket real-time updates** - Ensure pipeline status updates include pre-computation progress

### **Frontend Component Updates Required**

#### **1. Dashboard Data Consumption**
**File**: `orchestrator/dashboard/client/src/pages/Dashboard.js`
- **No changes required** - existing `dashboardApi.getAnalyticsData()` calls remain unchanged
- **Response format preserved** - React components continue working as-is
- **Performance improvements automatic** - faster API responses without frontend changes

#### **2. Sparkline Components** 
**File**: `orchestrator/dashboard/client/src/components/dashboard/ROASSparkline.jsx`
- **No changes required** - existing `dashboardApi.getAnalyticsChartData()` calls remain unchanged
- **Chart data format preserved** - existing chart rendering logic unchanged
- **Backend optimization transparent** to frontend

#### **3. Service Layer**
**File**: `orchestrator/dashboard/client/src/services/dashboardApi.js`
- **No API contract changes** - method signatures remain identical
- **Enhanced performance** - same API calls return faster
- **No debugging code changes** - existing logging continues working

#### **4. Comprehensive Service Dependencies**
**File**: `orchestrator/dashboard/client/src/services/api.js`
- **Review 20+ API endpoints** for dashboard data dependencies
- **Update hardcoded response format expectations** if any exist
- **Ensure error handling compatibility** with new backend logic
- **Validate cohort analysis integration** with pre-computed data sources

#### **5. Additional Frontend Components**
**Files**: Various React components consuming dashboard data
- **`GraphModal.js`** - Chart modal displays using dashboard data
- **`DebugModal.js`** - Debug interfaces may reference raw vs pre-computed data
- **Cohort pipeline components** - Ensure integration with new data structure
- **Meta action components** - Verify compatibility with breakdown data changes

#### **6. Frontend State Management**
**Files**: Dashboard state and hooks
- **`useOverviewChartData.js`** - Overview chart data consumption patterns
- **`useColumnValidation.js`** - Column validation logic may need updates
- **Dashboard state management** - Ensure state updates work with new response structures
- **Caching mechanisms** - Update any frontend caching to work with pre-computed data timing

### **API Response Format Compatibility**
**CRITICAL**: All existing API response formats **must remain unchanged** to ensure zero frontend impact:

#### **1. Primary Analytics Endpoint**
```javascript
// /api/dashboard/analytics/data response (preserve exactly):
{
  "success": true,
  "data": [
    {
      "id": "campaign_123",
      "campaign_name": "Example Campaign", 
      "spend": 1500.00,
      "mixpanel_trials_started": 45,
      "estimated_revenue_adjusted": 2250.00,
      "profit": 750.00,
      "trial_accuracy_ratio": 85.5,
      "purchase_accuracy_ratio": 92.3,
      // ... all existing fields preserved exactly
    }
  ],
  "breakdown_data": [...], // If breakdown requested
  "metadata": {...}        // Existing metadata structure
}
```

#### **2. Chart Data Endpoint**
```javascript
// /api/dashboard/analytics/chart-data response (preserve exactly):
{
  "success": true,
  "chart_data": {
    "dates": ["2025-01-01", "2025-01-02", ...],
    "spend": [150.00, 200.00, ...],
    "revenue": [450.00, 600.00, ...],
    "roas": [3.0, 3.0, ...],
    "trials": [10, 15, ...],
    "purchases": [3, 5, ...]
  }
}
```

#### **3. Configuration Endpoint**
```javascript
// /api/dashboard/configurations response (preserve exactly):
{
  "success": true,
  "configurations": [
    {
      "key": "basic_ad_data",
      "name": "Basic Ad Performance",
      "description": "Standard campaign metrics"
    }
  ]
}
```

#### **4. Error Response Format**
```javascript
// All error responses (preserve exactly):
{
  "success": false,
  "error": "Specific error message",
  "error_code": "optional_error_code"
}
```

### **Critical Integration Points**

#### **1. Breakdown Data Structure**
**CRITICAL**: Breakdown responses must maintain exact same nested structure:
```javascript
// Country breakdown example (preserve exactly):
{
  "breakdown_data": {
    "US": {
      "spend": 1200.00,
      "trials": 35,
      "revenue": 2100.00
    },
    "CA": {
      "spend": 300.00,
      "trials": 10,
      "revenue": 450.00
    }
  }
}
```

#### **2. Real-time Update Compatibility**
- **WebSocket message formats** - Preserve existing pipeline status update structure
- **Loading state indicators** - Maintain existing boolean flags and progress percentages
- **Error propagation** - Keep same error message format and severity levels

#### **3. Caching Behavior**
- **Cache key structures** - Maintain existing cache key generation logic
- **Cache invalidation** - Preserve existing cache clearing triggers
- **Data freshness indicators** - Keep existing timestamp and staleness detection

**No frontend modifications required** - only backend optimization with preserved interfaces.

### **Service Integration Requirements**

#### **1. Analytics Query Service Integration**
**File**: `orchestrator/dashboard/services/analytics_query_service.py`
- **Database connection handling** - Update to efficiently access both main and breakdown tables
- **Query optimization** - Replace complex aggregation queries with simple SELECT statements
- **Breakdown mapping service** - Ensure `BreakdownMappingService` works with pre-computed breakdown data
- **Error handling** - Maintain existing error response patterns while optimizing queries
- **Memory management** - Remove heavy in-memory calculations, rely on pre-computed data

#### **2. Dashboard Service Integration**  
**File**: `orchestrator/dashboard/services/dashboard_service.py`
- **Configuration management** - Update data configuration logic for pre-computed tables
- **Data validation** - Adapt existing validation to work with pre-computed data structure
- **Collection triggers** - Ensure manual data collection integrates with new pipeline flow
- **Health monitoring** - Add pre-computed table health checks to existing health monitoring

#### **3. Background Services Compatibility**
**Files**: Various background and utility services
- **Pipeline monitoring** - Update pipeline status tracking to include pre-computation progress
- **Data freshness validation** - Adapt existing freshness checks for pre-computed data
- **Error reporting** - Ensure pre-computation errors integrate with existing error reporting
- **Logging systems** - Maintain existing log format while adding pre-computation logging

### **Database Transaction Management**

#### **1. Read Transaction Optimization**
- **Connection pooling** - Optimize database connections for pre-computed table access
- **Transaction isolation** - Ensure read consistency while pre-computation pipeline runs
- **Query optimization** - Leverage indexes on pre-computed tables for sub-second responses
- **Memory efficiency** - Reduce memory usage by eliminating runtime calculations

#### **2. Data Consistency Management**
- **Pipeline coordination** - Ensure dashboard reads don't conflict with pre-computation writes
- **Atomic updates** - Maintain data consistency during pre-computation table updates
- **Fallback mechanisms** - Handle cases where pre-computed data is temporarily unavailable
- **Version control** - Track pre-computation versions to ensure data accuracy

### **WebSocket & Real-time Integration**

#### **1. Pipeline Status Updates**
- **Pre-computation progress** - Add Module 8 progress tracking to existing WebSocket messages
- **Error notifications** - Integrate pre-computation errors with existing real-time error system
- **Completion notifications** - Update pipeline completion messages to include pre-computation status
- **Performance metrics** - Add pre-computation timing to existing pipeline performance tracking

#### **2. Dashboard Real-time Features**
- **Data refresh indicators** - Update existing refresh logic to work with pre-computed data timing
- **Background updates** - Ensure background data refresh works with new pre-computation schedule
- **User notifications** - Maintain existing user notification patterns for data updates
- **Auto-refresh logic** - Adapt existing auto-refresh to optimize with pre-computed data availability

## Expected Performance Improvements

### Current Performance Baseline
- **Dashboard Load Time**: 3-8 seconds for complex queries
- **Memory Usage**: High due to real-time aggregations
- **Database Connections**: Multiple concurrent connections for calculations

### Expected Post-Implementation Performance  
- **Dashboard Load Time**: < 1 second for any query (goal - validate with testing)
- **Sparkline Load Time**: < 50ms for 28 days of data (goal - validate with testing)
- **Memory Usage**: Minimal - simple SELECT operations
- **Database Connections**: Single connection for data retrieval
- **Scalability**: Supports unlimited date ranges without performance degradation
- **Performance Validation**: All performance claims require validation during implementation phase

## Data Storage Estimates

### Storage Requirements
- **Daily Records**: ~10,000 entity-date-breakdown combinations per day
- **Annual Storage**: ~3.65M records per year
- **Record Size**: ~500 bytes per record
- **Total Annual Storage**: ~1.8GB per year (highly efficient)

### Retention Strategy
- **Full History**: Maintain complete historical data
- **Compression**: Use database compression for older data
- **Archival**: Move data older than 2 years to archive tables

## Success Metrics

### Performance Metrics
- Dashboard load time reduction: > 80%
- Database query complexity reduction: > 90%  
- Memory usage reduction: > 70%

### Data Quality Metrics
- Accuracy validation: 99.9% match with current calculations
- Data completeness: 100% coverage for all entity-date combinations
- Freshness: Data available within 1 hour of pipeline completion

### Business Impact
- Improved user experience with faster dashboard
- Reduced infrastructure costs due to optimized queries
- Enhanced reliability with pre-validated data
- Simplified maintenance with centralized calculations

---

*This specification provides a comprehensive framework for implementing pre-computed dashboard metrics that leverages existing infrastructure while dramatically improving performance and maintaining full functionality and data accuracy.*