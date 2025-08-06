# Pre-Computation Specification for Dashboard Optimization

## Overview

This specification outlines the implementation of a pre-computation system to optimize dashboard performance by calculating and storing all metrics in advance. Instead of computing metrics on-demand during dashboard requests, all calculations will be performed during the master pipeline execution and stored in optimized data structures for fast retrieval.

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

#### Table 1: Core Daily Metrics (Overall Data)
```sql
CREATE TABLE entity_daily_metrics (
    entity_type TEXT NOT NULL,           -- 'campaign', 'adset', 'ad'
    entity_id TEXT NOT NULL,            -- campaign_id, adset_id, or ad_id
    date DATE NOT NULL,                 -- Daily granularity
    
    -- Meta Advertising Metrics
    meta_spend DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_impressions INTEGER NOT NULL DEFAULT 0,
    meta_clicks INTEGER NOT NULL DEFAULT 0,
    meta_trial_count INTEGER NOT NULL DEFAULT 0,
    meta_purchase_count INTEGER NOT NULL DEFAULT 0,
    
    -- Mixpanel Metrics
    mixpanel_trial_count INTEGER NOT NULL DEFAULT 0,
    mixpanel_purchase_count INTEGER NOT NULL DEFAULT 0,
    
    -- User Lists (comma-separated distinct_ids)
    trial_user_ids TEXT,                -- "user1,user2,user3"
    post_trial_user_ids TEXT,           -- Users in post-trial phase (7+ days)
    converted_user_ids TEXT,            -- Users who converted from trial
    trial_refund_user_ids TEXT,         -- Users who refunded after trial conversion
    purchase_user_ids TEXT,             -- Users who made direct purchases
    purchase_refund_user_ids TEXT,      -- Users who refunded direct purchases
    
    -- Conversion Rate Metrics
    trial_conversion_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_conversion_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    trial_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    purchase_refund_rate_estimated DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    purchase_refund_rate_actual DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    
    -- Revenue Metrics (USD)
    actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    actual_refunds_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    net_actual_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    estimated_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    adjusted_estimated_revenue_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    
    -- Performance Metrics
    profit_usd DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    estimated_roas DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    trial_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    purchase_accuracy_ratio DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    
    -- Cost Metrics (USD)
    mixpanel_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    mixpanel_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_cost_per_trial DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    meta_cost_per_purchase DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    click_to_trial_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
    
    -- Metadata
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2),
    
    PRIMARY KEY (entity_type, entity_id, date)
);
```

#### Table 2: Country Breakdown Metrics
```sql
CREATE TABLE entity_country_breakdown (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    date DATE NOT NULL,
    country_code TEXT NOT NULL,         -- 'US', 'CA', 'UK', etc.
    
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
    data_quality_score DECIMAL(3,2),
    
    PRIMARY KEY (entity_type, entity_id, date, country_code),
    FOREIGN KEY (entity_type, entity_id, date) 
        REFERENCES entity_daily_metrics(entity_type, entity_id, date)
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
All user lists stored as comma-separated strings for simplicity and efficiency. **CRITICAL**: The count fields must equal the number of users in their corresponding user lists:
```
trial_user_ids: "distinct_id_1,distinct_id_2,distinct_id_3"
post_trial_user_ids: "distinct_id_2,distinct_id_3"
converted_user_ids: "distinct_id_2"
trial_refund_user_ids: "distinct_id_2"
purchase_user_ids: "distinct_id_4,distinct_id_5"
purchase_refund_user_ids: "distinct_id_5"
```

**Usage**:
- **Count Users**: `LENGTH(trial_user_ids) - LENGTH(REPLACE(trial_user_ids, ',', '')) + 1`
- **Split in Code**: `trial_user_ids.split(',')` 
- **Check Membership**: `',' + trial_user_ids + ',' LIKE '%,user_id,%'`

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
- **Note**: Meta tracks these through conversion events configured in their system

#### **Meta Purchase Count**
- **Definition**: Number of purchase conversions reported by Meta advertising platform  
- **Source**: `ad_performance_daily.meta_purchases`
- **Country Breakdown**: Uses `ad_performance_daily_country.meta_purchases` summed by country
- **Note**: These are direct purchases, not trial conversions

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

### Master Pipeline Integration
Add new step after existing value estimation:
```yaml
- description: Pre-compute all dashboard metrics for optimized data retrieval
  file: ../pre_processing_pipeline/04_precompute_dashboard_metrics.py
  id: "⚡ Pre-processing - Pre-compute Dashboard Metrics"
  tested: false
```

### Computation Process Flow

1. **Data Collection Phase**
   - Query all Meta advertising data by entity and date
   - Query all Mixpanel user and event data with attribution
   - Apply existing breakdown mappings (country, device)

2. **Metric Calculation Phase**  
   - Apply all existing calculator logic from dashboard
   - Calculate metrics for each entity × date × breakdown combination
   - Generate user lists for trials, purchases, and refunds

3. **Aggregation Phase**
   - Create 'ALL' records by summing individual breakdown records
   - Perform hierarchical rollups (ad → adset → campaign)
   - Calculate accuracy-adjusted metrics

4. **Storage Phase**
   - Truncate and repopulate `pre_computed_metrics` table
   - Apply data quality validation
   - Update metadata timestamps

### Error Handling & Data Quality
- **Missing Data**: Default to 0 values with quality score indicators
- **Attribution Gaps**: Track entities without proper attribution mapping
- **Date Consistency**: Ensure all dates have records (even if zeros)
- **Validation**: Compare pre-computed totals against current dashboard for accuracy

## Dashboard API Optimization

### New Query Patterns
Instead of complex real-time calculations, dashboard will use simple SELECT queries:

```sql
-- Overall data for single entity, single date
SELECT * FROM entity_daily_metrics 
WHERE entity_type = ? AND entity_id = ? AND date = ?;

-- Overall data for date range aggregation
SELECT 
  SUM(meta_spend) as total_spend,
  SUM(mixpanel_trial_count) as total_trials,
  AVG(trial_accuracy_ratio) as avg_accuracy
FROM entity_daily_metrics 
WHERE entity_type = ? AND entity_id = ? 
  AND date BETWEEN ? AND ?;

-- Country breakdown for single date
SELECT country_code, meta_spend, mixpanel_trial_count, adjusted_estimated_revenue_usd
FROM entity_country_breakdown
WHERE entity_type = ? AND entity_id = ? AND date = ?;
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
FROM entity_daily_metrics 
WHERE entity_type = 'campaign' 
  AND entity_id = 'your_campaign_id'
  AND date BETWEEN '2025-01-01' AND '2025-01-14'

UNION ALL

SELECT 
    'country' as breakdown_type,
    country_code,
    SUM(meta_spend) as total_spend,
    SUM(mixpanel_trial_count) as total_trials,
    SUM(mixpanel_purchase_count) as total_purchases,
    SUM(adjusted_estimated_revenue_usd) as total_revenue,
    SUM(profit_usd) as total_profit,
    AVG(trial_accuracy_ratio) as avg_trial_accuracy
FROM entity_country_breakdown 
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
FROM entity_daily_metrics 
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
FROM entity_daily_metrics 
WHERE date BETWEEN (target_date - 27) AND target_date
GROUP BY date
ORDER BY date;
```

#### **Optimized Index for Sparkline Queries**
```sql
-- Critical index for ultra-fast sparkline retrieval (already defined above)
-- idx_sparkline_lookup ON entity_daily_metrics(entity_type, entity_id, date)

-- Additional index for overview sparklines  
CREATE INDEX idx_overview_sparkline ON entity_daily_metrics(date);
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

### Phase 1: Core Infrastructure (Week 1)
- Create `entity_daily_metrics` and `entity_country_breakdown` table schemas
- Implement basic computation pipeline step
- Handle overall data aggregation

### Phase 2: Country Breakdown Support (Week 2)  
- Implement country breakdown pre-computation in `entity_country_breakdown` table
- Integrate with existing `breakdown_mapping_country` table

### Phase 3: Sparkline Optimization (Week 3)
- Implement optimized sparkline indexes
- Update dashboard API to use daily record queries for sparklines
- Remove current sparkline cache and API complexity

### Phase 4: Dashboard Integration (Week 4)
- Modify dashboard API to use pre-computed data
- Update frontend to handle optimized data structure
- Replace real-time sparkline calculations with daily record queries
- Performance testing and validation

### Phase 5: Advanced Features (Week 5)
- Add data quality monitoring
- Implement incremental updates for recent dates
- Add cache invalidation strategies

## Expected Performance Improvements

### Current Performance Baseline
- **Dashboard Load Time**: 3-8 seconds for complex queries
- **Memory Usage**: High due to real-time aggregations
- **Database Connections**: Multiple concurrent connections for calculations

### Expected Post-Implementation Performance  
- **Dashboard Load Time**: < 1 second for any query
- **Sparkline Load Time**: < 50ms for 28 days of data
- **Memory Usage**: Minimal - simple SELECT operations
- **Database Connections**: Single connection for data retrieval
- **Scalability**: Supports unlimited date ranges without performance degradation

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

*This specification provides a comprehensive framework for implementing pre-computed dashboard metrics that will dramatically improve performance while maintaining full functionality and data accuracy.*