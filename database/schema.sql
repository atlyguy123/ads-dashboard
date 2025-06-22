-- ========================================
-- MIXPANEL DATABASE SCHEMA (SINGLE SOURCE OF TRUTH)
-- ========================================
-- Database Location: /database/mixpanel_data.db
-- Last Updated: Consolidated schema including all analytics capabilities
-- Status: AUTHORITATIVE - All code should reference this schema
-- ========================================

-- ========================================
-- CORE USER DATA TABLES
-- ========================================

-- Primary User Table
-- Status: EXISTS - Needs 2 columns added (valid_user, economic_tier)
CREATE TABLE mixpanel_user (
    distinct_id TEXT PRIMARY KEY,
    abi_ad_id TEXT, -- Attribution ad ID (matches Meta ad_id)
    abi_campaign_id TEXT, -- Attribution campaign ID (matches Meta campaign_id)
    abi_ad_set_id TEXT, -- Attribution ad set ID (matches Meta adset_id)
    country TEXT, -- ISO 3166-1 alpha-2 code
    region TEXT,
    city TEXT,
    has_abi_attribution BOOLEAN DEFAULT FALSE,
    profile_json TEXT,
    first_seen DATETIME, -- Changed from TEXT to DATETIME
    last_updated DATETIME, -- Changed from TEXT to DATETIME
    valid_user BOOLEAN DEFAULT FALSE, -- Flag for user validity
    economic_tier TEXT -- Economic classification ("premium", "standard", "basic", "free")
);

-- Event Tracking Table
-- Status: EXISTS - No changes needed
CREATE TABLE mixpanel_event (
    event_uuid TEXT PRIMARY KEY,
    event_name TEXT NOT NULL,
    abi_ad_id TEXT, -- Attribution ad ID (matches Meta ad_id)
    abi_campaign_id TEXT, -- Attribution campaign ID (matches Meta campaign_id)
    abi_ad_set_id TEXT, -- Attribution ad set ID (matches Meta adset_id)
    distinct_id TEXT NOT NULL,
    event_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    country TEXT,
    region TEXT,
    revenue_usd DECIMAL(10,2), -- Changed from REAL to DECIMAL
    raw_amount DECIMAL(10,2), -- Changed from REAL to DECIMAL
    currency TEXT,
    refund_flag BOOLEAN DEFAULT FALSE,
    is_late_event BOOLEAN DEFAULT FALSE,
    trial_expiration_at_calc DATETIME, -- Changed from TEXT to DATETIME
    event_json TEXT,
    FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
);

-- CONSOLIDATED USER PRODUCT TABLE (REPLACES BOTH fact_user_products AND user_product_metrics)
-- Status: CONSOLIDATED FROM ANALYTICS DB + PLANNED STRUCTURE
-- Purpose: Complete user-product analytics with lifecycle tracking, attribution, and conversion metrics
-- Note: This table consolidates the rich analytics from user_product_metrics with the planned fact_user_products structure

-- ========================================
-- ANALYTICS TABLES (MERGED FROM ANALYTICS DB)
-- ========================================

-- CONSOLIDATED User Product Metrics & Lifecycle Tracking
-- Status: CONSOLIDATED TABLE - Replaces both fact_user_products and original user_product_metrics
-- Purpose: Complete user-product analytics combining rich conversion metrics with lifecycle tracking
CREATE TABLE user_product_metrics (
    user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    distinct_id TEXT NOT NULL,
    product_id TEXT NOT NULL, 
    credited_date DATE NOT NULL, -- Changed from TEXT to DATE
    country TEXT, 
    region TEXT, 
    device TEXT, 
    current_status TEXT NOT NULL, 
    current_value DECIMAL(10,2) NOT NULL, -- Changed from REAL to DECIMAL
    value_status TEXT NOT NULL, 
    segment_id TEXT, 
    accuracy_score TEXT, 
    trial_conversion_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    trial_converted_to_refund_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    initial_purchase_to_refund_rate DECIMAL(5,4), -- Changed from REAL to DECIMAL
    price_bucket DECIMAL(10,2), -- Changed from REAL to DECIMAL
    assignment_type TEXT, -- Price bucket assignment method: 'conversion', 'inherited_prior', 'inherited_closest', 'no_event', 'no_conversions_ever'
    last_updated_ts DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    -- Additional fields from planned fact_user_products structure
    valid_lifecycle BOOLEAN DEFAULT FALSE, -- Whether this user-product lifecycle can be trusted for analysis
    store TEXT, -- Store identifier: "app_store" or "play_store"
    UNIQUE (distinct_id, product_id),
    FOREIGN KEY (distinct_id) REFERENCES mixpanel_user(distinct_id)
);

-- Pipeline Status Monitoring
-- Status: MERGED FROM ANALYTICS DB
-- Purpose: Track analytics pipeline execution status
CREATE TABLE pipeline_status (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    started_at DATETIME,
    completed_at DATETIME,
    progress_percentage INTEGER DEFAULT 0,
    current_step TEXT,
    error_message TEXT,
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    processed_users INTEGER DEFAULT 0,
    total_users INTEGER DEFAULT 0
);

-- ========================================
-- META ADVERTISING PERFORMANCE TABLES
-- ========================================
-- Note: Meta API breakdowns are mutually exclusive, requiring separate tables

-- Table for aggregated daily performance without breakdowns ("all" view)
CREATE TABLE ad_performance_daily (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date)
);

-- Table for country-level geographic breakdowns
CREATE TABLE ad_performance_daily_country (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    country TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, country)
);

-- Table for region-level geographic breakdowns  
CREATE TABLE ad_performance_daily_region (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    region TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, region)
);

-- Table for device breakdowns
CREATE TABLE ad_performance_daily_device (
    ad_id TEXT NOT NULL,
    date DATE NOT NULL, -- Changed from TEXT to DATE
    device TEXT NOT NULL,
    adset_id TEXT,
    campaign_id TEXT,
    ad_name TEXT,
    adset_name TEXT,
    campaign_name TEXT,
    spend DECIMAL(10,2), -- Changed from REAL to DECIMAL
    impressions INTEGER,
    clicks INTEGER,
    meta_trials INTEGER,
    meta_purchases INTEGER,
    PRIMARY KEY (ad_id, date, device)
);

-- ========================================
-- SUPPORTING TABLES
-- ========================================

-- Currency Exchange Rates
-- Status: EXISTS - No changes needed
CREATE TABLE currency_fx (
    date_day DATE NOT NULL, -- Changed from TEXT to DATE
    currency_code CHAR(3) NOT NULL,
    usd_rate DECIMAL(10,6) NOT NULL, -- Changed from REAL to DECIMAL (higher precision for FX rates)
    PRIMARY KEY (date_day, currency_code)
);

-- ETL Pipeline Control
-- Status: EXISTS - No changes needed
CREATE TABLE etl_job_control (
    job_name TEXT PRIMARY KEY,
    last_run_timestamp DATETIME, -- Changed from TEXT to DATETIME
    last_success_timestamp DATETIME, -- Changed from TEXT to DATETIME
    status TEXT, -- 'running', 'success', 'failed'
    error_message TEXT,
    run_duration_seconds INTEGER
);

-- Daily Event Processing Tracker
-- Status: EXISTS - No changes needed
CREATE TABLE processed_event_days (
    date_day DATE PRIMARY KEY, -- Changed from TEXT to DATE
    events_processed INTEGER,
    processing_timestamp DATETIME, -- Changed from TEXT to DATETIME
    status TEXT -- 'complete', 'partial', 'failed'
);

-- Dynamic Schema Discovery
-- Status: EXISTS - No changes needed
CREATE TABLE discovered_properties (
    property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_name TEXT NOT NULL UNIQUE,
    property_type TEXT, -- 'event' or 'user'
    first_seen_date DATE, -- Changed from TEXT to DATE
    last_seen_date DATE, -- Changed from TEXT to DATE
    sample_value TEXT
);

CREATE TABLE discovered_property_values (
    value_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER,
    property_value TEXT,
    first_seen_date DATE, -- Changed from TEXT to DATE
    last_seen_date DATE, -- Changed from TEXT to DATE
    occurrence_count INTEGER DEFAULT 1,
    FOREIGN KEY (property_id) REFERENCES discovered_properties(property_id)
);

-- ========================================
-- PIPELINE MANAGEMENT TABLES
-- ========================================

-- Pipeline Execution History
-- Status: EXISTS - No changes needed
CREATE TABLE refresh_pipeline_history (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    start_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    end_time DATETIME, -- Changed from TEXT to DATETIME
    status TEXT, -- 'running', 'completed', 'failed', 'interrupted'
    records_processed INTEGER,
    error_details TEXT,
    execution_parameters TEXT -- JSON blob of parameters used
);

-- Interrupted Pipeline Recovery
-- Status: EXISTS - No changes needed
CREATE TABLE interrupted_pipelines (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    interruption_time DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    last_processed_record TEXT,
    recovery_checkpoint TEXT, -- JSON blob for recovery state
    status TEXT -- 'interrupted', 'recovering', 'recovered'
);

-- Dashboard Caching
-- Status: EXISTS - No changes needed
CREATE TABLE dashboard_refresh_cache (
    cache_key TEXT PRIMARY KEY,
    cache_value TEXT, -- JSON blob
    created_timestamp DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    expires_timestamp DATETIME, -- Changed from TEXT to DATETIME
    refresh_count INTEGER DEFAULT 1
);

-- Geographic Reference Data
-- Status: EXISTS - No changes needed
CREATE TABLE continent_country (
    country_code CHAR(2) PRIMARY KEY, -- ISO 3166-1 alpha-2
    country_name TEXT NOT NULL,
    continent_code CHAR(2),
    continent_name TEXT,
    region TEXT,
    sub_region TEXT
);

-- Saved Analysis Views
-- Status: EXISTS - No changes needed
CREATE TABLE saved_views (
    view_id INTEGER PRIMARY KEY AUTOINCREMENT,
    view_name TEXT NOT NULL,
    view_description TEXT,
    view_sql TEXT NOT NULL,
    created_by TEXT,
    created_timestamp DATETIME NOT NULL, -- Changed from TEXT to DATETIME
    last_modified DATETIME, -- Changed from TEXT to DATETIME
    view_parameters TEXT, -- JSON blob for parameterized views
    is_public BOOLEAN DEFAULT FALSE
);

-- ========================================
-- INDEXES FOR PERFORMANCE
-- ========================================

-- User table indexes
CREATE INDEX idx_mixpanel_user_country ON mixpanel_user(country);
CREATE INDEX idx_mixpanel_user_has_abi ON mixpanel_user(has_abi_attribution);
CREATE INDEX idx_mixpanel_user_first_seen ON mixpanel_user(first_seen);
CREATE INDEX idx_mixpanel_user_valid_user ON mixpanel_user(valid_user);
CREATE INDEX idx_mixpanel_user_economic_tier ON mixpanel_user(economic_tier);
CREATE INDEX idx_mixpanel_user_abi_ad_id ON mixpanel_user(abi_ad_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_user_abi_campaign_id ON mixpanel_user(abi_campaign_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_user_abi_ad_set_id ON mixpanel_user(abi_ad_set_id); -- Attribution lookup

-- Event table indexes
CREATE INDEX idx_mixpanel_event_distinct_id ON mixpanel_event(distinct_id);
CREATE INDEX idx_mixpanel_event_name ON mixpanel_event(event_name);
CREATE INDEX idx_mixpanel_event_time ON mixpanel_event(event_time);
CREATE INDEX idx_mixpanel_event_country ON mixpanel_event(country);
CREATE INDEX idx_mixpanel_event_revenue ON mixpanel_event(revenue_usd);
CREATE INDEX idx_mixpanel_event_abi_ad_id ON mixpanel_event(abi_ad_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_event_abi_campaign_id ON mixpanel_event(abi_campaign_id); -- Attribution lookup
CREATE INDEX idx_mixpanel_event_abi_ad_set_id ON mixpanel_event(abi_ad_set_id); -- Attribution lookup

-- Consolidated User Product Metrics indexes (combines all analytics and lifecycle tracking indexes)
CREATE INDEX idx_upm_distinct_id ON user_product_metrics (distinct_id);
CREATE INDEX idx_upm_product_id ON user_product_metrics (product_id);
CREATE INDEX idx_upm_credited_date ON user_product_metrics (credited_date);
CREATE INDEX idx_upm_country ON user_product_metrics (country);
CREATE INDEX idx_upm_region ON user_product_metrics (region);
CREATE INDEX idx_upm_device ON user_product_metrics (device);
CREATE INDEX idx_upm_valid_lifecycle ON user_product_metrics (valid_lifecycle);
CREATE INDEX idx_upm_store ON user_product_metrics (store);
CREATE INDEX idx_upm_price_bucket ON user_product_metrics (price_bucket);
CREATE INDEX idx_upm_assignment_type ON user_product_metrics (assignment_type);

-- Meta advertising performance table indexes
CREATE INDEX idx_ad_perf_date ON ad_performance_daily (date);
CREATE INDEX idx_ad_perf_campaign ON ad_performance_daily (campaign_id);
CREATE INDEX idx_ad_perf_adset ON ad_performance_daily (adset_id);
CREATE INDEX idx_ad_perf_ad_id ON ad_performance_daily (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_country_date ON ad_performance_daily_country (date);
CREATE INDEX idx_ad_perf_country_campaign ON ad_performance_daily_country (campaign_id);
CREATE INDEX idx_ad_perf_country_ad_id ON ad_performance_daily_country (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_region_date ON ad_performance_daily_region (date);
CREATE INDEX idx_ad_perf_region_campaign ON ad_performance_daily_region (campaign_id);
CREATE INDEX idx_ad_perf_region_ad_id ON ad_performance_daily_region (ad_id); -- NEW: For attribution joins
CREATE INDEX idx_ad_perf_device_date ON ad_performance_daily_device (date);
CREATE INDEX idx_ad_perf_device_campaign ON ad_performance_daily_device (campaign_id);
CREATE INDEX idx_ad_perf_device_ad_id ON ad_performance_daily_device (ad_id); -- NEW: For attribution joins

-- Additional indexes for join performance
CREATE INDEX idx_currency_fx_date ON currency_fx (date_day);
CREATE INDEX idx_etl_job_status ON etl_job_control (status);
CREATE INDEX idx_pipeline_history_name_time ON refresh_pipeline_history (pipeline_name, start_time);
CREATE INDEX idx_dashboard_cache_expires ON dashboard_refresh_cache (expires_timestamp);

-- ========================================
-- MERGE BENEFITS & RELATIONSHIPS
-- ========================================

/*
POST-MERGE ADVANTAGES:
1. Single database eliminates sync complexity
2. Direct joins possible between user_product_metrics and mixpanel_user
3. Can correlate detailed analytics with attribution data
4. Simplified backup/maintenance procedures
5. Better performance for cross-dataset queries

CONSOLIDATED USER_PRODUCT_METRICS TABLE BENEFITS:
- Replaces both fact_user_products (planned) and user_product_metrics (from analytics DB)
- Contains comprehensive analytics: conversion rates, attribution, lifecycle tracking
- distinct_id maps consistently with other tables (was user_id in original analytics DB)
- Rich attribution data can JOIN with ad performance tables via abi_ad_id
- Lifecycle validity tracking with valid_lifecycle boolean field
- Store tracking for app_store vs play_store differentiation

KEY RELATIONSHIPS AFTER CONSOLIDATION:
- user_product_metrics.distinct_id can JOIN mixpanel_user.distinct_id
- mixpanel_user attribution fields (abi_ad_id, abi_campaign_id, abi_ad_set_id) provide user-level attribution
- Combined analytics enable complete user acquisition-to-conversion journey analysis via user table joins
- price_bucket enables revenue cohort analysis

ATTRIBUTION FIELD CONSISTENCY:
- abi_ad_id (TEXT) - consistent across mixpanel_user, mixpanel_event, and all ad_performance tables
- abi_campaign_id (TEXT) - consistent across mixpanel_user, mixpanel_event, and all ad_performance tables
- abi_ad_set_id (TEXT) - consistent across mixpanel_user, mixpanel_event, and all ad_performance tables

MIGRATION IMPACT:
- Zero downtime merge process with field mapping (user_id → distinct_id)
- All existing analytics queries continue to work with minor field name adjustment
- Enhanced analytics capabilities immediately available
- Single source of truth for user-product relationships
- File size increase: ~13,799 user_product_metrics records with full analytics data
*/ 