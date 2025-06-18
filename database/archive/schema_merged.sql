-- ========================================
-- MERGED MIXPANEL DATABASE SCHEMA
-- ========================================
-- Database Location: /database/mixpanel_data.db (MERGED)
-- Last Updated: Merged from mixpanel_pipeline.md + mixpanel_analytics.db
-- ========================================

-- ========================================
-- CORE USER DATA TABLES
-- ========================================

-- Primary User Table
-- Status: EXISTS - Needs 2 columns added (valid_user, economic_tier)
CREATE TABLE fact_mixpanel_user (
    distinct_id TEXT PRIMARY KEY,
    ad_sk CHAR(64) NULL REFERENCES dim_ad(ad_sk),
    country TEXT, -- ISO 3166-1 alpha-2 code
    region TEXT,
    city TEXT,
    has_abi_attribution BOOLEAN DEFAULT FALSE,
    profile_json TEXT,
    first_seen DATE,
    last_updated DATE,
    -- NEW COLUMNS TO BE ADDED:
    valid_user BOOLEAN DEFAULT FALSE, -- Flag for user validity
    economic_tier TEXT -- Economic classification ("premium", "standard", "basic", "free")
);

-- Event Tracking Table
-- Status: EXISTS - No changes needed
CREATE TABLE fact_mixpanel_event (
    event_uuid TEXT PRIMARY KEY,
    event_name TEXT NOT NULL,
    ad_sk CHAR(64) NULL REFERENCES dim_ad(ad_sk),
    adset_sk INT NULL REFERENCES dim_adset(adset_sk),
    campaign_sk INT NULL REFERENCES dim_campaign(campaign_sk),
    distinct_id TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    country TEXT,
    region TEXT,
    revenue_usd NUMERIC(18, 4),
    raw_amount NUMERIC(18, 4),
    currency TEXT,
    refund_flag BOOLEAN DEFAULT FALSE,
    is_late_event BOOLEAN DEFAULT FALSE,
    trial_expiration_at_calc TIMESTAMP NULL,
    event_json TEXT
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
    distinct_id TEXT NOT NULL REFERENCES fact_mixpanel_user(distinct_id), -- Consistent with other tables (was user_id in analytics DB)
    product_id TEXT NOT NULL, 
    credited_date TEXT NOT NULL, 
    country TEXT, 
    region TEXT, 
    device TEXT, 
    abi_ad_id TEXT, 
    abi_campaign_id TEXT, 
    abi_ad_set_id TEXT, 
    current_status TEXT NOT NULL, 
    current_value REAL NOT NULL, 
    value_status TEXT NOT NULL, 
    segment_id TEXT, 
    accuracy_score TEXT, 
    trial_conversion_rate REAL, 
    trial_converted_to_refund_rate REAL, 
    initial_purchase_to_refund_rate REAL, 
    price_bucket REAL, 
    last_updated_ts TEXT NOT NULL,
    -- Additional fields from planned fact_user_products structure
    valid_lifecycle BOOLEAN DEFAULT FALSE, -- Whether this user-product lifecycle can be trusted for analysis
    store TEXT, -- Store identifier: "app_store" or "play_store"
    UNIQUE (distinct_id, product_id)
);

-- Pipeline Status Monitoring
-- Status: MERGED FROM ANALYTICS DB
-- Purpose: Track analytics pipeline execution status
CREATE TABLE pipeline_status (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    progress_percentage INTEGER DEFAULT 0,
    current_step TEXT,
    error_message TEXT,
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    processed_users INTEGER DEFAULT 0,
    total_users INTEGER DEFAULT 0
);

-- ========================================
-- ADVERTISING ATTRIBUTION TABLES
-- ========================================

-- Campaign Level Data
-- Status: EXISTS - No changes needed
CREATE TABLE dim_campaign (
    campaign_sk INT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    account_id TEXT,
    account_name TEXT,
    objective TEXT,
    status TEXT,
    created_time TIMESTAMP,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    updated_time TIMESTAMP
);

-- Ad Set Level Data
-- Status: EXISTS - No changes needed
CREATE TABLE dim_adset (
    adset_sk INT PRIMARY KEY,
    adset_id TEXT NOT NULL,
    adset_name TEXT,
    campaign_sk INT REFERENCES dim_campaign(campaign_sk),
    campaign_id TEXT,
    status TEXT,
    optimization_goal TEXT,
    billing_event TEXT,
    bid_amount NUMERIC(10, 2),
    daily_budget NUMERIC(10, 2),
    lifetime_budget NUMERIC(10, 2),
    created_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    updated_time TIMESTAMP
);

-- Individual Ad Creative Data
-- Status: EXISTS - No changes needed
CREATE TABLE dim_ad (
    ad_sk CHAR(64) PRIMARY KEY,
    ad_id TEXT NOT NULL,
    ad_name TEXT,
    adset_sk INT REFERENCES dim_adset(adset_sk),
    adset_id TEXT,
    campaign_sk INT REFERENCES dim_campaign(campaign_sk),
    campaign_id TEXT,
    status TEXT,
    creative_id TEXT,
    creative_name TEXT,
    creative_body TEXT,
    creative_title TEXT,
    creative_link_url TEXT,
    created_time TIMESTAMP,
    updated_time TIMESTAMP
);

-- Daily Aggregated Advertising Performance
-- Status: EXISTS - No changes needed
CREATE TABLE fact_meta_daily (
    date_day DATE NOT NULL,
    account_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    adset_id TEXT NOT NULL,
    ad_id TEXT NOT NULL,
    country TEXT,
    device_platform TEXT,
    impressions BIGINT DEFAULT 0,
    clicks BIGINT DEFAULT 0,
    spend NUMERIC(10, 4) DEFAULT 0,
    reach BIGINT DEFAULT 0,
    frequency NUMERIC(10, 4) DEFAULT 0,
    video_plays BIGINT DEFAULT 0,
    video_p25_watched BIGINT DEFAULT 0,
    video_p50_watched BIGINT DEFAULT 0,
    video_p75_watched BIGINT DEFAULT 0,
    video_p100_watched BIGINT DEFAULT 0,
    PRIMARY KEY (date_day, account_id, campaign_id, adset_id, ad_id, country, device_platform)
);

-- ========================================
-- SUPPORTING TABLES
-- ========================================

-- Currency Exchange Rates
-- Status: EXISTS - No changes needed
CREATE TABLE fact_currency_fx (
    date_day DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    usd_rate NUMERIC(18, 8) NOT NULL,
    PRIMARY KEY (date_day, currency_code)
);

-- ETL Pipeline Control
-- Status: EXISTS - No changes needed
CREATE TABLE etl_job_control (
    job_name TEXT PRIMARY KEY,
    last_run_timestamp TIMESTAMP,
    last_success_timestamp TIMESTAMP,
    status TEXT, -- 'running', 'success', 'failed'
    error_message TEXT,
    run_duration_seconds INTEGER
);

-- Daily Event Processing Tracker
-- Status: EXISTS - No changes needed
CREATE TABLE processed_event_days (
    date_day DATE PRIMARY KEY,
    events_processed BIGINT,
    processing_timestamp TIMESTAMP,
    status TEXT -- 'complete', 'partial', 'failed'
);

-- Dynamic Schema Discovery
-- Status: EXISTS - No changes needed
CREATE TABLE dim_discovered_properties (
    property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_name TEXT NOT NULL UNIQUE,
    property_type TEXT, -- 'event' or 'user'
    first_seen_date DATE,
    last_seen_date DATE,
    sample_value TEXT
);

CREATE TABLE dim_discovered_property_values (
    value_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER REFERENCES dim_discovered_properties(property_id),
    property_value TEXT,
    first_seen_date DATE,
    last_seen_date DATE,
    occurrence_count INTEGER DEFAULT 1
);

-- ========================================
-- PIPELINE MANAGEMENT TABLES
-- ========================================

-- Pipeline Execution History
-- Status: EXISTS - No changes needed
CREATE TABLE refresh_pipeline_history (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
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
    interruption_time TIMESTAMP NOT NULL,
    last_processed_record TEXT,
    recovery_checkpoint TEXT, -- JSON blob for recovery state
    status TEXT -- 'interrupted', 'recovering', 'recovered'
);

-- Dashboard Caching
-- Status: EXISTS - No changes needed
CREATE TABLE dashboard_refresh_cache (
    cache_key TEXT PRIMARY KEY,
    cache_value TEXT, -- JSON blob
    created_timestamp TIMESTAMP NOT NULL,
    expires_timestamp TIMESTAMP,
    refresh_count INTEGER DEFAULT 1
);

-- Geographic Reference Data
-- Status: EXISTS - No changes needed
CREATE TABLE dim_continent_country (
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
    created_timestamp TIMESTAMP NOT NULL,
    last_modified TIMESTAMP,
    view_parameters TEXT, -- JSON blob for parameterized views
    is_public BOOLEAN DEFAULT FALSE
);

-- ========================================
-- INDEXES FOR PERFORMANCE
-- ========================================

-- User table indexes
CREATE INDEX idx_mixpanel_user_country ON fact_mixpanel_user(country);
CREATE INDEX idx_mixpanel_user_has_abi ON fact_mixpanel_user(has_abi_attribution);
CREATE INDEX idx_mixpanel_user_first_seen ON fact_mixpanel_user(first_seen);
CREATE INDEX idx_mixpanel_user_valid_user ON fact_mixpanel_user(valid_user);
CREATE INDEX idx_mixpanel_user_economic_tier ON fact_mixpanel_user(economic_tier);

-- Event table indexes
CREATE INDEX idx_mixpanel_event_distinct_id ON fact_mixpanel_event(distinct_id);
CREATE INDEX idx_mixpanel_event_name ON fact_mixpanel_event(event_name);
CREATE INDEX idx_mixpanel_event_time ON fact_mixpanel_event(event_time);
CREATE INDEX idx_mixpanel_event_country ON fact_mixpanel_event(country);
CREATE INDEX idx_mixpanel_event_revenue ON fact_mixpanel_event(revenue_usd);

-- Consolidated User Product Metrics indexes (combines all analytics and lifecycle tracking indexes)
CREATE INDEX idx_upm_distinct_id ON user_product_metrics (distinct_id);
CREATE INDEX idx_upm_product_id ON user_product_metrics (product_id);
CREATE INDEX idx_upm_credited_date ON user_product_metrics (credited_date);
CREATE INDEX idx_upm_country ON user_product_metrics (country);
CREATE INDEX idx_upm_region ON user_product_metrics (region);
CREATE INDEX idx_upm_device ON user_product_metrics (device);
CREATE INDEX idx_upm_abi_ad_id ON user_product_metrics (abi_ad_id);
CREATE INDEX idx_upm_abi_campaign_id ON user_product_metrics (abi_campaign_id);
CREATE INDEX idx_upm_abi_ad_set_id ON user_product_metrics (abi_ad_set_id);
CREATE INDEX idx_upm_valid_lifecycle ON user_product_metrics (valid_lifecycle);
CREATE INDEX idx_upm_store ON user_product_metrics (store);
CREATE INDEX idx_upm_price_bucket ON user_product_metrics (price_bucket);

-- Advertising table indexes
CREATE INDEX idx_dim_ad_campaign_sk ON dim_ad(campaign_sk);
CREATE INDEX idx_dim_ad_adset_sk ON dim_ad(adset_sk);
CREATE INDEX idx_dim_adset_campaign_sk ON dim_adset(campaign_sk);
CREATE INDEX idx_fact_meta_daily_date ON fact_meta_daily(date_day);
CREATE INDEX idx_fact_meta_daily_country ON fact_meta_daily(country);

-- ========================================
-- MERGE BENEFITS & RELATIONSHIPS
-- ========================================

/*
POST-MERGE ADVANTAGES:
1. Single database eliminates sync complexity
2. Direct joins possible between user_product_metrics and fact_mixpanel_user
3. Can correlate detailed analytics with attribution data
4. Simplified backup/maintenance procedures
5. Better performance for cross-dataset queries

CONSOLIDATED USER_PRODUCT_METRICS TABLE BENEFITS:
- Replaces both fact_user_products (planned) and user_product_metrics (from analytics DB)
- Contains comprehensive analytics: conversion rates, attribution, lifecycle tracking
- distinct_id maps consistently with other tables (was user_id in original analytics DB)
- Rich attribution data can JOIN with dim_ad, dim_adset, dim_campaign tables
- Lifecycle validity tracking with valid_lifecycle boolean field
- Store tracking for app_store vs play_store differentiation

KEY RELATIONSHIPS AFTER CONSOLIDATION:
- user_product_metrics.distinct_id can JOIN fact_mixpanel_user.distinct_id
- user_product_metrics attribution fields (abi_ad_id, abi_campaign_id, abi_ad_set_id) can JOIN advertising dimension tables
- Combined analytics enable complete user acquisition-to-conversion journey analysis
- price_bucket enables revenue cohort analysis

MIGRATION IMPACT:
- Zero downtime merge process with field mapping (user_id → distinct_id)
- All existing analytics queries continue to work with minor field name adjustment
- Enhanced analytics capabilities immediately available
- Single source of truth for user-product relationships
- File size increase: ~13,799 user_product_metrics records with full analytics data
*/ 