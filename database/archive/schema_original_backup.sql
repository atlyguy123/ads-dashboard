-- ========================================
-- MIXPANEL DATABASE SCHEMA
-- ========================================
-- Database Location: /database/mixpanel_data.db
-- Last Updated: Generated from mixpanel_pipeline.md specification
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

-- User Product Association Table
-- Status: NEW TABLE - Needs to be created
-- Purpose: Track which preset product IDs each user has used and their lifecycle validity
CREATE TABLE fact_user_products (
    user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    distinct_id TEXT NOT NULL REFERENCES fact_mixpanel_user(distinct_id),
    product_id TEXT NOT NULL,
    valid_lifecycle BOOLEAN DEFAULT FALSE, -- Whether this user-product lifecycle can be trusted for analysis
    price_bucket REAL NOT NULL, -- Monetary price value with decimals
    store TEXT NOT NULL, -- Store identifier: "app_store" or "play_store"
    UNIQUE (distinct_id, product_id)
);

-- Indexes for fact_user_products
CREATE INDEX idx_user_products_distinct_id ON fact_user_products(distinct_id);
CREATE INDEX idx_user_products_product_id ON fact_user_products(product_id);
CREATE INDEX idx_user_products_valid_lifecycle ON fact_user_products(valid_lifecycle);
CREATE INDEX idx_user_products_store ON fact_user_products(store);
CREATE INDEX idx_user_products_price_bucket ON fact_user_products(price_bucket);

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
-- COMMON INDEXES FOR PERFORMANCE
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

-- Advertising table indexes
CREATE INDEX idx_dim_ad_campaign_sk ON dim_ad(campaign_sk);
CREATE INDEX idx_dim_ad_adset_sk ON dim_ad(adset_sk);
CREATE INDEX idx_dim_adset_campaign_sk ON dim_adset(campaign_sk);
CREATE INDEX idx_fact_meta_daily_date ON fact_meta_daily(date_day);
CREATE INDEX idx_fact_meta_daily_country ON fact_meta_daily(country);

-- ========================================
-- NOTES AND USAGE EXAMPLES
-- ========================================

/*
FACT_USER_PRODUCTS TABLE USAGE:
- If there are 26 preset product IDs and a user has used 3 of them, 
  there will be 3 rows for that user
- Each row indicates whether that specific user-product relationship 
  has a valid lifecycle for analysis
- Revenue data is extracted from associated events in fact_mixpanel_event
- price_bucket contains the monetary value with decimal precision
- store field indicates purchase channel: "app_store" or "play_store"

PIPELINE PROCESSING MODULES:
1. Download/Update Data - Ensures last 90 days present and current
2. Ingest Data - Creates DB if needed, prevents duplicates, sets valid_user=TRUE by default
3. Set HasABI Attribution - Flags users with ABI data/campaign values
4. Check Broken Users - Identifies users with missing/broken data
5. Count User Events - Analyzes event patterns and lifecycle
6. Assign Economic Tier - Assigns economic classification to users

ECONOMIC TIER VALUES:
- "premium" - High-value users
- "standard" - Regular paying users  
- "basic" - Low-tier paying users
- "free" - Non-paying users

STORE VALUES:
- "app_store" - Apple App Store purchases
- "play_store" - Google Play Store purchases
*/ 