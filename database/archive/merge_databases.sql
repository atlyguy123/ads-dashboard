-- ========================================
-- DATABASE MERGE SCRIPT
-- Merge mixpanel_analytics.db into mixpanel_data.db
-- ========================================

-- Step 1: Attach the analytics database
ATTACH DATABASE '/Users/joshuakaufman/Ads Dashboard V3 copy 12 - updated ingest copy/database/mixpanel_analytics.db' AS analytics;

-- Step 2: Create the consolidated user_product_metrics table in main database
-- This table combines fields from both user_product_metrics (analytics) and fact_user_products (data)
CREATE TABLE IF NOT EXISTS user_product_metrics (
    user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    distinct_id TEXT NOT NULL,  -- Synonymous with user_id, using distinct_id for consistency with other tables
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
    -- Additional fields from fact_user_products
    valid_lifecycle BOOLEAN DEFAULT FALSE,
    store TEXT, -- "app_store" or "play_store"
    UNIQUE (distinct_id, product_id)
);

-- Step 3: Create indexes for consolidated user_product_metrics table
CREATE INDEX IF NOT EXISTS idx_upm_distinct_id ON user_product_metrics (distinct_id);
CREATE INDEX IF NOT EXISTS idx_upm_credited_date ON user_product_metrics (credited_date);
CREATE INDEX IF NOT EXISTS idx_upm_country ON user_product_metrics (country);
CREATE INDEX IF NOT EXISTS idx_upm_region ON user_product_metrics (region);
CREATE INDEX IF NOT EXISTS idx_upm_device ON user_product_metrics (device);
CREATE INDEX IF NOT EXISTS idx_upm_product_id ON user_product_metrics (product_id);
CREATE INDEX IF NOT EXISTS idx_upm_abi_ad_id ON user_product_metrics (abi_ad_id);
CREATE INDEX IF NOT EXISTS idx_upm_abi_campaign_id ON user_product_metrics (abi_campaign_id);
CREATE INDEX IF NOT EXISTS idx_upm_abi_ad_set_id ON user_product_metrics (abi_ad_set_id);
CREATE INDEX IF NOT EXISTS idx_upm_valid_lifecycle ON user_product_metrics (valid_lifecycle);
CREATE INDEX IF NOT EXISTS idx_upm_store ON user_product_metrics (store);

-- Step 4: Copy data from analytics database with field mapping
-- Map user_id to distinct_id and add default values for new fields
INSERT OR REPLACE INTO user_product_metrics 
(distinct_id, product_id, credited_date, country, region, device, abi_ad_id, abi_campaign_id, abi_ad_set_id, 
 current_status, current_value, value_status, segment_id, accuracy_score, trial_conversion_rate, 
 trial_converted_to_refund_rate, initial_purchase_to_refund_rate, price_bucket, last_updated_ts, 
 valid_lifecycle, store)
SELECT 
    user_id AS distinct_id,  -- Map user_id to distinct_id
    product_id, credited_date, country, region, device, abi_ad_id, abi_campaign_id, abi_ad_set_id,
    current_status, current_value, value_status, segment_id, accuracy_score, trial_conversion_rate,
    trial_converted_to_refund_rate, initial_purchase_to_refund_rate, price_bucket, last_updated_ts,
    FALSE AS valid_lifecycle,  -- Default value for new field
    NULL AS store              -- Default value for new field (can be updated later)
FROM analytics.user_product_metrics;

-- Step 4.5: Remove the original fact_user_products table since it's replaced by consolidated user_product_metrics
DROP TABLE IF EXISTS fact_user_products;

-- Step 5: Create pipeline_status table (even though it's empty, for completeness)
CREATE TABLE IF NOT EXISTS pipeline_status (
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

-- Step 6: Copy pipeline_status data (if any)
INSERT OR REPLACE INTO pipeline_status 
SELECT * FROM analytics.pipeline_status;

-- Step 7: Detach the analytics database
DETACH DATABASE analytics;

-- Verification queries
SELECT 'user_product_metrics' as table_name, COUNT(*) as row_count FROM user_product_metrics
UNION ALL
SELECT 'pipeline_status' as table_name, COUNT(*) as row_count FROM pipeline_status; 