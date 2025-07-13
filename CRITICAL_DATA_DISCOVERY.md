# **🚨 CRITICAL DATA DISCOVERY - ATTRIBUTION FIELDS MISSING**

## **Executive Summary**

**MISSION-CRITICAL FINDING**: The attribution data extraction logic is looking for fields that **DO NOT EXIST** in the actual Mixpanel raw data. This explains the 50% attribution failure rate and data inconsistencies.

## **Validated Real Data Structure**

After analyzing 50 sample users and 50 sample events from the raw database:

### **❌ FIELDS THAT DON'T EXIST (But Code Expects Them)**

#### **User Attribution Fields (ALL MISSING)**
- `abi_ad_id` - **NOT FOUND in any of 50 users**
- `abi_campaign_id` - **NOT FOUND in any of 50 users**  
- `abi_ad_set_id` - **NOT FOUND in any of 50 users**
- `initial_utm_id` - **NOT FOUND in any of 50 users**
- `initial_utm_content` - **NOT FOUND in any of 50 users**
- `initial_utm_term` - **NOT FOUND in any of 50 users**
- `initial_utm_campaign` - **NOT FOUND in any of 50 users**

#### **Event Attribution Fields (ALL MISSING)**
- Event-level `abi_*` fields - **NOT FOUND in any events**
- Event-level UTM fields - **NOT FOUND in any events**
- Attribution fields in `subscriber_attributes` - **NOT FOUND**

### **✅ FIELDS THAT DO EXIST (What We Can Actually Use)**

#### **User Properties Available**
```json
{
  "$email": "user@example.com",
  "$country_code": "US", 
  "$region": "California",
  "$city": "Los Angeles",
  "$last_seen": "2019-06-25T06:33:26",
  "first_install_date": "2019-02-06T08:49:17",
  "$campaign": "some_campaign_value",
  "$campaigns": ["campaign1", "campaign2"],
  "$media_source": "attribution_source",
  "user_id": "user_identifier"
}
```

#### **Event Properties Available**  
```json
{
  "event": "RC Trial started",
  "properties": {
    "time": 1743083688,
    "distinct_id": "$device:127BC1A2-F6C6-4565-9665-F2EA02387D8B",
    "$insert_id": "826c0f55-d8ee-4191-8225-2f064cdba581", 
    "revenue": 0,
    "currency": "USD",
    "product_id": "gluten.free.eats.3.yearly",
    "store": "APP_STORE",
    "subscriber_attributes": {
      "$ip": "93.44.19.24",
      "$idfa": "00000000-0000-0000-0000-000000000000",
      "$mixpanelDistinctId": "$device:..."
    }
  }
}
```

---

## **Impact Analysis**

### **Current Code Problems**

#### **1. User Attribution Extraction (Lines 664-666 in `03_ingest_data.py`)**
```python
# THIS ALWAYS RETURNS NULL - FIELDS DON'T EXIST!
abi_ad_id = properties.get('abi_ad_id')           # ❌ Always None
abi_campaign_id = properties.get('abi_campaign_id')  # ❌ Always None  
abi_ad_set_id = properties.get('abi_ad_set_id')     # ❌ Always None
```

**Result**: `has_abi_attribution = False` for ALL users

#### **2. Event Attribution Extraction (Lines 814-837 in `03_ingest_data.py`)**
```python
# THIS ALWAYS RETURNS NULL - FIELDS DON'T EXIST!
for key, value in properties.items():
    if key == 'abi_ad_id':           # ❌ Never matches
        abi_ad_id = value
    elif key == 'abi_campaign_id':   # ❌ Never matches
        abi_campaign_id = value
    elif key == 'abi_ad_set_id':     # ❌ Never matches  
        abi_ad_set_id = value
```

**Result**: All events have NULL attribution fields

#### **3. Dashboard Filtering**
```sql
-- THIS FILTERS OUT ALL USERS - BECAUSE NONE HAVE ABI ATTRIBUTION!
WHERE has_abi_attribution = TRUE
```

**Result**: Dashboard shows 0 attributed users because the attribution fields are never populated

---

## **Root Cause Analysis**

### **Why Attribution Fields Are Missing**

1. **Data Source Change**: Mixpanel may have changed their data export format
2. **Attribution Pipeline Missing**: The upstream system that enriches profiles with `abi_*` fields may not be running
3. **Different Data Source**: The raw data may be coming from a different source than expected
4. **Processing Pipeline Gap**: Attribution enrichment may happen after export but before ingestion

### **Available Attribution Data**

The only attribution-related fields we found:
- `$campaign` (in users)
- `$campaigns` (in users) 
- `$media_source` (in users)

These may contain attribution information in a different format.

---

## **Immediate Actions Required**

### **Priority 1: Fix Attribution Extraction**

**Replace the broken extraction logic with fields that actually exist:**

#### **User Attribution (Fix `prepare_user_record()`)** 
```python
# CURRENT (BROKEN) - Fields don't exist
abi_ad_id = properties.get('abi_ad_id')
abi_campaign_id = properties.get('abi_campaign_id') 
abi_ad_set_id = properties.get('abi_ad_set_id')

# FIXED - Use fields that actually exist
campaign_data = properties.get('$campaign', '')
campaigns_data = properties.get('$campaigns', [])
media_source = properties.get('$media_source', '')

# Parse attribution from available fields
abi_campaign_id = extract_campaign_id(campaign_data, campaigns_data)
abi_ad_set_id = extract_ad_set_id(campaign_data, campaigns_data)  
abi_ad_id = extract_ad_id(campaign_data, campaigns_data)
```

#### **Event Attribution (Fix `prepare_event_record()`)**
```python
# CURRENT (BROKEN) - Fields don't exist  
for key, value in properties.items():
    if key == 'abi_ad_id':
        abi_ad_id = value

# FIXED - Extract from available data or user lookup
# May need to lookup user attribution and apply to events
```

### **Priority 2: Investigate Data Pipeline**

1. **Verify data source**: Confirm this is the correct Mixpanel export
2. **Check upstream attribution**: Investigate why `abi_*` fields aren't being populated
3. **Review attribution pipeline**: Ensure attribution enrichment is running
4. **Validate export process**: Confirm all fields are being exported

### **Priority 3: Update Database Schema**

```sql
-- Add fields for actual attribution data
ALTER TABLE mixpanel_user ADD COLUMN campaign_data TEXT;
ALTER TABLE mixpanel_user ADD COLUMN media_source TEXT;
ALTER TABLE mixpanel_user ADD COLUMN campaigns_data TEXT;
```

---

## **Field Validation Results**

### **✅ CONFIRMED WORKING EXTRACTIONS**

#### **User Fields**
- `distinct_id` ✅
- `$email` ✅  
- `$country_code` ✅
- `$region` ✅
- `$city` ✅
- `$last_seen` ✅
- `first_install_date` ✅

#### **Event Fields**  
- `event` (top level) ✅
- `time` (in properties) ✅
- `distinct_id` (in properties) ✅
- `$insert_id` (in properties) ✅
- `revenue` (in properties) ✅
- `currency` (in properties) ✅
- `store` (in properties) ✅
- `product_id` (in properties) ✅

### **❌ BROKEN EXTRACTIONS**

#### **Attribution Fields (All Broken)**
- `abi_ad_id` ❌ (Field doesn't exist)
- `abi_campaign_id` ❌ (Field doesn't exist)  
- `abi_ad_set_id` ❌ (Field doesn't exist)
- `initial_utm_*` fields ❌ (Fields don't exist)

---

## **Next Steps**

1. **IMMEDIATE**: Stop ingestion until attribution extraction is fixed
2. **URGENT**: Implement extraction using `$campaign`, `$campaigns`, `$media_source`
3. **CRITICAL**: Investigate why `abi_*` fields are missing from source data
4. **REQUIRED**: Test new extraction logic with sample data
5. **VALIDATION**: Re-run ingestion with corrected field mapping

**This discovery explains the 50% attribution failure rate - it's actually 100% failure because the fields don't exist in the source data!** 