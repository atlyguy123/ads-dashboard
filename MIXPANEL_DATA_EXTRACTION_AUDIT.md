# **MIXPANEL DATA EXTRACTION AUDIT**
**Mission-Critical Data Validation Report**

## **Executive Summary**

This document provides a comprehensive audit of every single data extraction and validation point in the Mixpanel pipeline. The audit reveals critical gaps in field name validation, missing fallback mechanisms, and inconsistent extraction patterns that could lead to data loss.

**🚨 CRITICAL FINDINGS**: 
- Multiple extraction inconsistencies discovered
- Missing fallback mechanisms for UTM fields  
- Inconsistent field naming patterns
- Potential data loss from format changes

---

## **MODULE 1: DOWNLOAD/UPDATE DATA (`01_download_update_data.py`)**

### **1.1 EVENT DATA EXTRACTION**

#### **Event Filtering Logic**
```python
EVENTS_TO_KEEP = [
    "RC Trial started", 
    "RC Trial converted", 
    "RC Cancellation", 
    "RC Initial purchase", 
    "RC Trial cancelled", 
    "RC Renewal"
]
```

#### **Event Name Extraction** (Lines 403, 450)
```python
event_name = event_data.get("event") or event_data.get("event_name")
```

**✅ VALIDATION STATUS**: CORRECT - Handles both old and new formats
**🔍 VERIFICATION NEEDED**: Confirm these are the only two possible field names

### **1.2 USER DATA EXTRACTION**

#### **User Distinct ID Extraction** (Lines 511, 560)
```python
# Line 511 (download_and_parse_user_file_to_memory)
distinct_id = user_data.get('mp_distinct_id') or user_data.get('abi_distinct_id') or user_data.get('distinct_id')

# Line 560 (download_and_store_user_file) 
distinct_id = user_data.get('mp_distinct_id') or user_data.get('distinct_id')
```

**🚨 CRITICAL ISSUE**: INCONSISTENT EXTRACTION LOGIC
- Memory function checks 3 fields: `mp_distinct_id`, `abi_distinct_id`, `distinct_id`
- Store function checks only 2 fields: `mp_distinct_id`, `distinct_id`
- Missing `abi_distinct_id` in storage function could cause data loss

**🔧 REQUIRED FIX**: Make both functions use identical extraction logic

---

## **MODULE 2: DATA INGESTION (`03_ingest_data.py`)**

### **2.1 USER DATA PROCESSING**

#### **User Filtering Logic** (Lines 386-401)
```python
def should_filter_user(distinct_id: str, email: str):
    if '@atly.com' in email_lower:
        return {'filter': True, 'reason': 'atly'}
    if 'test' in email_lower:
        return {'filter': True, 'reason': 'test'}
    if '@steps.me' in email_lower:
        return {'filter': True, 'reason': 'steps'}
```

#### **Email Extraction** (Line 453)
```python
email = properties.get('$email', '')
```

**✅ VALIDATION STATUS**: CORRECT - Uses standard Mixpanel email field
**🔍 VERIFICATION NEEDED**: Confirm this is the only email field location

#### **Attribution Data Extraction** (Lines 664-666)
```python
abi_ad_id = properties.get('abi_ad_id')
abi_campaign_id = properties.get('abi_campaign_id')
abi_ad_set_id = properties.get('abi_ad_set_id')
```

**🚨 CRITICAL ISSUE**: NO FALLBACK TO UTM FIELDS
- Only checks direct `abi_*` fields
- Missing fallback extraction for `initial_utm_*` fields
- This causes the 50% attribution failure rate documented in attribution_inconsistency_report.md

**🔧 REQUIRED FIX**: Add fallback extraction:
```python
# Missing fallback logic
if not abi_campaign_id:
    abi_campaign_id = properties.get('initial_utm_id')
if not abi_ad_set_id:
    abi_ad_set_id = properties.get('initial_utm_content')
```

#### **Geographic Data Extraction** (Lines 675-677)
```python
country = properties.get('$country_code')
region = properties.get('$region')
city = properties.get('$city')
```

**✅ VALIDATION STATUS**: CORRECT - Uses standard Mixpanel geo fields
**🔍 VERIFICATION NEEDED**: Confirm these field names are current

#### **Timestamp Extraction** (Lines 683, 693)
```python
first_seen = properties.get('first_install_date') or properties.get('$ae_first_app_open_date')
last_updated = properties.get('$last_seen')
```

**✅ VALIDATION STATUS**: CORRECT - Has fallback mechanism
**🔍 VERIFICATION NEEDED**: Confirm these are the only timestamp field options

### **2.2 EVENT DATA PROCESSING**

#### **Event UUID Extraction** (Line 783)
```python
event_uuid = event_data.get('insert_id') or properties.get('$insert_id')
```

**✅ VALIDATION STATUS**: CORRECT - Handles both locations
**🔍 VERIFICATION NEEDED**: Confirm these are the only UUID field locations

#### **Event Distinct ID Extraction** (Line 785)
```python
distinct_id = event_data.get('distinct_id') or properties.get('distinct_id')
```

**✅ VALIDATION STATUS**: CORRECT - Handles both locations
**🔍 VERIFICATION NEEDED**: Confirm these are the only distinct_id field locations

#### **Event Name Extraction** (Line 787)
```python
event_name = event_data.get('event') or event_data.get('event_name')
```

**✅ VALIDATION STATUS**: CORRECT - Matches download module logic

#### **Event Timestamp Extraction** (Lines 797, 804)
```python
timestamp = event_data.get('time') or properties.get('time', 0)
```

**✅ VALIDATION STATUS**: CORRECT - Handles both locations with fallback

#### **Event Attribution Extraction** (Lines 814-837)
```python
# Check subscriber_attributes for attribution
subscriber_attrs = properties.get('subscriber_attributes', {})
if subscriber_attrs:
    for key, value in subscriber_attrs.items():
        if 'ad_id' in key.lower():
            abi_ad_id = value
        elif 'campaign_id' in key.lower():
            abi_campaign_id = value
        elif 'adset_id' in key.lower() or 'ad_set_id' in key.lower():
            abi_ad_set_id = value

# Check direct properties for attribution
for key, value in properties.items():
    if key == 'abi_ad_id':
        abi_ad_id = value
    elif key == 'abi_campaign_id':
        abi_campaign_id = value
    elif key == 'abi_ad_set_id':
        abi_ad_set_id = value
```

**✅ VALIDATION STATUS**: COMPREHENSIVE - Checks multiple locations
**🔍 VERIFICATION NEEDED**: Confirm subscriber_attributes structure is current

#### **Revenue Data Extraction** (Lines 850, 854)
```python
revenue = float(properties.get('revenue', 0)) if properties.get('revenue') else 0
currency = properties.get('currency', 'USD') or 'USD'
```

**✅ VALIDATION STATUS**: CORRECT - Has safe parsing and defaults
**🔍 VERIFICATION NEEDED**: Confirm these are the only revenue field locations

#### **Trial Expiration Extraction** (Line 862)
```python
expiration_at = properties.get('expiration_at')
```

**✅ VALIDATION STATUS**: BASIC - Single field check
**🔍 VERIFICATION NEEDED**: Check if other expiration field names exist

---

## **CRITICAL VALIDATION REQUIREMENTS**

### **1. Field Name Validation**

**🚨 URGENT**: Validate all field names against current Mixpanel schema:

#### **User Properties Fields**
- `$email` - Email address
- `$country_code` - ISO country code
- `$region` - Geographic region
- `$city` - City name
- `$last_seen` - Last activity timestamp
- `first_install_date` - First app installation
- `$ae_first_app_open_date` - Alternative install date
- `abi_ad_id` - ABI attribution ad ID
- `abi_campaign_id` - ABI attribution campaign ID
- `abi_ad_set_id` - ABI attribution ad set ID
- `initial_utm_id` - UTM campaign ID
- `initial_utm_content` - UTM content/ad set
- `initial_utm_term` - UTM term/ad name
- `initial_utm_campaign` - UTM campaign name

#### **Event Properties Fields**
- `event` vs `event_name` - Event name field
- `insert_id` vs `$insert_id` - Unique event ID
- `distinct_id` - User identifier
- `time` - Event timestamp
- `revenue` - Revenue amount
- `currency` - Currency code
- `expiration_at` - Trial expiration
- `subscriber_attributes` - Subscription metadata

#### **User Data Root Fields**
- `mp_distinct_id` - Mixpanel distinct ID
- `abi_distinct_id` - ABI distinct ID
- `distinct_id` - Generic distinct ID

### **2. Missing Fallback Mechanisms**

**🔧 REQUIRED FIXES**:

#### **User Attribution Fallback**
```python
# MISSING: Fallback to UTM fields when ABI fields are empty
if not abi_campaign_id:
    abi_campaign_id = properties.get('initial_utm_id')
if not abi_ad_set_id:
    abi_ad_set_id = properties.get('initial_utm_content')  
if not abi_ad_id:
    # Extract from initial_utm_term or use constructed ID
    abi_ad_id = properties.get('initial_utm_term_mapped_id')
```

#### **Event Attribution Fallback**
```python
# MISSING: Event-level UTM fallback extraction
if not abi_campaign_id:
    abi_campaign_id = properties.get('utm_campaign_id')
if not abi_ad_set_id:
    abi_ad_set_id = properties.get('utm_content')
```

### **3. Inconsistent Extraction Logic**

**🔧 REQUIRED FIXES**:

#### **User Distinct ID Consistency**
Make both download functions use identical logic:
```python
# STANDARDIZE to this pattern in both functions
distinct_id = user_data.get('mp_distinct_id') or user_data.get('abi_distinct_id') or user_data.get('distinct_id')
```

#### **Event Name Consistency**
Ensure both modules use identical extraction:
```python
# STANDARDIZE to this pattern in both modules
event_name = event_data.get('event') or event_data.get('event_name')
```

### **4. Data Type Validation**

**🔧 REQUIRED ENHANCEMENTS**:

#### **Timestamp Validation**
```python
# MISSING: Comprehensive timestamp format handling
def parse_timestamp_safely(timestamp_value):
    if isinstance(timestamp_value, str):
        # Handle ISO format
        if 'T' in timestamp_value or 'Z' in timestamp_value:
            return datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
        # Handle epoch string
        try:
            return datetime.fromtimestamp(float(timestamp_value))
        except:
            return datetime.now()
    elif isinstance(timestamp_value, (int, float)):
        return datetime.fromtimestamp(timestamp_value)
    else:
        return datetime.now()
```

#### **Revenue Validation**
```python
# MISSING: Enhanced revenue parsing
def parse_revenue_safely(revenue_value):
    if revenue_value is None:
        return 0.0
    if isinstance(revenue_value, str):
        # Handle currency symbols and formatting
        cleaned = re.sub(r'[^\d.-]', '', revenue_value)
        try:
            return float(cleaned)
        except:
            return 0.0
    elif isinstance(revenue_value, (int, float)):
        return float(revenue_value)
    else:
        return 0.0
```

---

## **VALIDATION TESTING REQUIREMENTS**

### **1. Edge Case Testing**

Test extraction with:
- Empty/null values
- Malformed JSON
- Missing properties sections
- Mixed data types
- Unicode characters
- Very large numbers
- Negative values
- Boolean values in string fields

### **2. Field Name Validation Testing**

Create test data with:
- All known field name variations
- Mixed old/new format combinations
- Missing required fields
- Extra unexpected fields

### **3. Attribution Logic Testing**

Test scenarios:
- Users with only ABI fields
- Users with only UTM fields  
- Users with both ABI and UTM fields
- Users with neither ABI nor UTM fields
- Conflicting ABI and UTM data

---

## **RECOMMENDED ACTIONS**

### **Priority 1: Critical Fixes**
1. **Fix user distinct_id extraction consistency** between download functions
2. **Implement UTM fallback logic** for attribution fields
3. **Validate all field names** against current Mixpanel schema
4. **Add comprehensive error handling** for malformed data

### **Priority 2: Enhancement**
1. **Implement safe data type parsing** for timestamps and revenue
2. **Add field name validation** with logging for unknown fields
3. **Create comprehensive test suite** for edge cases
4. **Add monitoring** for extraction failure rates

### **Priority 3: Monitoring**
1. **Add extraction metrics** to track success/failure rates
2. **Implement field usage tracking** to identify schema changes
3. **Create alerting** for extraction anomalies
4. **Add data quality reporting** for validation failures

---

## **CONCLUSION**

This audit identifies critical gaps in the Mixpanel data extraction pipeline that could lead to significant data loss. The most critical issue is the missing fallback mechanism for UTM fields, which is causing a 50% attribution failure rate as documented in the attribution inconsistency report.

**All identified issues must be addressed with surgical precision to ensure data integrity and prevent business impact from incomplete attribution data.** 