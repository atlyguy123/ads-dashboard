# **ATTRIBUTION FIELD MAPPING - DEFINITIVE GUIDE**

## **Critical Discovery Summary**

After analyzing **497,139 users**, we found that **237,851 users (47.8%)** have attribution data, but the extraction logic is using **WRONG FIELD NAMES**.

## **🚨 FIELD NAME CORRECTIONS**

### **PRIMARY ABI ATTRIBUTION FIELDS**

| Current (WRONG) | Correct Field Name | Example Value |
|----------------|-------------------|---------------|
| `abi_ad_id` | `abi_~ad_id` | `"120215537057420178"` |
| `abi_campaign_id` | `abi_~campaign_id` | `"120215537057500178"` |
| `abi_ad_set_id` | `abi_~ad_set_id` | `"120215537057340178"` |

### **ADDITIONAL ABI FIELDS AVAILABLE**

| Field Name | Description | Example |
|------------|-------------|---------|
| `abi_~ad_name` | Ad creative name | `"existing_3"` |
| `abi_~ad_set_name` | Ad set name | `"ppc_atly_fb_advantage_WORLD_redirect_feb_25_app Ad set"` |
| `abi_~campaign` | Campaign name | `"ppc_atly_fb_advantage_WORLD_redirect_feb_25_app"` |
| `abi_~advertising_partner_name` | Platform | `"Facebook"` |
| `abi_~channel` | Channel | `"facebook"` |

### **UTM FALLBACK FIELDS**

| Field Name | Maps To | Example |
|------------|---------|---------|
| `abi_utm_campaign` | `abi_campaign_id` | `"ppc_atly_fb_advantage_WORLD_redirect_feb_25_app"` |
| `abi_utm_content` | `abi_ad_set_id` | `"ppc_atly_fb_advantage_WORLD_redirect_feb_25_app Ad set"` |
| `abi_utm_term` | `abi_ad_id` | `"existing_3"` |
| `abi_utm_source` | Platform | `"facebook"` |
| `abi_utm_medium` | Medium | `"cpc"` |

## **📊 DATA STATISTICS**

- **Total Users**: 497,139
- **Users with ABI fields**: 237,851 (47.8%)
- **Users with UTM fields**: 205,369 (41.3%)
- **Users with campaign fields**: 275,480 (55.4%)

## **🔧 REQUIRED CODE CHANGES**

### **1. Fix User Attribution Extraction (`prepare_user_record()`)**

```python
# CURRENT (BROKEN)
abi_ad_id = properties.get('abi_ad_id')
abi_campaign_id = properties.get('abi_campaign_id')
abi_ad_set_id = properties.get('abi_ad_set_id')

# FIXED
abi_ad_id = properties.get('abi_~ad_id')
abi_campaign_id = properties.get('abi_~campaign_id')
abi_ad_set_id = properties.get('abi_~ad_set_id')

# ADD FALLBACK TO UTM FIELDS
if not abi_campaign_id:
    abi_campaign_id = properties.get('abi_utm_campaign')
if not abi_ad_set_id:
    abi_ad_set_id = properties.get('abi_utm_content')
if not abi_ad_id:
    abi_ad_id = properties.get('abi_utm_term')
```

### **2. Enhanced Extraction with All Available Fields**

```python
def extract_complete_attribution(properties):
    """Extract all available attribution data"""
    attribution = {
        'abi_ad_id': properties.get('abi_~ad_id'),
        'abi_campaign_id': properties.get('abi_~campaign_id'),
        'abi_ad_set_id': properties.get('abi_~ad_set_id'),
        'abi_ad_name': properties.get('abi_~ad_name'),
        'abi_ad_set_name': properties.get('abi_~ad_set_name'),
        'abi_campaign_name': properties.get('abi_~campaign'),
        'abi_platform': properties.get('abi_~advertising_partner_name'),
        'abi_channel': properties.get('abi_~channel'),
    }
    
    # UTM fallbacks
    if not attribution['abi_campaign_id']:
        attribution['abi_campaign_id'] = properties.get('abi_utm_campaign')
    if not attribution['abi_ad_set_id']:
        attribution['abi_ad_set_id'] = properties.get('abi_utm_content')
    if not attribution['abi_ad_id']:
        attribution['abi_ad_id'] = properties.get('abi_utm_term')
    
    return attribution
```

## **📋 VALIDATION CHECKLIST**

- [ ] Update field names in `prepare_user_record()`
- [ ] Add UTM fallback logic
- [ ] Update event attribution extraction
- [ ] Test against sample of 237,851 users with ABI attribution
- [ ] Verify dashboard shows correct attribution counts
- [ ] Re-run ingestion pipeline with fixes
- [ ] Validate 47.8% attribution rate achievement

## **📁 FILE REFERENCES**

**Files to modify:**
- `pipelines/mixpanel_pipeline/03_ingest_data.py` (Lines 664-666, 814-837)
- `pipelines/mixpanel_pipeline/03_ingest_data_test.py` (Similar lines)

**Validation files:**
- `complete_user_analysis_20250713_114541.json` - Complete user data
- `attribution_summary_20250713_114541.txt` - Summary statistics

## **🎯 EXPECTED OUTCOME**

After implementing these fixes:
- **Attribution rate should jump from ~0% to 47.8%**
- **Dashboard should show 237,851 attributed users instead of 0**
- **50% attribution failure resolved**

**This single field name correction will fix the primary attribution issue!** 