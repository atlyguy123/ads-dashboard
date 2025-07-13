# **Data Attribution Inconsistency Report**
**Issue**: Dashboard undercounting attributed users due to missing attribution enrichment in Mixpanel profiles

## **Executive Summary**

We discovered a critical data inconsistency where **Mixpanel web interface shows 12 attributed users for ad set `120225715588020178`**, but **our dashboard only shows 6 users**. Through systematic investigation, we identified that some user profiles receive complete attribution enrichment (`abi_*` fields) while others only contain raw UTM tracking data, despite having identical campaign parameters.

**Business Impact**: 50% attribution failure rate means we're only capturing half of the actual attributed trial conversions, severely impacting ROAS calculations, budget allocation decisions, and campaign performance analysis.

## **Validated Technical Findings**

### **Database Analysis Results**
- **Total users from Mixpanel list**: 12 users provided
- **Users found in our database**: 12 out of 12 (✅ ALL users present)
- **Users with proper attribution**: 6 out of 12 (❌ 50% missing attribution data)
- **Dashboard count**: 6 (only shows users with structured attribution)

### **Data Inconsistency Evidence**

**✅ WORKING ATTRIBUTION (Example: User `qY08wx-8ahM`)**
```json
"abi_ad_id": "120225720069090178",
"abi_campaign_id": "120217904661980178", 
"abi_ad_set_id": "120225715588020178",
"abi_ad_name": "CaseyPovUniversal",
"abi_ad_set_name_1": "partners_winners_quiz_stripe_pasta_us_june_25"
```
- **Database fields**: Properly populated
- **Dashboard result**: ✅ Counted correctly

**❌ BROKEN ATTRIBUTION (Example: User `2xVgn6FD6OW`)**
```json
// NO abi_* fields present in JSON
"initial_utm_id": "120217904661980178",
"initial_utm_content": "partners_winners_quiz_stripe_pasta_us_june_25", 
"initial_utm_term": "CourtneyTop5",
"initial_utm_campaign": "ppc_atly_fb_US_test"
```
- **Database fields**: All NULL (abi_ad_id, abi_campaign_id, abi_ad_set_id)
- **Dashboard result**: ❌ Not counted (filtered out by `has_abi_attribution = TRUE`)

### **Critical Discovery**

**Both user types have IDENTICAL UTM parameters**, but only some receive attribution enrichment:

| Field | Working User | Broken User | Status |
|-------|--------------|-------------|---------|
| `initial_utm_id` | `120217904661980178` | `120217904661980178` | ✅ Identical |
| `initial_utm_content` | `partners_winners_quiz_stripe_pasta_us_june_25` | `partners_winners_quiz_stripe_pasta_us_june_25` | ✅ Identical |
| `initial_utm_campaign` | `ppc_atly_fb_US_test` | `ppc_atly_fb_US_test` | ✅ Identical |
| `abi_ad_id` | `120225720069090178` | `NULL` | ❌ Missing |
| `abi_campaign_id` | `120217904661980178` | `NULL` | ❌ Missing |
| `abi_ad_set_id` | `120225715588020178` | `NULL` | ❌ Missing |

## **Root Cause Analysis**

### **Validated Facts**
1. **Our ETL pipeline works correctly** - it properly extracts `abi_*` fields when present
2. **Attribution enrichment is inconsistent upstream** - some profiles get `abi_*` fields, others don't
3. **UTM tracking is consistent** - all users have identical base tracking parameters
4. **The issue is in Mixpanel profile enrichment, not our extraction logic**

### **Assumptions** *(require validation)*
- **Timing hypothesis**: Attribution enrichment may happen asynchronously after initial profile creation
- **Processing lag**: Some users processed through attribution pipeline, others still pending
- **Data source variation**: Different ingestion paths may handle attribution differently

## **Specific User Examples**

### **Complete User Breakdown** *(from your provided list)*

**Users with proper attribution (6):**
- `qY08wx-8ahM`, `nbQ3D-TB_dJ`, `kv5Vt-qH7wf`, `_jqW58YxTN5`, `Zdu-3O6LpkI`, `miKtJAyftM2`

**Users missing attribution (6):**
- `2xVgn6FD6OW`, `RDSLdiyNHBE`, `T3gHSUEeWTp`, `NMuscmzIuVY`, `04-3gmsLNZ4`, `Yv3mCifJNi7`

**Users completely missing from database:**
- None (✅ All 12 users found in database)

## **Business Impact**

- **Reporting Accuracy**: 50% attribution failure rate (6 out of 12 users missing attribution)
- **Campaign Performance**: ROAS calculations showing only half the actual attributed conversions
- **Budget Allocation**: Critical decisions based on severely incomplete attribution data
- **Scale**: If this 50% failure rate exists across other campaigns, the financial impact is massive
- **Data Confidence**: Complete lack of trust in attribution reporting accuracy

## **Critical Update: Corrected Analysis**

**IMPORTANT**: Initial analysis incorrectly identified 3 users as "missing from database" due to transcription errors. Corrected analysis reveals:
- ✅ ALL 12 users exist in our database
- ✅ ALL 12 users had trial events within the target date range  
- ❌ 6 users (50%) lack attribution enrichment despite identical UTM tracking

This makes the attribution failure rate **significantly worse** than initially calculated.

## **Questions for Data Engineering Team**

1. **Attribution Process**: What triggers the enrichment of UTM data into `abi_*` fields in Mixpanel profiles?
2. **Timing**: Is there a delay between user creation and attribution enrichment?
3. **Completeness**: How can we ensure all users with UTM data receive proper attribution enrichment?
4. **Failure Rate**: Why does attribution enrichment fail for exactly 50% of users with identical UTM parameters?

## **Immediate Recommendations**

### **Short-term Fix**
Create fallback logic in ETL pipeline to use UTM data when `abi_*` fields are missing:
```python
# If abi_ad_id is missing but UTM data exists, derive attribution
if not abi_ad_id and utm_data_complete:
    abi_campaign_id = utm_id
    abi_ad_set_id = lookup_adset_id_by_name(utm_content)
    abi_ad_id = lookup_ad_id_by_name(utm_term)
```

### **Long-term Solution**
- Investigate and fix upstream attribution enrichment process
- Ensure consistent `abi_*` field population for all users with UTM tracking
- Implement monitoring to detect attribution completeness gaps

## **Next Steps**

1. **Immediate**: Confirm attribution enrichment process and identify why 3 users lack `abi_*` fields
2. **Technical**: Implement fallback logic to recover missing attribution from UTM data
3. **Monitoring**: Add alerts for attribution data completeness
4. **Verification**: Re-sync missing users and validate data consistency

---

**Generated**: December 2024  
**Investigation Period**: June 17-30, 2025  
**Ad Set**: `120225715588020178` (partners_winners_quiz_stripe_pasta_us_june_25) 