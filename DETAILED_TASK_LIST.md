# DETAILED TASK LIST: Tooltip Enhancement & Data Discrepancy Resolution

## PROBLEM ANALYSIS

### Current Issues
1. **Data Discrepancy**: Frontend shows 82 trials but tooltip shows 88 users
   - Database shows: 87 trial events, 87 unique trial users, 88 tooltip users
   - **ROOT CAUSE**: user_product_metrics table has 88 users with conversion rates, but only 87 users actually started trials
   - This suggests a data integrity issue where some users have rate data without corresponding trial events

2. **Tooltip Formatting**: Currently shows "User 1 (US, APP_STORE)" but user wants:
   - Format: "U1" (short user labels)
   - Include: region, price bracket, country, store
   - Current backend data available: country, store, price_bucket, but region data exists only in mixpanel_user table

### Technical Findings
- Backend tooltip query: `get_user_details_for_tooltip()` in `analytics_query_service.py`
- Frontend tooltip component: `ConversionRateTooltip` in `DashboardGrid.js`
- Data sources: `user_product_metrics` joined with `mixpanel_user`
- Schema fields available: country, region, store, price_bucket, economic_tier

## TASK BREAKDOWN

### PHASE 1: Data Discrepancy Investigation & Resolution

#### Task 1.1: Deep Data Audit
- [x] COMPLETED: Identified discrepancy (88 tooltip users vs 87 trial users)
- [ ] **CRITICAL**: Investigate why user_product_metrics has more users than trial events
- [ ] Query users with rates but no trial events
- [ ] Determine if this is data corruption or valid business logic
- [ ] Document findings and recommend resolution

#### Task 1.2: Fix Data Integrity Issue
- [ ] If data corruption: Identify and remove invalid user_product_metrics records
- [ ] If valid business logic: Update tooltip query to match actual trial users
- [ ] Ensure consistency between frontend trial count and tooltip user count
- [ ] Add validation to prevent future discrepancies

### PHASE 2: Tooltip Enhancement

#### Task 2.1: Backend Data Enhancement
- [ ] Modify `get_user_details_for_tooltip()` query to include all required fields:
  - [ ] Join `mixpanel_user.region` (user-level region data)
  - [ ] Include `user_product_metrics.price_bucket` 
  - [ ] Include `mixpanel_user.economic_tier`
  - [ ] Ensure `user_product_metrics.country` and `store` are included
- [ ] Format price_bucket as currency in backend
- [ ] Add field validation and null handling

#### Task 2.2: Frontend Tooltip Formatting
- [ ] Update `ConversionRateTooltip` component in `DashboardGrid.js`:
  - [ ] Change user labels from "User {index + 1}" to "U{index + 1}"
  - [ ] Enhance user display format to include all fields:
    - Current: `User 1 (US, APP_STORE)`
    - New: `U1 - US • Victoria • $61.23 • APP_STORE • Tier1`
- [ ] Update modal display for click-to-expand functionality
- [ ] Ensure all new fields are properly displayed

#### Task 2.3: User Experience Enhancements
- [ ] Verify tooltip displays properly with enhanced data
- [ ] Test hover/click functionality with new format
- [ ] Ensure responsive design with longer text
- [ ] Add loading states for enhanced data

### PHASE 3: Testing & Validation

#### Task 3.1: Data Validation
- [ ] Test query performance with enhanced joins
- [ ] Verify all fields are properly populated
- [ ] Test edge cases (null values, missing data)
- [ ] Validate user count consistency

#### Task 3.2: Frontend Validation  
- [ ] Test tooltip display with various data scenarios
- [ ] Verify modal functionality works with enhanced data
- [ ] Test responsive behavior with longer text
- [ ] Cross-browser compatibility testing

#### Task 3.3: Integration Testing
- [ ] End-to-end testing of tooltip functionality
- [ ] Verify data discrepancy is resolved
- [ ] Performance testing with enhanced data
- [ ] User acceptance testing

## IMPLEMENTATION PRIORITY

1. **IMMEDIATE (Critical)**: Task 1.1 - Data Discrepancy Investigation
2. **HIGH**: Task 1.2 - Fix Data Integrity Issue  
3. **HIGH**: Task 2.1 - Backend Data Enhancement
4. **MEDIUM**: Task 2.2 - Frontend Tooltip Formatting
5. **MEDIUM**: Task 2.3 - User Experience Enhancements
6. **LOW**: Task 3.x - Testing & Validation

## EXPECTED OUTCOMES

### Fixed Data Discrepancy
- Frontend trial count matches tooltip user count
- Data integrity maintained across all queries
- Clear documentation of data logic

### Enhanced Tooltip Display
```
Current: User 1 (US, APP_STORE)
New:     U1 - US • Victoria • $61.23 • APP_STORE • Tier1
```

### Improved User Experience
- Concise but informative user labels
- All relevant user context displayed
- Consistent data across all views
- Fast and responsive tooltip interactions

## RISK MITIGATION

- **Data Loss**: Backup current data before any modifications
- **Performance**: Test query performance with enhanced joins
- **UI Breakage**: Implement gradual rollout with fallbacks
- **Browser Compatibility**: Test across all supported browsers

## ACCEPTANCE CRITERIA

1. ✅ Data discrepancy resolved (trial count = tooltip user count)
2. ✅ Tooltip shows "U1, U2, U3..." format
3. ✅ All fields displayed: country, region, price bracket, store, tier
4. ✅ Modal functionality works with enhanced data
5. ✅ No performance degradation
6. ✅ No UI regression issues 