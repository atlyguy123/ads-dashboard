# 🎯 Sparkline Tooltip Fix Plan

## **Problem Summary**
The sparkline tooltip displays incorrect values compared to the main dashboard:
- **Trial Accuracy**: 60.5% (tooltip) vs 75.86% (dashboard) ❌
- **Rolling Revenue**: $887 (tooltip) vs $930 (dashboard) ❌  
- **Rolling Spend**: $1,687.94 (tooltip) vs $1,687.94 (dashboard) ✅

## **Root Cause Analysis**
1. **Time Period Fragmentation**: Dashboard uses user-selected date range, chart API uses hardcoded 20-day period
2. **Calculation Method Divergence**: Dashboard uses period-wide accuracy, chart API uses daily aggregation
3. **Data Source Architecture**: Different aggregation SQL queries and time windows

## **Solution Strategy: Option 3 - Update Chart API to Match Dashboard Logic**
Surgically modify the chart API to use identical calculation methods and date ranges as the main dashboard.

---

## **📋 DETAILED IMPLEMENTATION PLAN**

### **Phase 1: Diagnostic Verification** 
**Goal**: Confirm exact discrepancies and data sources

#### **Task 1.1**: Create Debug Script
- [ ] Create `debug_sparkline_detailed.py` 
- [ ] Add dashboard API call with exact same parameters as frontend
- [ ] Add chart API call with exact same parameters as sparkline
- [ ] Log both responses side-by-side for comparison
- [ ] Verify specific discrepancies match user report

#### **Task 1.2**: Analyze Date Range Handling
- [ ] Log dashboard date range parameters being used
- [ ] Log chart API date range calculation (hardcoded 20-day logic)
- [ ] Identify exact date range differences
- [ ] Document period overlap/differences

#### **Task 1.3**: Trace Accuracy Calculations
- [ ] Add debug logging to dashboard `_format_record()` accuracy calculation
- [ ] Add debug logging to chart `get_chart_data()` accuracy calculation  
- [ ] Compare calculation inputs (trial/meta trial counts)
- [ ] Document exact mathematical differences

---

### **Phase 2: Chart API Date Range Alignment**
**Goal**: Make chart API use exact same date range as dashboard

#### **Task 2.1**: Modify Chart API Parameters
- [ ] Update `/analytics/chart-data` endpoint to accept dashboard date range
- [ ] Modify `get_chart_data()` to use passed date range instead of hardcoded 20-day logic
- [ ] Ensure rolling calculations still work with variable date ranges
- [ ] Add validation that date range has sufficient data for rolling calculations

#### **Task 2.2**: Update Frontend Chart API Calls
- [ ] Modify `ROASSparkline.jsx` to pass dashboard date range to chart API
- [ ] Ensure `dashboardParams.start_date` and `dashboardParams.end_date` are used
- [ ] Add error handling for insufficient date range
- [ ] Test with various date range sizes

#### **Task 2.3**: Rolling Window Logic Adjustment
- [ ] Modify rolling calculation to work with any date range (not just 20 days)
- [ ] Ensure 3-day rolling window still functions correctly
- [ ] Handle edge cases where date range < 3 days
- [ ] Maintain backward compatibility

---

### **Phase 3: Accuracy Calculation Standardization**
**Goal**: Use identical accuracy calculation methods in both APIs

#### **Task 3.1**: Replace Period-Wide Accuracy in Chart API
- [ ] Remove `overall_accuracy_ratio` calculation from `get_chart_data()`
- [ ] Replace with per-day modular calculator calls
- [ ] Use `AccuracyCalculators.calculate_trial_accuracy_ratio()` for each day
- [ ] Ensure same decimal/percentage conversion as dashboard

#### **Task 3.2**: Standardize Rolling Revenue Calculation
- [ ] Update rolling revenue calculation to use period-wide accuracy (like dashboard)
- [ ] Replace daily accuracy adjustment with aggregate accuracy adjustment
- [ ] Use exact same `RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment()` logic
- [ ] Ensure mathematical consistency with dashboard

#### **Task 3.3**: Unify Calculator System Usage
- [ ] Ensure chart API uses identical `CalculationInput` structures as dashboard
- [ ] Verify all calculator function calls match dashboard implementation
- [ ] Add validation that chart calculations match dashboard totals
- [ ] Test accuracy ratio edge cases (zero values, high ratios)

---

### **Phase 4: Data Consistency Validation**
**Goal**: Ensure chart API returns data consistent with dashboard

#### **Task 4.1**: Add Cross-Validation
- [ ] Create validation function that compares chart API totals with dashboard totals
- [ ] Implement automatic alerts when discrepancies exceed threshold
- [ ] Add debug endpoints that return both calculation sets
- [ ] Log discrepancies for monitoring

#### **Task 4.2**: Integration Testing
- [ ] Test with multiple date ranges (3 days, 14 days, 30 days)
- [ ] Test with different entity types (campaigns, adsets, ads)
- [ ] Test with breakdown entities (country breakdowns)
- [ ] Verify edge cases (zero spend, zero conversions, high accuracy ratios)

#### **Task 4.3**: Frontend Validation
- [ ] Add frontend validation that compares tooltip values with dashboard values
- [ ] Implement warning system for significant discrepancies
- [ ] Add debugging mode that shows calculation details
- [ ] Test across different campaigns and time periods

---

### **Phase 5: Performance & Optimization**
**Goal**: Ensure fix doesn't impact performance

#### **Task 5.1**: Performance Monitoring
- [ ] Benchmark chart API response times before and after changes
- [ ] Monitor database query performance
- [ ] Ensure no significant regression in sparkline load times
- [ ] Optimize any new queries if needed

#### **Task 5.2**: Caching Strategy
- [ ] Evaluate if chart data caching strategy needs updates
- [ ] Ensure cached data consistency with new calculation methods
- [ ] Add cache invalidation for date range changes
- [ ] Test cache hit rates and performance

---

### **Phase 6: Documentation & Monitoring**
**Goal**: Document changes and establish monitoring

#### **Task 6.1**: Technical Documentation
- [ ] Update API documentation for chart endpoint changes
- [ ] Document new date range parameter requirements
- [ ] Add calculation method documentation
- [ ] Create troubleshooting guide for discrepancies

#### **Task 6.2**: Monitoring & Alerts
- [ ] Add monitoring for accuracy calculation discrepancies
- [ ] Create alerts for significant chart vs dashboard differences
- [ ] Log calculation performance metrics
- [ ] Set up automated testing for key scenarios

---

## **🔧 IMPLEMENTATION CHECKLIST**

### **Critical Success Metrics**
- [ ] **Trial Accuracy Match**: Tooltip shows 75.86% (matches dashboard)
- [ ] **Revenue Match**: Tooltip shows $930 (matches dashboard) 
- [ ] **Spend Consistency**: Tooltip continues to show $1,687.94
- [ ] **No Performance Regression**: Chart load times remain under 2 seconds
- [ ] **Cross-Campaign Validation**: Fix works across all campaigns and date ranges

### **Validation Tests**
- [ ] **Savannah Old Sale Campaign**: Original problem case must be fixed
- [ ] **Multiple Date Ranges**: 3-day, 14-day, 30-day periods
- [ ] **Edge Cases**: Zero conversions, high accuracy ratios, breakdown entities
- [ ] **Different Entity Types**: Campaigns, adsets, ads
- [ ] **Performance**: Response times within acceptable limits

### **Rollback Plan**
- [ ] **Git Branch**: Create feature branch for all changes
- [ ] **Database Backup**: Backup before any schema changes
- [ ] **Rollback Script**: Quick revert process if issues arise
- [ ] **Monitoring**: Watch for errors after deployment

---

## **📊 EXPECTED OUTCOME**

After implementation:
1. **Sparkline tooltip accuracy ratio**: 75.86% (matches dashboard)
2. **Sparkline tooltip rolling revenue**: $930 (matches dashboard)
3. **Sparkline tooltip rolling spend**: $1,687.94 (unchanged)
4. **Mathematical consistency**: All calculations use identical logic
5. **Performance maintained**: No significant response time regression
6. **Future-proof**: Chart API automatically stays in sync with dashboard changes

---

## **⚠️ CRITICAL NOTES**

1. **Modular Calculator Dependency**: Ensure chart API uses exact same calculator functions as dashboard
2. **Date Range Validation**: Chart API must validate sufficient data for rolling calculations
3. **Backward Compatibility**: Maintain support for existing chart API calls during transition
4. **Testing Rigor**: Test extensively with real campaign data before production deployment
5. **Monitoring Setup**: Establish ongoing monitoring to catch future discrepancies early

---

## **🎯 SUCCESS DEFINITION**

**The fix is complete when**:
- User can hover over sparkline and see identical accuracy ratios as dashboard
- Rolling revenue in tooltip matches dashboard estimated revenue
- No mathematical discrepancies between any tooltip and dashboard values
- Performance remains acceptable across all use cases
- Automated monitoring prevents future regressions 