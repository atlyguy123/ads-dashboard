/**
 * DASHBOARD COLUMNS CONFIGURATION
 * 
 * ⚠️  SINGLE SOURCE OF TRUTH ⚠️
 * This is the ONLY place where dashboard columns should be defined.
 * All other files should import from here.
 * 
 * 📋 NEED HELP? Read: src/config/Column README.md for complete instructions
 * 
 * Quick Steps:
 * 1. Add column definition here
 * 2. Add field to backend API response  
 * 3. Add formatting logic to DashboardGrid.js renderCellValue()
 * 4. Test everything works
 */

export const AVAILABLE_COLUMNS = [
  // User's required columns in exact order
  { key: 'name', label: 'Name', defaultVisible: true, alwaysVisible: true },
  { key: 'trials_combined', label: 'Trials', subtitle: '(Mixpanel | Meta)', defaultVisible: true },
  { key: 'trial_conversion_rate', label: 'Trial Conversion Rate', subtitle: '(Trials → Purchases)', defaultVisible: true },
  { key: 'avg_trial_refund_rate', label: 'Trial Refund Rate', subtitle: '(Refunded Trials)', defaultVisible: true },
  { key: 'purchases_combined', label: 'Purchases', subtitle: '(Mixpanel | Meta)', defaultVisible: true },
  { key: 'purchase_refund_rate', label: 'Purchase Refund Rate', subtitle: '(Refunded Purchases)', defaultVisible: true },
  { key: 'spend', label: 'Spend', defaultVisible: true },
  { key: 'estimated_revenue_adjusted', label: 'Estimated Revenue', subtitle: '(Adjusted)', defaultVisible: true },
  { key: 'profit', label: 'Profit', defaultVisible: true },
  { key: 'estimated_roas', label: 'ROAS', defaultVisible: true },
  { key: 'performance_impact_score', label: 'Performance Impact Score', defaultVisible: true },
  
  // All other columns - set to defaultVisible: false
  { key: 'campaign_name', label: 'Campaign', defaultVisible: false },
  { key: 'adset_name', label: 'Ad Set', defaultVisible: false },
  
  // Legacy individual columns (replaced by combined columns)
  { key: 'meta_trials_started', label: 'Trials (Meta)', defaultVisible: false },
  { key: 'mixpanel_trials_started', label: 'Trials (Mixpanel)', defaultVisible: false },
  { key: 'trial_accuracy_ratio', label: 'Trial Accuracy Ratio', defaultVisible: false },
  { key: 'meta_purchases', label: 'Purchases (Meta)', defaultVisible: false },
  { key: 'mixpanel_purchases', label: 'Purchases (Mixpanel)', defaultVisible: false },
  { key: 'purchase_accuracy_ratio', label: 'Purchase Accuracy Ratio', defaultVisible: false },
  { key: 'impressions', label: 'Impressions', defaultVisible: false },
  { key: 'clicks', label: 'Clicks', defaultVisible: false },
  { key: 'mixpanel_trials_ended', label: 'Trials Ended (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_trials_in_progress', label: 'Trials In Progress (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_refunds_usd', label: 'Actual Refunds (Events)', defaultVisible: false },
  { key: 'mixpanel_revenue_usd', label: 'Actual Revenue (Events)', defaultVisible: false },
  { key: 'mixpanel_conversions_net_refunds', label: 'Net Conversions (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_cost_per_trial', label: 'Cost per Trial (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_cost_per_purchase', label: 'Cost per Purchase (Mixpanel)', defaultVisible: false },
  { key: 'meta_cost_per_trial', label: 'Cost per Trial (Meta)', defaultVisible: false },
  { key: 'meta_cost_per_purchase', label: 'Cost per Purchase (Meta)', defaultVisible: false },
  { key: 'click_to_trial_rate', label: 'Click to Trial Rate', defaultVisible: false },
  { key: 'estimated_revenue_usd', label: 'Estimated Revenue (Base)', defaultVisible: false },
  { key: 'mixpanel_revenue_net', label: 'Net Actual Revenue', defaultVisible: false },
  
  // Comprehensive revenue fields from pre-computed data
  { key: 'actual_revenue_usd', label: 'Actual Revenue', subtitle: '(Completed Purchases)', defaultVisible: false },
  { key: 'actual_refunds_usd', label: 'Actual Refunds', subtitle: '(USD)', defaultVisible: false },
  { key: 'net_actual_revenue_usd', label: 'Net Revenue', subtitle: '(After Refunds)', defaultVisible: false },
  
  // Comprehensive conversion rate fields (estimated vs actual)
  { key: 'trial_conversion_rate_estimated', label: 'Trial Conversion Rate (Est.)', subtitle: '(Meta Estimate)', defaultVisible: false },
  { key: 'trial_refund_rate_estimated', label: 'Trial Refund Rate (Est.)', subtitle: '(Meta Estimate)', defaultVisible: false },
  { key: 'purchase_refund_rate_estimated', label: 'Purchase Refund Rate (Est.)', subtitle: '(Meta Estimate)', defaultVisible: false },
  
  // User lists (typically hidden but available for debugging)
  { key: 'trial_users_list', label: 'Trial User IDs', defaultVisible: false },
  { key: 'post_trial_user_ids', label: 'Post-Trial User IDs', defaultVisible: false },
  { key: 'converted_user_ids', label: 'Converted User IDs', defaultVisible: false },
  { key: 'trial_refund_user_ids', label: 'Trial Refund User IDs', defaultVisible: false },
  { key: 'purchase_user_ids', label: 'Purchase User IDs', defaultVisible: false },
  { key: 'purchase_refund_user_ids', label: 'Purchase Refund User IDs', defaultVisible: false },
  
  { key: 'segment_accuracy_average', label: 'Avg. Accuracy', defaultVisible: false }
];

/**
 * Helper functions for column management
 */
export const getColumnByKey = (key) => {
  return AVAILABLE_COLUMNS.find(col => col.key === key);
};

export const getDefaultVisibleColumns = () => {
  return AVAILABLE_COLUMNS.filter(col => col.defaultVisible);
};

export const getAllColumnKeys = () => {
  return AVAILABLE_COLUMNS.map(col => col.key);
};

/**
 * Validates that a column order array contains all available columns
 */
export const validateColumnOrder = (columnOrder) => {
  const allKeys = getAllColumnKeys();
  const missing = allKeys.filter(key => !columnOrder.includes(key));
  const extra = columnOrder.filter(key => !allKeys.includes(key));
  
  return {
    isValid: missing.length === 0 && extra.length === 0,
    missing,
    extra,
    expected: allKeys.length,
    actual: columnOrder.length
  };
};

/**
 * Auto-migrates column order to include new columns
 */
export const migrateColumnOrder = (savedColumnOrder) => {
  const allKeys = getAllColumnKeys();
  
  // Start with saved order
  const migrated = [...(savedColumnOrder || [])];
  
  // Add missing columns at the end
  allKeys.forEach(key => {
    if (!migrated.includes(key)) {
      migrated.push(key);
    }
  });
  
  // Remove obsolete columns
  return migrated.filter(key => allKeys.includes(key));
};

/**
 * Auto-migrates column visibility to include new columns
 * RESPECTS USER PREFERENCES - only adds defaults for truly NEW columns
 */
export const migrateColumnVisibility = (savedVisibility) => {
  const migrated = {};
  
  AVAILABLE_COLUMNS.forEach(col => {
    if (savedVisibility && savedVisibility.hasOwnProperty(col.key)) {
      // ALWAYS respect saved user preferences (never override)
      migrated[col.key] = savedVisibility[col.key];
    } else {
      // Only use defaults for genuinely NEW columns that user hasn't seen
      migrated[col.key] = col.defaultVisible || false;
    }
  });
  
  return migrated;
}; 