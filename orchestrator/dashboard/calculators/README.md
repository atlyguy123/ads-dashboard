# Dashboard Calculators System

## 🎯 Overview

This folder contains a **modular calculation system** for all dashboard metrics. Each metric has its own dedicated function, ensuring:

- **Single Responsibility**: Each function calculates exactly one metric
- **Testability**: Easy to unit test individual calculations  
- **Maintainability**: Clear separation of concerns
- **Reliability**: Consistent error handling and data validation

## 📁 File Organization

### **Base System**
- **`__init__.py`** - Module imports and system overview
- **`base_calculators.py`** - Common utilities and data structures

### **Calculation Modules**
- **`revenue_calculators.py`** - Revenue, refunds, profit calculations
- **`roas_calculators.py`** - ROAS with accuracy ratio adjustments
- **`accuracy_calculators.py`** - Trial/purchase accuracy ratios
- **`cost_calculators.py`** - Cost per trial/purchase metrics
- **`rate_calculators.py`** - Click-to-trial and conversion rates
- **`database_calculators.py`** - Pass-through for pre-calculated values

---

## 🔧 Implementation Status

### ✅ Phase 1: Structure Created
- [x] Folder structure created
- [x] Base classes and utilities implemented
- [x] All calculator modules with placeholder functions
- [x] Comprehensive documentation

### ✅ Phase 2: Integration Complete
- [x] Update `_format_record()` to use calculator functions
- [x] Update Mixpanel-only functions to use calculators  
- [x] Implement 8-day logic for Mixpanel revenue in all functions
- [x] Test all calculations work correctly
- [x] Error handling and input validation implemented

### ✅ Phase 3: Revenue Clarity Fix Complete
- [x] **FIXED CRITICAL ISSUE**: Separated actual vs estimated revenue
- [x] Updated analytics service to use actual Mixpanel events for `mixpanel_revenue_usd`
- [x] Updated frontend labels to be crystal clear:
  - "Actual Revenue (Events)" vs "Estimated Revenue (Predictions)"
  - "Net Actual Revenue" vs "Actual Refunds (Events)"
- [x] Updated all documentation to reflect the separation
- [x] No more confusion between "Revenue (Mixpanel)" and "Mixpanel Revenue"

---

## 🎯 **Revenue Metrics - What Each One Means**

### **For Dashboard Users (Non-Technical)**

| **Dashboard Column** | **What It Represents** | **Source** |
|---------------------|-------------------------|------------|
| **"Actual Revenue (Events)"** | Real money you received from customers who purchased | 'RC Initial purchase' + 'RC Trial converted' events |
| **"Actual Refunds (Events)"** | Real money you refunded to customers who cancelled | Mixpanel cancellation events |
| **"Net Actual Revenue"** | Real profit after refunds (Actual Revenue - Actual Refunds) | Calculated |
| **"Estimated Revenue (Predictions)"** | Expected money based on current user behaviors | AI predictions |

### **Key Difference:**
- **ACTUAL** = Money that has already changed hands ✅
- **ESTIMATED** = Money we predict will change hands based on user lifecycle 🔮

---

## 📊 Calculator Functions Reference

### **Revenue Calculators** (`revenue_calculators.py`)

**🚨 CRITICAL: Clear Separation of Actual vs Estimated Revenue**

| Function | Purpose | Source Data | Type |
|----------|---------|-------------|------|
| `calculate_mixpanel_revenue_usd()` | **ACTUAL** revenue from purchase events | `mixpanel_event.revenue_usd` from 'RC Initial purchase', 'RC Trial converted' | **ACTUAL** |
| `calculate_mixpanel_refunds_usd()` | **ACTUAL** refunds from cancellation events | `mixpanel_event.revenue_usd` (cancellations) | **ACTUAL** |
| `calculate_mixpanel_revenue_net()` | **ACTUAL** net revenue (actual revenue - actual refunds) | Calculated from actual values above | **ACTUAL** |
| `calculate_estimated_revenue_usd()` | **ESTIMATED** revenue from predictions | `user_product_metrics.current_value` | **ESTIMATED** |
| `calculate_profit()` | Profit calculation | `estimated_revenue - spend` | **CALCULATED** |

**Key Differences:**
- **ACTUAL**: Real money from events that actually happened
- **ESTIMATED**: Predicted money based on user lifecycle models
- **Dashboard Labels**: 
  - "Actual Revenue (Events)" = Real Mixpanel purchase events
  - "Estimated Revenue (Predictions)" = Lifecycle-based predictions
  - "Net Actual Revenue" = Actual revenue minus actual refunds

### **ROAS Calculators** (`roas_calculators.py`)

| Function | Purpose | Logic |
|----------|---------|-------|
| `calculate_estimated_roas()` | ROAS with accuracy adjustment | `adjusted_revenue / spend` |

### **Accuracy Calculators** (`accuracy_calculators.py`)

| Function | Purpose | Formula |
|----------|---------|---------|
| `calculate_trial_accuracy_ratio()` | Trial tracking accuracy | `(mixpanel_trials / meta_trials) * 100` |
| `calculate_purchase_accuracy_ratio()` | Purchase tracking accuracy | `(mixpanel_purchases / meta_purchases) * 100` |

### **Cost Calculators** (`cost_calculators.py`)

| Function | Purpose | Formula |
|----------|---------|---------|
| `calculate_mixpanel_cost_per_trial()` | Cost per trial (Mixpanel) | `spend / mixpanel_trials` |
| `calculate_mixpanel_cost_per_purchase()` | Cost per purchase (Mixpanel) | `spend / mixpanel_purchases` |
| `calculate_meta_cost_per_trial()` | Cost per trial (Meta) | `spend / meta_trials` |
| `calculate_meta_cost_per_purchase()` | Cost per purchase (Meta) | `spend / meta_purchases` |

### **Rate Calculators** (`rate_calculators.py`)

| Function | Purpose | Formula |
|----------|---------|---------|
| `calculate_click_to_trial_rate()` | Click to trial conversion | `(mixpanel_trials / clicks) * 100` |

### **Database Calculators** (`database_calculators.py`)

| Function | Purpose | Source |
|----------|---------|--------|
| `calculate_trial_conversion_rate()` | Trial conversion rate | Database (pre-calculated) |
| `calculate_trial_to_purchase_rate()` | Trial to purchase rate | Database (pre-calculated) |
| `calculate_avg_trial_refund_rate()` | Average trial refund rate | Database (pre-calculated) |
| `calculate_purchase_refund_rate()` | Purchase refund rate | Database (pre-calculated) |

---

## 🔄 Integration Plan

### **Step 1: Update `_format_record()` Function**

Replace inline calculations with calculator calls:

```python
# OLD WAY (inline calculation)
formatted['trial_accuracy_ratio'] = round((mixpanel_trials / meta_trials) * 100, 2) if meta_trials > 0 else 0.0

# NEW WAY (using calculator)
from dashboard.calculators import CalculationInput, AccuracyCalculators

calc_input = CalculationInput(raw_record=record)
formatted['trial_accuracy_ratio'] = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
```

### **Step 2: Update Mixpanel-Only Functions**

Replace manual calculations in:
- `_get_mixpanel_campaign_data()`
- `_get_mixpanel_adset_data()`
- `_get_mixpanel_ad_data()`

### **Step 3: Testing**

Verify all calculations produce identical results to current implementation.

---

## 💡 Usage Examples

### **Basic Usage**
```python
from dashboard.calculators import CalculationInput, RevenueCalculators, ROASCalculators

# Create input from raw record
calc_input = CalculationInput(raw_record=record_dict)

# Calculate individual metrics
revenue = RevenueCalculators.calculate_mixpanel_revenue_usd(calc_input)
profit = RevenueCalculators.calculate_profit(calc_input)
roas = ROASCalculators.calculate_estimated_roas(calc_input)
```

### **Complete Record Processing**
```python
def process_record_with_calculators(raw_record):
    calc_input = CalculationInput(raw_record=raw_record)
    
    return {
        # Revenue metrics
        'mixpanel_revenue_usd': RevenueCalculators.calculate_mixpanel_revenue_usd(calc_input),
        'mixpanel_revenue_net': RevenueCalculators.calculate_mixpanel_revenue_net(calc_input),
        'estimated_revenue_usd': RevenueCalculators.calculate_estimated_revenue_usd(calc_input),
        'profit': RevenueCalculators.calculate_profit(calc_input),
        
        # ROAS
        'estimated_roas': ROASCalculators.calculate_estimated_roas(calc_input),
        
        # Accuracy ratios
        'trial_accuracy_ratio': AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input),
        'purchase_accuracy_ratio': AccuracyCalculators.calculate_purchase_accuracy_ratio(calc_input),
        
        # Cost metrics  
        'mixpanel_cost_per_trial': CostCalculators.calculate_mixpanel_cost_per_trial(calc_input),
        'mixpanel_cost_per_purchase': CostCalculators.calculate_mixpanel_cost_per_purchase(calc_input),
        
        # Rates
        'click_to_trial_rate': RateCalculators.calculate_click_to_trial_rate(calc_input),
        
        # Database pass-through
        'trial_conversion_rate': DatabaseCalculators.calculate_trial_conversion_rate(calc_input),
    }
```

---

## 🚨 Critical Implementation Notes

### **Mixpanel Revenue 8-Day Logic**
The `calculate_mixpanel_revenue_usd()` function must implement:
- **Initial Purchase**: `event_time BETWEEN start_date AND end_date`
- **Trial Converted**: `event_time BETWEEN (start_date + 8 days) AND (end_date + 8 days)`

### **Database vs Calculation Sources**
- **Estimated Revenue**: From `user_product_metrics.current_value` (by credited_date)
- **Mixpanel Revenue**: From `mixpanel_event.revenue_usd` (with 8-day logic)
- **Conversion Rates**: Pre-calculated in database (pass-through)

### **Error Handling**
- All functions use `safe_divide()` and `safe_percentage()` utilities
- Invalid inputs return 0.0 with logging
- Input validation on all functions