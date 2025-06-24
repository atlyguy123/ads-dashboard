"""
Dashboard Calculators Module

This module contains all calculation logic for dashboard metrics, organized into logical categories.
Each calculator is standalone and handles specific types of calculations.

=== CALCULATOR ORGANIZATION ===

📊 BASE_CALCULATORS.PY
- CalculationInput: Standardized input data structure
- BaseCalculator: Common utilities (safe_divide, safe_percentage, etc.)

💰 REVENUE_CALCULATORS.PY  
- calculate_mixpanel_revenue_usd: Revenue from mixpanel_event with 8-day trial logic
- calculate_mixpanel_refunds_usd: Refunds from mixpanel_event cancellation events
- calculate_mixpanel_revenue_net: Net revenue (revenue - refunds)
- calculate_estimated_revenue_usd: Revenue from user_product_metrics by credited_date
- calculate_profit: Estimated revenue minus spend

📈 ROAS_CALCULATORS.PY
- calculate_estimated_roas: ROAS with trial accuracy ratio adjustment
- calculate_performance_impact_score: Performance Impact Score (spend × ROAS²)

🎯 ACCURACY_CALCULATORS.PY
- calculate_trial_accuracy_ratio: (Mixpanel trials / Meta trials) * 100
- calculate_purchase_accuracy_ratio: (Mixpanel purchases / Meta purchases) * 100

💸 COST_CALCULATORS.PY
- calculate_mixpanel_cost_per_trial: Spend / Mixpanel trials
- calculate_mixpanel_cost_per_purchase: Spend / Mixpanel purchases  
- calculate_meta_cost_per_trial: Spend / Meta trials
- calculate_meta_cost_per_purchase: Spend / Meta purchases

📊 RATE_CALCULATORS.PY
- calculate_click_to_trial_rate: (Mixpanel trials / clicks) * 100

🗄️ DATABASE_CALCULATORS.PY
- calculate_trial_conversion_rate: Pass-through from database
- calculate_trial_refund_rate: Pass-through from database
- calculate_purchase_refund_rate: Pass-through from database

=== USAGE ===

from dashboard.calculators import CalculationInput
from dashboard.calculators.revenue_calculators import RevenueCalculators
from dashboard.calculators.roas_calculators import ROASCalculators

# Create input
calc_input = CalculationInput(raw_record=record_dict)

# Calculate values
revenue = RevenueCalculators.calculate_mixpanel_revenue_usd(calc_input)
roas = ROASCalculators.calculate_estimated_roas(calc_input)

=== DESIGN PRINCIPLES ===

✅ Single Responsibility: Each function calculates exactly one metric
✅ Standalone: No dependencies between calculation functions
✅ Testable: Easy to unit test individual calculations
✅ Type Safe: Clear input/output types
✅ Error Handling: Safe division and null handling
✅ Documentation: Clear docstrings explaining the calculation logic
"""

# Import all calculator classes for easy access
from .base_calculators import CalculationInput, BaseCalculator
from .revenue_calculators import RevenueCalculators
from .roas_calculators import ROASCalculators  
from .accuracy_calculators import AccuracyCalculators
from .cost_calculators import CostCalculators
from .rate_calculators import RateCalculators
from .database_calculators import DatabaseCalculators

__all__ = [
    'CalculationInput',
    'BaseCalculator', 
    'RevenueCalculators',
    'ROASCalculators',
    'AccuracyCalculators', 
    'CostCalculators',
    'RateCalculators',
    'DatabaseCalculators'
] 