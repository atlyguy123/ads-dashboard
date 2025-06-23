"""
Revenue Calculators

This module handles all revenue-related calculations with CLEAR separation between:

ACTUAL REVENUE (from Mixpanel events):
- Mixpanel revenue from actual purchase events (with 8-day trial conversion logic)
- Mixpanel refunds from actual cancellation events
- Net Mixpanel revenue (actual revenue - actual refunds)

ESTIMATED REVENUE (from user lifecycle predictions):
- Estimated revenue from user_product_metrics based on conversion probabilities
- Profit calculations (estimated revenue - spend)

This separation ensures users understand the difference between actual observed 
revenue and predicted revenue based on user lifecycle stages.
"""

from .base_calculators import BaseCalculator, CalculationInput
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class RevenueCalculators(BaseCalculator):
    """Revenue calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_mixpanel_revenue_usd(calc_input: CalculationInput) -> float:
        """
        Get ACTUAL Mixpanel revenue from real purchase events (NOT estimated).
        
        This is ACTUAL OBSERVED REVENUE from Mixpanel purchase events with 8-day trial logic:
        - Initial Purchase events: event_time BETWEEN start_date AND end_date
        - Trial Converted/Renewal events: event_time BETWEEN (start_date + 8 days) AND (end_date + 8 days)
        - Source: mixpanel_event.revenue_usd from actual purchase events
        
        This is DIFFERENT from estimated_revenue_usd which is based on predictions.
        This is ACTUAL money received that Mixpanel tracked.
        
        Args:
            calc_input: Contains raw_record with 'mixpanel_revenue_usd' field (now actual revenue)
            
        Returns:
            float: ACTUAL Mixpanel revenue in USD from events (rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Get pre-calculated value from database query (already includes 8-day logic)
        raw_value = calc_input.raw_mixpanel_revenue_usd
        return RevenueCalculators.safe_round(raw_value)
    
    @staticmethod
    def calculate_mixpanel_refunds_usd(calc_input: CalculationInput) -> float:
        """
        Get ACTUAL Mixpanel refunds from real cancellation events (NOT estimated).
        
        This is ACTUAL OBSERVED REFUNDS from Mixpanel cancellation events:
        - RC Cancellation events: event_time BETWEEN start_date AND end_date
        - Source: ABS(mixpanel_event.revenue_usd) for actual cancellation events
        
        This represents ACTUAL refunds that occurred and were tracked by Mixpanel.
        
        Args:
            calc_input: Contains raw_record with 'mixpanel_refunds_usd' field (now actual refunds)
            
        Returns:
            float: ACTUAL Mixpanel refunds in USD from events (positive value, rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Get pre-calculated value from database query
        raw_value = calc_input.raw_mixpanel_refunds_usd
        return RevenueCalculators.safe_round(raw_value)
    
    @staticmethod
    def calculate_mixpanel_revenue_net(calc_input: CalculationInput) -> float:
        """
        Calculate net ACTUAL Mixpanel revenue (actual revenue - actual refunds).
        
        This is the TRUE NET REVENUE after accounting for actual refunds/cancellations.
        This uses ACTUAL revenue from events, not estimated revenue.
        
        Formula: actual_mixpanel_revenue - actual_mixpanel_refunds
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Net ACTUAL Mixpanel revenue in USD (can be negative if refunds exceed revenue)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        revenue = RevenueCalculators.calculate_mixpanel_revenue_usd(calc_input)
        refunds = RevenueCalculators.calculate_mixpanel_refunds_usd(calc_input)
        
        return RevenueCalculators.safe_subtract(revenue, refunds)
    
    @staticmethod 
    def calculate_estimated_revenue_usd(calc_input: CalculationInput) -> float:
        """
        Get ESTIMATED revenue from user lifecycle predictions (NOT actual events).
        
        This is PREDICTED REVENUE based on user lifecycle stage and conversion probabilities:
        - Source: SUM(user_product_metrics.current_value) 
        - WHERE credited_date BETWEEN start_date AND end_date
        - Based on: Conversion probability models and user lifecycle stages
        
        This is DIFFERENT from mixpanel_revenue_usd which is actual observed revenue.
        This represents what we EXPECT to earn based on current user states.
        
        Args:
            calc_input: Contains raw_record with 'estimated_revenue_usd' field
            
        Returns:
            float: ESTIMATED revenue in USD based on predictions (rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Get pre-calculated value from database query (already aggregated by credited_date)
        raw_value = calc_input.raw_estimated_revenue_usd
        return RevenueCalculators.safe_round(raw_value)
    
    @staticmethod
    def calculate_profit(calc_input: CalculationInput) -> float:
        """
        Calculate profit (accuracy-adjusted estimated revenue - spend).
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Profit in USD (can be negative)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Use accuracy-adjusted estimated revenue for profit calculation
        estimated_revenue = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(calc_input)
        spend = calc_input.spend
        
        return RevenueCalculators.safe_subtract(estimated_revenue, spend)
    
    @staticmethod
    def calculate_estimated_revenue_with_accuracy_adjustment(calc_input: CalculationInput) -> float:
        """
        Calculate estimated revenue with accuracy ratio adjustment based on event priority.
        
        Logic:
        1. Get base estimated revenue from user_product_metrics
        2. Determine event priority (trials vs purchases dominance)
        3. Apply appropriate accuracy ratio adjustment:
           - If trials dominant: use trial accuracy ratio
           - If purchases dominant: use purchase accuracy ratio
           - If equal: default to trial accuracy ratio
        4. Adjust revenue by dividing by (accuracy_ratio / 100) to compensate for Meta/Mixpanel dropoff
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Accuracy-adjusted estimated revenue in USD (rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Import here to avoid circular imports
        from .accuracy_calculators import AccuracyCalculators
        
        # Get base estimated revenue
        estimated_revenue = RevenueCalculators.calculate_estimated_revenue_usd(calc_input)
        
        # Determine event priority based on which metric is dominant
        mixpanel_trials = calc_input.mixpanel_trials_started
        mixpanel_purchases = calc_input.mixpanel_purchases
        
        # Determine which accuracy ratio to use based on event priority
        if mixpanel_trials == 0 and mixpanel_purchases == 0:
            # Default to trial accuracy when both are zero
            accuracy_ratio = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
        elif mixpanel_trials > mixpanel_purchases:
            # Trials are dominant - use trial accuracy ratio
            accuracy_ratio = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
        elif mixpanel_purchases > mixpanel_trials:
            # Purchases are dominant - use purchase accuracy ratio
            accuracy_ratio = AccuracyCalculators.calculate_purchase_accuracy_ratio(calc_input)
        else:
            # Equal or tie - default to trial accuracy ratio
            accuracy_ratio = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
        
        # Apply accuracy ratio adjustment if available and not 100%
        if accuracy_ratio > 0 and accuracy_ratio != 100.0:
            # Adjust estimated revenue by dividing by accuracy ratio (to account for Meta/Mixpanel dropoff)
            # This compensates for the difference between Meta and Mixpanel event counts
            adjusted_revenue = estimated_revenue / (accuracy_ratio / 100)
            return RevenueCalculators.safe_round(adjusted_revenue)
        else:
            # No adjustment needed if accuracy ratio is 100% or unavailable
            return estimated_revenue 