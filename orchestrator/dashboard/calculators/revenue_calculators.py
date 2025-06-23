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
        Calculate profit (estimated revenue - spend).
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Profit in USD (can be negative)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        estimated_revenue = RevenueCalculators.calculate_estimated_revenue_usd(calc_input)
        spend = calc_input.spend
        
        return RevenueCalculators.safe_subtract(estimated_revenue, spend) 