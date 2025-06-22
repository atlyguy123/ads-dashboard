"""
Revenue Calculators

This module handles all revenue-related calculations:
- Mixpanel revenue from events (with 8-day trial conversion logic)
- Mixpanel refunds from cancellation events
- Net Mixpanel revenue (revenue - refunds)
- Estimated revenue from user_product_metrics
- Profit calculations (revenue - spend)
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
        Get Mixpanel revenue from pre-calculated database query results.
        
        This value is calculated by the analytics service using 8-day logic:
        - Initial Purchase events: event_time BETWEEN start_date AND end_date
        - Trial Converted/Renewal events: event_time BETWEEN (start_date + 8 days) AND (end_date + 8 days)
        - Uses: mixpanel_event.revenue_usd from matching events
        
        Args:
            calc_input: Contains raw_record with 'mixpanel_revenue_usd' field
            
        Returns:
            float: Total Mixpanel revenue in USD (rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Get pre-calculated value from database query (already includes 8-day logic)
        raw_value = calc_input.raw_mixpanel_revenue_usd
        return RevenueCalculators.safe_round(raw_value)
    
    @staticmethod
    def calculate_mixpanel_refunds_usd(calc_input: CalculationInput) -> float:
        """
        Get Mixpanel refunds from pre-calculated database query results.
        
        This value is calculated by the analytics service from:
        - RC Cancellation events: event_time BETWEEN start_date AND end_date
        - Uses: ABS(mixpanel_event.revenue_usd) for cancellation events
        
        Args:
            calc_input: Contains raw_record with 'mixpanel_refunds_usd' field
            
        Returns:
            float: Total Mixpanel refunds in USD (positive value, rounded to 2 decimal places)
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        # Get pre-calculated value from database query
        raw_value = calc_input.raw_mixpanel_refunds_usd
        return RevenueCalculators.safe_round(raw_value)
    
    @staticmethod
    def calculate_mixpanel_revenue_net(calc_input: CalculationInput) -> float:
        """
        Calculate net Mixpanel revenue (revenue - refunds).
        
        This is the true revenue after accounting for refunds/cancellations.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Net Mixpanel revenue in USD
        """
        if not RevenueCalculators.validate_input(calc_input):
            return 0.0
            
        revenue = RevenueCalculators.calculate_mixpanel_revenue_usd(calc_input)
        refunds = RevenueCalculators.calculate_mixpanel_refunds_usd(calc_input)
        
        return RevenueCalculators.safe_subtract(revenue, refunds)
    
    @staticmethod 
    def calculate_estimated_revenue_usd(calc_input: CalculationInput) -> float:
        """
        Get estimated revenue from pre-calculated database query results.
        
        This value is calculated by the analytics service from:
        - SUM(user_product_metrics.current_value) 
        - WHERE credited_date BETWEEN start_date AND end_date
        - Uses: user_product_metrics table for value estimations
        
        Args:
            calc_input: Contains raw_record with 'estimated_revenue_usd' field
            
        Returns:
            float: Estimated revenue in USD (rounded to 2 decimal places)
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