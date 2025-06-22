"""
Cost Calculators

This module handles cost per action calculations:
- Cost per trial (Meta and Mixpanel)
- Cost per purchase (Meta and Mixpanel)
"""

from .base_calculators import BaseCalculator, CalculationInput
import logging

logger = logging.getLogger(__name__)


class CostCalculators(BaseCalculator):
    """Cost per action calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_mixpanel_cost_per_trial(calc_input: CalculationInput) -> float:
        """
        Calculate cost per trial using Mixpanel trial data.
        
        Formula: spend / mixpanel_trials_started
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Cost per trial in USD
        """
        if not CostCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        mixpanel_trials = calc_input.mixpanel_trials_started
        
        return CostCalculators.safe_divide(
            numerator=spend,
            denominator=mixpanel_trials,
            default=0.0,
            decimal_places=2
        )
    
    @staticmethod
    def calculate_mixpanel_cost_per_purchase(calc_input: CalculationInput) -> float:
        """
        Calculate cost per purchase using Mixpanel purchase data.
        
        Formula: spend / mixpanel_purchases
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Cost per purchase in USD
        """
        if not CostCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        mixpanel_purchases = calc_input.mixpanel_purchases
        
        return CostCalculators.safe_divide(
            numerator=spend,
            denominator=mixpanel_purchases,
            default=0.0,
            decimal_places=2
        )
    
    @staticmethod
    def calculate_meta_cost_per_trial(calc_input: CalculationInput) -> float:
        """
        Calculate cost per trial using Meta trial data.
        
        Formula: spend / meta_trials_started
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Cost per trial in USD
        """
        if not CostCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        meta_trials = calc_input.meta_trials_started
        
        return CostCalculators.safe_divide(
            numerator=spend,
            denominator=meta_trials,
            default=0.0,
            decimal_places=2
        )
    
    @staticmethod
    def calculate_meta_cost_per_purchase(calc_input: CalculationInput) -> float:
        """
        Calculate cost per purchase using Meta purchase data.
        
        Formula: spend / meta_purchases
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Cost per purchase in USD
        """
        if not CostCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        meta_purchases = calc_input.meta_purchases
        
        return CostCalculators.safe_divide(
            numerator=spend,
            denominator=meta_purchases,
            default=0.0,
            decimal_places=2
        ) 