"""
Rate Calculators

This module handles rate calculations:
- Click to trial rate
- Other conversion rates
"""

from .base_calculators import BaseCalculator, CalculationInput
import logging

logger = logging.getLogger(__name__)


class RateCalculators(BaseCalculator):
    """Rate calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_click_to_trial_rate(calc_input: CalculationInput) -> float:
        """
        Calculate click to trial rate (Mixpanel trials / clicks * 100).
        
        This measures how many clicks result in trial starts.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Click to trial rate as percentage (0-100)
        """
        if not RateCalculators.validate_input(calc_input):
            return 0.0
            
        mixpanel_trials = calc_input.mixpanel_trials_started
        clicks = calc_input.clicks
        
        return RateCalculators.safe_percentage(
            numerator=mixpanel_trials,
            denominator=clicks,
            default=0.0,
            decimal_places=2
        ) 