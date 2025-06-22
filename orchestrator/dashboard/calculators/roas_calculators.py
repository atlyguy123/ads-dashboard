"""
ROAS Calculators

This module handles ROAS (Return on Ad Spend) calculations with accuracy ratio adjustments.
"""

from .base_calculators import BaseCalculator, CalculationInput
import logging

logger = logging.getLogger(__name__)


class ROASCalculators(BaseCalculator):
    """ROAS calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_estimated_roas(calc_input: CalculationInput) -> float:
        """
        Calculate ROAS with trial accuracy ratio adjustment.
        
        Logic:
        1. Get estimated revenue from user_product_metrics
        2. Get trial accuracy ratio (Mixpanel trials / Meta trials)
        3. If accuracy ratio exists and != 100%:
           - Adjust revenue by dividing by (accuracy_ratio / 100)
           - ROAS = adjusted_revenue / spend
        4. If no accuracy ratio or spend = 0:
           - ROAS = estimated_revenue / spend
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: ROAS value (revenue / spend with accuracy adjustment)
        """
        if not ROASCalculators.validate_input(calc_input):
            return 0.0
            
        # Import here to avoid circular imports
        from .revenue_calculators import RevenueCalculators
        from .accuracy_calculators import AccuracyCalculators
        
        estimated_revenue = RevenueCalculators.calculate_estimated_revenue_usd(calc_input)
        spend = calc_input.spend
        trial_accuracy_ratio = AccuracyCalculators.calculate_trial_accuracy_ratio(calc_input)
        
        # Can't calculate ROAS without spend
        if spend <= 0:
            return 0.0
        
        # Apply accuracy ratio adjustment if available and not 100%
        if trial_accuracy_ratio > 0 and trial_accuracy_ratio != 100.0:
            # Adjust estimated revenue by dividing by accuracy ratio (to account for Meta/Mixpanel dropoff)
            # This compensates for the difference between Meta and Mixpanel trial counts
            adjusted_revenue = estimated_revenue / (trial_accuracy_ratio / 100)
            roas = adjusted_revenue / spend
        else:
            # Fallback to standard calculation if no trial accuracy ratio
            roas = estimated_revenue / spend
        
        return ROASCalculators.safe_round(roas, 2) 