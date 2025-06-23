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
        Calculate ROAS with accuracy ratio adjustment based on event priority.
        
        Logic:
        1. Get accuracy-adjusted estimated revenue (with event priority logic)
        2. ROAS = adjusted_revenue / spend
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: ROAS value (accuracy-adjusted revenue / spend)
        """
        if not ROASCalculators.validate_input(calc_input):
            return 0.0
            
        # Import here to avoid circular imports
        from .revenue_calculators import RevenueCalculators
        
        # Use the new accuracy-adjusted estimated revenue method
        adjusted_revenue = RevenueCalculators.calculate_estimated_revenue_with_accuracy_adjustment(calc_input)
        spend = calc_input.spend
        
        # Can't calculate ROAS without spend
        if spend <= 0:
            return 0.0
        
        roas = adjusted_revenue / spend
        return ROASCalculators.safe_round(roas, 2) 