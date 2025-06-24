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
    
    @staticmethod
    def calculate_performance_impact_score(calc_input: CalculationInput) -> float:
        """
        Calculate Performance Impact Score (spend × ROAS²).
        
        This metric prioritizes campaigns with both high efficiency (ROAS) and meaningful scale (spend).
        The ROAS is squared to exponentially reward exceptional performance while the spend component
        ensures campaigns have meaningful scale worth optimizing.
        
        Formula: spend × ROAS²
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Performance Impact Score (spend × ROAS²)
        """
        if not ROASCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        roas = ROASCalculators.calculate_estimated_roas(calc_input)
        
        # Can't calculate impact without spend
        if spend <= 0:
            return 0.0
        
        impact_score = spend * (roas ** 2)
        return ROASCalculators.safe_round(impact_score, 2) 