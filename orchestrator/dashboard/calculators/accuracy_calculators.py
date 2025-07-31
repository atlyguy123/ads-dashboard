"""
Accuracy Calculators

This module handles accuracy ratio calculations comparing Mixpanel vs Meta metrics.
"""

from .base_calculators import BaseCalculator, CalculationInput
import logging

logger = logging.getLogger(__name__)


class AccuracyCalculators(BaseCalculator):
    """Accuracy ratio calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_trial_accuracy_ratio(calc_input: CalculationInput) -> float:
        """
        Calculate trial accuracy ratio (Mixpanel trials / Meta trials * 100).
        
        This measures how well Mixpanel trial tracking matches Meta trial tracking.
        
        Special case: When meta_trials = 0 and mixpanel_trials > 0, 
        treat as 100% accuracy (perfect tracking) instead of 0%.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Trial accuracy ratio as percentage (0-100+)
        """
        if not AccuracyCalculators.validate_input(calc_input):
            return 0.0
            
        mixpanel_trials = calc_input.mixpanel_trials_started
        meta_trials = calc_input.meta_trials_started
        
        # Special case: If meta_trials = 0 but mixpanel_trials > 0, treat as 100% accuracy
        if meta_trials == 0 and mixpanel_trials > 0:
            return 100.0
        
        return AccuracyCalculators.safe_percentage(
            numerator=mixpanel_trials,
            denominator=meta_trials,
            default=0.0,
            decimal_places=2
        )
    
    @staticmethod
    def calculate_purchase_accuracy_ratio(calc_input: CalculationInput) -> float:
        """
        Calculate purchase accuracy ratio (Mixpanel purchases / Meta purchases * 100).
        
        This measures how well Mixpanel purchase tracking matches Meta purchase tracking.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Purchase accuracy ratio as percentage (0-100+)
        """
        if not AccuracyCalculators.validate_input(calc_input):
            return 0.0
            
        mixpanel_purchases = calc_input.mixpanel_purchases
        meta_purchases = calc_input.meta_purchases
        
        return AccuracyCalculators.safe_percentage(
            numerator=mixpanel_purchases,
            denominator=meta_purchases,
            default=0.0,
            decimal_places=2
        ) 