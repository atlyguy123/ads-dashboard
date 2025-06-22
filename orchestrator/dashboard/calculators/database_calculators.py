"""
Database Calculators

This module handles pass-through calculations for values that are pre-calculated in the database:
- Trial conversion rates
- Refund rates
- Other pre-calculated metrics
"""

from .base_calculators import BaseCalculator, CalculationInput
import logging

logger = logging.getLogger(__name__)


class DatabaseCalculators(BaseCalculator):
    """Database pass-through calculation functions for dashboard metrics"""
    
    @staticmethod
    def calculate_trial_conversion_rate(calc_input: CalculationInput) -> float:
        """
        Get trial conversion rate from database (pass-through).
        
        This value is pre-calculated in the database and stored as a decimal (0.0-1.0).
        We convert it to percentage (0-100) for display.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Trial conversion rate as percentage (0-100)
        """
        if not DatabaseCalculators.validate_input(calc_input):
            return 0.0
            
        # Get from database as decimal, convert to percentage
        decimal_rate = calc_input.avg_trial_conversion_rate
        percentage_rate = decimal_rate * 100
        
        return DatabaseCalculators.safe_round(percentage_rate, 2)
    
    @staticmethod
    def calculate_trial_to_purchase_rate(calc_input: CalculationInput) -> float:
        """
        Get trial to purchase rate from database (pass-through).
        
        Note: This is currently the same as trial_conversion_rate in the database.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Trial to purchase rate as percentage (0-100)
        """
        # Currently the same as trial conversion rate
        return DatabaseCalculators.calculate_trial_conversion_rate(calc_input)
    
    @staticmethod
    def calculate_avg_trial_refund_rate(calc_input: CalculationInput) -> float:
        """
        Get average trial refund rate from database (pass-through).
        
        This value is pre-calculated in the database and stored as a decimal (0.0-1.0).
        We convert it to percentage (0-100) for display.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Average trial refund rate as percentage (0-100)
        """
        if not DatabaseCalculators.validate_input(calc_input):
            return 0.0
            
        # Get from database as decimal, convert to percentage
        decimal_rate = calc_input.avg_trial_refund_rate
        percentage_rate = decimal_rate * 100
        
        return DatabaseCalculators.safe_round(percentage_rate, 2)
    
    @staticmethod
    def calculate_purchase_refund_rate(calc_input: CalculationInput) -> float:
        """
        Get purchase refund rate from database (pass-through).
        
        This value is pre-calculated in the database and stored as a decimal (0.0-1.0).
        We convert it to percentage (0-100) for display.
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Purchase refund rate as percentage (0-100)
        """
        if not DatabaseCalculators.validate_input(calc_input):
            return 0.0
            
        # Get from database as decimal, convert to percentage
        decimal_rate = calc_input.avg_purchase_refund_rate
        percentage_rate = decimal_rate * 100
        
        return DatabaseCalculators.safe_round(percentage_rate, 2) 