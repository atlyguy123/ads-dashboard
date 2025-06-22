"""
Base Calculator Classes and Utilities

This module provides the foundation for all dashboard calculations:
- CalculationInput: Standardized input data structure
- BaseCalculator: Common calculation utilities
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, Union
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


@dataclass
class CalculationInput:
    """
    Standardized input structure for all calculation functions.
    
    This ensures all calculators receive data in a consistent format and
    provides convenient property access to commonly used fields.
    """
    raw_record: Dict[str, Any]
    config: Optional[Dict[str, Any]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    # === QUICK ACCESS PROPERTIES ===
    # These provide type-safe access to commonly used fields
    
    @property
    def spend(self) -> float:
        """Total spend amount"""
        return float(self.raw_record.get('spend', 0) or 0)
    
    @property
    def impressions(self) -> int:
        """Number of impressions"""
        return int(self.raw_record.get('impressions', 0) or 0)
    
    @property
    def clicks(self) -> int:
        """Number of clicks"""
        return int(self.raw_record.get('clicks', 0) or 0)
        
    @property
    def mixpanel_trials_started(self) -> int:
        """Number of trials started (Mixpanel)"""
        return int(self.raw_record.get('mixpanel_trials_started', 0) or 0)
    
    @property
    def meta_trials_started(self) -> int:
        """Number of trials started (Meta)"""
        return int(self.raw_record.get('meta_trials_started', 0) or 0)
        
    @property
    def mixpanel_purchases(self) -> int:
        """Number of purchases (Mixpanel)"""
        return int(self.raw_record.get('mixpanel_purchases', 0) or 0)
        
    @property
    def meta_purchases(self) -> int:
        """Number of purchases (Meta)"""
        return int(self.raw_record.get('meta_purchases', 0) or 0)
    
    # Raw database values (may need further calculation)
    @property
    def raw_mixpanel_revenue_usd(self) -> float:
        """Raw Mixpanel revenue from record (may not be calculated yet)"""
        return float(self.raw_record.get('mixpanel_revenue_usd', 0) or 0)
        
    @property 
    def raw_mixpanel_refunds_usd(self) -> float:
        """Raw Mixpanel refunds from record (may not be calculated yet)"""
        return float(self.raw_record.get('mixpanel_refunds_usd', 0) or 0)
        
    @property
    def raw_estimated_revenue_usd(self) -> float:
        """Raw estimated revenue from record (from user_product_metrics)"""
        return float(self.raw_record.get('estimated_revenue_usd', 0) or 0)
    
    # Database pass-through values
    @property
    def avg_trial_conversion_rate(self) -> float:
        """Trial conversion rate from database (as decimal, not percentage)"""
        return float(self.raw_record.get('avg_trial_conversion_rate', 0) or 0)
        
    @property
    def avg_trial_refund_rate(self) -> float:
        """Trial refund rate from database (as decimal, not percentage)"""  
        return float(self.raw_record.get('avg_trial_refund_rate', 0) or 0)
        
    @property
    def avg_purchase_refund_rate(self) -> float:
        """Purchase refund rate from database (as decimal, not percentage)"""
        return float(self.raw_record.get('avg_purchase_refund_rate', 0) or 0)


class BaseCalculator:
    """
    Base class providing common calculation utilities.
    
    All calculator classes should inherit from this to access shared
    mathematical operations and error handling.
    """
    
    @staticmethod
    def safe_divide(numerator: Union[int, float], denominator: Union[int, float], 
                   default: float = 0.0, decimal_places: int = 2) -> float:
        """
        Perform safe division with default value for zero denominator.
        
        Args:
            numerator: The number to divide
            denominator: The number to divide by
            default: Value to return if denominator is 0
            decimal_places: Number of decimal places to round to
            
        Returns:
            Division result rounded to specified decimal places, or default if denominator is 0
        """
        try:
            if denominator == 0:
                return default
            result = float(numerator) / float(denominator)
            return round(result, decimal_places)
        except (TypeError, ValueError) as e:
            logger.warning(f"safe_divide error: {e}, returning default {default}")
            return default
    
    @staticmethod
    def safe_percentage(numerator: Union[int, float], denominator: Union[int, float], 
                       default: float = 0.0, decimal_places: int = 2) -> float:
        """
        Calculate percentage with safe division.
        
        Args:
            numerator: The number to convert to percentage of denominator
            denominator: The total amount
            default: Value to return if denominator is 0
            decimal_places: Number of decimal places to round to
            
        Returns:
            Percentage (0-100) rounded to specified decimal places
        """
        try:
            if denominator == 0:
                return default
            result = (float(numerator) / float(denominator)) * 100
            return round(result, decimal_places)
        except (TypeError, ValueError) as e:
            logger.warning(f"safe_percentage error: {e}, returning default {default}")
            return default
    
    @staticmethod 
    def safe_round(value: Union[int, float], decimal_places: int = 2) -> float:
        """
        Safely round a numeric value.
        
        Args:
            value: The value to round
            decimal_places: Number of decimal places
            
        Returns:
            Rounded value or 0.0 if value is not numeric
        """
        try:
            return round(float(value), decimal_places)
        except (TypeError, ValueError) as e:
            logger.warning(f"safe_round error: {e}, returning 0.0")
            return 0.0
    
    @staticmethod
    def safe_subtract(minuend: Union[int, float], subtrahend: Union[int, float], 
                     decimal_places: int = 2) -> float:
        """
        Safely subtract two values.
        
        Args:
            minuend: The number to subtract from
            subtrahend: The number to subtract
            decimal_places: Number of decimal places to round to
            
        Returns:
            Subtraction result rounded to specified decimal places
        """
        try:
            result = float(minuend) - float(subtrahend)
            return round(result, decimal_places)
        except (TypeError, ValueError) as e:
            logger.warning(f"safe_subtract error: {e}, returning 0.0")
            return 0.0
            
    @staticmethod
    def validate_input(calc_input: CalculationInput) -> bool:
        """
        Validate that CalculationInput has required data.
        
        Args:
            calc_input: The input to validate
            
        Returns:
            True if input is valid, False otherwise
        """
        if not isinstance(calc_input, CalculationInput):
            logger.error("Input must be CalculationInput instance")
            return False
            
        if not isinstance(calc_input.raw_record, dict):
            logger.error("raw_record must be a dictionary")
            return False
            
        return True 