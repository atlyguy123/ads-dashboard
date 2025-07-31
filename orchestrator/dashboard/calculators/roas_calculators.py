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
        Calculate Performance Impact Score (spend × ROAS²) with time-frame scaling.
        
        This metric prioritizes campaigns with both high efficiency (ROAS) and meaningful scale (spend).
        The ROAS is capped at 4.0 before squaring to prevent over-weighting of extremely high ROAS values.
        The spend component ensures campaigns have meaningful scale worth optimizing.
        
        The score is then scaled based on the time frame to maintain consistent thresholds:
        - 1 day: score × (7/1) = score × 7 (normalize up for less data)
        - 7 days: score × (7/7) = score × 1 (baseline)
        - 14 days: score × (7/14) = score × 0.5 (normalize down for more data)
        - etc.
        
        Formula: (spend × min(ROAS, 4.0)²) × (7 / days)
        
        Args:
            calc_input: Standardized calculation input containing raw record data
            
        Returns:
            float: Time-scaled Performance Impact Score
        """
        if not ROASCalculators.validate_input(calc_input):
            return 0.0
            
        spend = calc_input.spend
        roas = ROASCalculators.calculate_estimated_roas(calc_input)
        
        # Can't calculate impact without spend
        if spend <= 0:
            return 0.0
        
        # Cap ROAS at 4.0 before squaring to prevent over-weighting
        capped_roas = min(roas, 4.0)
        
        # Calculate impact score: spend × ROAS²
        # SIMPLIFIED: Removed time scaling as requested - just spend × ROAS²
        impact_score = spend * (capped_roas ** 2)
        
        # DEBUG: Log the simplified calculation
        logger.info(f"🧮 PERFORMANCE IMPACT CALCULATOR (SIMPLIFIED):")
        logger.info(f"   💰 Spend: ${spend}")
        logger.info(f"   📈 ROAS: {roas:.4f}")
        logger.info(f"   📈 Capped ROAS: {capped_roas:.4f}")
        logger.info(f"   🎯 Final score (spend × capped_roas²): ${spend} × {capped_roas:.4f}² = ${impact_score:.2f}")
        
        return ROASCalculators.safe_round(impact_score, 2)
    
    @staticmethod
    def _calculate_time_scale_factor(calc_input: CalculationInput) -> float:
        """
        Calculate time scale factor based on date range.
        
        Args:
            calc_input: Calculation input containing date range in config
            
        Returns:
            float: Scale factor (days / 7), defaults to 1.0 if dates unavailable
        """
        try:
            if not calc_input.start_date or not calc_input.end_date:
                logger.info(f"⏰ TIME SCALE DEBUG: No dates provided, defaulting to 1.0")
                return 1.0  # Default to 7-day baseline if dates not available
            
            from datetime import datetime
            
            start_date = datetime.strptime(calc_input.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(calc_input.end_date, '%Y-%m-%d')
            
            # Calculate days (inclusive of both start and end dates)
            days_diff = (end_date - start_date).days + 1
            days = max(1, days_diff)  # Ensure minimum 1 day
            
            # Scale factor: 7 days is baseline (scale = 1.0)
            # Inverse scaling: More days = lower scale factor (normalize down)
            scale_factor = 7.0 / days
            
            logger.info(f"⏰ TIME SCALE DEBUG:")
            logger.info(f"   📅 Start date: {calc_input.start_date}")
            logger.info(f"   📅 End date: {calc_input.end_date}")
            logger.info(f"   📅 Days difference: {days_diff}")
            logger.info(f"   📅 Final days: {days}")
            logger.info(f"   🔢 Scale factor: 7 / {days} = {scale_factor:.4f}")
            
            return scale_factor
            
        except Exception as e:
            logger.warning(f"Error calculating time scale factor: {e}, defaulting to 1.0")
            return 1.0 