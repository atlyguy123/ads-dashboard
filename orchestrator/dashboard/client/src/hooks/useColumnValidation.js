import { useEffect } from 'react';
import { AVAILABLE_COLUMNS, validateColumnOrder } from '../config/columns';

/**
 * Custom hook that validates column consistency and logs warnings
 * This helps catch issues during development
 */
export const useColumnValidation = (columnOrder, columnVisibility) => {
  useEffect(() => {
    // Validate column order
    const orderValidation = validateColumnOrder(columnOrder);
    if (!orderValidation.isValid) {
      console.warn('🚨 Column Order Validation Failed:', {
        validation: orderValidation,
        missing_columns: orderValidation.missing,
        extra_columns: orderValidation.extra
      });
    }

    // Validate column visibility has all required columns
    const allColumnKeys = AVAILABLE_COLUMNS.map(col => col.key);
    const visibilityKeys = Object.keys(columnVisibility);
    const missingInVisibility = allColumnKeys.filter(key => !visibilityKeys.includes(key));
    const extraInVisibility = visibilityKeys.filter(key => !allColumnKeys.includes(key));

    if (missingInVisibility.length > 0 || extraInVisibility.length > 0) {
      console.warn('🚨 Column Visibility Validation Failed:', {
        missing_in_visibility: missingInVisibility,
        extra_in_visibility: extraInVisibility,
        expected_count: allColumnKeys.length,
        actual_count: visibilityKeys.length
      });
    }

    // Log successful validation in development
    if (process.env.NODE_ENV === 'development' && orderValidation.isValid && missingInVisibility.length === 0) {
      console.log('✅ Column validation passed:', {
        order_valid: true,
        visibility_valid: true,
        total_columns: AVAILABLE_COLUMNS.length
      });
    }
  }, [columnOrder, columnVisibility]);

  // Return validation status
  const orderValidation = validateColumnOrder(columnOrder);
  const allColumnKeys = AVAILABLE_COLUMNS.map(col => col.key);
  const visibilityKeys = Object.keys(columnVisibility);
  const visibilityValid = allColumnKeys.every(key => visibilityKeys.includes(key));

  return {
    isValid: orderValidation.isValid && visibilityValid,
    orderValidation,
    visibilityValid
  };
}; 