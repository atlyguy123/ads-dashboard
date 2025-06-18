/**
 * Utility functions for applying action mappings to Meta API data
 */

/**
 * Load action mappings from localStorage
 * @returns {Object} Action mappings object
 */
export const loadActionMappings = () => {
  try {
    const savedMappings = localStorage.getItem('meta_action_mappings');
    return savedMappings ? JSON.parse(savedMappings) : {};
  } catch (error) {
    console.error('Error loading action mappings:', error);
    return {};
  }
};

/**
 * Apply action mappings to a single record
 * @param {Object} record - Meta API record
 * @param {Object} mappings - Action mappings configuration
 * @returns {Object} Record with mapped business concepts
 */
export const applyMappingsToRecord = (record, mappings = null) => {
  if (!mappings) {
    mappings = loadActionMappings();
  }

  const mappedRecord = { ...record };
  
  // Apply mappings to each business concept
  Object.entries(mappings).forEach(([conceptName, mapping]) => {
    const { actionTypes, aggregationType } = mapping;
    
    // Calculate mapped values for actions (counts)
    const actionValues = getActionValuesByTypes(record.actions, actionTypes);
    mappedRecord[`${conceptName}_count`] = aggregateValues(actionValues, aggregationType);
    
    // Calculate mapped values for action_values (monetary values)
    const monetaryValues = getActionValuesByTypes(record.action_values, actionTypes);
    mappedRecord[`${conceptName}_value`] = aggregateValues(monetaryValues, aggregationType);
    
    // Calculate mapped values for conversions
    const conversionCounts = getActionValuesByTypes(record.conversions, actionTypes);
    mappedRecord[`${conceptName}_conversions`] = aggregateValues(conversionCounts, aggregationType);
    
    // Calculate mapped values for conversion_values
    const conversionValues = getActionValuesByTypes(record.conversion_values, actionTypes);
    mappedRecord[`${conceptName}_conversion_value`] = aggregateValues(conversionValues, aggregationType);
    
    // Store the breakdown for debugging
    mappedRecord[`${conceptName}_breakdown`] = {
      count: actionValues,
      value: monetaryValues,
      conversions: conversionCounts,
      conversion_value: conversionValues
    };
  });
  
  return mappedRecord;
};

/**
 * Get values from action array for specific action types
 * @param {Array} actions - Array of action objects
 * @param {Array} targetActionTypes - Action types to filter for
 * @returns {Array} Array of values for matching action types
 */
const getActionValuesByTypes = (actions, targetActionTypes) => {
  if (!Array.isArray(actions) || !Array.isArray(targetActionTypes)) {
    return [];
  }
  
  return actions
    .filter(action => targetActionTypes.includes(action.action_type))
    .map(action => parseFloat(action.value) || 0);
};

/**
 * Aggregate values according to the specified aggregation type
 * @param {Array} values - Array of numeric values
 * @param {string} aggregationType - 'sum', 'count', 'average'
 * @returns {number} Aggregated value
 */
const aggregateValues = (values, aggregationType) => {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  
  switch (aggregationType) {
    case 'sum':
      return values.reduce((sum, val) => sum + val, 0);
    case 'count':
      return values.length;
    case 'average':
      return values.reduce((sum, val) => sum + val, 0) / values.length;
    default:
      return values.reduce((sum, val) => sum + val, 0);
  }
};

/**
 * Apply mappings to an array of records
 * @param {Array} records - Array of Meta API records
 * @param {Object} mappings - Action mappings configuration
 * @returns {Array} Array of records with mapped business concepts
 */
export const applyMappingsToRecords = (records, mappings = null) => {
  if (!Array.isArray(records)) {
    return records;
  }
  
  if (!mappings) {
    mappings = loadActionMappings();
  }
  
  return records.map(record => applyMappingsToRecord(record, mappings));
};

/**
 * Get summary statistics for mapped concepts across multiple records
 * @param {Array} records - Array of records with applied mappings
 * @param {Object} mappings - Action mappings configuration
 * @returns {Object} Summary statistics for each concept
 */
export const getMappedConceptSummary = (records, mappings = null) => {
  if (!Array.isArray(records) || records.length === 0) {
    return {};
  }
  
  if (!mappings) {
    mappings = loadActionMappings();
  }
  
  const summary = {};
  
  Object.keys(mappings).forEach(conceptName => {
    const counts = records.map(r => r[`${conceptName}_count`] || 0);
    const values = records.map(r => r[`${conceptName}_value`] || 0);
    const conversions = records.map(r => r[`${conceptName}_conversions`] || 0);
    const conversionValues = records.map(r => r[`${conceptName}_conversion_value`] || 0);
    
    summary[conceptName] = {
      total_count: counts.reduce((sum, val) => sum + val, 0),
      total_value: values.reduce((sum, val) => sum + val, 0),
      total_conversions: conversions.reduce((sum, val) => sum + val, 0),
      total_conversion_value: conversionValues.reduce((sum, val) => sum + val, 0),
      avg_count: counts.reduce((sum, val) => sum + val, 0) / counts.length,
      avg_value: values.reduce((sum, val) => sum + val, 0) / values.length,
      records_with_data: records.filter(r => (r[`${conceptName}_count`] || 0) > 0).length
    };
  });
  
  return summary;
};

/**
 * Format a record for better display, including mapped concepts
 * @param {Object} record - Record with applied mappings
 * @param {Object} mappings - Action mappings configuration
 * @returns {Object} Formatted record for display
 */
export const formatRecordForDisplay = (record, mappings = null) => {
  if (!mappings) {
    mappings = loadActionMappings();
  }
  
  const formatted = { ...record };
  
  // Add a section for mapped business concepts
  formatted.business_metrics = {};
  
  Object.keys(mappings).forEach(conceptName => {
    formatted.business_metrics[conceptName] = {
      count: record[`${conceptName}_count`] || 0,
      value: record[`${conceptName}_value`] || 0,
      conversions: record[`${conceptName}_conversions`] || 0,
      conversion_value: record[`${conceptName}_conversion_value`] || 0
    };
  });
  
  return formatted;
};

/**
 * Validate that action mappings are properly configured
 * @param {Object} mappings - Action mappings to validate
 * @returns {Object} Validation result with errors and warnings
 */
export const validateActionMappings = (mappings) => {
  const errors = [];
  const warnings = [];
  
  Object.entries(mappings).forEach(([conceptName, mapping]) => {
    if (!conceptName || conceptName.trim() === '') {
      errors.push('Concept name cannot be empty');
    }
    
    if (!mapping.actionTypes || !Array.isArray(mapping.actionTypes) || mapping.actionTypes.length === 0) {
      errors.push(`Concept '${conceptName}' has no action types defined`);
    }
    
    if (!['sum', 'count', 'average'].includes(mapping.aggregationType)) {
      errors.push(`Concept '${conceptName}' has invalid aggregation type: ${mapping.aggregationType}`);
    }
    
    // Check for overlapping action types
    const actionTypes = mapping.actionTypes;
    const duplicates = actionTypes.filter((item, index) => actionTypes.indexOf(item) !== index);
    if (duplicates.length > 0) {
      warnings.push(`Concept '${conceptName}' has duplicate action types: ${duplicates.join(', ')}`);
    }
  });
  
  return { errors, warnings, isValid: errors.length === 0 };
}; 