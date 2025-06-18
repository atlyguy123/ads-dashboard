import {
  ACTION_METRICS,
  ACTION_TYPE_ALLOWED_BREAKDOWNS,
  GEOGRAPHY_GROUP,
  DEVICE_GROUP,
  PLACEMENT_GROUP,
  TIME_GROUP,
  ASSET_GROUP,
  DELIVERY_ONLY_FIELDS,
  VALID_PAIRS,
  VIRTUAL_OS_MAP
} from './metaConstants';

// Helper to recognise a virtual OS ID
export const isVirtualOS = id => id === 'os_ios' || id === 'os_and';

// Helper to remove falsy entries from an object before storing to localStorage
export const compactObject = (obj) =>
  Object.fromEntries(Object.entries(obj).filter(([_, v]) => v));

// Validate combinations of selected fields and breakdowns
export const validateMetaBreakdownCombo = (fields, breakdowns) => {
  const selectedFieldIds = Object.entries(fields)
    .filter(([_, isSelected]) => isSelected)
    .map(([id, _]) => id);
    
  const selectedBreakdownIds = Object.entries(breakdowns)
    .filter(([_, isSelected]) => isSelected)
    .map(([id, _]) => id)
    .sort();
  
  const hasActionMetrics = selectedFieldIds.some(field => ACTION_METRICS.includes(field));
  const errors = [];
  let valid = true;
  
  // Check breakdown limit
  if (selectedBreakdownIds.length > 2) {
    errors.push("Maximum of 2 breakdowns allowed");
    valid = false;
  }
  
  // Check action metric restrictions
  if (hasActionMetrics) {
    if (selectedBreakdownIds.length > 1) {
      errors.push("When using action metrics, you can select at most 1 breakdown because Meta adds 'action_type' as an implicit breakdown");
      valid = false;
    }
    
    if (selectedBreakdownIds.length === 1 && !ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(selectedBreakdownIds[0])) {
      errors.push(
        `'${selectedBreakdownIds[0]}' is no longer valid. Use one of: ` +
        ACTION_TYPE_ALLOWED_BREAKDOWNS.join(', ')
      );
      valid = false;
    }
  }
  
  // Check group restrictions if we have multiple breakdowns
  if (selectedBreakdownIds.length === 2) {
    const [bd1, bd2] = selectedBreakdownIds;
    
    const isDeliveryOnlyRequest = selectedFieldIds.length > 0 && 
      selectedFieldIds.every(field => DELIVERY_ONLY_FIELDS.includes(field));

    const isPairInValidPairs = VALID_PAIRS.some(pair => 
      (pair[0] === bd1 && pair[1] === bd2) || 
      (pair[1] === bd1 && pair[0] === bd2)
    );

    let pairErrorFound = false; // Flag to prevent multiple errors for the same pair

    // Hourly breakdowns must always stand alone
    if (TIME_GROUP.includes(bd1) || TIME_GROUP.includes(bd2)) {
      errors.push("Hourly breakdowns must stand alone");
      valid = false;
      pairErrorFound = true;
    }

    if (!pairErrorFound) {
      if (isPairInValidPairs) {
        // Pair is in VALID_PAIRS. No further same-group or general validity check needed for these.
        // Hourly check already done.
      } else {
        // Pair is NOT in VALID_PAIRS. Check for same-group violations.
        if (
          (GEOGRAPHY_GROUP.includes(bd1) && GEOGRAPHY_GROUP.includes(bd2)) ||
          (DEVICE_GROUP.includes(bd1) && DEVICE_GROUP.includes(bd2)) ||
          (PLACEMENT_GROUP.includes(bd1) && PLACEMENT_GROUP.includes(bd2)) ||
          // ASSET_GROUP has a special condition with isDeliveryOnlyRequest
          (!isDeliveryOnlyRequest && ASSET_GROUP.includes(bd1) && ASSET_GROUP.includes(bd2))
          // TIME_GROUP is implicitly handled by the hourly check above if they are in the same group
        ) {
          errors.push(`Cannot select multiple breakdowns from the same group (${bd1}, ${bd2}) unless explicitly allowed in VALID_PAIRS.`);
          valid = false;
          pairErrorFound = true;
        }

        // If no specific group error was found, and it's not in VALID_PAIRS, then it's generally not supported.
        if (!pairErrorFound) {
          errors.push(`The combination of '${bd1}' and '${bd2}' is not supported by Meta.`);
          valid = false;
          // pairErrorFound = true; // Not strictly needed as it's the last check for this path
        }
      }
    }
  }
  
  return { valid, errors };
};

// Check if a breakdown should be disabled based on current selections
export const isBreakdownDisabled = (breakdownId, selectedFields, selectedBreakdowns) => {
  // If this breakdown is already selected, it's not disabled (can always be deselected)
  if (selectedBreakdowns[breakdownId]) {
    return false;
  }
  
  const hasAction = Object.entries(selectedFields)
    .some(([k,v]) => v && ACTION_METRICS.includes(k));
  if (hasAction) {
    // allow at most ONE extra breakdown and it must be in ACTION_TYPE_ALLOWED_BREAKDOWNS
    if (!ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(breakdownId)) return true;
    if (Object.values(selectedBreakdowns).filter(Boolean).length >= 1) return true;
  }
  
  const selectedBreakdownIds = Object.entries(selectedBreakdowns)
    .filter(([_, isSelected]) => isSelected)
    .map(([id, _]) => id)
    .sort(); // enforce order deterministically

  // treat os_ios/os_and as mutually exclusive
  const hasVirtualOSSelected = selectedBreakdownIds.some(isVirtualOS);
  if (hasVirtualOSSelected && isVirtualOS(breakdownId) && !selectedBreakdownIds.includes(breakdownId)) return true; // can't pick both, unless it's to deselect current

  // Rule 1: Limit to max 2 breakdowns
  if (selectedBreakdownIds.length >= 2) {
    return true;
  }

  // Check if any action metrics are selected - this implicitly adds action_type as a breakdown
  const hasActionMetrics = Object.entries(selectedFields)
    .some(([field, isSelected]) => isSelected && ACTION_METRICS.includes(field));
  
  // When action metrics are selected (which adds implicit action_type breakdown)
  if (hasActionMetrics) {
    // Only allow breakdowns specifically allowed with action_type
    if (!ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(breakdownId)) return true;
    if (selectedBreakdownIds.length >= 1) return true;
  }

  // No restrictions if nothing is selected yet
  if (selectedBreakdownIds.length === 0) {
    return false;
  }

  const currentBreakdown = selectedBreakdownIds[0];
  
  // Rule 2: Check "pick-only-one" groups
  if (
    (GEOGRAPHY_GROUP.includes(currentBreakdown) && GEOGRAPHY_GROUP.includes(breakdownId)) ||
    (DEVICE_GROUP.includes(currentBreakdown) && DEVICE_GROUP.includes(breakdownId)) ||
    (PLACEMENT_GROUP.includes(currentBreakdown) && PLACEMENT_GROUP.includes(breakdownId)) ||
    (TIME_GROUP.includes(currentBreakdown) && TIME_GROUP.includes(breakdownId)) ||
    (ASSET_GROUP.includes(currentBreakdown) && ASSET_GROUP.includes(breakdownId))
  ) {
    return true;
  }

  // Rule 3: Hourly breakdowns must stand alone
  if (
    (TIME_GROUP.includes(currentBreakdown) && !TIME_GROUP.includes(breakdownId)) ||
    (TIME_GROUP.includes(breakdownId) && !TIME_GROUP.includes(currentBreakdown))
  ) {
    return true;
  }

  // Rule 4: Check if the pair is in the known-good list
  const isPairValid = VALID_PAIRS.some(pair => 
    (pair[0] === currentBreakdown && pair[1] === breakdownId) || 
    (pair[1] === currentBreakdown && pair[0] === breakdownId)
  );
  
  // Return whether this pair is valid for selection
  return !isPairValid;
};

// Get tooltip message explaining why a breakdown is disabled
export const getDisabledTooltip = (breakdownId, selectedFields, selectedBreakdowns) => {
  // Create a simulated state as if this breakdown was selected
  const simulatedBreakdowns = { ...selectedBreakdowns };
  simulatedBreakdowns[breakdownId] = true;
  
  // Validate the simulated combo
  const validation = validateMetaBreakdownCombo(selectedFields, simulatedBreakdowns);
  
  // Return the first error message if invalid
  if (!validation.valid) {
    return validation.errors[0]; 
  }
  
  return ""; // Should not happen as isBreakdownDisabled would have returned false
};

// Get the comma-separated list of selected fields
export const getSelectedFieldsString = (selectedFields) => {
  return Object.entries(selectedFields)
    .filter(([_, isSelected]) => isSelected)
    .map(([fieldId, _]) => fieldId)
    .join(',');
};

// Get the comma-separated list of selected breakdowns
export const getSelectedBreakdownsString = (selectedBreakdowns) => {
  return Object.entries(selectedBreakdowns)
    .filter(([_, isSelected]) => isSelected)
    .map(([breakdownId, _]) => breakdownId)
    .join(',');
};

// Build API parameters for Meta requests
export const buildApiParams = (startDateInput, endDateInput, incrementInput, selectedFields, selectedBreakdowns, actionBreakdowns = null) => {
  const fields = Object.entries(selectedFields)
                  .filter(([,v]) => v).map(([k]) => k).join(',');

  const active = Object.entries(selectedBreakdowns)
                  .filter(([,v]) => v).map(([k]) => k);

  const hasIOS  = active.includes('os_ios');
  const hasAND  = active.includes('os_and');

  // real breakdown list we will send
  const breakdowns = active.filter(id => !isVirtualOS(id));

  // ensure impression_device is included if a virtual OS was picked
  if ((hasIOS || hasAND) && !breakdowns.includes('impression_device'))
      breakdowns.push('impression_device');

  const params = {
    start_date: startDateInput,
    end_date:   endDateInput,
    time_increment: parseInt(incrementInput,10) || 1,
    fields
  };

  if (breakdowns.length) params.breakdowns = breakdowns.join(',');

  // Add action_breakdowns parameter if provided
  if (actionBreakdowns) {
    params.action_breakdowns = actionBreakdowns;
  }

  // add filtering for virtual OS
  if (hasIOS || hasAND) {
    const key  = hasIOS ? 'os_ios' : 'os_and';
    params.filtering = JSON.stringify([{
      field: 'impression_device',
      operator: 'IN',
      value: VIRTUAL_OS_MAP[key].filterValues
    }]);
  }

  return params;
};

// Validate date format (YYYY-MM-DD)
export const isValidDate = (dateStr) => {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateStr)) return false;
  
  const [year, month, day] = dateStr.split('-').map(Number);
  const date = new Date(year, month - 1, day);
  
  return date.getFullYear() === year && 
         date.getMonth() === month - 1 && 
         date.getDate() === day;
}; 