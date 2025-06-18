import React, { useState, useEffect } from 'react';
import { api } from '../../services/api';
import {
  FIELD_CATEGORIES,
  BREAKDOWN_CATEGORIES,
  ACTION_METRICS,
  ACTION_TYPE_ALLOWED_BREAKDOWNS
} from './utils/metaConstants';
import {
  validateMetaBreakdownCombo,
  isBreakdownDisabled,
  getDisabledTooltip,
  getSelectedFieldsString,
  getSelectedBreakdownsString,
  buildApiParams,
  isValidDate
} from './utils/metaApiUtils';
import { applyMappingsToRecords, getMappedConceptSummary, loadActionMappings } from './utils/actionMappingUtils';

const LiveApiTester = ({
  startDateInput,
  setStartDateInput,
  endDateInput,
  setEndDateInput,
  incrementInput,
  setIncrementInput,
  selectedFields,
  setSelectedFields,
  selectedBreakdowns,
  setSelectedBreakdowns,
  error,
  setError
}) => {
  // API request state
  const [isLoading, setIsLoading] = useState(false);
  const [loadingProgress, setLoadingProgress] = useState({ stage: '', pagesLoaded: 0, percentage: 0 });
  const [results, setResults] = useState(null);
  const [asyncJobInfo, setAsyncJobInfo] = useState(null);
  const [jobPollingInterval, setJobPollingInterval] = useState(null);
  
  // Show/hide instructions and breakdowns
  const [showInstructions, setShowInstructions] = useState(false);
  const [showBreakdowns, setShowBreakdowns] = useState(false);
  const [showMappedData, setShowMappedData] = useState(true);
  const [actionMappings, setActionMappings] = useState({});

  // Load action mappings on component mount
  useEffect(() => {
    const mappings = loadActionMappings();
    setActionMappings(mappings);
  }, []);

  // Helper function to format nested action values for better readability
  const formatNestedValues = (data) => {
    if (!Array.isArray(data)) return data;
    
    return data.map(record => {
      const formatted = { ...record };
      
      // Format common nested fields
      ['actions', 'action_values', 'conversions', 'conversion_values'].forEach(field => {
        if (formatted[field] && Array.isArray(formatted[field])) {
          formatted[field + '_formatted'] = formatted[field].reduce((acc, item) => {
            acc[item.action_type] = item.value;
            return acc;
          }, {});
        }
      });
      
      return formatted;
    });
  };

  // Check for action metrics and deselect incompatible breakdowns when fields change
  useEffect(() => {
    // Determine if any action metrics are selected
    const hasActionMetrics = Object.entries(selectedFields)
      .some(([field, isSelected]) => isSelected && ACTION_METRICS.includes(field));
    
    if (hasActionMetrics) {
      // Get currently selected breakdown IDs
      const selectedBreakdownIds = Object.entries(selectedBreakdowns)
        .filter(([_, isSelected]) => isSelected)
        .map(([id, _]) => id)
        .sort(); // enforce order deterministically
      
      // If we have more than one breakdown selected with action metrics
      if (selectedBreakdownIds.length > 1) {
        // Create a new object with incompatible breakdowns turned off
        const updatedBreakdowns = {...selectedBreakdowns};
        
        // Keep only the first action-compatible breakdown if one exists
        let keptOne = false;
        
        selectedBreakdownIds.forEach(breakdownId => {
          if (ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(breakdownId) && !keptOne) {
            // Keep the first compatible breakdown
            keptOne = true;
          } else {
            // Deselect all others
            updatedBreakdowns[breakdownId] = false;
          }
        });
        
        // Update the state with the fixed breakdown selection
        setSelectedBreakdowns(updatedBreakdowns);
        
        // Show a notification or alert to inform the user
        setError("Some breakdowns were deselected because with action metrics, at most one breakdown is allowed");
        setTimeout(() => setError(null), 5000); // Clear the error after 5 seconds
      }
      
      // If only one breakdown is selected and it's not action-compatible
      else if (selectedBreakdownIds.length === 1 && !ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(selectedBreakdownIds[0])) {
        const updatedBreakdowns = {...selectedBreakdowns};
        updatedBreakdowns[selectedBreakdownIds[0]] = false;
        setSelectedBreakdowns(updatedBreakdowns);
        
        setError(`Selected breakdown '${selectedBreakdownIds[0]}' was deselected because it's incompatible with action metrics`);
        setTimeout(() => setError(null), 5000); // Clear the error after 5 seconds
      }
    }
  }, [selectedFields, selectedBreakdowns, setSelectedBreakdowns, setError]);

  // Toggle a field selection
  const toggleField = (fieldId) => {
    setSelectedFields(prev => ({
      ...prev,
      [fieldId]: !prev[fieldId]
    }));
  };
  
  // Toggle a breakdown selection
  const toggleBreakdown = (breakdownId) => {
    if (isBreakdownDisabled(breakdownId, selectedFields, selectedBreakdowns)) {
      return; // Don't toggle if disabled
    }
    
    setSelectedBreakdowns(prev => ({
      ...prev,
      [breakdownId]: !prev[breakdownId]
    }));
  };

  // Clean up polling interval on unmount
  useEffect(() => {
    return () => {
      if (jobPollingInterval) {
        clearInterval(jobPollingInterval);
      }
    };
  }, [jobPollingInterval]);

  // Poll async job status
  const pollJobStatus = async (reportRunId) => {
    try {
      const statusResponse = await api.checkMetaJobStatus(reportRunId);
      
      setLoadingProgress(prev => ({
        ...prev,
        stage: `Processing in Meta servers... (${statusResponse.async_status})`,
        percentage: statusResponse.async_percent_completion || 0
      }));
      
      if (statusResponse.async_status === 'Job Completed') {
        // Job is done, fetch results
        setLoadingProgress(prev => ({ ...prev, stage: 'Fetching results...', percentage: 100 }));
        
        const resultsResponse = await api.getMetaJobResults(reportRunId);
        
        if (resultsResponse.data && resultsResponse.data.data) {
          // Apply action mappings to the results
          const mappedData = applyMappingsToRecords(resultsResponse.data.data, actionMappings);
          const conceptSummary = getMappedConceptSummary(mappedData, actionMappings);
          
          setResults({
            ...resultsResponse,
            data: {
              ...resultsResponse.data,
              data: mappedData,
              business_summary: conceptSummary
            }
          });
        } else {
          setResults(resultsResponse);
        }
        
        // Clear polling
        if (jobPollingInterval) {
          clearInterval(jobPollingInterval);
          setJobPollingInterval(null);
        }
        
        setIsLoading(false);
        setAsyncJobInfo(null);
        
      } else if (statusResponse.async_status === 'Job Failed') {
        setError('Meta API job failed. Please try again.');
        if (jobPollingInterval) {
          clearInterval(jobPollingInterval);
          setJobPollingInterval(null);
        }
        setIsLoading(false);
        setAsyncJobInfo(null);
      }
      
    } catch (err) {
      console.error('Error polling job status:', err);
      
      // If we already have results, don't show this as an error - just stop polling
      if (results && results.data) {
        console.log('Job status check failed but we already have results - stopping polling');
        if (jobPollingInterval) {
          clearInterval(jobPollingInterval);
          setJobPollingInterval(null);
        }
        setIsLoading(false);
        setAsyncJobInfo(null);
        return;
      }
      
      // Only show error if we don't have results yet
      setError('Error checking job status: ' + (err.message || 'Unknown error'));
      if (jobPollingInterval) {
        clearInterval(jobPollingInterval);
        setJobPollingInterval(null);
      }
      setIsLoading(false);
      setAsyncJobInfo(null);
    }
  };

  // Handle submitting the form to fetch Meta data
  const handleFetchData = async () => {
    if (!startDateInput || !endDateInput) {
      setError('Start date and end date are required');
      return;
    }
    
    if (!isValidDate(startDateInput) || !isValidDate(endDateInput)) {
      setError('Start or end date is invalid. Use YYYY-MM-DD format.');
      return;
    }
    
    const fieldsString = getSelectedFieldsString(selectedFields);
    if (!fieldsString) {
      setError('At least one field must be selected');
      return;
    }
    
    // Check for incompatible combinations before submitting
    const validation = validateMetaBreakdownCombo(selectedFields, selectedBreakdowns);
    if (!validation.valid) {
      setError(validation.errors[0]); // Show first error
      return;
    }
    
    setIsLoading(true);
    setError(null);
    setResults(null);
    setAsyncJobInfo(null);
    setLoadingProgress({ stage: 'Initializing request...', pagesLoaded: 0, percentage: 0 });
    
    try {
      // Build request parameters
      const params = buildApiParams(startDateInput, endDateInput, incrementInput, selectedFields, selectedBreakdowns);
      
      setLoadingProgress(prev => ({ ...prev, stage: 'Sending request to Meta API...' }));
      
      // Make the API request
      const response = await api.fetchMetaData(params);
      
      // Check if this is an async job
      if (response.async_job && response.report_run_id) {
        // This is an async job - start polling
        setAsyncJobInfo({
          reportRunId: response.report_run_id,
          status: response.async_status
        });
        
        setLoadingProgress(prev => ({ 
          ...prev, 
          stage: `Started async job (${response.async_status})...`,
          percentage: 0
        }));
        
        // Start polling every 3 seconds
        const interval = setInterval(() => {
          pollJobStatus(response.report_run_id);
        }, 3000);
        
        setJobPollingInterval(interval);
        
        // Poll immediately once
        pollJobStatus(response.report_run_id);
        
      } else {
        // This is a synchronous response
        if (response.data && response.data.data) {
          // Apply action mappings to the results
          const mappedData = applyMappingsToRecords(response.data.data, actionMappings);
          const conceptSummary = getMappedConceptSummary(mappedData, actionMappings);
          
          setResults({
            ...response,
            data: {
              ...response.data,
              data: mappedData,
              business_summary: conceptSummary
            }
          });
        } else {
          setResults(response);
        }
        
        if (response.meta?.pages_fetched > 0) {
          setLoadingProgress({ 
            stage: 'Processing results...', 
            pagesLoaded: response.meta.pages_fetched,
            percentage: 100
          });
        }
        
        // Short delay to show the processing message
        setTimeout(() => {
          setIsLoading(false);
        }, 500);
      }
      
    } catch (err) {
      setError(err.response?.data?.error || err.message || 'An unknown error occurred');
      setIsLoading(false);
      setAsyncJobInfo(null);
    }
  };

  // Handle predefined API request templates
  const handlePredefinedRequest = async (templateType) => {
    if (!startDateInput || !endDateInput) {
      setError('Start date and end date are required');
      return;
    }
    
    if (!isValidDate(startDateInput) || !isValidDate(endDateInput)) {
      setError('Start or end date is invalid. Use YYYY-MM-DD format.');
      return;
    }

    setIsLoading(true);
    setError(null);
    setResults(null);
    setAsyncJobInfo(null);
    setLoadingProgress({ stage: 'Initializing predefined request...', pagesLoaded: 0, percentage: 0 });

    try {
      let params;
      
      if (templateType === 'country') {
        // Country-level conversions template
        params = {
          start_date: startDateInput,
          end_date: endDateInput,
          time_increment: parseInt(incrementInput, 10) || 1,
          fields: 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend,actions',
          breakdowns: 'country'
        };
      } else if (templateType === 'device') {
        // Device-level conversions template (impression_device requires placement)
        params = {
          start_date: startDateInput,
          end_date: endDateInput,
          time_increment: parseInt(incrementInput, 10) || 1,
          fields: 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend,actions',
          breakdowns: 'impression_device,placement'
        };
      } else if (templateType === 'region') {
        // Region-level conversions template
        params = {
          start_date: startDateInput,
          end_date: endDateInput,
          time_increment: parseInt(incrementInput, 10) || 1,
          fields: 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend,actions',
          breakdowns: 'region'
        };
      } else if (templateType === 'country_device') {
        // EXACT RECIPE: Country breakdown with device action splitting
        params = {
          start_date: startDateInput,
          end_date: endDateInput,
          time_increment: parseInt(incrementInput, 10) || 1,
          fields: 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend,actions',
          breakdowns: 'country',
          action_breakdowns: 'action_device',
          limit: 500  // As specified in your recipe
        };
      }

      setLoadingProgress(prev => ({ ...prev, stage: 'Sending request to Meta API...' }));

      // Make the API request
      const response = await api.fetchMetaData(params);
      
      // Check if this is an async job
      if (response.async_job && response.report_run_id) {
        // This is an async job - start polling
        setAsyncJobInfo({
          reportRunId: response.report_run_id,
          status: response.async_status
        });
        
        setLoadingProgress(prev => ({ 
          ...prev, 
          stage: `Started async job (${response.async_status})...`,
          percentage: 0
        }));
        
        // Start polling every 3 seconds
        const interval = setInterval(() => {
          pollJobStatus(response.report_run_id);
        }, 3000);
        
        setJobPollingInterval(interval);
        
        // Poll immediately once
        pollJobStatus(response.report_run_id);
        
      } else {
        // This is a synchronous response
        if (response.data && response.data.data) {
          // Apply action mappings to the results
          const mappedData = applyMappingsToRecords(response.data.data, actionMappings);
          const conceptSummary = getMappedConceptSummary(mappedData, actionMappings);
          
          setResults({
            ...response,
            data: {
              ...response.data,
              data: mappedData,
              business_summary: conceptSummary
            }
          });
        } else {
          setResults(response);
        }
        
        if (response.meta?.pages_fetched > 0) {
          setLoadingProgress({ 
            stage: 'Processing results...', 
            pagesLoaded: response.meta.pages_fetched,
            percentage: 100
          });
        }
        
        // Short delay to show the processing message
        setTimeout(() => {
          setIsLoading(false);
        }, 500);
      }
      
    } catch (err) {
      setError(err.response?.data?.error || err.message || 'An unknown error occurred');
      setIsLoading(false);
      setAsyncJobInfo(null);
    }
  };

  // Toggle instructions visibility
  const toggleInstructions = () => {
    setShowInstructions(!showInstructions);
  };
  
  // Select all fields in a category
  const selectAllInCategory = (categoryFields) => {
    const newSelectedFields = { ...selectedFields };
    categoryFields.forEach(field => {
      newSelectedFields[field.id] = true;
    });
    setSelectedFields(newSelectedFields);
  };
  
  // Deselect all fields in a category
  const deselectAllInCategory = (categoryFields) => {
    const newSelectedFields = { ...selectedFields };
    categoryFields.forEach(field => {
      newSelectedFields[field.id] = false;
    });
    setSelectedFields(newSelectedFields);
  };
  
  // Select all breakdowns in a category
  const selectAllBreakdownsInCategory = (categoryBreakdowns) => {
    // Check if all breakdowns in this category are enabled first
    const allEnabled = categoryBreakdowns.every(breakdown => 
      !isBreakdownDisabled(breakdown.id, selectedFields, selectedBreakdowns)
    );
    
    if (!allEnabled) {
      return; // Don't select all if any are disabled
    }
    
    const newSelectedBreakdowns = { ...selectedBreakdowns };
    categoryBreakdowns.forEach(breakdown => {
      newSelectedBreakdowns[breakdown.id] = true;
    });
    setSelectedBreakdowns(newSelectedBreakdowns);
  };
  
  // Deselect all breakdowns in a category
  const deselectAllBreakdownsInCategory = (categoryBreakdowns) => {
    const newSelectedBreakdowns = { ...selectedBreakdowns };
    categoryBreakdowns.forEach(breakdown => {
      newSelectedBreakdowns[breakdown.id] = false;
    });
    setSelectedBreakdowns(newSelectedBreakdowns);
  };

  return (
    <div>
      {/* Instructions */}
      <div className="bg-blue-50 dark:bg-blue-900 rounded-lg shadow p-4 mb-6">
        <div className="flex justify-between items-center cursor-pointer" onClick={toggleInstructions}>
          <h2 className="text-lg font-semibold text-blue-800 dark:text-blue-200">
            How to use this tool
          </h2>
          <button className="text-blue-500">
            {showInstructions ? 'Hide' : 'Show'} instructions
          </button>
        </div>
        
        {showInstructions && (
          <div className="mt-4 text-sm text-blue-800 dark:text-blue-200">
            <p className="mb-2">
              This tool allows you to fetch data from the Meta Ads API for a specified date range.
            </p>
            
            <h3 className="font-semibold mt-3 mb-1">Parameters:</h3>
            <ul className="list-disc pl-5 space-y-1">
              <li><strong>Start Date and End Date:</strong> Define the time period for data retrieval (YYYY-MM-DD format)</li>
              <li><strong>Time Increment:</strong> Controls how data is grouped:
                <ul className="list-disc pl-5 mt-1">
                  <li>Set to <strong>0</strong> to get aggregated data for the entire date range</li>
                  <li>Set to <strong>1</strong> for daily breakdowns</li>
                  <li>Set to <strong>7</strong> for weekly breakdowns</li>
                  <li>Set to <strong>30</strong> for monthly breakdowns (approximate)</li>
                </ul>
              </li>
              <li><strong>Fields:</strong> Click on the pills to select which data fields to retrieve</li>
              <li><strong>Breakdowns:</strong> Select dimension breakdowns to segment your data (optional)</li>
            </ul>
            
            <p className="mt-3">
              <strong>Breakdown Rules:</strong>
            </p>
            <ul className="list-disc pl-5 space-y-1">
              <li>Maximum of 2 breakdown dimensions allowed at once</li>
              <li>Certain breakdowns cannot be used together (disabled options will appear grayed out)</li>
              <li>Breakdowns in the same category (e.g., multiple geography options) cannot be combined</li>
              <li>Hourly stats breakdowns must be used alone</li>
              <li><strong>Action/Conversion Metrics Limitations:</strong>
                <ul className="list-disc pl-5 mt-1 text-red-800 dark:text-red-300">
                  <li>When selecting <strong>⚡ action metrics</strong> Meta always adds an internal <code>action_type</code> breakdown</li>
                  <li>With <strong>⚡ action metrics</strong> you may add <strong>only one</strong> other breakdown, and it must be <code>conversion_destination</code></li>
                  <li>Common breakdowns like country, age, gender, device_platform <strong>cannot</strong> be used with action metrics</li>
                  <li>To get both action data and country/device breakdowns, you need to run separate reports and join them offline</li>
                </ul>
              </li>
              <li>Your selected fields and parameters are saved automatically for future sessions</li>
            </ul>
          </div>
        )}
      </div>

      {/* Data Retrieval Section */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">Fetch Meta API Data</h2>
        
        <div className="mb-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div>
              <label className="block text-sm font-medium mb-1" htmlFor="start_date_input">
                Start Date (YYYY-MM-DD)<span className="text-red-500"> *</span>
              </label>
              <input
                type="text"
                id="start_date_input"
                value={startDateInput}
                onChange={(e) => setStartDateInput(e.target.value)}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="e.g., 2023-05-01"
                required
                disabled={isLoading}
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1" htmlFor="end_date_input">
                End Date (YYYY-MM-DD)<span className="text-red-500"> *</span>
              </label>
              <input
                type="text"
                id="end_date_input"
                value={endDateInput}
                onChange={(e) => setEndDateInput(e.target.value)}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="e.g., 2023-05-31"
                required
                disabled={isLoading}
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1" htmlFor="increment_input">
                Time Increment (days)
              </label>
              <input
                type="number"
                id="increment_input"
                value={incrementInput}
                onChange={(e) => setIncrementInput(e.target.value)}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="1"
                min="0"
                disabled={isLoading}
              />
              <p className="text-xs text-gray-500 mt-1">
                0 = entire range, 1 = daily, 7 = weekly, 30 = monthly
              </p>
            </div>
          </div>
          
          {/* Field Selection */}
          <div className="mb-6">
            <div className="flex justify-between items-center mb-2">
              <label className="block text-sm font-medium">
                Select Fields<span className="text-red-500"> *</span>
              </label>
              <div className="text-sm">
                <span className="text-gray-500">{Object.values(selectedFields).filter(Boolean).length} fields selected</span>
              </div>
            </div>
            
            {FIELD_CATEGORIES.map((category, index) => (
              <div key={index} className="mb-4">
                <div className="flex justify-between items-center mb-2">
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">{category.name}</h3>
                  <div className="space-x-2">
                    <button 
                      type="button" 
                      onClick={() => selectAllInCategory(category.fields)}
                      className="text-xs text-blue-600 hover:text-blue-800"
                      disabled={isLoading}
                    >
                      Select All
                    </button>
                    <button 
                      type="button" 
                      onClick={() => deselectAllInCategory(category.fields)}
                      className="text-xs text-red-600 hover:text-red-800"
                      disabled={isLoading}
                    >
                      Clear
                    </button>
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {category.fields.map((field) => {
                    const isAction = field.isActionMetric;
                    return (
                      <button
                        key={field.id}
                        type="button"
                        onClick={() => toggleField(field.id)}
                        className={`px-3 py-1 rounded-full text-sm ${
                          selectedFields[field.id] 
                            ? 'bg-blue-500 text-white' 
                            : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300'
                        }`}
                        disabled={isLoading}
                      >
                        {field.label}
                        <span className="ml-1 text-xs">{isAction && '⚡'}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
          
          {/* Breakdowns Toggle */}
          <div className="mb-4">
            <button
              type="button"
              onClick={() => setShowBreakdowns(!showBreakdowns)}
              className="text-blue-600 hover:text-blue-800 text-sm font-medium flex items-center"
            >
              <span>{showBreakdowns ? '- Hide' : '+ Show'} Breakdowns (Advanced)</span>
            </button>
          </div>
          
          {/* Breakdowns Selection */}
          {showBreakdowns && (
            <div className="mb-6 pl-4 border-l-2 border-blue-200">
              <div className="flex justify-between items-center mb-2">
                <label className="block text-sm font-medium">
                  Select Breakdowns (Optional)
                </label>
                <div className="text-sm">
                  <span className="text-gray-500">{Object.values(selectedBreakdowns).filter(Boolean).length} breakdowns selected</span>
                </div>
              </div>
              
              <div className="text-xs bg-gray-100 dark:bg-gray-700 p-2 rounded mb-3 text-gray-800 dark:text-gray-300">
                <strong>Note:</strong> The Meta API has strict rules about which breakdowns can be used together.
                <ul className="mt-1 ml-2 list-disc pl-4">
                  <li>Maximum 2 breakdowns allowed</li>
                  <li><strong>Important:</strong> When using <strong>⚡ action metrics</strong> Meta always adds an internal <code>action_type</code> breakdown</li>
                  <li>With <strong>⚡ action metrics</strong> you may add <strong>only one</strong> other breakdown, and it must be <code>conversion_destination</code></li>
                  <li>Disabled options are incompatible with your current selection</li>
                </ul>
              </div>
              
              {BREAKDOWN_CATEGORIES.map((category, index) => (
                <div key={index} className="mb-4">
                  <div className="flex justify-between items-center mb-2">
                    <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">{category.name}</h3>
                    <div className="space-x-2">
                      <button 
                        type="button" 
                        onClick={() => selectAllBreakdownsInCategory(category.breakdowns)}
                        className="text-xs text-blue-600 hover:text-blue-800"
                        disabled={isLoading || category.breakdowns.some(b => isBreakdownDisabled(b.id, selectedFields, selectedBreakdowns))}
                      >
                        Select All
                      </button>
                      <button 
                        type="button" 
                        onClick={() => deselectAllBreakdownsInCategory(category.breakdowns)}
                        className="text-xs text-red-600 hover:text-red-800"
                        disabled={isLoading}
                      >
                        Clear
                      </button>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {category.breakdowns.map((breakdown) => {
                      const isDisabled = isBreakdownDisabled(breakdown.id, selectedFields, selectedBreakdowns);
                      const disabledTooltip = isDisabled ? getDisabledTooltip(breakdown.id, selectedFields, selectedBreakdowns) : '';
                      const isActionCompatible = ACTION_TYPE_ALLOWED_BREAKDOWNS.includes(breakdown.id);
                      
                      return (
                        <button
                          key={breakdown.id}
                          type="button"
                          onClick={() => toggleBreakdown(breakdown.id)}
                          className={`px-3 py-1 rounded-full text-sm ${
                            selectedBreakdowns[breakdown.id] 
                              ? 'bg-green-500 text-white' 
                              : isDisabled
                                ? 'bg-gray-300 dark:bg-gray-600 text-gray-500 dark:text-gray-400 cursor-not-allowed'
                                : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300'
                          }`}
                          disabled={isLoading || isDisabled}
                          title={disabledTooltip}
                        >
                          {breakdown.label}
                          {isActionCompatible && 
                            <span className="ml-1 text-xs" title="Compatible with action metrics">★</span>
                          }
                        </button>
                      );
                    })}
                  </div>
                  {category.name === "Others" && (
                    <div className="text-xs text-gray-500 mt-1">
                      ★ Breakdowns marked with a star are compatible with action metrics (actions, conversions, ROAS)
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          <div className="flex space-x-4">
            <button
              type="button"
              onClick={handleFetchData}
              disabled={isLoading}
              className="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {isLoading ? 'Loading...' : 'Fetch Data'}
            </button>
          </div>
        </div>

        {/* Loading indicator */}
        {isLoading && (
          <div className="mt-4 p-4 bg-slate-50 dark:bg-slate-800 rounded-lg border border-blue-200 dark:border-blue-800">
            <div className="flex items-center mb-3">
              <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-500 mr-3"></div>
              <span className="font-medium">{loadingProgress.stage}</span>
              {asyncJobInfo && (
                <span className="ml-2 px-2 py-1 bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 text-xs rounded-full">
                  Job ID: {asyncJobInfo.reportRunId.substring(0, 8)}...
                </span>
              )}
            </div>
            
            {/* Progress bar */}
            <div className="bg-gray-200 dark:bg-gray-700 rounded-full h-3 w-full mb-2">
              <div 
                className={`h-3 rounded-full transition-all duration-300 ${
                  asyncJobInfo 
                    ? 'bg-gradient-to-r from-blue-500 to-purple-500' 
                    : 'bg-blue-600 animate-pulse'
                }`}
                style={{ 
                  width: asyncJobInfo ? `${loadingProgress.percentage}%` : '100%' 
                }}
              ></div>
            </div>
            
            {/* Progress details */}
            <div className="flex justify-between items-center text-sm text-gray-600 dark:text-gray-400">
              <span>
                {asyncJobInfo ? (
                  `${loadingProgress.percentage}% complete`
                ) : (
                  loadingProgress.pagesLoaded > 0 ? 
                    `${loadingProgress.pagesLoaded} pages loaded` : 
                    'Processing...'
                )}
              </span>
              {asyncJobInfo && (
                <span className="text-xs font-mono bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">
                  Async Mode
                </span>
              )}
            </div>
            
                         {/* Async job info */}
             {asyncJobInfo && (
               <div className="mt-3 p-3 bg-blue-50 dark:bg-blue-900/20 rounded border border-blue-200 dark:border-blue-800">
                 <div className="flex justify-between items-start">
                   <div className="text-sm flex-1">
                     <div className="font-medium text-blue-800 dark:text-blue-200 mb-1">
                       📊 Meta is processing your request on their servers
                     </div>
                     <div className="text-blue-600 dark:text-blue-300 text-xs">
                       Large requests are processed asynchronously for better performance. 
                       You'll see progress updates every few seconds.
                     </div>
                   </div>
                   <button
                     onClick={() => {
                       if (jobPollingInterval) {
                         clearInterval(jobPollingInterval);
                         setJobPollingInterval(null);
                       }
                       setIsLoading(false);
                       setAsyncJobInfo(null);
                       setLoadingProgress({ stage: '', pagesLoaded: 0, percentage: 0 });
                     }}
                     className="ml-3 px-3 py-1 bg-red-100 hover:bg-red-200 dark:bg-red-900 dark:hover:bg-red-800 
                              text-red-800 dark:text-red-200 text-xs rounded border border-red-300 dark:border-red-700 
                              transition-colors duration-200"
                   >
                     Cancel
                   </button>
                 </div>
               </div>
             )}
          </div>
        )}
      </div>

      {/* Futuristic API Command Center */}
      <div className="relative overflow-hidden bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 rounded-3xl shadow-2xl border border-purple-500/20 mb-6">
        {/* Animated background particles */}
        <div className="absolute inset-0 overflow-hidden">
          <div className="absolute top-0 left-1/4 w-2 h-2 bg-cyan-400 rounded-full animate-pulse opacity-60"></div>
          <div className="absolute top-1/3 right-1/4 w-1 h-1 bg-purple-400 rounded-full animate-ping opacity-40"></div>
          <div className="absolute bottom-1/4 left-1/3 w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse opacity-50"></div>
          <div className="absolute top-1/2 right-1/3 w-1 h-1 bg-pink-400 rounded-full animate-ping opacity-30"></div>
        </div>
        
        {/* Holographic grid overlay */}
        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-cyan-500/5 to-transparent animate-pulse"></div>
        
        <div className="relative p-8">
          {/* Futuristic Header */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center space-x-3 mb-4">
              <div className="w-3 h-3 bg-gradient-to-r from-cyan-400 to-blue-500 rounded-full animate-pulse"></div>
              <h2 className="text-3xl font-bold bg-gradient-to-r from-cyan-300 via-purple-300 to-pink-300 bg-clip-text text-transparent tracking-wide">
                QUANTUM API COMMAND CENTER
              </h2>
              <div className="w-3 h-3 bg-gradient-to-r from-pink-400 to-purple-500 rounded-full animate-pulse"></div>
            </div>
            <div className="flex justify-center items-center space-x-2 mb-2">
              <div className="h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent w-20"></div>
              <span className="text-cyan-300 text-sm font-mono tracking-wider">NEURAL INTERFACE v2100.1</span>
              <div className="h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent w-20"></div>
            </div>
            <p className="text-slate-300 text-sm max-w-2xl mx-auto leading-relaxed">
              Advanced Meta API templates with quantum-enhanced data processing and holographic visualization
            </p>
          </div>
          
                     {/* Template Grid */}
           <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-4">
            
            {/* Country Template - Enhanced */}
            <div className="group relative">
              {/* Glow effect */}
              <div className="absolute -inset-1 bg-gradient-to-r from-emerald-500 to-cyan-500 rounded-2xl blur opacity-20 group-hover:opacity-40 transition duration-300"></div>
              
              <div className="relative bg-gradient-to-br from-slate-800/80 to-slate-900/80 backdrop-blur-xl border border-emerald-500/30 rounded-2xl p-6 hover:border-emerald-400/60 transition-all duration-300">
                {/* Header with icon */}
                <div className="flex items-center space-x-3 mb-4">
                  <div className="w-10 h-10 bg-gradient-to-br from-emerald-400 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-emerald-500/20">
                    <span className="text-white font-bold text-lg">🌍</span>
                  </div>
                  <div>
                    <h3 className="text-xl font-bold text-emerald-300 tracking-wide">GEO-MATRIX SCAN</h3>
                    <div className="text-xs text-emerald-400/70 font-mono">MODULE_001 • COUNTRY_ANALYTICS</div>
                  </div>
                </div>
                
                {/* Description */}
                <p className="text-slate-300 text-sm mb-4 leading-relaxed">
                  Quantum-enhanced geographical performance analysis. Maps ad efficiency across global territories with neural pattern recognition.
                </p>
                
                {/* Tech specs panel */}
                <div className="bg-slate-900/60 rounded-xl p-4 mb-6 border border-emerald-500/20">
                  <div className="text-xs text-emerald-400 font-mono mb-2">NEURAL PARAMETERS:</div>
                  <div className="text-xs text-slate-300 font-mono space-y-1">
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">fields:</span>
                      <span className="text-yellow-300">ad_id, ad_name, campaign_*, impressions, clicks, spend, actions</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">breakdowns:</span>
                      <span className="text-emerald-300">country</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">output:</span>
                      <span className="text-pink-300">ad-country matrix pairs</span>
                    </div>
                  </div>
                </div>
                
                {/* Execute button */}
                <button
                  type="button"
                  onClick={() => handlePredefinedRequest('country')}
                  disabled={isLoading || !startDateInput || !endDateInput}
                  className="w-full h-14 bg-gradient-to-r from-emerald-500 to-cyan-500 text-white font-bold rounded-xl 
                           hover:from-emerald-400 hover:to-cyan-400 disabled:from-gray-600 disabled:to-gray-700 
                           transition-all duration-300 transform hover:scale-[1.02] active:scale-[0.98] 
                           shadow-lg shadow-emerald-500/25 hover:shadow-emerald-500/40 
                           disabled:opacity-50 disabled:cursor-not-allowed
                           relative overflow-hidden group/btn"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 transform -skew-x-12 -translate-x-full group-hover/btn:translate-x-full transition-transform duration-700"></div>
                  <span className="relative flex items-center justify-center space-x-2">
                    <span>{isLoading ? '⚡ PROCESSING...' : '🚀 EXECUTE GEO-SCAN'}</span>
                  </span>
                </button>
              </div>
            </div>

            {/* Device Template - Enhanced */}
            <div className="group relative">
              {/* Glow effect */}
              <div className="absolute -inset-1 bg-gradient-to-r from-purple-500 to-pink-500 rounded-2xl blur opacity-20 group-hover:opacity-40 transition duration-300"></div>
              
              <div className="relative bg-gradient-to-br from-slate-800/80 to-slate-900/80 backdrop-blur-xl border border-purple-500/30 rounded-2xl p-6 hover:border-purple-400/60 transition-all duration-300">
                {/* Header with icon */}
                <div className="flex items-center space-x-3 mb-4">
                  <div className="w-10 h-10 bg-gradient-to-br from-purple-400 to-pink-500 rounded-xl flex items-center justify-center shadow-lg shadow-purple-500/20">
                    <span className="text-white font-bold text-lg">📱</span>
                  </div>
                  <div>
                    <h3 className="text-xl font-bold text-purple-300 tracking-wide">DEVICE-NEXUS PROBE</h3>
                    <div className="text-xs text-purple-400/70 font-mono">MODULE_002 • DEVICE_ANALYTICS</div>
                  </div>
                </div>
                
                {/* Description */}
                <p className="text-slate-300 text-sm mb-4 leading-relaxed">
                  Multi-dimensional device ecosystem analysis. Identifies user behavior patterns across all connected platforms with AI precision.
                </p>
                
                {/* Tech specs panel */}
                <div className="bg-slate-900/60 rounded-xl p-4 mb-6 border border-purple-500/20">
                  <div className="text-xs text-purple-400 font-mono mb-2">NEURAL PARAMETERS:</div>
                  <div className="text-xs text-slate-300 font-mono space-y-1">
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">fields:</span>
                      <span className="text-yellow-300">ad_id, ad_name, campaign_*, impressions, clicks, spend, actions</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">breakdowns:</span>
                      <span className="text-purple-300">impression_device</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">actions:</span>
                      <span className="text-pink-300">action_device [optional_split]</span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-cyan-400">output:</span>
                      <span className="text-pink-300">ad-device matrix pairs</span>
                    </div>
                  </div>
                </div>
                
                {/* Execute button */}
                <button
                  type="button"
                  onClick={() => handlePredefinedRequest('device')}
                  disabled={isLoading || !startDateInput || !endDateInput}
                  className="w-full h-14 bg-gradient-to-r from-purple-500 to-pink-500 text-white font-bold rounded-xl 
                           hover:from-purple-400 hover:to-pink-400 disabled:from-gray-600 disabled:to-gray-700 
                           transition-all duration-300 transform hover:scale-[1.02] active:scale-[0.98] 
                           shadow-lg shadow-purple-500/25 hover:shadow-purple-500/40 
                           disabled:opacity-50 disabled:cursor-not-allowed
                           relative overflow-hidden group/btn"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 transform -skew-x-12 -translate-x-full group-hover/btn:translate-x-full transition-transform duration-700"></div>
                  <span className="relative flex items-center justify-center space-x-2">
                    <span>{isLoading ? '⚡ PROCESSING...' : '🔬 EXECUTE DEVICE-SCAN'}</span>
                  </span>
                                 </button>
               </div>
             </div>

             {/* Region Template - Enhanced */}
             <div className="group relative">
               {/* Glow effect */}
               <div className="absolute -inset-1 bg-gradient-to-r from-orange-500 to-red-500 rounded-2xl blur opacity-20 group-hover:opacity-40 transition duration-300"></div>
               
               <div className="relative bg-gradient-to-br from-slate-800/80 to-slate-900/80 backdrop-blur-xl border border-orange-500/30 rounded-2xl p-6 hover:border-orange-400/60 transition-all duration-300">
                 {/* Header with icon */}
                 <div className="flex items-center space-x-3 mb-4">
                   <div className="w-10 h-10 bg-gradient-to-br from-orange-400 to-red-500 rounded-xl flex items-center justify-center shadow-lg shadow-orange-500/20">
                     <span className="text-white font-bold text-lg">🗺️</span>
                   </div>
                   <div>
                     <h3 className="text-xl font-bold text-orange-300 tracking-wide">REGIONAL-ZONE MAPPER</h3>
                     <div className="text-xs text-orange-400/70 font-mono">MODULE_003 • REGION_ANALYTICS</div>
                   </div>
                 </div>
                 
                 {/* Description */}
                 <p className="text-slate-300 text-sm mb-4 leading-relaxed">
                   Continental-scale performance intelligence. Aggregates advertising metrics across major world regions with territorial AI clustering.
                 </p>
                 
                 {/* Tech specs panel */}
                 <div className="bg-slate-900/60 rounded-xl p-4 mb-6 border border-orange-500/20">
                   <div className="text-xs text-orange-400 font-mono mb-2">NEURAL PARAMETERS:</div>
                   <div className="text-xs text-slate-300 font-mono space-y-1">
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">fields:</span>
                       <span className="text-yellow-300">ad_id, ad_name, campaign_*, impressions, clicks, spend, actions</span>
                     </div>
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">breakdowns:</span>
                       <span className="text-orange-300">region</span>
                     </div>
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">output:</span>
                       <span className="text-pink-300">ad-region matrix pairs</span>
                     </div>
                   </div>
                 </div>
                 
                 {/* Execute button */}
                 <button
                   type="button"
                   onClick={() => handlePredefinedRequest('region')}
                   disabled={isLoading || !startDateInput || !endDateInput}
                   className="w-full h-14 bg-gradient-to-r from-orange-500 to-red-500 text-white font-bold rounded-xl 
                            hover:from-orange-400 hover:to-red-400 disabled:from-gray-600 disabled:to-gray-700 
                            transition-all duration-300 transform hover:scale-[1.02] active:scale-[0.98] 
                            shadow-lg shadow-orange-500/25 hover:shadow-orange-500/40 
                            disabled:opacity-50 disabled:cursor-not-allowed
                            relative overflow-hidden group/btn"
                 >
                   <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 transform -skew-x-12 -translate-x-full group-hover/btn:translate-x-full transition-transform duration-700"></div>
                   <span className="relative flex items-center justify-center space-x-2">
                     <span>{isLoading ? '⚡ PROCESSING...' : '🌐 EXECUTE REGION-SCAN'}</span>
                   </span>
                 </button>
               </div>
             </div>

             {/* Country + Device Hybrid Template */}
             <div className="group relative">
               {/* Glow effect */}
               <div className="absolute -inset-1 bg-gradient-to-r from-yellow-500 to-amber-500 rounded-2xl blur opacity-20 group-hover:opacity-40 transition duration-300"></div>
               
               <div className="relative bg-gradient-to-br from-slate-800/80 to-slate-900/80 backdrop-blur-xl border border-yellow-500/30 rounded-2xl p-6 hover:border-yellow-400/60 transition-all duration-300">
                 {/* Header with icon */}
                 <div className="flex items-center space-x-3 mb-4">
                   <div className="w-10 h-10 bg-gradient-to-br from-yellow-400 to-amber-500 rounded-xl flex items-center justify-center shadow-lg shadow-yellow-500/20">
                     <span className="text-white font-bold text-lg">🌍📱</span>
                   </div>
                   <div>
                     <h3 className="text-lg font-bold text-yellow-300 tracking-wide">HYBRID GEO-DEVICE</h3>
                     <div className="text-xs text-yellow-400/70 font-mono">MODULE_004 • CROSS_DIMENSIONAL</div>
                   </div>
                 </div>
                 
                 {/* Description */}
                 <p className="text-slate-300 text-xs mb-3 leading-relaxed">
                   TESTED RECIPE: Attempts to get country rows with device-attributed actions. Meta API has confirmed this combination is not supported.
                 </p>
                 
                 {/* Tech specs panel */}
                 <div className="bg-slate-900/60 rounded-xl p-3 mb-4 border border-red-500/20">
                   <div className="text-xs text-red-400 font-mono mb-2">FAILED PARAMS:</div>
                   <div className="text-xs text-slate-300 font-mono space-y-1">
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">fields:</span>
                       <span className="text-yellow-300">campaign_*, impressions, actions</span>
                     </div>
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">breakdowns:</span>
                       <span className="text-yellow-300">country</span>
                     </div>
                     <div className="flex items-center space-x-2">
                       <span className="text-cyan-400">action_breakdowns:</span>
                       <span className="text-pink-300">action_device</span>
                     </div>
                     <div className="flex items-center space-x-2">
                       <span className="text-red-400">error:</span>
                       <span className="text-red-300">Invalid combination</span>
                     </div>
                   </div>
                 </div>
                 
                 {/* Error */}
                 <div className="bg-red-900/40 border border-red-500/30 rounded-lg p-2 mb-4">
                   <div className="text-xs text-red-200">
                     ❌ <strong>Meta API Error:</strong> "Current combination of data breakdown columns (action_device, country) is invalid"
                   </div>
                 </div>
                 
                 {/* Execute button */}
                 <button
                   type="button"
                   onClick={() => handlePredefinedRequest('country_device')}
                   disabled={isLoading || !startDateInput || !endDateInput}
                   className="w-full h-12 bg-gradient-to-r from-red-500 to-red-600 text-white font-bold rounded-xl 
                            hover:from-red-400 hover:to-red-500 disabled:from-gray-600 disabled:to-gray-700 
                            transition-all duration-300 transform hover:scale-[1.02] active:scale-[0.98] 
                            shadow-lg shadow-red-500/25 hover:shadow-red-500/40 
                            disabled:opacity-50 disabled:cursor-not-allowed
                            relative overflow-hidden group/btn text-sm"
                 >
                   <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 transform -skew-x-12 -translate-x-full group-hover/btn:translate-x-full transition-transform duration-700"></div>
                   <span className="relative flex items-center justify-center space-x-2">
                     <span>{isLoading ? '⚡ FAILING...' : '❌ DEMO ERROR'}</span>
                   </span>
                 </button>
               </div>
             </div>
           </div>
          
          {/* Status Indicator */}
          {!startDateInput || !endDateInput ? (
            <div className="mt-8 relative">
              <div className="absolute -inset-1 bg-gradient-to-r from-orange-500 to-red-500 rounded-2xl blur opacity-20"></div>
              <div className="relative bg-slate-900/80 backdrop-blur-xl border border-orange-500/40 rounded-2xl p-4">
                <div className="flex items-center space-x-3">
                  <div className="w-8 h-8 bg-gradient-to-br from-orange-400 to-red-500 rounded-lg flex items-center justify-center">
                    <span className="text-white font-bold">⚠️</span>
                  </div>
                  <div>
                    <div className="text-orange-300 font-bold">TEMPORAL PARAMETERS REQUIRED</div>
                    <div className="text-orange-200 text-sm">Initialize start and end dates in the chronometer above to activate quantum templates</div>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="mt-8 text-center">
              <div className="inline-flex items-center space-x-2 text-cyan-300 text-sm">
                <div className="w-2 h-2 bg-cyan-400 rounded-full animate-pulse"></div>
                <span className="font-mono">QUANTUM TEMPLATES • READY FOR EXECUTION</span>
                <div className="w-2 h-2 bg-cyan-400 rounded-full animate-pulse"></div>
              </div>
            </div>
          )}
          
          {/* Technical Explanation */}
          <div className="mt-8 bg-slate-900/60 backdrop-blur-xl border border-blue-500/20 rounded-2xl p-6">
            <h3 className="text-lg font-bold text-blue-300 mb-4 flex items-center">
              <span className="mr-2">🧠</span>
              DIMENSIONAL ANALYSIS LIMITATIONS
            </h3>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-slate-300">
              <div>
                <h4 className="font-semibold text-cyan-300 mb-2">✅ Single Dimension (Supported)</h4>
                <ul className="space-y-1 text-xs">
                  <li>• <span className="text-emerald-300">Country only</span> - Shows performance by country</li>
                  <li>• <span className="text-purple-300">Device only</span> - Shows performance by device + placement</li>
                  <li>• <span className="text-orange-300">Region only</span> - Shows performance by region/state</li>
                </ul>
              </div>
              
              <div>
                <h4 className="font-semibold text-red-300 mb-2">❌ Hybrid Experiment Failed</h4>
                <div className="text-xs">
                  <p className="mb-2">The <span className="text-red-300">HYBRID GEO-DEVICE</span> tested Meta's <code>action_breakdowns</code>:</p>
                  <ul className="space-y-1">
                    <li>• <span className="text-cyan-300">Goal:</span> Country rows + device action splits</li>
                    <li>• <span className="text-red-300">Error:</span> "Invalid combination of data breakdown columns"</li>
                    <li>• <span className="text-red-300">Result:</span> Meta API rejects this combination</li>
                  </ul>
                </div>
              </div>
            </div>
            
            <div className="mt-6 p-4 bg-red-900/30 border border-red-500/30 rounded-lg">
              <h4 className="font-semibold text-red-300 mb-2">⚠️ Attribution Problem</h4>
              <p className="text-xs text-red-200 leading-relaxed">
                Separate API calls cannot be combined for full attribution. Example: You cannot determine 
                "iOS users in USA vs Android users in USA" by making separate country and device calls, 
                because there's no way to cross-reference the user data between calls.
              </p>
            </div>
            
            <div className="mt-4 p-4 bg-blue-900/30 border border-blue-500/30 rounded-lg">
              <h4 className="font-semibold text-blue-300 mb-2">💡 Confirmed Solution</h4>
              <p className="text-xs text-blue-200 leading-relaxed">
                The <span className="text-red-300">HYBRID GEO-DEVICE</span> test confirms Meta API doesn't support 
                cross-dimensional attribution. For geo-device insights, you must make separate API calls 
                (country-only and device-only) and analyze the data independently. Full cross-attribution is not possible.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Results Section */}
      {results && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Results</h2>
          
          <div className="mb-4">
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded">
                <span className="font-medium">Total Records:</span> {results.meta?.total_records || 0}
              </div>
              <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded">
                <span className="font-medium">Pages Fetched:</span> {results.meta?.pages_fetched || 0}
              </div>
              <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded">
                <span className="font-medium">Date Range:</span> {results.meta?.start_date} to {results.meta?.end_date}
              </div>
              <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded">
                <span className="font-medium">Time Increment:</span> {results.meta?.time_increment || 1} day(s)
              </div>
            </div>
            
            <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded mb-4">
              <span className="font-medium">Fields:</span> {results.meta?.fields}
            </div>
            
            {results.meta?.breakdowns && (
              <div className="bg-gray-100 dark:bg-gray-700 p-3 rounded mb-4">
                <span className="font-medium">Breakdowns:</span> {results.meta?.breakdowns}
              </div>
            )}
          </div>
          
          {results.data && results.data.data && results.data.data.length > 0 ? (
            <div>
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-medium">Data Sample ({results.data.data.length} records)</h3>
                <div className="flex items-center space-x-4">
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={showMappedData}
                      onChange={(e) => setShowMappedData(e.target.checked)}
                      className="mr-2"
                    />
                    Show Business Concepts
                  </label>
                </div>
              </div>

              {/* Business Concept Summary */}
              {showMappedData && Object.keys(actionMappings).length > 0 && results.data.business_summary && (
                <div className="mb-6">
                  <h4 className="text-md font-medium mb-3">Business Concepts Summary</h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-4">
                    {Object.entries(results.data.business_summary).map(([conceptName, summary]) => (
                      <div key={conceptName} className="bg-blue-50 dark:bg-blue-900 p-4 rounded-lg">
                        <h5 className="font-medium text-blue-800 dark:text-blue-200 capitalize">{conceptName}</h5>
                        <div className="text-sm text-blue-600 dark:text-blue-300 mt-2 space-y-1">
                          <div>Count: {summary.total_count.toFixed(0)}</div>
                          <div>Value: ${summary.total_value.toFixed(2)}</div>
                          <div>Conversions: {summary.total_conversions.toFixed(0)}</div>
                          <div>Conv. Value: ${summary.total_conversion_value.toFixed(2)}</div>
                          <div className="text-xs">Records: {summary.records_with_data}/{results.data.data.length}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                  {Object.keys(actionMappings).length === 0 && (
                    <div className="text-sm text-gray-500 italic">
                      No action mappings configured. Go to the Action Mapping tab to set up business concepts.
                    </div>
                  )}
                </div>
              )}

              {/* Raw Data Display */}
              <div className="relative">
                <div className="flex justify-between items-center mb-2">
                  <h4 className="text-md font-medium">Raw JSON Data</h4>
                  <button
                    onClick={(e) => {
                      const jsonData = JSON.stringify(formatNestedValues(results.data.data), null, 2);
                      navigator.clipboard.writeText(jsonData).then(() => {
                        // Show a temporary success message
                        const button = e.target;
                        const originalText = button.textContent;
                        button.textContent = '✅ Copied!';
                        button.classList.add('bg-green-500', 'text-white');
                        setTimeout(() => {
                          button.textContent = originalText;
                          button.classList.remove('bg-green-500', 'text-white');
                        }, 2000);
                      }).catch(err => {
                        console.error('Failed to copy:', err);
                        alert('Failed to copy to clipboard');
                      });
                    }}
                    className="px-3 py-1 bg-blue-500 hover:bg-blue-600 text-white text-sm rounded 
                             transition-colors duration-200 flex items-center space-x-1"
                  >
                    <span>📋</span>
                    <span>Copy All JSON</span>
                  </button>
                </div>
                <div className="bg-gray-100 dark:bg-gray-900 p-4 rounded overflow-auto max-h-96">
                  <pre className="text-xs">{JSON.stringify(formatNestedValues(results.data.data), null, 2)}</pre>
                </div>
              </div>
            </div>
          ) : (
            <p className="text-gray-500">No data returned from API</p>
          )}
        </div>
      )}
    </div>
  );
};

export default LiveApiTester; 