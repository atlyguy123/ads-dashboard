import React, { useState, useEffect, useRef } from 'react';
import { api } from '../../services/api';
import DataCoverageDisplay from './DataCoverageDisplay';
import JobManager from './JobManager';
import HistoricalDataViewer from './HistoricalDataViewer';
import { getSelectedFieldsString, getSelectedBreakdownsString, validateMetaBreakdownCombo, isBreakdownDisabled, getDisabledTooltip } from './utils/metaApiUtils';
import { FIELD_CATEGORIES, BREAKDOWN_CATEGORIES, ACTION_TYPE_ALLOWED_BREAKDOWNS } from './utils/metaConstants';

const HistoricalDataManager = ({
  startDateInput,
  setStartDateInput,
  endDateInput,
  setEndDateInput,
  selectedFields,
  setSelectedFields,
  selectedBreakdowns,
  setSelectedBreakdowns,
  error,
  setError
}) => {
  // Historical data state
  const [activeJobs, setActiveJobs] = useState([]);
  const [configurations, setConfigurations] = useState([]);
  const [dataCoverage, setDataCoverage] = useState(null);
  const [missingDates, setMissingDates] = useState([]);
  const [showDataViewer, setShowDataViewer] = useState(false);
  const [selectedConfig, setSelectedConfig] = useState(null);
  const [exportData, setExportData] = useState(null);
  
  // Field/breakdown selection UI state
  const [showFieldSelector, setShowFieldSelector] = useState(true);
  const [showBreakdownSelector, setShowBreakdownSelector] = useState(false);
  
  // Ref for job polling interval - THIS FIXES THE ESLINT ERROR
  const jobPollingRef = useRef(null);

  // useEffect for polling job status
  useEffect(() => {
    const hasRunningJobs = activeJobs.some(job => 
      job.status === 'running' || job.status === 'starting'
    );
    
    if (hasRunningJobs) {
      jobPollingRef.current = setInterval(() => {
        pollActiveJobs();
      }, 2000); // Poll every 2 seconds
    } else {
      if (jobPollingRef.current) {
        clearInterval(jobPollingRef.current);
      }
    }

    return () => {
      if (jobPollingRef.current) {
        clearInterval(jobPollingRef.current);
      }
    };
  }, [activeJobs]);

  // Load configurations when component mounts
  useEffect(() => {
    loadConfigurations();
    updateDataCoverage();
  }, [selectedFields, selectedBreakdowns]);

  // Clear stale jobs on component mount
  useEffect(() => {
    setActiveJobs([]);
    // Clean up any old completed jobs periodically
    const cleanupInterval = setInterval(() => {
      setActiveJobs(prev => prev.filter(job => {
        // Keep running jobs and recently completed jobs (last 5 minutes)
        if (job.status === 'running' || job.status === 'starting') {
          return true;
        }
        
        // For completed jobs, check if they're recent
        if (job.end_time) {
          const endTime = new Date(job.end_time);
          const now = new Date();
          const timeDiff = now - endTime;
          return timeDiff < 5 * 60 * 1000; // Keep for 5 minutes
        }
        
        // Keep jobs without end_time for now (they might still be processing)
        return true;
      }));
    }, 60000); // Check every minute
    
    return () => clearInterval(cleanupInterval);
  }, []);

  // Functions for historical data management
  const loadConfigurations = async () => {
    try {
      const configs = await api.getHistoricalConfigurations();
      setConfigurations(configs);
    } catch (error) {
      console.error('Error loading configurations:', error);
    }
  };

  const updateDataCoverage = async () => {
    const fieldsString = getSelectedFieldsString(selectedFields);
    const breakdownsString = getSelectedBreakdownsString(selectedBreakdowns);
    
    if (!fieldsString) return;

    try {
      const coverage = await api.getDataCoverage({
        fields: fieldsString,
        breakdowns: breakdownsString,
        start_date: startDateInput || undefined,
        end_date: endDateInput || undefined
      });
      setDataCoverage(coverage);

      if (startDateInput && endDateInput) {
        const missing = await api.getMissingDates({
          start_date: startDateInput,
          end_date: endDateInput,
          fields: fieldsString,
          breakdowns: breakdownsString
        });
        setMissingDates(missing.missing_dates || []);
      }
    } catch (error) {
      console.error('Error updating data coverage:', error);
    }
  };

  const startHistoricalCollection = async () => {
    if (!startDateInput || !endDateInput) {
      setError('Start date and end date are required for historical collection');
      return;
    }

    const fieldsString = getSelectedFieldsString(selectedFields);
    if (!fieldsString) {
      setError('At least one field must be selected');
      return;
    }

    // Validate field/breakdown combination
    const validation = validateMetaBreakdownCombo(selectedFields, selectedBreakdowns);
    if (!validation.valid) {
      setError(validation.errors[0]);
      return;
    }

    try {
      const params = {
        start_date: startDateInput,
        end_date: endDateInput,
        fields: fieldsString,
        breakdowns: getSelectedBreakdownsString(selectedBreakdowns) || undefined
      };

      const result = await api.startHistoricalCollection(params);
      
      // Add to active jobs
      const newJob = {
        job_id: result.job_id,
        status: 'running',
        start_date: startDateInput,
        end_date: endDateInput,
        fields: fieldsString,
        breakdowns: getSelectedBreakdownsString(selectedBreakdowns) || '',
        progress_percentage: 0
      };
      
      setActiveJobs(prev => [...prev, newJob]);
      setError(null);
      
      // Update data coverage
      updateDataCoverage();
      
    } catch (error) {
      setError(error.response?.data?.error || error.message || 'Failed to start historical collection');
    }
  };

  const pollActiveJobs = async () => {
    const runningJobs = activeJobs.filter(job => 
      job.status === 'running' || job.status === 'starting'
    );

    if (runningJobs.length === 0) {
      return; // No jobs to poll, but keep completed jobs in the list for display
    }

    const updatedJobs = await Promise.all(
      runningJobs.map(async (job) => {
        try {
          const status = await api.getHistoricalJobStatus(job.job_id);
          return { ...job, ...status };
        } catch (error) {
          console.error(`Error polling job ${job.job_id}:`, error);
          
          // If job not found (404), it might have completed successfully
          // Check if data was actually collected by refreshing coverage
          if (error.response?.status === 404) {
            console.log(`Job ${job.job_id} not found in active tracking, checking if data was collected...`);
            
            // Update data coverage to see if data actually exists
            updateDataCoverage();
            
            // Mark job as potentially completed
            return { 
              ...job, 
              status: 'completed_unknown',
              message: 'Job completed but status unavailable. Check data coverage for results.'
            };
          }
          
          // For other errors, keep the job as-is for now
          return job;
        }
      })
    );

    setActiveJobs(prev => {
      const nonRunningJobs = prev.filter(job => 
        !runningJobs.find(rj => rj.job_id === job.job_id)
      );
      return [...nonRunningJobs, ...updatedJobs];
    });

    // Update data coverage if any jobs completed or had status issues
    if (updatedJobs.some(job => 
      job.status === 'completed' || 
      job.status === 'completed_with_errors' ||
      job.status === 'completed_unknown'
    )) {
      updateDataCoverage();
    }
  };

  const cancelJob = async (jobId) => {
    try {
      await api.cancelHistoricalJob(jobId);
      setActiveJobs(prev => 
        prev.map(job => 
          job.job_id === jobId 
            ? { ...job, status: 'cancelled' }
            : job
        )
      );
    } catch (error) {
      console.error('Error cancelling job:', error);
    }
  };

  const exportSelectedData = async () => {
    if (!selectedConfig) return;

    try {
      const exported = await api.exportHistoricalData({
        start_date: startDateInput,
        end_date: endDateInput,
        fields: selectedConfig.fields,
        breakdowns: selectedConfig.breakdowns
      });
      
      setExportData(exported);
    } catch (error) {
      console.error('Error exporting data:', error);
      setError('Failed to export data');
    }
  };

  // Field and breakdown selection functions
  const toggleField = (fieldId) => {
    setSelectedFields(prev => ({
      ...prev,
      [fieldId]: !prev[fieldId]
    }));
  };
  
  const toggleBreakdown = (breakdownId) => {
    if (isBreakdownDisabled(breakdownId, selectedFields, selectedBreakdowns)) {
      return; // Don't toggle if disabled
    }
    
    setSelectedBreakdowns(prev => ({
      ...prev,
      [breakdownId]: !prev[breakdownId]
    }));
  };

  const selectAllInCategory = (categoryFields) => {
    const newSelectedFields = { ...selectedFields };
    categoryFields.forEach(field => {
      newSelectedFields[field.id] = true;
    });
    setSelectedFields(newSelectedFields);
  };
  
  const deselectAllInCategory = (categoryFields) => {
    const newSelectedFields = { ...selectedFields };
    categoryFields.forEach(field => {
      newSelectedFields[field.id] = false;
    });
    setSelectedFields(newSelectedFields);
  };

  const selectAllBreakdownsInCategory = (categoryBreakdowns) => {
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
  
  const deselectAllBreakdownsInCategory = (categoryBreakdowns) => {
    const newSelectedBreakdowns = { ...selectedBreakdowns };
    categoryBreakdowns.forEach(breakdown => {
      newSelectedBreakdowns[breakdown.id] = false;
    });
    setSelectedBreakdowns(newSelectedBreakdowns);
  };

  return (
    <div className="space-y-6">
      {/* Historical Data Viewer - Auto-loads all existing data */}
      <HistoricalDataViewer />

      {/* Data Coverage Summary */}
      <DataCoverageDisplay 
        dataCoverage={dataCoverage}
        missingDates={missingDates}
        configurations={configurations}
      />

      {/* Active Jobs */}
      <JobManager 
        activeJobs={activeJobs}
        cancelJob={cancelJob}
      />

      {/* Historical Collection Configuration */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Configure Historical Collection</h2>
        
        {/* Date Range */}
        <div className="mb-6">
          <h3 className="text-lg font-medium mb-3">Date Range</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">
                Start Date (YYYY-MM-DD)<span className="text-red-500"> *</span>
              </label>
              <input
                type="text"
                value={startDateInput}
                onChange={(e) => setStartDateInput(e.target.value)}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="e.g., 2023-05-01"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">
                End Date (YYYY-MM-DD)<span className="text-red-500"> *</span>
              </label>
              <input
                type="text"
                value={endDateInput}
                onChange={(e) => setEndDateInput(e.target.value)}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="e.g., 2023-05-31"
              />
            </div>
          </div>
        </div>

        {/* Field Selection */}
        <div className="mb-6">
          <div className="flex justify-between items-center mb-3">
            <h3 className="text-lg font-medium">
              Select Fields for Collection<span className="text-red-500"> *</span>
            </h3>
            <div className="flex items-center space-x-4">
              <span className="text-sm text-gray-500">
                {Object.values(selectedFields).filter(Boolean).length} fields selected
              </span>
              <button
                type="button"
                onClick={() => setShowFieldSelector(!showFieldSelector)}
                className="text-blue-600 hover:text-blue-800 text-sm"
              >
                {showFieldSelector ? 'Hide' : 'Show'} Fields
              </button>
            </div>
          </div>
          
          {showFieldSelector && (
            <div className="border rounded p-4 bg-gray-50 dark:bg-gray-700">
              {FIELD_CATEGORIES.map((category, index) => (
                <div key={index} className="mb-4">
                  <div className="flex justify-between items-center mb-2">
                    <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">{category.name}</h4>
                    <div className="space-x-2">
                      <button 
                        type="button" 
                        onClick={() => selectAllInCategory(category.fields)}
                        className="text-xs text-blue-600 hover:text-blue-800"
                      >
                        Select All
                      </button>
                      <button 
                        type="button" 
                        onClick={() => deselectAllInCategory(category.fields)}
                        className="text-xs text-red-600 hover:text-red-800"
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
                              : 'bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-gray-300'
                          }`}
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
          )}
        </div>

        {/* Breakdown Selection */}
        <div className="mb-6">
          <div className="flex justify-between items-center mb-3">
            <h3 className="text-lg font-medium">Select Breakdowns (Optional)</h3>
            <div className="flex items-center space-x-4">
              <span className="text-sm text-gray-500">
                {Object.values(selectedBreakdowns).filter(Boolean).length} breakdowns selected
              </span>
              <button
                type="button"
                onClick={() => setShowBreakdownSelector(!showBreakdownSelector)}
                className="text-blue-600 hover:text-blue-800 text-sm"
              >
                {showBreakdownSelector ? 'Hide' : 'Show'} Breakdowns
              </button>
            </div>
          </div>
          
          {showBreakdownSelector && (
            <div className="border rounded p-4 bg-gray-50 dark:bg-gray-700">
              <div className="text-xs bg-yellow-100 dark:bg-yellow-900 p-2 rounded mb-3 text-yellow-800 dark:text-yellow-300">
                <strong>Historical Collection Rules:</strong>
                <ul className="mt-1 ml-2 list-disc pl-4">
                  <li>Maximum 2 breakdowns allowed</li>
                  <li>With <strong>⚡ action metrics</strong>, only <code>conversion_destination</code> breakdown is allowed</li>
                  <li>Historical jobs will collect data for ALL days in the date range with the selected configuration</li>
                </ul>
              </div>
              
              {BREAKDOWN_CATEGORIES.map((category, index) => (
                <div key={index} className="mb-4">
                  <div className="flex justify-between items-center mb-2">
                    <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">{category.name}</h4>
                    <div className="space-x-2">
                      <button 
                        type="button" 
                        onClick={() => selectAllBreakdownsInCategory(category.breakdowns)}
                        className="text-xs text-blue-600 hover:text-blue-800"
                        disabled={category.breakdowns.some(b => isBreakdownDisabled(b.id, selectedFields, selectedBreakdowns))}
                      >
                        Select All
                      </button>
                      <button 
                        type="button" 
                        onClick={() => deselectAllBreakdownsInCategory(category.breakdowns)}
                        className="text-xs text-red-600 hover:text-red-800"
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
                                : 'bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-300'
                          }`}
                          disabled={isDisabled}
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
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Collection Summary */}
        {startDateInput && endDateInput && Object.values(selectedFields).some(Boolean) && (
          <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900 rounded-lg">
            <h4 className="font-medium text-blue-800 dark:text-blue-200 mb-2">Collection Summary</h4>
            <div className="text-sm text-blue-700 dark:text-blue-300 space-y-1">
              <div><strong>Date Range:</strong> {startDateInput} to {endDateInput}</div>
              <div><strong>Fields:</strong> {getSelectedFieldsString(selectedFields)}</div>
              {getSelectedBreakdownsString(selectedBreakdowns) && (
                <div><strong>Breakdowns:</strong> {getSelectedBreakdownsString(selectedBreakdowns)}</div>
              )}
              {missingDates.length > 0 && (
                <div className="text-orange-700 dark:text-orange-300">
                  <strong>{missingDates.length} days</strong> need to be collected
                  {missingDates.length <= 5 && ` (${missingDates.join(', ')})`}
                </div>
              )}
            </div>
          </div>
        )}

        {/* Start Collection Button */}
        <button
          onClick={startHistoricalCollection}
          disabled={!startDateInput || !endDateInput || !Object.values(selectedFields).some(Boolean)}
          className="px-6 py-3 bg-green-600 text-white rounded hover:bg-green-700 disabled:bg-gray-400"
        >
          Start Historical Collection Job
        </button>
      </div>

      {/* Data Export Section */}
      {exportData && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Exported Data</h2>
          <div className="bg-gray-100 dark:bg-gray-900 p-4 rounded overflow-auto max-h-96">
            <pre className="text-xs">{JSON.stringify(exportData, null, 2)}</pre>
          </div>
        </div>
      )}
    </div>
  );
};

export default HistoricalDataManager; 