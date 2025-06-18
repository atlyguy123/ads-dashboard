import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../services/api';

// Helper function to safely access localStorage
const safeLocalStorage = {
  getItem: (key) => {
    try {
      if (typeof window !== 'undefined' && window.localStorage) {
        return localStorage.getItem(key);
      }
      return null;
    } catch (err) {
      console.error('Error accessing localStorage:', err);
      return null;
    }
  },
  setItem: (key, value) => {
    try {
      if (typeof window !== 'undefined' && window.localStorage) {
        localStorage.setItem(key, value);
        return true;
      }
      return false;
    } catch (err) {
      console.error('Error setting localStorage:', err);
      return false;
    }
  }
};

export const MixpanelDebugPage = () => {
  // Date input state
  const [startDateInput, setStartDateInput] = useState('');
  
  // Mixpanel data loading state
  const [lastLoadTimestamp, setLastLoadTimestamp] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStatus, setProcessingStatus] = useState(null);
  const [processingError, setProcessingError] = useState(null);
  const [processingProgress, setProcessingProgress] = useState({
    stage: null, // download, extract, filter, ingest
    percent: 0,
    currentDate: null
  });
  
  // DB test state
  const [isTestingDb, setIsTestingDb] = useState(false);
  const [dbTestResults, setDbTestResults] = useState(null);
  const [dbTestError, setDbTestError] = useState(null);

  // Request history state
  const [requestHistory, setRequestHistory] = useState([]);

  // Add new state for database stats
  const [dbtStats, setDbStats] = useState({
    totalEvents: 0, 
    totalUsers: 0,
    eventBreakdown: [],
    monthlyBreakdown: [],
    dailyBreakdown: [],
    dateRange: { earliest: null, latest: null }
  });
  const [isLoadingStats, setIsLoadingStats] = useState(false);

  // Add new state for user events lookup
  const [userIdInput, setUserIdInput] = useState('');
  const [userEvents, setUserEvents] = useState([]);
  const [isLoadingUserEvents, setIsLoadingUserEvents] = useState(false);
  const [userEventsError, setUserEventsError] = useState(null);

  // Add new state for database reset
  const [isResettingDatabase, setIsResettingDatabase] = useState(false);
  const [resetDatabaseStatus, setResetDatabaseStatus] = useState(null);
  const [resetDatabaseError, setResetDatabaseError] = useState(null);

  // Add new state for refresh data functionality
  const [isRefreshingData, setIsRefreshingData] = useState(false);
  const [refreshDataStatus, setRefreshDataStatus] = useState(null);
  const [refreshDataError, setRefreshDataError] = useState(null);

  // Add new state for continue from last date functionality
  const [latestProcessedDate, setLatestProcessedDate] = useState(null);
  const [isLoadingLatestDate, setIsLoadingLatestDate] = useState(false);
  const [continueFromLastDateError, setContinueFromLastDateError] = useState(null);

  // Fetch the last load timestamp from the API
  const fetchLastLoadTimestamp = useCallback(async () => {
    try {
      const result = await api.getMixpanelDebugSyncTS();
      setLastLoadTimestamp(result.last_load_timestamp);
    } catch (err) {
      console.error('Error fetching last load timestamp:', err);
    }
  }, []);

  // Check the current processing status
  const checkProcessStatus = useCallback(async () => {
    try {
      const status = await api.getMixpanelProcessStatus();
      
      setProcessingProgress({
        stage: status.current_stage,
        percent: status.percent_complete,
        currentDate: status.current_date
      });
      
      if (status.is_complete) {
        setIsProcessing(false);
        setProcessingStatus('Processing complete!');
        fetchLastLoadTimestamp(); // Update the last timestamp
      } else if (status.error) {
        setIsProcessing(false);
        setProcessingError(status.error);
      }
    } catch (err) {
      console.error('Error checking process status:', err);
    }
  }, [fetchLastLoadTimestamp]);

  // Load saved values and request history from localStorage on initial render
  useEffect(() => {
    // Load request history
    const savedHistory = safeLocalStorage.getItem('mixpanelRequestHistory');
    if (savedHistory) {
      try {
        setRequestHistory(JSON.parse(savedHistory));
      } catch (e) {
        console.error('Error parsing request history:', e);
        setRequestHistory([]);
      }
    }

    // Fetch the last load timestamp
    fetchLastLoadTimestamp();

    // Set up polling for process status
    const interval = setInterval(() => {
      if (isProcessing) {
        checkProcessStatus();
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [isProcessing, checkProcessStatus, fetchLastLoadTimestamp]);

  // Handle starting the Mixpanel data processing
  const handleStartProcessing = async () => {
    if (!startDateInput) {
      setProcessingError('Start date is required');
      return;
    }
    
    setIsProcessing(true);
    setProcessingStatus(null);
    setProcessingError(null);
    setProcessingProgress({
      stage: 'initializing',
      percent: 0,
      currentDate: null
    });

    try {
      // Start the processing pipeline
      const result = await api.startMixpanelProcessing({
        start_date: startDateInput,
        wipe_folder: true // Always wipe the folder as requested
      });
      
      if (result.success) {
        setProcessingStatus('Processing started successfully');
      } else {
        throw new Error(result.error || 'Failed to start processing');
      }
    } catch (err) {
      setProcessingError(err.message || 'An unknown error occurred');
      setIsProcessing(false);
    }
  };

  // Handle canceling the current process
  const handleCancelProcessing = async () => {
    try {
      await api.cancelMixpanelProcessing();
      setIsProcessing(false);
      setProcessingStatus('Processing canceled');
    } catch (err) {
      setProcessingError(err.message || 'Failed to cancel processing');
    }
  };

  // Handle resetting the last sync counter
  const handleResetSyncCounter = async () => {
    setProcessingStatus(null);
    setProcessingError(null);

    try {
      const result = await api.resetMixpanelDebugSyncTS();
      setProcessingStatus(result.message);
      setLastLoadTimestamp(null);
    } catch (err) {
      setProcessingError(err.message || 'An unknown error occurred');
    }
  };

  // Handle resetting all database data
  const handleResetDatabase = async () => {
    // Show confirmation dialog
    const confirmed = window.confirm(
      'Are you sure you want to reset ALL Mixpanel data in the database? This action cannot be undone and will delete all events, users, and processing history.'
    );
    
    if (!confirmed) {
      return;
    }

    setIsResettingDatabase(true);
    setResetDatabaseStatus(null);
    setResetDatabaseError(null);
    setProcessingStatus(null);
    setProcessingError(null);

    try {
      const result = await api.resetMixpanelDatabase();
      setResetDatabaseStatus(result.message);
      setLastLoadTimestamp(null);
      // Refresh database stats after reset
      fetchDatabaseStats();
    } catch (err) {
      setResetDatabaseError(err.message || 'An unknown error occurred');
    } finally {
      setIsResettingDatabase(false);
    }
  };

  // Handle refreshing data by clearing data directories
  const handleRefreshData = async () => {
    // Show confirmation dialog
    const confirmed = window.confirm(
      'Are you sure you want to refresh all data? This will delete all downloaded files in the data/events and data/users directories. You will need to re-download data after this action.'
    );
    
    if (!confirmed) {
      return;
    }

    setIsRefreshingData(true);
    setRefreshDataStatus(null);
    setRefreshDataError(null);
    setProcessingStatus(null);
    setProcessingError(null);

    try {
      const result = await api.refreshMixpanelData();
      setRefreshDataStatus(result.message);
    } catch (err) {
      setRefreshDataError(err.message || 'An unknown error occurred');
    } finally {
      setIsRefreshingData(false);
    }
  };

  // Handle DB test to find users with RC Trial started events and abi_ad_id
  const handleTestDb = async () => {
    setIsTestingDb(true);
    setDbTestResults(null);
    setDbTestError(null);

    try {
      // Get 3 random users who did RC Trial started and have abi_ad_id set
      const eventResults = await api.getTestDbEvents();
      setDbTestResults(eventResults);
    } catch (err) {
      setDbTestError(err.message || 'An unknown error occurred');
    } finally {
      setIsTestingDb(false);
    }
  };
  
  // Format timestamp for display
  const formatTimestamp = (timestamp) => {
    if (typeof timestamp === 'number') {
      return new Date(timestamp).toLocaleString();
    } else if (timestamp) {
      return new Date(timestamp).toLocaleString();
    }
    return 'Never';
  };
  
  // Get progress bar width percentage
  const getProgressWidth = () => {
    return `${processingProgress.percent}%`;
  };

  // Get stage label for display
  const getStageLabel = () => {
    const stage = processingProgress.stage;
    
    switch (stage) {
      case 'download':
        return 'Downloading data';
      case 'extract':
        return 'Extracting data';
      case 'filter':
        return 'Filtering data';
      case 'ingest':
        return 'Ingesting data';
      case 'initializing':
        return 'Initializing';
      default:
        return 'Processing';
    }
  };

  // Handle searching for user events
  const handleSearchUserEvents = async () => {
    if (!userIdInput.trim()) {
      setUserEventsError('User ID is required');
      return;
    }

    setIsLoadingUserEvents(true);
    setUserEvents([]);
    setUserEventsError(null);

    try {
      const events = await api.getUserEvents(userIdInput.trim());
      // Sort events by timestamp (oldest first)
      const sortedEvents = events.sort((a, b) => a.time - b.time);
      setUserEvents(sortedEvents);
    } catch (err) {
      setUserEventsError(err.message || 'Failed to fetch user events');
    } finally {
      setIsLoadingUserEvents(false);
    }
  };

  useEffect(() => {
    // Fetch database stats when component mounts
    fetchDatabaseStats();
  }, []);

  const fetchDatabaseStats = async () => {
    setIsLoadingStats(true);
    try {
      const stats = await api.getMixpanelDatabaseStats();
      setDbStats(stats);
    } catch (error) {
      console.error("Error fetching database stats:", error);
    } finally {
      setIsLoadingStats(false);
    }
  };

  // Handle continuing from the last processed date
  const handleContinueFromLastDate = async () => {
    setIsLoadingLatestDate(true);
    setContinueFromLastDateError(null);
    setProcessingError(null);
    setProcessingStatus(null);

    try {
      // Get the latest processed date
      const dateResult = await api.getLatestProcessedDate();
      
      if (!dateResult.has_processed_dates) {
        setContinueFromLastDateError('No processed dates found. Please use the regular "Start Processing" with a start date.');
        setIsLoadingLatestDate(false);
        return;
      }

      if (!dateResult.next_date_to_process) {
        setContinueFromLastDateError('Unable to determine next date to process.');
        setIsLoadingLatestDate(false);
        return;
      }

      setLatestProcessedDate(dateResult.latest_processed_date);
      
      // Start processing from the next date
      setIsProcessing(true);
      setProcessingStatus(null);
      setProcessingError(null);
      setContinueFromLastDateError(null);
      setProcessingProgress({
        stage: 'initializing',
        percent: 0,
        currentDate: null
      });

      const result = await api.startMixpanelProcessing({
        start_date: dateResult.next_date_to_process,
        wipe_folder: true
      });
      
      if (result.success) {
        setProcessingStatus(`Processing started from ${dateResult.next_date_to_process} (continuing after ${dateResult.latest_processed_date})`);
      } else {
        throw new Error(result.error || 'Failed to start processing');
      }
    } catch (err) {
      setContinueFromLastDateError(err.message || 'An unknown error occurred');
      setIsProcessing(false);
    } finally {
      setIsLoadingLatestDate(false);
    }
  };

  return (
    <div className="flex">
      {/* Request History Sidebar */}
      <div className="w-64 bg-gray-50 dark:bg-gray-800 p-4 border-r min-h-[calc(100vh-10rem)] overflow-y-auto">
        <h2 className="text-lg font-semibold mb-4">Processing History</h2>
        {requestHistory.length === 0 ? (
          <p className="text-sm text-gray-500 dark:text-gray-400">No processing history</p>
        ) : (
          <ul className="space-y-2">
            {requestHistory.map(request => (
              <li 
                key={request.id}
                className="p-2 border rounded hover:bg-gray-100 dark:hover:bg-gray-700"
              >
                <div className="text-sm font-medium">{request.startDate} to {request.endDate}</div>
                <div className="text-xs text-gray-500 dark:text-gray-400">{formatTimestamp(request.timestamp)}</div>
              </li>
            ))}
          </ul>
        )}
      </div>
      
      {/* Main Content */}
      <div className="flex-1 p-6 max-w-5xl">
        <h1 className="text-2xl font-bold mb-6">Mixpanel Data Processor</h1>

        {/* Data Processing Section */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">Process Mixpanel Data</h2>
          
          <div className="mb-4">
            <p className="text-sm text-gray-600 dark:text-gray-300 mb-2">
              Last full load: {lastLoadTimestamp ? formatTimestamp(lastLoadTimestamp) : 'Never'}
            </p>

            <div className="mb-4">
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
                disabled={isProcessing}
              />
              <p className="text-xs text-gray-500 mt-1">
                Data will be processed from this date until today
              </p>
            </div>

            <div className="flex space-x-4">
              <button
                type="button"
                onClick={handleStartProcessing}
                disabled={isProcessing || !startDateInput}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:bg-gray-400"
              >
                {isProcessing ? 'Processing...' : 'Start Processing'}
              </button>

              <div className="relative group">
                <button
                  type="button"
                  onClick={handleContinueFromLastDate}
                  disabled={isProcessing || isLoadingLatestDate}
                  className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:bg-gray-400"
                >
                  {isLoadingLatestDate ? 'Loading...' : 'Continue from Last Date'}
                </button>
                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-800 text-white text-sm rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                  Automatically continues processing from the day after the last successfully processed date
                </div>
              </div>

              {isProcessing && (
                <button
                  type="button"
                  onClick={handleCancelProcessing}
                  className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
                >
                  Cancel Processing
                </button>
              )}

              <div className="relative group">
                <button
                  type="button"
                  onClick={handleResetSyncCounter}
                  disabled={isProcessing || !lastLoadTimestamp}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 disabled:bg-gray-400"
                >
                  Reset Last Sync Counter
                </button>
                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-800 text-white text-sm rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                  Resets the timestamp tracking when data was last processed, allowing re-processing of all dates
                </div>
              </div>

              <div className="relative group">
                <button
                  type="button"
                  onClick={handleRefreshData}
                  disabled={isProcessing || isRefreshingData}
                  className="px-4 py-2 bg-orange-600 text-white rounded hover:bg-orange-700 disabled:bg-gray-400"
                >
                  {isRefreshingData ? 'Refreshing...' : 'Refresh Data'}
                </button>
                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-800 text-white text-sm rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                  Deletes all downloaded files in data/events and data/users directories
                </div>
              </div>

              <div className="relative group">
                <button
                  type="button"
                  onClick={handleResetDatabase}
                  disabled={isProcessing || isResettingDatabase}
                  className="px-4 py-2 bg-red-800 text-white rounded hover:bg-red-900 disabled:bg-gray-400"
                >
                  {isResettingDatabase ? 'Resetting...' : 'Reset All Data'}
                </button>
                <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-800 text-white text-sm rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                  Completely wipes all data from the database including events, users, and processing history
                </div>
              </div>
            </div>
          </div>

          {isProcessing && (
            <div className="mt-6">
              <h3 className="text-md font-medium mb-2">{getStageLabel()}</h3>
              <div className="w-full bg-gray-200 rounded-full h-4 mb-2">
                <div 
                  className="bg-blue-600 h-4 rounded-full transition-all duration-300" 
                  style={{ width: getProgressWidth() }}
                ></div>
              </div>
              <div className="flex justify-between text-sm text-gray-600">
                <span>{processingProgress.percent}% complete</span>
                {processingProgress.currentDate && (
                  <span>Current date: {processingProgress.currentDate}</span>
                )}
              </div>
            </div>
          )}

          {processingStatus && !processingError && !isProcessing && (
            <div className="mt-4 p-3 bg-green-100 dark:bg-green-800 text-green-800 dark:text-green-100 rounded">
              {processingStatus}
            </div>
          )}

          {processingError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {processingError}
            </div>
          )}

          {resetDatabaseStatus && (
            <div className="mt-4 p-3 bg-green-100 dark:bg-green-800 text-green-800 dark:text-green-100 rounded">
              {resetDatabaseStatus}
            </div>
          )}

          {resetDatabaseError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {resetDatabaseError}
            </div>
          )}

          {refreshDataStatus && (
            <div className="mt-4 p-3 bg-green-100 dark:bg-green-800 text-green-800 dark:text-green-100 rounded">
              {refreshDataStatus}
            </div>
          )}

          {refreshDataError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {refreshDataError}
            </div>
          )}

          {continueFromLastDateError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {continueFromLastDateError}
            </div>
          )}
        </div>

        {/* Test DB Section */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">Test Database</h2>
          
          <p className="mb-4 text-sm text-gray-600 dark:text-gray-300">
            This will find 3 random users who did the "RC Trial started" event and have their abi_ad_id set.
          </p>

          <button
            type="button"
            onClick={handleTestDb}
            disabled={isTestingDb}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:bg-gray-400"
          >
            {isTestingDb ? 'Testing...' : 'Test DB'}
          </button>

          {dbTestError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {dbTestError}
            </div>
          )}

          {dbTestResults && (
            <div className="mt-4">
              <h3 className="text-lg font-medium mb-2">Test Results</h3>
              
              {dbTestResults.map((user, index) => (
                <div key={index} className="bg-gray-100 dark:bg-gray-900 p-4 rounded mb-4">
                  <h4 className="font-bold text-lg mb-2">User {index + 1}</h4>
                  <div className="grid grid-cols-2 gap-2 mb-2">
                    <div className="font-semibold">Name:</div>
                    <div>{user.name}</div>
                    
                    <div className="font-semibold">Distinct ID:</div>
                    <div>{user.distinct_id}</div>
                    
                    <div className="font-semibold">Ad ID:</div>
                    <div>{user.abi_ad_id}</div>
                    
                    <div className="font-semibold">Event:</div>
                    <div>{user.event_name}</div>
                  </div>
                  
                  <div className="mt-2">
                    <div className="font-semibold mb-1">Event Properties:</div>
                    <pre className="bg-gray-200 dark:bg-gray-800 p-2 rounded text-xs overflow-auto max-h-60">
                      {JSON.stringify(user.event_properties, null, 2)}
                    </pre>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* User Events Section */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">User Event History</h2>
          
          <div className="mb-4">
            <label className="block text-sm font-medium mb-1" htmlFor="user_id_input">
              User ID<span className="text-red-500"> *</span>
            </label>
            <div className="flex">
              <input
                type="text"
                id="user_id_input"
                value={userIdInput}
                onChange={(e) => setUserIdInput(e.target.value)}
                className="flex-1 p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                placeholder="Enter user ID or distinct ID"
                required
              />
              <button
                type="button"
                onClick={handleSearchUserEvents}
                disabled={isLoadingUserEvents || !userIdInput.trim()}
                className="ml-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:bg-gray-400"
              >
                {isLoadingUserEvents ? 'Loading...' : 'Search'}
              </button>
            </div>
          </div>

          {userEventsError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {userEventsError}
            </div>
          )}

          {isLoadingUserEvents && (
            <div className="flex justify-center items-center py-8">
              <p className="text-gray-500">Loading user events...</p>
            </div>
          )}

          {!isLoadingUserEvents && userEvents.length > 0 && (
            <div className="mt-4">
              <h3 className="text-lg font-medium mb-2">Event Timeline for User</h3>
              
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                  <thead className="bg-gray-50 dark:bg-gray-800">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Time</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Event Name</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Details</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                    {userEvents.map((event, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">
                          {new Date(event.time).toLocaleString()}
                        </td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">
                          {event.name}
                        </td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">
                          <button
                            className="text-blue-500 hover:text-blue-700"
                            onClick={() => {
                              alert(JSON.stringify(event.properties, null, 2));
                            }}
                          >
                            View Properties
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!isLoadingUserEvents && userEvents.length === 0 && userIdInput && !userEventsError && (
            <div className="flex justify-center items-center py-8">
              <p className="text-gray-500">No events found for this user</p>
            </div>
          )}
        </div>

        {/* Add Database Stats Section */}
        <div className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold">Database Statistics</h2>
            <button 
              onClick={fetchDatabaseStats} 
              className="px-3 py-1 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm"
              disabled={isLoadingStats}
            >
              {isLoadingStats ? 'Loading...' : 'Refresh'}
            </button>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <div className="bg-gray-50 dark:bg-gray-700 p-3 rounded border border-gray-200 dark:border-gray-600">
              <h3 className="text-md font-medium mb-2">Total Events</h3>
              <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">{dbtStats.totalEvents.toLocaleString()}</p>
            </div>
            <div className="bg-gray-50 dark:bg-gray-700 p-3 rounded border border-gray-200 dark:border-gray-600">
              <h3 className="text-md font-medium mb-2">Total Users</h3>
              <p className="text-2xl font-bold text-green-600 dark:text-green-400">{dbtStats.totalUsers.toLocaleString()}</p>
            </div>
          </div>

          {/* Date Range Info */}
          {dbtStats.dateRange && (dbtStats.dateRange.earliest || dbtStats.dateRange.latest) && (
            <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900 rounded border border-blue-200 dark:border-blue-700">
              <h3 className="text-md font-medium mb-2 text-blue-800 dark:text-blue-200">Data Range</h3>
              <div className="text-sm text-blue-700 dark:text-blue-300">
                <p>Earliest Event: {dbtStats.dateRange.earliest ? new Date(dbtStats.dateRange.earliest).toLocaleString() : 'N/A'}</p>
                <p>Latest Event: {dbtStats.dateRange.latest ? new Date(dbtStats.dateRange.latest).toLocaleString() : 'N/A'}</p>
              </div>
            </div>
          )}

          {/* Monthly Breakdown */}
          {dbtStats.monthlyBreakdown && dbtStats.monthlyBreakdown.length > 0 && (
            <div className="mb-4">
              <h3 className="text-md font-medium mb-2">Monthly Breakdown</h3>
              <div className="bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded overflow-hidden">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                  <thead className="bg-gray-50 dark:bg-gray-800">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Month</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Events</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Users</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Event Types</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                    {dbtStats.monthlyBreakdown.map((month, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200 font-medium">{month.month}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{month.events.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{month.users.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{month.uniqueEvents}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Daily Breakdown (Last 30 Days) */}
          {dbtStats.dailyBreakdown && dbtStats.dailyBreakdown.length > 0 && (
            <div className="mb-4">
              <h3 className="text-md font-medium mb-2">Daily Breakdown (Last 30 Days)</h3>
              <div className="bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded overflow-hidden max-h-96 overflow-y-auto">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                  <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Date</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Events</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Users</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Event Types</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                    {dbtStats.dailyBreakdown.map((day, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200 font-medium">{day.date}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{day.events.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{day.users.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{day.uniqueEvents}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
          
          {dbtStats.eventBreakdown && dbtStats.eventBreakdown.length > 0 && (
            <div>
              <h3 className="text-md font-medium mb-2">Event Breakdown</h3>
              <div className="bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded overflow-hidden">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                  <thead className="bg-gray-50 dark:bg-gray-800">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Event Name</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Count</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Percentage</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                    {dbtStats.eventBreakdown.map((event, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{event.name}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{event.count.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">
                          {((event.count / dbtStats.totalEvents) * 100).toFixed(2)}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}; 