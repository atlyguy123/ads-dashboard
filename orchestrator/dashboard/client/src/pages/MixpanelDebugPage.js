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

  // Database stats state
  const [dbStats, setDbStats] = useState({
    totalEvents: 0, 
    totalUsers: 0,
    eventBreakdown: [],
    monthlyBreakdown: [],
    dailyBreakdown: [],
    dateRange: { earliest: null, latest: null }
  });
  const [isLoadingStats, setIsLoadingStats] = useState(false);

  // User events lookup state
  const [userIdInput, setUserIdInput] = useState('');
  const [userEvents, setUserEvents] = useState([]);
  const [isLoadingUserEvents, setIsLoadingUserEvents] = useState(false);
  const [userEventsError, setUserEventsError] = useState(null);

  // Reset raw data state
  const [isResettingRawData, setIsResettingRawData] = useState(false);
  const [resetRawDataStatus, setResetRawDataStatus] = useState(null);
  const [resetRawDataError, setResetRawDataError] = useState(null);

  // S3 connection test state
  const [isTestingS3, setIsTestingS3] = useState(false);
  const [s3TestResults, setS3TestResults] = useState(null);
  const [s3TestError, setS3TestError] = useState(null);

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
        fetchDatabaseStats(); // Refresh stats after completion
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
    
    // Fetch database stats
    fetchDatabaseStats();

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
        wipe_folder: true
      });
      
      if (result.success) {
        setProcessingStatus('Processing started successfully');
        
        // Add to request history
        const newRequest = {
          id: Date.now(),
          startDate: startDateInput,
          endDate: 'today',
          timestamp: Date.now()
        };
        const updatedHistory = [newRequest, ...requestHistory.slice(0, 9)]; // Keep last 10
        setRequestHistory(updatedHistory);
        safeLocalStorage.setItem('mixpanelRequestHistory', JSON.stringify(updatedHistory));
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

  // Handle reset raw data only
  const handleResetRawData = async () => {
    // Show confirmation dialog
    const confirmed = window.confirm(
      'This will delete ALL raw Mixpanel data from the database.\n\n' +
      'You can then manually run the pipeline to re-download data.\n\n' +
      'This action cannot be undone. Are you sure you want to continue?'
    );
    
    if (!confirmed) {
      return;
    }

    setIsResettingRawData(true);
    setResetRawDataStatus(null);
    setResetRawDataError(null);
    setProcessingStatus(null);
    setProcessingError(null);

    try {
      setResetRawDataStatus('Deleting all raw data...');
      await api.resetMixpanelDatabase();
      
      setResetRawDataStatus('✅ Raw data deleted successfully! You can now run the pipeline to re-download data.');
      setLastLoadTimestamp(null);
      
      // Clear the database stats since we reset everything
      setDbStats({
        totalEvents: 0, 
        totalUsers: 0,
        eventBreakdown: [],
        monthlyBreakdown: [],
        dailyBreakdown: [],
        dateRange: { earliest: null, latest: null }
      });
    } catch (err) {
      setResetRawDataError(err.message || 'An unknown error occurred');
    } finally {
      setIsResettingRawData(false);
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

  // Handle S3 connection test
  const handleTestS3 = async () => {
    setIsTestingS3(true);
    setS3TestResults(null);
    setS3TestError(null);

    try {
      const results = await api.testS3Connection();
      setS3TestResults(results);
    } catch (err) {
      setS3TestError(err.message || 'An unknown error occurred');
    } finally {
      setIsTestingS3(false);
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

  const fetchDatabaseStats = async () => {
    setIsLoadingStats(true);
    try {
      const stats = await api.getMixpanelDatabaseStats();
      // Ensure the stats object has the expected structure
      if (stats && typeof stats === 'object') {
        setDbStats({
          totalEvents: stats.totalEvents || 0,
          totalUsers: stats.totalUsers || 0,
          eventBreakdown: stats.eventBreakdown || [],
          monthlyBreakdown: stats.monthlyBreakdown || [],
          dailyBreakdown: stats.dailyBreakdown || [],
          dateRange: stats.dateRange || { earliest: null, latest: null }
        });
      }
    } catch (error) {
      console.error("Error fetching database stats:", error);
      // Don't update state on error - keep the default values
    } finally {
      setIsLoadingStats(false);
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
        <h1 className="text-2xl font-bold mb-6">Mixpanel Data Manager</h1>

        {/* Quick Actions Section */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">Quick Actions</h2>
          
          <div className="mb-4">
            <p className="text-sm text-gray-600 dark:text-gray-300 mb-4">
              Last full load: {lastLoadTimestamp ? formatTimestamp(lastLoadTimestamp) : 'Never'}
            </p>

            <div className="flex flex-wrap gap-4">
              <button
                type="button"
                onClick={handleResetRawData}
                disabled={isProcessing || isResettingRawData}
                className="px-6 py-3 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:bg-gray-400 font-medium"
              >
                {isResettingRawData ? 'Deleting Raw Data...' : 'Delete All Raw Data'}
              </button>

              <button
                type="button"
                onClick={handleTestDb}
                disabled={isTestingDb || isProcessing}
                className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 font-medium"
              >
                {isTestingDb ? 'Testing...' : 'Test Database'}
              </button>

              <button
                type="button"
                onClick={handleTestS3}
                disabled={isTestingS3 || isProcessing}
                className="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-400 font-medium"
              >
                {isTestingS3 ? 'Testing S3...' : 'Test S3 Connection'}
              </button>

              {isProcessing && (
                <button
                  type="button"
                  onClick={handleCancelProcessing}
                  className="px-6 py-3 bg-gray-600 text-white rounded-lg hover:bg-gray-700 font-medium"
                >
                  Cancel Processing
                </button>
              )}
            </div>
          </div>

          {/* Custom Date Processing */}
          <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
            <h3 className="text-lg font-medium mb-3">Custom Date Processing</h3>
            <div className="flex gap-4 items-end">
              <div className="flex-1">
                <label className="block text-sm font-medium mb-1" htmlFor="start_date_input">
                  Start Date (YYYY-MM-DD)
                </label>
                <input
                  type="text"
                  id="start_date_input"
                  value={startDateInput}
                  onChange={(e) => setStartDateInput(e.target.value)}
                  className="w-full p-2 border rounded dark:bg-gray-600 dark:border-gray-500"
                  placeholder="e.g., 2025-01-01"
                  disabled={isProcessing}
                />
              </div>
              <button
                type="button"
                onClick={handleStartProcessing}
                disabled={isProcessing || !startDateInput}
                className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:bg-gray-400"
              >
                {isProcessing ? 'Processing...' : 'Start Processing'}
              </button>
            </div>
            <p className="text-xs text-gray-500 mt-1">
              Data will be processed from this date until today
            </p>
          </div>

          {/* Status Messages */}
          {resetRawDataStatus && (
            <div className="mt-4 p-3 bg-blue-100 dark:bg-blue-800 text-blue-800 dark:text-blue-100 rounded">
              {resetRawDataStatus}
            </div>
          )}

          {resetRawDataError && (
            <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Error: {resetRawDataError}
            </div>
          )}

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
        </div>

        {/* Database Statistics Section */}
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
              <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">{(dbStats?.totalEvents || 0).toLocaleString()}</p>
            </div>
            <div className="bg-gray-50 dark:bg-gray-700 p-3 rounded border border-gray-200 dark:border-gray-600">
              <h3 className="text-md font-medium mb-2">Total Users</h3>
              <p className="text-2xl font-bold text-green-600 dark:text-green-400">{(dbStats?.totalUsers || 0).toLocaleString()}</p>
            </div>
          </div>

          {/* Date Range Info */}
          {dbStats.dateRange && (dbStats.dateRange.earliest || dbStats.dateRange.latest) && (
            <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900 rounded border border-blue-200 dark:border-blue-700">
              <h3 className="text-md font-medium mb-2 text-blue-800 dark:text-blue-200">Data Range</h3>
              <div className="text-sm text-blue-700 dark:text-blue-300">
                <p>Earliest Event: {dbStats.dateRange.earliest ? new Date(dbStats.dateRange.earliest).toLocaleString() : 'N/A'}</p>
                <p>Latest Event: {dbStats.dateRange.latest ? new Date(dbStats.dateRange.latest).toLocaleString() : 'N/A'}</p>
              </div>
            </div>
          )}

          {/* Event Breakdown */}
          {dbStats.eventBreakdown && dbStats.eventBreakdown.length > 0 && (
            <div className="mb-4">
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
                    {dbStats.eventBreakdown.map((event, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{event.name}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{event.count.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">
                          {((event.count / (dbStats?.totalEvents || 1)) * 100).toFixed(2)}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Daily Breakdown (Last 30 Days) */}
          {dbStats.dailyBreakdown && dbStats.dailyBreakdown.length > 0 && (
            <div className="mb-4">
              <h3 className="text-md font-medium mb-2">Daily Breakdown (Last 30 Days)</h3>
              <div className="bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded overflow-hidden max-h-96 overflow-y-auto">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                  <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Date</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Events</th>
                      <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Users</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                    {dbStats.dailyBreakdown.map((day, index) => (
                      <tr key={index} className={index % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200 font-medium">{day.date}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{day.events.toLocaleString()}</td>
                        <td className="px-4 py-2 text-sm text-gray-900 dark:text-gray-200">{day.users.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        {/* Database Testing Section */}
        {dbTestResults && (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
            <h2 className="text-xl font-semibold mb-4">Database Test Results</h2>
            
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

        {dbTestError && (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
            <div className="p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              Test Error: {dbTestError}
            </div>
          </div>
        )}

        {/* S3 Connection Test Results */}
        {s3TestResults && (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
            <h2 className="text-xl font-semibold mb-4">S3 Connection Test Results</h2>
            
            <div className="space-y-4">
              <div className="p-3 bg-green-100 dark:bg-green-800 text-green-800 dark:text-green-100 rounded">
                ✅ S3 connection test completed successfully!
              </div>
              
              <div>
                <h3 className="font-semibold mb-2">User Data:</h3>
                <div className="p-3 bg-gray-100 dark:bg-gray-700 rounded">
                  <div>Status: {s3TestResults.user_data?.status || 'Unknown'}</div>
                  <div>Files found: {s3TestResults.user_data?.files_count || 0}</div>
                  {s3TestResults.user_data?.sample_file && (
                    <div>Sample file: {s3TestResults.user_data.sample_file}</div>
                  )}
                </div>
              </div>
              
              <div>
                <h3 className="font-semibold mb-2">Event Data:</h3>
                <div className="p-3 bg-gray-100 dark:bg-gray-700 rounded">
                  <div>Status: {s3TestResults.event_data?.status || 'Unknown'}</div>
                  <div>Date range: {s3TestResults.event_data?.date_range || 'None'}</div>
                  <div>Available days: {s3TestResults.event_data?.available_days || 0}</div>
                  <div>Missing days: {s3TestResults.event_data?.missing_days || 0}</div>
                  <div>Coverage: {s3TestResults.event_data?.coverage_percentage || 0}%</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {s3TestError && (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
            <div className="p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
              S3 Test Error: {s3TestError}
            </div>
          </div>
        )}

        {/* User Events Section */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">User Event History</h2>
          
          <div className="mb-4">
            <div className="flex gap-4 items-end">
              <div className="flex-1">
                <label className="block text-sm font-medium mb-1" htmlFor="user_id_input">
                  User ID or Distinct ID
                </label>
                <input
                  type="text"
                  id="user_id_input"
                  value={userIdInput}
                  onChange={(e) => setUserIdInput(e.target.value)}
                  className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
                  placeholder="Enter user ID or distinct ID"
                />
              </div>
              <button
                type="button"
                onClick={handleSearchUserEvents}
                disabled={isLoadingUserEvents || !userIdInput.trim()}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:bg-gray-400"
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
      </div>
    </div>
  );
}; 