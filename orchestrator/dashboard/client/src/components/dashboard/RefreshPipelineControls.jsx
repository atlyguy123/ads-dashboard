import React, { useState, useEffect } from 'react';
import { 
  RefreshCw, 
  Settings, 
  Clock, 
  AlertCircle, 
  CheckCircle, 
  XCircle,
  Play,
  Pause,
  Loader,
  ChevronDown,
  ChevronUp
} from 'lucide-react';
import { refreshPipelineApi } from '../../services/refreshPipelineApi';
import { DashboardControls } from './DashboardControls';

const STAGES = [
  { id: 'mixpanel', name: 'Update Mixpanel Data', description: 'Fetching latest events and users' },
  { id: 'meta', name: 'Update Meta Data', description: 'Updating ad performance data' },
  { id: 'conversion', name: 'Run Conversion Analysis', description: 'Analyzing conversion probabilities' },
  { id: 'dashboard', name: 'Refresh Dashboard', description: 'Loading dashboard data' },
  { id: 'pipelines', name: 'Generate Row Pipelines', description: 'Running analysis for all campaigns/ads' }
];

const RefreshPipelineControls = ({ 
  onRefresh, 
  isLoading = false, 
  configurations = {}, 
  selectedConfig = null,
  onConfigChange,
  lastUpdated = null,
  onGetCurrentParams = null,
  onColumnVisibilityChange = null,
  onColumnOrderChange = null 
}) => {
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [lastRefreshData, setLastRefreshData] = useState(null);
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const [debugMode, setDebugMode] = useState(() => {
    // Load debug mode from localStorage
    const saved = localStorage.getItem('refreshPipeline_debugMode');
    return saved ? JSON.parse(saved) : false;
  });
  const [debugDaysOverride, setDebugDaysOverride] = useState(() => {
    // Load debug days from localStorage  
    const saved = localStorage.getItem('refreshPipeline_debugDays');
    return saved ? parseInt(saved) : 5;
  });
  const [hasError, setHasError] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');
  const [isExpanded, setIsExpanded] = useState(false); // Manual expand/collapse
  const [userHasCollapsed, setUserHasCollapsed] = useState(false); // Track if user manually collapsed
  
  // Save debug settings to localStorage when they change
  useEffect(() => {
    localStorage.setItem('refreshPipeline_debugMode', JSON.stringify(debugMode));
  }, [debugMode]);

  useEffect(() => {
    localStorage.setItem('refreshPipeline_debugDays', debugDaysOverride.toString());
  }, [debugDaysOverride]);

  // Fetch initial status
  useEffect(() => {
    loadPipelineStatus();
    loadLastRefreshInfo();
  }, []);

  // Auto-expand when pipeline starts running, but respect user's manual collapse
  useEffect(() => {
    if (pipelineStatus?.is_running && !isExpanded && !userHasCollapsed) {
      setIsExpanded(true);
    }
    // Reset user collapse state when pipeline stops
    if (!pipelineStatus?.is_running && userHasCollapsed) {
      setUserHasCollapsed(false);
    }
  }, [pipelineStatus?.is_running, isExpanded, userHasCollapsed]);

  // Poll for pipeline status updates with connection resilience
  useEffect(() => {
    let interval;
    let failureCount = 0;
    
    const pollStatus = async () => {
      try {
        await loadPipelineStatus();
        failureCount = 0; // Reset failure count on success
      } catch (error) {
        failureCount++;
        console.warn(`Pipeline status poll failed (attempt ${failureCount}):`, error);
        
        // If we've failed too many times, don't mark as error immediately
        // This prevents "failed" status when it's just a connection issue
        if (failureCount >= 5) {
          setHasError(true);
          setErrorMessage(`Connection lost: ${error.message}`);
        }
      }
    };
    
    if (pipelineStatus?.is_running) {
      interval = setInterval(pollStatus, 2000); // Poll every 2 seconds when running
    }
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [pipelineStatus?.is_running]);

  const loadPipelineStatus = async () => {
    try {
      const status = await refreshPipelineApi.getRefreshPipelineStatus();
      setPipelineStatus(status);
      setHasError(false);
      setErrorMessage('');
    } catch (error) {
      console.error('Failed to load pipeline status:', error);
      setHasError(true);
      setErrorMessage(`Failed to load status: ${error.message}`);
    }
  };

  const loadLastRefreshInfo = async () => {
    try {
      const data = await refreshPipelineApi.getLastRefreshTime();
      setLastRefreshData(data.last_refresh_data);
    } catch (error) {
      console.error('Failed to load last refresh info:', error);
    }
  };

  const startRefreshPipeline = async () => {
    try {
      const options = {};
      
      // TODO: Remove later - debug code
      if (debugMode) {
        options.debug_mode = true;
        if (debugDaysOverride) {
          options.debug_days_override = debugDaysOverride;
        }
      }
      
      await refreshPipelineApi.startRefreshPipeline(options);
      // Immediately update status after successful pipeline start
      await loadPipelineStatus();
      setIsExpanded(true);
      setHasError(false);
      setErrorMessage('');
    } catch (error) {
      console.error('Failed to start refresh pipeline:', error);
      setHasError(true);
      setErrorMessage(`Failed to start pipeline: ${error.message}`);
    }
  };

  const cancelRefreshPipeline = async () => {
    try {
      const result = await refreshPipelineApi.cancelRefreshPipeline();
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Cancel pipeline request failed');
      }
      
      await loadPipelineStatus();
      console.log('Pipeline cancelled successfully');
    } catch (error) {
      console.error('Failed to cancel refresh pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to cancel pipeline';
      if (error.message.includes('not running')) {
        errorMessage = 'No pipeline is currently running to cancel';
      } else if (error.message) {
        errorMessage = `Failed to cancel pipeline: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const resumeFromStage = async (stageIndex) => {
    try {
      const options = {};
      
      // TODO: Remove later - debug code
      if (debugMode) {
        options.debug_mode = true;
        if (debugDaysOverride) {
          options.debug_days_override = debugDaysOverride;
        }
      }
      
      const result = await refreshPipelineApi.resumeRefreshPipeline(stageIndex, options);
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Resume pipeline request failed');
      }
      
      // Immediately update status after successful pipeline resume
      await loadPipelineStatus();
      setIsExpanded(true);
      setHasError(false);
      setErrorMessage('');
      
      console.log(`Pipeline resumed successfully from stage ${stageIndex + 1}`);
    } catch (error) {
      console.error('Failed to resume refresh pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to resume pipeline';
      if (error.message.includes('already running')) {
        errorMessage = 'Cannot resume: A pipeline is already running';
      } else if (error.message.includes('stage_index')) {
        errorMessage = 'Invalid stage index for resume operation';
      } else if (error.message) {
        errorMessage = `Failed to resume pipeline: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const dismissInterrupted = async () => {
    if (!pipelineStatus?.interrupted_pipeline?.pipeline_id) {
      console.warn('No interrupted pipeline ID found to dismiss');
      return;
    }
    
    try {
      const result = await refreshPipelineApi.dismissInterruptedPipeline(pipelineStatus.interrupted_pipeline.pipeline_id);
      
      // Validate response
      if (!result || !result.success) {
        throw new Error(result?.error || 'Dismiss interrupted pipeline request failed');
      }
      
      await loadPipelineStatus();
      console.log('Interrupted pipeline notification dismissed successfully');
    } catch (error) {
      console.error('Failed to dismiss interrupted pipeline:', error);
      setHasError(true);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to dismiss notification';
      if (error.message.includes('pipeline_id')) {
        errorMessage = 'Invalid pipeline ID for dismiss operation';
      } else if (error.message.includes('not found')) {
        errorMessage = 'Interrupted pipeline not found';
      } else if (error.message) {
        errorMessage = `Failed to dismiss notification: ${error.message}`;
      }
      
      setErrorMessage(errorMessage);
    }
  };

  const formatInterruptedTime = (interruptedData) => {
    if (!interruptedData?.start_time) return '';
    try {
      const date = new Date(interruptedData.start_time);
      const now = new Date();
      const isToday = date.toDateString() === now.toDateString();
      
      if (isToday) {
        return `today at ${date.toLocaleTimeString()}`;
      } else {
        return date.toLocaleString();
      }
    } catch (e) {
      return 'unknown time';
    }
  };

  const formatLastRefreshTime = (refreshData) => {
    if (!refreshData || !refreshData.start_time) return 'Never';
    try {
      return new Date(refreshData.start_time).toLocaleString();
    } catch (e) {
      return 'Invalid date';
    }
  };

  const getStageStatus = (stageId, stageIndex) => {
    if (!pipelineStatus) return 'pending';
    
    if (pipelineStatus.stages_failed?.includes(stageId)) return 'failed';
    if (pipelineStatus.stages_completed?.includes(stageId)) return 'completed';
    if (pipelineStatus.current_stage === stageId && pipelineStatus.is_running) return 'running';
    if (pipelineStatus.is_running && stageIndex < STAGES.findIndex(s => s.id === pipelineStatus.current_stage)) return 'completed';
    
    return 'pending';
  };

  const getStageIcon = (status, size = 16) => {
    switch (status) {
      case 'completed': return <CheckCircle size={size} className="text-green-500" />;
      case 'running': return <Loader size={size} className="text-blue-500 animate-spin" />;
      case 'failed': return <XCircle size={size} className="text-red-500" />;
      default: return <Clock size={size} className="text-gray-400" />;
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'completed': return 'Completed';
      case 'running': return 'Running';
      case 'failed': return 'Failed';
      default: return 'Pending';
    }
  };

  const getOverallStatus = () => {
    if (!pipelineStatus) return { text: 'Idle', color: 'text-gray-600', icon: Clock };
    
    if (pipelineStatus.is_running) {
      return { 
        text: `Running (${pipelineStatus.overall_progress}%)`, 
        color: 'text-blue-600', 
        icon: Loader,
        spinning: true 
      };
    }
    
    if (lastRefreshData?.status === 'completed') {
      return { text: 'Completed', color: 'text-green-600', icon: CheckCircle };
    }
    
    if (lastRefreshData?.status === 'failed') {
      return { text: 'Failed', color: 'text-red-600', icon: XCircle };
    }
    
    return { text: 'Ready', color: 'text-gray-600', icon: Clock };
  };

  const renderCompactStagesList = () => {
    if (!pipelineStatus) return null;
    
    return (
      <div className="flex items-center space-x-2">
        {STAGES.map((stage, index) => {
          const status = getStageStatus(stage.id, index);
          return (
            <div key={stage.id} className="flex items-center">
              {getStageIcon(status, 14)}
              {index < STAGES.length - 1 && (
                <div className="w-3 h-px bg-gray-300 dark:bg-gray-600 mx-1" />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  const renderExpandedStages = () => {
    if (!isExpanded) return null;
    
    return (
      <div className="mt-4 px-4 pb-4">
        {/* Timeline-style stages container */}
        <div className="relative">
          {/* Connecting line down the left side */}
          <div className="absolute left-5 top-6 bottom-6 w-0.5 bg-gray-200 dark:bg-gray-600"></div>
          
          <div className="space-y-2">
            {STAGES.map((stage, index) => {
              const status = getStageStatus(stage.id, index);
              
              return (
                <div key={stage.id} className="relative flex items-start space-x-4 py-2">
                  {/* Stage indicator */}
                  <div className={`relative z-10 flex-shrink-0 w-10 h-10 rounded-full flex items-center justify-center border-2 transition-all ${
                    status === 'completed' 
                      ? 'bg-green-500 border-green-500 text-white shadow-sm' 
                      : status === 'running' 
                      ? 'bg-blue-500 border-blue-500 text-white shadow-sm' 
                      : status === 'failed' 
                      ? 'bg-red-500 border-red-500 text-white shadow-sm' 
                      : 'bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400'
                  }`}>
                    {status === 'completed' ? (
                      <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                      </svg>
                    ) : status === 'running' ? (
                      <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    ) : status === 'failed' ? (
                      <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                      </svg>
                    ) : (
                      <span className="text-sm font-medium">{index + 1}</span>
                    )}
                  </div>

                  {/* Stage content */}
                  <div className="flex-1 min-w-0 pb-2">
                    <div className="flex items-center justify-between">
                      <div className="flex-1 min-w-0">
                        <h4 className={`text-sm font-medium ${
                          status === 'completed' ? 'text-green-700 dark:text-green-300' :
                          status === 'running' ? 'text-blue-700 dark:text-blue-300' :
                          status === 'failed' ? 'text-red-700 dark:text-red-300' :
                          'text-gray-700 dark:text-gray-300'
                        }`}>
                          {stage.name}
                        </h4>
                        <p className="text-xs text-gray-600 dark:text-gray-400 mt-0.5">
                          {stage.description}
                        </p>
                      </div>
                      
                      {/* Status badge */}
                      <span className={`px-2 py-1 text-xs font-medium rounded-full whitespace-nowrap ml-3 ${
                        status === 'completed' ? 'bg-green-100 text-green-700 dark:bg-green-900/50 dark:text-green-300' :
                        status === 'running' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300' :
                        status === 'failed' ? 'bg-red-100 text-red-700 dark:bg-red-900/50 dark:text-red-300' :
                        'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400'
                      }`}>
                        {getStatusText(status)}
                      </span>
                    </div>

                    {/* Progress bar for running stage */}
                    {status === 'running' && pipelineStatus?.current_stage === stage.id && (
                      <div className="mt-2 pr-3">
                        <div className="flex justify-between text-xs text-blue-600 dark:text-blue-400 mb-1">
                          <span>Progress</span>
                          <span>{pipelineStatus.stage_progress}%</span>
                        </div>
                        <div className="w-full bg-blue-100 dark:bg-blue-900/30 rounded-full h-1.5">
                          <div 
                            className="bg-blue-500 h-1.5 rounded-full transition-all duration-300"
                            style={{ width: `${pipelineStatus.stage_progress}%` }}
                          />
                        </div>
                        {pipelineStatus.current_operation && (
                          <p className="text-xs text-blue-600 dark:text-blue-400 mt-1 italic">
                            {pipelineStatus.current_operation}
                          </p>
                        )}
                      </div>
                    )}
                    
                    {/* Error details for failed stage */}
                    {status === 'failed' && pipelineStatus?.errors?.length > 0 && (
                      <div className="mt-2 pr-3">
                        {pipelineStatus.errors
                          .filter(error => error.stage === stage.id)
                          .map((error, index) => (
                            <div key={index} className="p-2 bg-red-50 dark:bg-red-900/20 rounded border border-red-200 dark:border-red-800">
                              <p className="text-xs text-red-700 dark:text-red-300">
                                {error.message}
                              </p>
                            </div>
                          ))}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  };

  const renderInterruptedPipelineNotification = () => {
    const interrupted = pipelineStatus?.interrupted_pipeline;
    if (!interrupted) return null;

    const stageName = STAGES[interrupted.interrupted_stage - 1]?.name || 'Unknown Stage';
    const completedCount = interrupted.stages_completed.length;
    const isToday = interrupted.is_same_day;

    return (
      <div className="mb-4 p-4 bg-amber-50 dark:bg-amber-900/20 rounded-lg border border-amber-200 dark:border-amber-800">
        <div className="flex items-start space-x-3">
          <AlertCircle size={20} className="text-amber-600 dark:text-amber-400 mt-0.5" />
          <div className="flex-1">
            <h4 className="text-sm font-medium text-amber-800 dark:text-amber-200">
              Pipeline Interrupted
            </h4>
            <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
              A pipeline started {formatInterruptedTime(interrupted)} was interrupted after completing {completedCount} of 5 stages.
              {isToday ? ` It was about to run "${stageName}".` : ' Since it was started on a different day, the data may be outdated.'}
            </p>
            
            <div className="flex items-center space-x-3 mt-3">
              {interrupted.can_resume ? (
                <>
                  <button
                    onClick={() => resumeFromStage(interrupted.interrupted_stage - 1)}
                    disabled={pipelineStatus?.is_running}
                    className="px-3 py-1.5 bg-amber-600 hover:bg-amber-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                  >
                    <Play size={14} className="mr-1" />
                    Resume from Stage {interrupted.interrupted_stage}
                  </button>
                  <button
                    onClick={startRefreshPipeline}
                    disabled={pipelineStatus?.is_running}
                    className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                  >
                    <RefreshCw size={14} className="mr-1" />
                    Start Fresh
                  </button>
                </>
              ) : (
                <button
                  onClick={startRefreshPipeline}
                  disabled={pipelineStatus?.is_running}
                  className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded flex items-center transition-colors duration-200"
                >
                  <RefreshCw size={14} className="mr-1" />
                  Start Fresh Pipeline
                </button>
              )}
              
              <button
                onClick={dismissInterrupted}
                className="px-3 py-1.5 text-amber-700 dark:text-amber-300 hover:text-amber-900 dark:hover:text-amber-100 text-sm font-medium transition-colors duration-200"
              >
                Dismiss
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  };

  const overallStatus = getOverallStatus();
  const StatusIcon = overallStatus.icon;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-soft border border-gray-200 dark:border-gray-700 mb-6">
      {/* Compact Header */}
      <div className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="flex items-center space-x-2">
              <RefreshCw size={18} className="text-gray-700 dark:text-gray-300" />
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                Data Pipeline
              </h3>
            </div>
            
            {/* Overall Status */}
            <div className="flex items-center space-x-2">
              <StatusIcon 
                size={16} 
                className={`${overallStatus.color} ${overallStatus.spinning ? 'animate-spin' : ''}`} 
              />
              <span className={`text-sm font-medium ${overallStatus.color}`}>
                {overallStatus.text}
              </span>
            </div>
          </div>

          <div className="flex items-center space-x-2">
            {/* Last Refresh Time */}
            <div className="text-xs text-gray-500 dark:text-gray-400 text-right mr-3">
              <div>Last: {formatLastRefreshTime(lastRefreshData)}</div>
              {lastRefreshData && (
                <div>{lastRefreshData.stages_completed?.length || 0}/5 stages</div>
              )}
            </div>
            
            {/* Compact Stages Visual */}
            {renderCompactStagesList()}
            
            {/* Expand/Collapse Toggle */}
            <button
              onClick={() => {
                setIsExpanded(!isExpanded);
                if (isExpanded) {
                  setUserHasCollapsed(true); // Remember that user manually collapsed
                }
              }}
              className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors rounded-md border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700"
              title={isExpanded ? 'Collapse Pipeline Details' : 'Expand Pipeline Details'}
            >
              {isExpanded ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
            </button>
          </div>
        </div>

        {/* Action Buttons Row */}
        <div className="flex items-center justify-between mt-3">
          <div className="flex items-center space-x-2">
            <button
              onClick={startRefreshPipeline}
              disabled={pipelineStatus?.is_running}
              className={`px-3 py-1.5 rounded text-sm font-medium flex items-center transition-colors duration-200
                         ${pipelineStatus?.is_running
                           ? 'bg-gray-300 dark:bg-gray-600 text-gray-500 dark:text-gray-400 cursor-not-allowed'
                           : 'bg-blue-600 hover:bg-blue-700 text-white'
                         }`}
            >
              {pipelineStatus?.is_running ? (
                <Loader size={14} className="mr-1 animate-spin" />
              ) : (
                <Play size={14} className="mr-1" />
              )}
              {pipelineStatus?.is_running ? 'Running...' : 'Run Pipeline'}
            </button>

            {pipelineStatus?.is_running && (
              <button
                onClick={cancelRefreshPipeline}
                className="px-3 py-1.5 rounded text-sm font-medium bg-red-600 hover:bg-red-700 text-white flex items-center transition-colors duration-200"
              >
                <Pause size={14} className="mr-1" />
                Cancel
              </button>
            )}

            <button
              onClick={() => setIsAdvancedOpen(true)}
              className="px-3 py-1.5 rounded text-sm font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600 flex items-center transition-colors duration-200"
            >
              <Settings size={14} className="mr-1" />
              Settings
            </button>
          </div>

          {/* Overall Progress Bar (when running) */}
          {pipelineStatus?.is_running && (
            <div className="flex items-center space-x-2 flex-1 max-w-xs ml-4">
              <div className="flex-1 bg-blue-200 dark:bg-blue-800 rounded-full h-2">
                <div 
                  className="bg-blue-600 dark:bg-blue-400 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${pipelineStatus.overall_progress}%` }}
                />
              </div>
              <span className="text-xs text-blue-700 dark:text-blue-300 font-medium min-w-[3rem]">
                {pipelineStatus.overall_progress}%
              </span>
            </div>
          )}
        </div>

        {/* Error Display */}
        {hasError && (
          <div className="mt-3 p-3 bg-red-50 dark:bg-red-900/20 rounded border border-red-200 dark:border-red-800">
            <div className="flex items-center">
              <AlertCircle size={14} className="text-red-500 mr-2" />
              <span className="text-sm font-medium text-red-800 dark:text-red-200">Error</span>
            </div>
            <p className="text-sm text-red-700 dark:text-red-300 mt-1">{errorMessage}</p>
          </div>
        )}
      </div>

      {/* Interrupted Pipeline Notification */}
      {renderInterruptedPipelineNotification()}

      {/* Expanded Stages View */}
      {renderExpandedStages()}

      {/* Advanced Settings Modal */}
      {isAdvancedOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6">
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Advanced Settings
                </h3>
                <button
                  onClick={() => setIsAdvancedOpen(false)}
                  className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                >
                  <XCircle size={24} />
                </button>
              </div>
              
              {/* Debug Mode Settings */}
              {process.env.NODE_ENV === 'development' && (
                <div className="mb-6 p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
                  <h4 className="text-sm font-medium text-yellow-800 dark:text-yellow-200 mb-3">
                    🚧 Debug Mode Settings (Development Only)
                  </h4>
                  <div className="space-y-3">
                    <label className="flex items-center">
                      <input
                        type="checkbox"
                        checked={debugMode}
                        onChange={(e) => setDebugMode(e.target.checked)}
                        className="mr-2"
                      />
                      <span className="text-sm text-yellow-700 dark:text-yellow-300">
                        Enable Debug Mode (uses shorter time ranges for testing)
                      </span>
                    </label>
                    {debugMode && (
                      <div className="ml-6">
                        <label className="flex items-center space-x-2">
                          <span className="text-sm text-yellow-700 dark:text-yellow-300">
                            Days to process:
                          </span>
                          <input
                            type="number"
                            value={debugDaysOverride}
                            onChange={(e) => setDebugDaysOverride(parseInt(e.target.value) || 5)}
                            className="w-20 px-2 py-1 border rounded text-sm"
                            min="1"
                            max="365"
                          />
                          <span className="text-xs text-yellow-600 dark:text-yellow-400">
                            (Default: 5 days instead of 30)
                          </span>
                        </label>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Include the existing DashboardControls */}
              <DashboardControls
                onRefresh={onRefresh}
                isLoading={isLoading}
                configurations={configurations}
                selectedConfig={selectedConfig}
                onConfigChange={onConfigChange}
                lastUpdated={lastUpdated}
                onGetCurrentParams={onGetCurrentParams}
                onColumnVisibilityChange={onColumnVisibilityChange}
                onColumnOrderChange={onColumnOrderChange}
              />
              
              <div className="flex justify-end mt-6">
                <button
                  onClick={() => setIsAdvancedOpen(false)}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Interrupted Pipeline Notification */}
      {renderInterruptedPipelineNotification()}
    </div>
  );
};

export { RefreshPipelineControls }; 