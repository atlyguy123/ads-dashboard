import React, { useState, useEffect, useRef } from 'react';
import { RefreshCw, Settings, Clock, Database, CheckCircle, AlertTriangle, Play, Circle, Loader, X, RotateCcw } from 'lucide-react';
import { Link } from 'react-router-dom';
import { io } from 'socket.io-client';
import { apiRequest } from '../config/api';

const DataPipelinePage = () => {
  const [pipelineStatus, setPipelineStatus] = useState('idle'); // 'idle', 'running', 'success', 'failed'
  const [lastSuccessfulRun, setLastSuccessfulRun] = useState(null);
  const [lastRunDuration, setLastRunDuration] = useState(null);
  const [estimatedCompletion, setEstimatedCompletion] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [retryAttempts, setRetryAttempts] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const [messageType, setMessageType] = useState('');
  const [currentRunStart, setCurrentRunStart] = useState(null);
  const [moduleStates, setModuleStates] = useState({});
  const [currentModule, setCurrentModule] = useState(null);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [moduleTimestamps, setModuleTimestamps] = useState({});
  
  const socketRef = useRef(null);
  const retryTimeoutRef = useRef(null);

  // Constants
  const MASTER_PIPELINE = 'master_pipeline';
  const MAX_RETRY_ATTEMPTS = 5;
  const RETRY_INTERVAL_MINUTES = 30;
  
  // Master pipeline modules (from pipeline.yaml)
  const MASTER_MODULES = [
    { id: "📊 Mixpanel - Download & Update Data", name: "Download Data" },
    { id: "📊 Mixpanel - Setup Database", name: "Setup Database" },
    { id: "📊 Mixpanel - Ingest Data", name: "Ingest Data" },
    { id: "📊 Mixpanel - Assign Product Information", name: "Product Info" },
    { id: "📊 Mixpanel - Set ABI Attribution", name: "ABI Attribution" },
    { id: "📊 Mixpanel - Validate Event Lifecycle", name: "Validate Events" },
    { id: "📊 Mixpanel - Assign Economic Tier", name: "Economic Tier" },
    { id: "⚙️ Pre-processing - Assign Credited Date", name: "Credited Date" },
    { id: "⚙️ Pre-processing - Assign Price Bucket", name: "Price Bucket" },
    { id: "⚙️ Pre-processing - Assign Conversion Rates", name: "Conversion Rates" },
    { id: "⚙️ Pre-processing - Estimate Values", name: "Estimate Values" },
    { id: "🔮 Meta - Update Data", name: "Meta Data" },
    { id: "🔮 Meta - Create ID Name Mapping", name: "ID Name Mapping" },
    { id: "🔮 Meta - Create Hierarchy Mapping", name: "Hierarchy Mapping" },
    { id: "📊 Mixpanel - Compute Daily Metrics", name: "Daily Metrics" }
  ];

  // Add WebSocket ID validation for debugging
  const validateWebSocketId = (stepId) => {
    const moduleExists = MASTER_MODULES.some(module => module.id === stepId);
    if (!moduleExists) {
      console.warn(`⚠️ WebSocket step ID not found in MASTER_MODULES: "${stepId}"`);
      console.warn('Available module IDs:', MASTER_MODULES.map(m => m.id));
    }
    return moduleExists;
  };

  // Initialize socket connection with reconnection logic
  useEffect(() => {
    const connectSocket = () => {
      const socketUrl = process.env.REACT_APP_API_URL || '';
      socketRef.current = io(socketUrl, {
        reconnection: true,
        reconnectionDelay: 1000,
        reconnectionAttempts: 10,
        timeout: 20000,
      });
      
      socketRef.current.on('connect', () => {
        console.log('✅ Connected to server');
        showMessage('Connected to server', 'success');
      });
      
      socketRef.current.on('disconnect', (reason) => {
        console.log('❌ Disconnected from server:', reason);
        showMessage('Disconnected from server - attempting to reconnect...', 'warning');
      });
      
      socketRef.current.on('reconnect', (attemptNumber) => {
        console.log(`🔄 Reconnected to server (attempt ${attemptNumber})`);
        showMessage('Reconnected to server', 'success');
      });
      
      socketRef.current.on('reconnect_error', (error) => {
        console.log('❌ Reconnection failed:', error);
      });
      
      socketRef.current.on('status_update', (data) => {
        if (data.pipeline === MASTER_PIPELINE) {
          handlePipelineStatusUpdate(data);
        }
      });
      
      socketRef.current.on('pipeline_reset', (data) => {
        if (data.pipeline === MASTER_PIPELINE) {
          handlePipelineReset(data);
        }
      });
    };

    connectSocket();

    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);

  // Load saved data and initialize
  useEffect(() => {
    loadSavedData();
    
    return () => {
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, []);

  // Mark that initial load is complete after first render
  useEffect(() => {
    const timer = setTimeout(() => {
      setIsInitialLoad(false);
      
      // Always check if pipeline is complete when page loads
      // This handles cases where the pipeline completed but frontend missed it
      setTimeout(() => {
        checkPipelineCompletion();
      }, 1000); // Delay to ensure all initial state is loaded
    }, 100); // Small delay to ensure all initial state is loaded
    
    return () => clearTimeout(timer);
  }, []);

  // Auto-save state when important values change (but not during initial load)
  useEffect(() => {
    if (!isInitialLoad && (pipelineStatus !== 'idle' || Object.keys(moduleStates).length > 0)) {
      savePipelineData();
    }
  }, [isInitialLoad, pipelineStatus, moduleStates, currentModule, retryAttempts]);

  const loadSavedData = () => {
    const saved = localStorage.getItem('masterPipelineData');
    if (saved) {
      try {
        const data = JSON.parse(saved);
        console.log('📂 Loading saved pipeline data:', data);
        
        setLastSuccessfulRun(data.lastSuccessfulRun ? new Date(data.lastSuccessfulRun) : null);
        setLastRunDuration(data.lastRunDuration || null);
        setRetryCount(data.retryCount || 0);
        setRetryAttempts(data.retryAttempts || 0);
        setPipelineStatus(data.status || 'idle');
        setModuleStates(data.moduleStates || {});
        setCurrentModule(data.currentModule || null);
        setCurrentRunStart(data.currentRunStart ? new Date(data.currentRunStart) : null);
        setEstimatedCompletion(data.estimatedCompletion ? new Date(data.estimatedCompletion) : null);
        
        // Load module timestamps
        if (data.moduleTimestamps) {
          const timestamps = {};
          Object.entries(data.moduleTimestamps).forEach(([id, timestamp]) => {
            timestamps[id] = new Date(timestamp);
          });
          setModuleTimestamps(timestamps);
        }
        
        // Always check backend sync on page load - especially important for running states
        // This handles cases where:
        // 1. Pipeline completed but frontend missed it
        // 2. Pipeline failed but frontend shows running
        // 3. Stale state from previous sessions
        console.log('🔄 Scheduling backend sync check...');
        setTimeout(() => {
          checkPipelineCompletion(data.moduleStates);
        }, 1000); // Delay to ensure all state is loaded
        
        // Clear obviously stale state (running for >6 hours)
        if (data.status === 'running' && data.currentRunStart) {
          const runStart = new Date(data.currentRunStart);
          const hoursRunning = (Date.now() - runStart.getTime()) / (1000 * 60 * 60);
          if (hoursRunning > 6) {
            console.warn('⚠️ Detected stale running state (>6 hours), clearing...');
            localStorage.removeItem('masterPipelineData');
            setPipelineStatus('idle');
            setModuleStates({});
            setCurrentModule(null);
            setCurrentRunStart(null);
            setEstimatedCompletion(null);
          }
        }
      } catch (e) {
        console.warn('Failed to parse saved pipeline data:', e);
        localStorage.removeItem('masterPipelineData');
      }
    }
  };

  const saveData = (data) => {
    const current = JSON.parse(localStorage.getItem('masterPipelineData') || '{}');
    const updated = { 
      ...current, 
      ...data,
      moduleStates: data.moduleStates || current.moduleStates || {},
      lastRunDuration: data.lastRunDuration || current.lastRunDuration || null
    };
    localStorage.setItem('masterPipelineData', JSON.stringify(updated));
  };

  const savePipelineData = () => {
    // Use current state values
    const dataToSave = {
      lastSuccessfulRun: lastSuccessfulRun?.toISOString(),
      lastRunDuration,
      status: pipelineStatus,
      retryCount,
      retryAttempts,
      moduleStates,
      currentModule,
      currentRunStart: currentRunStart?.toISOString(),
      estimatedCompletion: estimatedCompletion?.toISOString(),
      moduleTimestamps: Object.fromEntries(
        Object.entries(moduleTimestamps).map(([id, timestamp]) => [id, timestamp.toISOString()])
      )
    };
    
    // Only save if we have meaningful data
    if (dataToSave.status && dataToSave.status !== 'idle' || Object.keys(dataToSave.moduleStates || {}).length > 0 || dataToSave.lastSuccessfulRun) {
      saveData(dataToSave);
    }
  };

  const runMasterPipeline = async () => {
    // Safety check: prevent multiple concurrent runs
    if (pipelineStatus === 'running') {
      console.log('⚠️ Pipeline is already running, skipping duplicate request');
      showMessage('Pipeline is already running', 'warning');
      return;
    }
    
    // Check if pipeline is already complete - don't run if complete
    if (pipelineStatus === 'success' && Object.keys(moduleStates).length > 0) {
      const allComplete = Object.values(moduleStates).every(state => state === 'complete');
      if (allComplete) {
        console.log('⚠️ Pipeline is already complete, use restart to run again');
        showMessage('Pipeline is already complete. Use the restart button to run again.', 'warning');
        return;
      }
    }
    
    console.log('🚀 Starting master pipeline...');
    setPipelineStatus('running');
    setStatusMessage('');
    setCurrentRunStart(new Date());
    
    // Initialize all modules as pending
    const initialModuleStates = {};
    MASTER_MODULES.forEach(module => {
      initialModuleStates[module.id] = 'pending';
    });
    console.log('📋 Initialized module states:', initialModuleStates);
    setModuleStates(initialModuleStates);
    setCurrentModule(null);
    
    // Calculate estimated completion time based on last run duration
    if (lastRunDuration) {
      const estimated = new Date(Date.now() + lastRunDuration);
      setEstimatedCompletion(estimated);
    }
    
    try {
      const response = await apiRequest(`/api/run/${MASTER_PIPELINE}`, { method: 'POST' });
      const result = await response.json();
      
      if (result.success) {
        console.log('✅ Pipeline started successfully');
        showMessage('Pipeline started successfully', 'success');
      } else {
        console.error('❌ Pipeline failed to start:', result.message);
        setPipelineStatus('failed');
        showMessage(`Error: ${result.message}`, 'error');
        handlePipelineFailure();
      }
    } catch (error) {
      console.error('❌ Pipeline start error:', error);
      setPipelineStatus('failed');
      showMessage('Error starting pipeline', 'error');
      handlePipelineFailure();
    }
  };

  const cancelPipeline = async () => {
    console.log('🛑 Cancelling master pipeline...');
    
    try {
      const response = await apiRequest(`/api/cancel/${MASTER_PIPELINE}`, { method: 'POST' });
      const result = await response.json();
      
      if (result.success) {
        console.log('✅ Pipeline cancelled successfully');
        setPipelineStatus('idle');
        setCurrentModule(null);
        setEstimatedCompletion(null);
        
        // Mark all running modules as cancelled
        const cancelledModuleStates = {};
        MASTER_MODULES.forEach(module => {
          const currentState = moduleStates[module.id] || 'pending';
          if (currentState === 'running') {
            cancelledModuleStates[module.id] = 'cancelled';
          } else {
            cancelledModuleStates[module.id] = currentState;
          }
        });
        setModuleStates(cancelledModuleStates);
        
        showMessage(result.message || 'Pipeline cancelled successfully', 'success');
      } else {
        console.error('❌ Pipeline cancel failed:', result.message);
        showMessage(`Cancel failed: ${result.message}`, 'error');
      }
    } catch (error) {
      console.error('❌ Pipeline cancel error:', error);
      showMessage('Error cancelling pipeline', 'error');
    }
  };

  const restartPipeline = async () => {
    console.log('🔄 Restarting master pipeline...');
    
    // Reset all module states to pending
    const resetModuleStates = {};
    MASTER_MODULES.forEach(module => {
      resetModuleStates[module.id] = 'pending';
    });
    
    setPipelineStatus('idle');
    setModuleStates(resetModuleStates);
    setCurrentModule(null);
    setEstimatedCompletion(null);
    setRetryAttempts(0);
    
    // Save the reset state
    saveData({
      status: 'idle',
      moduleStates: resetModuleStates,
      currentModule: null,
      estimatedCompletion: null,
      retryAttempts: 0
    });
    
    showMessage('Pipeline reset, starting fresh run...', 'success');
    
    // Start the pipeline again after a short delay
    setTimeout(() => {
      runMasterPipeline();
    }, 1000);
  };

  const resetAllSteps = async () => {
    console.log(`🔄 RESET ALL: Resetting all steps in pipeline '${MASTER_PIPELINE}'`);
    
    const isStuckState = pipelineStatus === 'running' && Object.keys(moduleStates).length > 0;
    const confirmMessage = isStuckState 
      ? `The pipeline appears to be stuck in a running state.\n\nThis will:\n• Force cancel any stuck processes\n• Reset all step statuses to pending\n• Clear all cached state\n• Fix synchronization issues\n\nThis should resolve the stuck state. Continue?`
      : `Are you sure you want to reset ALL steps in the master pipeline?\n\nThis will:\n• Cancel any running steps\n• Reset all step statuses to pending\n• Clear all progress and error states\n\nThis action cannot be undone.`;
    
    if (!window.confirm(confirmMessage)) {
      return;
    }
    
    // Show immediate feedback for stuck states
    if (isStuckState) {
      showMessage('Forcing reset of stuck pipeline state...', 'warning');
    }
    
    try {
      // First, clear all local state immediately to provide instant feedback
      console.log('🧹 RESET ALL: Clearing all local state immediately');
      setPipelineStatus('idle');
      setModuleStates({});
      setCurrentModule(null);
      setEstimatedCompletion(null);
      setRetryAttempts(0);
      setModuleTimestamps({});
      setCurrentRunStart(null);
      
      // Clear localStorage completely
      localStorage.removeItem('masterPipelineData');
      localStorage.removeItem('pipelineLastTrigger');
      
      // Call the backend reset API
      const response = await apiRequest(`/api/reset-all/${MASTER_PIPELINE}`, {
        method: 'POST'
      });
      
      const result = await response.json();
      console.log(`🔄 RESET ALL: Reset response:`, result);
      
      if (result.success) {
        showMessage(result.message, 'success');
        console.log(`✅ RESET ALL: Successfully reset all steps in pipeline '${MASTER_PIPELINE}'`);
        
        // Set clean state
        const resetModuleStates = {};
        MASTER_MODULES.forEach(module => {
          resetModuleStates[module.id] = 'pending';
        });
        
        setModuleStates(resetModuleStates);
        
        // Save the clean reset state
        saveData({
          status: 'idle',
          moduleStates: resetModuleStates,
          currentModule: null,
          estimatedCompletion: null,
          retryAttempts: 0,
          moduleTimestamps: {},
          currentRunStart: null
        });
        
        // Force a WebSocket reconnection to ensure clean state
        if (socketRef.current) {
          console.log('🔌 RESET ALL: Forcing WebSocket reconnection for clean state');
          socketRef.current.disconnect();
          setTimeout(() => {
            socketRef.current.connect();
          }, 1000);
        }
        
      } else {
        showMessage(`Reset all failed: ${result.message}`, 'error');
        console.log(`❌ RESET ALL: Failed to reset all steps in pipeline '${MASTER_PIPELINE}': ${result.message}`);
      }
    } catch (error) {
      console.error(`❌ RESET ALL: Error resetting all steps in pipeline '${MASTER_PIPELINE}':`, error);
      showMessage(`Error resetting all steps: ${error.message}`, 'error');
      
      // Even if the API call failed, we've already cleared local state, 
      // which should fix most stuck state issues
      showMessage('Local state cleared - page refresh may be needed if issues persist', 'warning');
    }
  };

  const handlePipelineReset = (data) => {
    console.log('🔄 WebSocket pipeline reset received:', data);
    
    // Immediately reset all local state to clean slate
    setPipelineStatus('idle');
    setModuleStates({});
    setCurrentModule(null);
    setEstimatedCompletion(null);
    setRetryAttempts(0);
    setModuleTimestamps({});
    setCurrentRunStart(null);
    
    // Clear localStorage
    localStorage.removeItem('masterPipelineData');
    localStorage.removeItem('pipelineLastTrigger');
    
    // Show success message
    showMessage(data.message || 'Pipeline reset completed', 'success');
    
    console.log('✅ Pipeline reset completed - all state cleared');
  };

  const handlePipelineStatusUpdate = (data) => {
    console.log('📡 WebSocket status update received:', data);
    
    // Only handle master_pipeline updates
    if (data.pipeline !== MASTER_PIPELINE) {
      console.log('⏭️  Ignoring update for pipeline:', data.pipeline);
      return;
    }
    
    // Handle step-level updates from backend
    if (data.step && data.status) {
      const stepId = data.step;
      
      // Validate that we can handle this step ID
      if (!validateWebSocketId(stepId)) {
        console.error(`❌ Received update for unknown step ID: "${stepId}"`);
        return;
      }
      
      // Map backend status to frontend status
      let frontendStatus;
      switch (data.status) {
        case 'running':
          frontendStatus = 'running';
          break;
        case 'success':
          frontendStatus = 'complete';
          break;
        case 'failed':
          frontendStatus = 'failed';
          break;
        case 'cancelled':
          frontendStatus = 'cancelled';
          break;
        default:
          frontendStatus = data.status;
      }
      
      // Update module state
      console.log(`🔄 Updating module ${stepId} to status: ${frontendStatus}`);
      setModuleStates(prev => {
        const newStates = { ...prev, [stepId]: frontendStatus };
        console.log('📊 Updated module states:', newStates);
        
        // Use the newStates directly for completion check
        const completedCount = Object.values(newStates).filter(state => state === 'complete').length;
        const failedCount = Object.values(newStates).filter(state => state === 'failed').length;
        const cancelledCount = Object.values(newStates).filter(state => state === 'cancelled').length;
        
        console.log(`📈 Progress check: ${completedCount}/${MASTER_MODULES.length} complete, ${failedCount} failed, ${cancelledCount} cancelled`);
        
        // If all modules are complete and none failed, mark pipeline as complete
        if (completedCount === MASTER_MODULES.length && failedCount === 0 && cancelledCount === 0) {
          console.log('🎉 All modules complete! Triggering pipeline completion...');
          // Use setTimeout to avoid race condition with state updates
          setTimeout(() => {
            handlePipelineComplete();
          }, 100);
        }
        
        return newStates;
      });
      
      // Update current module
      if (frontendStatus === 'running') {
        console.log(`▶️  Setting current module to: ${stepId}`);
        setCurrentModule(stepId);
      } else if (frontendStatus === 'complete' || frontendStatus === 'failed') {
        // Clear current module if this was the running one
        setCurrentModule(currentMod => currentMod === stepId ? null : currentMod);
        
        // Check if this failure means the whole pipeline failed
        if (frontendStatus === 'failed') {
          handlePipelineFailure(stepId);
          return;
        }
        
        // Check if this cancellation means the whole pipeline was cancelled
        if (frontendStatus === 'cancelled') {
          console.log('🛑 Module cancelled, pipeline may have been cancelled');
          return;
        }
      }
    }
  };

  const handlePipelineComplete = () => {
    const endTime = new Date();
    const duration = currentRunStart ? endTime - currentRunStart : null;
    
    setPipelineStatus('success');
    setLastSuccessfulRun(endTime);
    setLastRunDuration(duration);
    setRetryCount(0);
    setRetryAttempts(0);
    setCurrentModule(null);
    setEstimatedCompletion(null);
    
    // Mark all modules as complete
    const completedModuleStates = {};
    MASTER_MODULES.forEach(module => {
      completedModuleStates[module.id] = 'complete';
    });
    setModuleStates(completedModuleStates);
    
    // Clear any retry timeouts
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
    
    // Clear trigger data to reset for other tabs
    localStorage.removeItem('pipelineLastTrigger');
    
    saveData({
      lastSuccessfulRun: endTime.toISOString(),
      lastRunDuration: duration,
      status: 'success',
      retryCount: 0,
      retryAttempts: 0,
      moduleStates: completedModuleStates,
      currentModule: null,
      currentRunStart: null,
      estimatedCompletion: null
    });
    
    showMessage('Pipeline completed successfully! Database updated.', 'success');
  };

  const handlePipelineFailure = (failedStep = null) => {
    const newRetryAttempts = retryAttempts + 1;
    setRetryAttempts(newRetryAttempts);
    setPipelineStatus('failed');
    setCurrentModule(null);
    setEstimatedCompletion(null);
    
    // Mark the failed module if we know which one it was
    let updatedModuleStates = moduleStates;
    if (failedStep) {
      const matchingModule = MASTER_MODULES.find(m => 
        m.id.includes(failedStep) || failedStep.includes(m.name) || m.id === failedStep
      );
      if (matchingModule) {
        updatedModuleStates = {
          ...moduleStates,
          [matchingModule.id]: 'failed'
        };
        setModuleStates(updatedModuleStates);
      }
    }
    
    saveData({
      status: 'failed',
      retryAttempts: newRetryAttempts,
      moduleStates: updatedModuleStates,
      currentModule: null,
      estimatedCompletion: null
    });
    
    if (newRetryAttempts < MAX_RETRY_ATTEMPTS) {
      // Schedule retry in 30 minutes
      const retryTime = new Date(Date.now() + RETRY_INTERVAL_MINUTES * 60 * 1000);
      
      const failureMessage = failedStep 
        ? `Pipeline failed at: ${failedStep}. Retry ${newRetryAttempts}/${MAX_RETRY_ATTEMPTS} scheduled for ${retryTime.toLocaleTimeString()}`
        : `Pipeline failed. Retry ${newRetryAttempts}/${MAX_RETRY_ATTEMPTS} scheduled for ${retryTime.toLocaleTimeString()}`;
      
      showMessage(failureMessage, 'warning');
      
      retryTimeoutRef.current = setTimeout(() => {
        console.log(`🔄 Automatic retry ${newRetryAttempts} triggered`);
        runMasterPipeline();
      }, RETRY_INTERVAL_MINUTES * 60 * 1000);
      
    } else {
      const finalFailureMessage = failedStep 
        ? `Pipeline failed at: ${failedStep} after ${MAX_RETRY_ATTEMPTS} attempts. Manual intervention required.`
        : `Pipeline failed after ${MAX_RETRY_ATTEMPTS} attempts. Manual intervention required.`;
      
      showMessage(finalFailureMessage, 'error');
      
      // Reset for next day
      setRetryAttempts(0);
      saveData({ retryAttempts: 0 });
    }
  };

  const showMessage = (message, type) => {
    setStatusMessage(message);
    setMessageType(type);
    
    setTimeout(() => {
      setStatusMessage('');
      setMessageType('');
    }, 8000);
  };

  const getModuleProgress = () => {
    const totalModules = MASTER_MODULES.length;
    const completedModules = Object.values(moduleStates).filter(state => state === 'complete').length;
    const failedModules = Object.values(moduleStates).filter(state => state === 'failed').length;
    const runningModules = Object.values(moduleStates).filter(state => state === 'running').length;
    const cancelledModules = Object.values(moduleStates).filter(state => state === 'cancelled').length;
    
    return {
      total: totalModules,
      completed: completedModules,
      failed: failedModules,
      running: runningModules,
      cancelled: cancelledModules,
      pending: totalModules - completedModules - failedModules - runningModules - cancelledModules,
      percentage: Math.round((completedModules / totalModules) * 100)
    };
  };

  const getEstimatedTimeRemaining = () => {
    if (!currentRunStart || !lastRunDuration || pipelineStatus !== 'running') {
      return null;
    }
    
    const progress = getModuleProgress();
    const elapsedTime = Date.now() - currentRunStart;
    const progressRatio = progress.completed / progress.total;
    
    if (progressRatio > 0) {
      const estimatedTotalTime = elapsedTime / progressRatio;
      const remainingTime = estimatedTotalTime - elapsedTime;
      
      if (remainingTime > 0) {
        const minutes = Math.ceil(remainingTime / (1000 * 60));
        return `~${minutes}m remaining`;
      }
    }
    
    // Fallback to using last run duration
    const averageModuleTime = lastRunDuration / MASTER_MODULES.length;
    const remainingModules = progress.total - progress.completed;
    const estimatedRemaining = remainingModules * averageModuleTime;
    
    if (estimatedRemaining > 0) {
      const minutes = Math.ceil(estimatedRemaining / (1000 * 60));
      return `~${minutes}m remaining`;
    }
    
    return null;
  };

  const getRelativeTime = (timestamp) => {
    const now = new Date();
    const diff = now - timestamp;
    const minutes = Math.floor(diff / (1000 * 60));
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) {
      return `${days} day${days > 1 ? 's' : ''} ago`;
    } else if (hours > 0) {
      return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    } else if (minutes > 0) {
      return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
    } else {
      return 'just now';
    }
  };

  const getModuleIcon = (state) => {
    switch (state) {
      case 'complete':
        return <CheckCircle className="h-4 w-4 text-green-600" />;
      case 'running':
        return <Loader className="h-4 w-4 text-blue-600 animate-spin" />;
      case 'failed':
        return <AlertTriangle className="h-4 w-4 text-red-600" />;
      case 'cancelled':
        return <X className="h-4 w-4 text-orange-600" />;
      default:
        return <Circle className="h-4 w-4 text-gray-400" />;
    }
  };

  const getStatusDisplay = () => {
    const progress = getModuleProgress();
    
    switch (pipelineStatus) {
      case 'running':
        const currentModuleName = currentModule ? MASTER_MODULES.find(m => m.id === currentModule)?.name : null;
        const timeRemaining = getEstimatedTimeRemaining();
        
        let runningDescription = `${progress.completed}/${progress.total} modules complete`;
        if (currentModuleName) {
          runningDescription = `Processing: ${currentModuleName} • ${runningDescription}`;
        }
        if (timeRemaining) {
          runningDescription += ` • ${timeRemaining}`;
        }
        
        return {
          icon: <RefreshCw className="h-8 w-8 text-blue-600 animate-spin" />,
          title: 'Pipeline Running',
          description: runningDescription,
          color: 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
        };
      case 'success':
        const successDescription = lastRunDuration 
          ? `All ${progress.total} modules completed in ${Math.round(lastRunDuration / (1000 * 60))} minutes`
          : 'All data processing completed successfully';
        
        return {
          icon: <CheckCircle className="h-8 w-8 text-green-600" />,
          title: 'Pipeline Complete',
          description: successDescription,
          color: 'border-green-500 bg-green-50 dark:bg-green-900/20'
        };
      case 'failed':
        const failedModules = progress.failed;
        const failureDescription = retryAttempts > 0 
          ? `Retry ${retryAttempts}/${MAX_RETRY_ATTEMPTS} - ${failedModules} module${failedModules !== 1 ? 's' : ''} failed`
          : failedModules > 0 
          ? `${failedModules} module${failedModules !== 1 ? 's' : ''} failed - manual intervention required`
          : 'Manual intervention required';
        
        return {
          icon: <AlertTriangle className="h-8 w-8 text-red-600" />,
          title: 'Pipeline Failed',
          description: failureDescription,
          color: 'border-red-500 bg-red-50 dark:bg-red-900/20'
        };
      default:
        return {
          icon: <Clock className="h-8 w-8 text-gray-600" />,
          title: 'Pipeline Ready',
          description: 'Ready to run when triggered',
          color: 'border-gray-300 bg-gray-50 dark:bg-gray-800'
        };
    }
  };

  const status = getStatusDisplay();

  // Helper function to check if pipeline is actually complete
  const checkPipelineCompletion = async (moduleStatesData = null) => {
    try {
      console.log('🔍 Checking pipeline completion status...');
      
      // Fetch current status from backend using existing endpoint
      const response = await apiRequest('/api/pipelines');
      if (!response.ok) {
        console.warn('Failed to fetch pipeline status from backend');
        return;
      }
      
      const pipelines = await response.json();
      const masterPipeline = pipelines.find(p => p.name === MASTER_PIPELINE);
      
      if (!masterPipeline) {
        console.warn('Master pipeline not found in backend response');
        return;
      }
      
      console.log('📊 Backend pipeline status:', masterPipeline.status);
      
      // Check if all modules are marked as success in backend
      if (masterPipeline.status && Object.keys(masterPipeline.status).length > 0) {
        const moduleIds = MASTER_MODULES.map(m => m.id);
        const allComplete = moduleIds.every(id => 
          masterPipeline.status[id] && masterPipeline.status[id].status === 'success'
        );
        
        if (allComplete) {
          console.log('🎉 Pipeline is actually complete! Updating frontend...');
          
          // Find the latest completion timestamp from all modules
          let latestTimestamp = null;
          let earliestTimestamp = null;
          
          moduleIds.forEach(id => {
            const moduleStatus = masterPipeline.status[id];
            if (moduleStatus && moduleStatus.timestamp) {
              const timestamp = new Date(moduleStatus.timestamp);
              if (!latestTimestamp || timestamp > latestTimestamp) {
                latestTimestamp = timestamp;
              }
              if (!earliestTimestamp || timestamp < earliestTimestamp) {
                earliestTimestamp = timestamp;
              }
            }
          });
          
          // Calculate actual duration if we have both start and end times
          let actualDuration = null;
          if (latestTimestamp && currentRunStart) {
            actualDuration = latestTimestamp - currentRunStart;
          } else if (latestTimestamp && earliestTimestamp) {
            // Fallback: use time between first and last module completion
            actualDuration = latestTimestamp - earliestTimestamp;
          }
          
          console.log('📅 Actual completion time:', latestTimestamp?.toISOString());
          console.log('⏱️ Actual duration:', actualDuration ? `${Math.round(actualDuration / (1000 * 60))} minutes` : 'unknown');
          
          // Update module states to reflect completion with timestamps
          const completedModuleStates = {};
          const moduleTimestamps = {};
          MASTER_MODULES.forEach(module => {
            completedModuleStates[module.id] = 'complete';
            const moduleStatus = masterPipeline.status[module.id];
            if (moduleStatus && moduleStatus.timestamp) {
              moduleTimestamps[module.id] = new Date(moduleStatus.timestamp);
            }
          });
          
          setModuleStates(completedModuleStates);
          setModuleTimestamps(moduleTimestamps);
          
          // Update pipeline status with actual completion time
          setPipelineStatus('success');
          if (latestTimestamp) {
            setLastSuccessfulRun(latestTimestamp);
          }
          if (actualDuration) {
            setLastRunDuration(actualDuration);
          }
          setRetryCount(0);
          setRetryAttempts(0);
          setCurrentModule(null);
          setEstimatedCompletion(null);
          
          // Clear any retry timeouts
          if (retryTimeoutRef.current) {
            clearTimeout(retryTimeoutRef.current);
            retryTimeoutRef.current = null;
          }
          
          // Clear trigger data to reset for other tabs
          localStorage.removeItem('pipelineLastTrigger');
          
          // Save the actual completion data
          saveData({
            lastSuccessfulRun: latestTimestamp?.toISOString(),
            lastRunDuration: actualDuration,
            status: 'success',
            retryCount: 0,
            retryAttempts: 0,
            moduleStates: completedModuleStates,
            moduleTimestamps: Object.fromEntries(
              Object.entries(moduleTimestamps).map(([id, timestamp]) => [id, timestamp.toISOString()])
            ),
            currentModule: null,
            currentRunStart: null,
            estimatedCompletion: null
          });
          
          showMessage('Pipeline completed successfully! Database updated.', 'success');
          
        } else {
          console.log('❌ Not all modules complete yet. Status check:');
          moduleIds.forEach(id => {
            const moduleStatus = masterPipeline.status[id];
            console.log(`  ${id}: ${moduleStatus ? moduleStatus.status : 'not found'}`);
          });
        }
      }
    } catch (error) {
      console.warn('Error checking pipeline completion:', error);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                Data Pipeline
              </h1>
              <p className="mt-2 text-gray-600 dark:text-gray-400">
                Automated data processing and updates
              </p>
            </div>
            
            <div className="flex items-center space-x-4">
              <Link
                to="/data-pipeline/debug"
                className="inline-flex items-center p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800"
                title="Debug Mode"
              >
                <Settings className="h-5 w-5" />
              </Link>
            </div>
          </div>
        </div>

        <div className="max-w-4xl mx-auto space-y-6">
          {/* Main Status Card */}
          <div className={`bg-white dark:bg-gray-800 rounded-lg shadow-sm border-l-4 ${status.color} p-6`}>
            <div className="flex items-center space-x-4">
              {status.icon}
              <div className="flex-1">
                <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
                  {status.title}
                </h2>
                <p className="text-gray-600 dark:text-gray-400 mt-1">
                  {status.description}
                </p>
              </div>
              
              <div className="flex items-center space-x-3">
                {/* Cancel Button - only show when running */}
                {pipelineStatus === 'running' && (
                  <button
                    onClick={cancelPipeline}
                    className="inline-flex items-center px-4 py-2 border border-red-300 text-sm font-medium rounded-md text-red-700 bg-red-50 hover:bg-red-100 focus:outline-none focus:ring-2 focus:ring-red-500 dark:border-red-600 dark:text-red-300 dark:bg-red-900/20 dark:hover:bg-red-900/40 transition-colors"
                  >
                    <X className="mr-2 h-4 w-4" />
                    Cancel Pipeline
                  </button>
                )}
                
                {/* Main Run Button */}
                <button
                  onClick={() => {
                    // If there are previous module states, clear them first (like restart)
                    if (Object.keys(moduleStates).length > 0 && pipelineStatus !== 'running') {
                      restartPipeline();
                    } else {
                      runMasterPipeline();
                    }
                  }}
                  disabled={pipelineStatus === 'running'}
                  className={`inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md transition-colors ${
                    pipelineStatus === 'running'
                      ? 'bg-gray-200 dark:bg-gray-700 text-gray-500 dark:text-gray-400 cursor-not-allowed'
                      : 'text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500'
                  }`}
                >
                  <Play className="mr-2 h-5 w-5" />
                  {pipelineStatus === 'running' ? 'Running Pipeline...' : 
                   (pipelineStatus === 'success' && Object.keys(moduleStates).length > 0) ? 'Restart Pipeline' : 
                   'Run Pipeline'}
                </button>

                {/* Reset All Button - Always Available */}
                <button
                  onClick={resetAllSteps}
                  className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md transition-colors text-white bg-gray-600 hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-500"
                  title="Reset all pipeline steps to pending status - Always available to fix stuck states"
                >
                  <RotateCcw className="mr-2 h-4 w-4" />
                  Reset All
                </button>
              </div>
            </div>
          </div>

          {/* Status Information Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Last Successful Run */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <div className="flex items-center space-x-3">
                <Database className="h-8 w-8 text-green-600" />
                <div>
                  <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                    Database Updated
                  </h3>
                  <p className="text-lg font-semibold text-gray-900 dark:text-gray-100 mt-1">
                    {lastSuccessfulRun 
                      ? lastSuccessfulRun.toLocaleDateString() + ' ' + lastSuccessfulRun.toLocaleTimeString()
                      : 'Never'
                    }
                  </p>
                  {retryAttempts > 0 && (
                    <p className="text-sm text-orange-600 dark:text-orange-400 mt-1">
                      {retryAttempts} failed attempt{retryAttempts > 1 ? 's' : ''}
                    </p>
                  )}
                </div>
              </div>
            </div>

            {/* Pipeline Type */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <div className="flex items-center space-x-3">
                <RefreshCw className="h-8 w-8 text-purple-600" />
                <div>
                  <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                    Pipeline Type
                  </h3>
                  <p className="text-lg font-semibold text-gray-900 dark:text-gray-100 mt-1">
                    Master Pipeline
                  </p>
                  <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                    Full data processing
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Module Progress Display */}
          {(pipelineStatus === 'running' || Object.keys(moduleStates).length > 0) && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Pipeline Progress
                </h3>
                {pipelineStatus === 'running' && (
                  <div className="text-sm text-gray-600 dark:text-gray-400">
                    {getEstimatedTimeRemaining() || 'Calculating...'}
                  </div>
                )}
              </div>
              
              {/* Progress Bar */}
              <div className="mb-6">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    {getModuleProgress().completed} of {getModuleProgress().total} modules complete
                  </span>
                  <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    {getModuleProgress().percentage}%
                  </span>
                </div>
                <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3">
                  <div 
                    className="bg-blue-600 h-3 rounded-full transition-all duration-500 ease-out"
                    style={{ width: `${getModuleProgress().percentage}%` }}
                  ></div>
                </div>
              </div>

              {/* Current Running Module */}
              {currentModule && pipelineStatus === 'running' && (
                <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                  <div className="flex items-center space-x-2">
                    <Loader className="h-4 w-4 text-blue-600 animate-spin" />
                    <span className="text-sm font-medium text-blue-800 dark:text-blue-200">
                      Currently running: {MASTER_MODULES.find(m => m.id === currentModule)?.name || currentModule}
                    </span>
                  </div>
                </div>
              )}

              {/* Module List */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {MASTER_MODULES.map((module) => {
                  const state = moduleStates[module.id] || 'pending';
                  const isActive = currentModule === module.id;
                  const moduleTimestamp = moduleTimestamps && moduleTimestamps[module.id];
                  
                  return (
                    <div 
                      key={module.id}
                      className={`flex items-center space-x-3 p-3 rounded-lg border transition-all ${
                        isActive 
                          ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800' 
                          : state === 'complete'
                          ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                          : state === 'failed'
                          ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                          : state === 'cancelled'
                          ? 'bg-orange-50 dark:bg-orange-900/20 border-orange-200 dark:border-orange-800'
                          : 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700'
                      }`}
                    >
                      {getModuleIcon(state)}
                      <div className="flex-1 min-w-0">
                        <p className={`text-sm font-medium truncate ${
                          state === 'complete' 
                            ? 'text-green-800 dark:text-green-200'
                            : state === 'failed'
                            ? 'text-red-800 dark:text-red-200'
                            : state === 'running'
                            ? 'text-blue-800 dark:text-blue-200'
                            : state === 'cancelled'
                            ? 'text-orange-800 dark:text-orange-200'
                            : 'text-gray-700 dark:text-gray-300'
                        }`}>
                          {module.name}
                        </p>
                        {state === 'complete' && moduleTimestamp && (
                          <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            Completed {getRelativeTime(moduleTimestamp)}
                          </p>
                        )}
                      </div>
                      <div className={`text-xs px-2 py-1 rounded-full ${
                        state === 'complete'
                          ? 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-200'
                          : state === 'failed'
                          ? 'bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-200'
                          : state === 'running'
                          ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-200'
                          : state === 'cancelled'
                          ? 'bg-orange-100 dark:bg-orange-900/40 text-orange-800 dark:text-orange-200'
                          : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                      }`}>
                        {state === 'pending' ? 'Pending' : 
                         state === 'running' ? 'Running' :
                         state === 'complete' ? (moduleTimestamp ? moduleTimestamp.toLocaleTimeString() : 'Done') :
                         state === 'failed' ? 'Failed' :
                         state === 'cancelled' ? 'Cancelled' : state}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Status Message */}
          {statusMessage && (
            <div className={`p-4 rounded-lg border ${
              messageType === 'success' 
                ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800' 
                : messageType === 'warning'
                ? 'bg-orange-50 dark:bg-orange-900/20 text-orange-700 dark:text-orange-300 border-orange-200 dark:border-orange-800'
                : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border-red-200 dark:border-red-800'
            }`}>
              <div className="flex items-center space-x-2">
                {messageType === 'success' && <CheckCircle className="h-5 w-5" />}
                {messageType === 'warning' && <Clock className="h-5 w-5" />}
                {messageType === 'error' && <AlertTriangle className="h-5 w-5" />}
                <span className="font-medium">{statusMessage}</span>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default DataPipelinePage; 