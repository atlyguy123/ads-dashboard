import React, { useState, useEffect, useRef } from 'react';
import { RefreshCw, Settings, Clock, Database, CheckCircle, AlertTriangle, Play, Circle, Loader } from 'lucide-react';
import { Link } from 'react-router-dom';
import { io } from 'socket.io-client';

const DataPipelinePage = () => {
  const [pipelineStatus, setPipelineStatus] = useState('idle'); // 'idle', 'running', 'success', 'failed'
  const [lastSuccessfulRun, setLastSuccessfulRun] = useState(null);
  const [lastRunDuration, setLastRunDuration] = useState(null);
  const [estimatedCompletion, setEstimatedCompletion] = useState(null);
  const [nextScheduledRun, setNextScheduledRun] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [retryAttempts, setRetryAttempts] = useState(0);
  const [countdown, setCountdown] = useState('');
  const [isManualRun, setIsManualRun] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');
  const [messageType, setMessageType] = useState('');
  const [currentRunStart, setCurrentRunStart] = useState(null);
  const [moduleStates, setModuleStates] = useState({});
  const [currentModule, setCurrentModule] = useState(null);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  
  const socketRef = useRef(null);
  const countdownIntervalRef = useRef(null);
  const retryTimeoutRef = useRef(null);

  // Constants
  const MASTER_PIPELINE = 'master_pipeline';
  const MAX_RETRY_ATTEMPTS = 5;
  const RETRY_INTERVAL_MINUTES = 30;
  const SCHEDULED_HOUR = 9; // 9 AM
  const JERUSALEM_TIMEZONE = 'Asia/Jerusalem';
  
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
    { id: "🔮 Meta - Update Data", name: "Meta Data" }
  ];

  // Initialize socket connection
  useEffect(() => {
    socketRef.current = io();
    
    socketRef.current.on('connect', () => {
      console.log('Connected to server');
    });
    
    socketRef.current.on('status_update', (data) => {
      if (data.pipeline === MASTER_PIPELINE) {
        handlePipelineStatusUpdate(data);
      }
    });

    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);

  // Load saved data and initialize
  useEffect(() => {
    loadSavedData();
    // loadSavedData() now handles setting nextScheduledRun and will call calculateNextScheduledRun if needed
    
    return () => {
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
      }
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, []);

  // Start countdown timer when nextScheduledRun is set
  useEffect(() => {
    if (nextScheduledRun) {
      startCountdownTimer();
    }
  }, [nextScheduledRun]);

  // Mark that initial load is complete after first render
  useEffect(() => {
    const timer = setTimeout(() => {
      setIsInitialLoad(false);
    }, 100); // Small delay to ensure all initial state is loaded
    
    return () => clearTimeout(timer);
  }, []);

  // Auto-save state when important values change (but not during initial load)
  useEffect(() => {
    if (!isInitialLoad && (pipelineStatus !== 'idle' || Object.keys(moduleStates).length > 0)) {
      savePipelineData();
    }
  }, [isInitialLoad, pipelineStatus, moduleStates, currentModule, retryAttempts]);

  // Check for scheduled runs every minute
  useEffect(() => {
    const checkScheduledRun = () => {
      const now = new Date();
      const jerusalemTime = new Date(now.toLocaleString("en-US", {timeZone: JERUSALEM_TIMEZONE}));
      
      if (nextScheduledRun && jerusalemTime >= nextScheduledRun && pipelineStatus === 'idle') {
        console.log('🕘 Scheduled run triggered at', jerusalemTime.toLocaleString());
        runMasterPipeline(false); // false = automatic run
      }
    };

    const interval = setInterval(checkScheduledRun, 60000); // Check every minute
    return () => clearInterval(interval);
  }, [nextScheduledRun, pipelineStatus]);

  const loadSavedData = () => {
    const saved = localStorage.getItem('masterPipelineData');
    if (saved) {
      try {
        const data = JSON.parse(saved);
        setLastSuccessfulRun(data.lastSuccessfulRun ? new Date(data.lastSuccessfulRun) : null);
        setLastRunDuration(data.lastRunDuration || null);
        setRetryCount(data.retryCount || 0);
        setRetryAttempts(data.retryAttempts || 0);
        setPipelineStatus(data.status || 'idle');
        setModuleStates(data.moduleStates || {});
        setCurrentModule(data.currentModule || null);
        setCurrentRunStart(data.currentRunStart ? new Date(data.currentRunStart) : null);
        setEstimatedCompletion(data.estimatedCompletion ? new Date(data.estimatedCompletion) : null);
        
        // Restore next scheduled run, but recalculate if it's in the past
        if (data.nextScheduledRun) {
          const savedNextRun = new Date(data.nextScheduledRun);
          const now = new Date();
          const jerusalemTime = new Date(now.toLocaleString("en-US", {timeZone: JERUSALEM_TIMEZONE}));
          
          if (savedNextRun > jerusalemTime) {
            setNextScheduledRun(savedNextRun);
          } else {
            // If saved run time is in the past, calculate next one and save it
            calculateNextScheduledRun(true);
          }
        } else {
          // No saved next run, calculate it and save it
          calculateNextScheduledRun(true);
        }
      } catch (e) {
        console.warn('Failed to parse saved pipeline data:', e);
        // If parsing fails, still calculate next scheduled run and save it
        calculateNextScheduledRun(true);
      }
    } else {
      // No saved data, calculate initial next scheduled run and save it
      calculateNextScheduledRun(true);
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
      nextScheduledRun: nextScheduledRun?.toISOString()
    };
    
    // Only save if we have meaningful data
    if (dataToSave.status && dataToSave.status !== 'idle' || Object.keys(dataToSave.moduleStates || {}).length > 0 || dataToSave.lastSuccessfulRun) {
      saveData(dataToSave);
    }
  };

  const calculateNextScheduledRun = (shouldSave = true) => {
    const now = new Date();
    const jerusalemTime = new Date(now.toLocaleString("en-US", {timeZone: JERUSALEM_TIMEZONE}));
    
    // Calculate next 9 AM Jerusalem time
    const nextRun = new Date(jerusalemTime);
    nextRun.setHours(SCHEDULED_HOUR, 0, 0, 0);
    
    // If we're past 9 AM today, schedule for tomorrow
    if (jerusalemTime.getHours() >= SCHEDULED_HOUR) {
      nextRun.setDate(nextRun.getDate() + 1);
    }
    
    setNextScheduledRun(nextRun);
    
    // Save the calculated next run time only if requested
    if (shouldSave) {
      saveData({
        nextScheduledRun: nextRun.toISOString()
      });
    }
    
    return nextRun;
  };

  const startCountdownTimer = () => {
    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
    }

    countdownIntervalRef.current = setInterval(() => {
      const now = new Date();
      const jerusalemTime = new Date(now.toLocaleString("en-US", {timeZone: JERUSALEM_TIMEZONE}));
      
      if (nextScheduledRun) {
        const diff = nextScheduledRun - jerusalemTime;
        
        if (diff > 0) {
          const hours = Math.floor(diff / (1000 * 60 * 60));
          const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
          const seconds = Math.floor((diff % (1000 * 60)) / 1000);
          
          setCountdown(`${hours}h ${minutes}m ${seconds}s`);
        } else {
          setCountdown('Checking for scheduled run...');
        }
      }
    }, 1000);
  };

  const runMasterPipeline = async (isManual = true) => {
    console.log('🚀 Starting master pipeline...');
    setIsManualRun(isManual);
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
      const response = await fetch(`/api/run/${MASTER_PIPELINE}`, { method: 'POST' });
      const result = await response.json();
      
      if (result.success) {
        console.log('✅ Pipeline started successfully');
        showMessage(`Pipeline ${isManual ? 'started manually' : 'started automatically'}`, 'success');
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
        default:
          frontendStatus = data.status;
      }
      
             // Update module state
      console.log(`🔄 Updating module ${stepId} to status: ${frontendStatus}`);
      setModuleStates(prev => {
        const newStates = { ...prev, [stepId]: frontendStatus };
        console.log('📊 Updated module states:', newStates);
        return newStates;
      });
      
      // Update current module
      if (frontendStatus === 'running') {
        console.log(`▶️  Setting current module to: ${stepId}`);
        setCurrentModule(stepId);
      } else if (frontendStatus === 'complete' || frontendStatus === 'failed') {
        // Clear current module if this was the running one
        if (currentModule === stepId) {
          setCurrentModule(null);
        }
        
        // Check if this failure means the whole pipeline failed
        if (frontendStatus === 'failed') {
          handlePipelineFailure(stepId);
          return;
        }
        
                 // Check if all modules are now complete
         const updatedStates = { ...moduleStates, [stepId]: frontendStatus };
         const completedCount = Object.values(updatedStates).filter(state => state === 'complete').length;
         const failedCount = Object.values(updatedStates).filter(state => state === 'failed').length;
         
         console.log(`📈 Progress check: ${completedCount}/${MASTER_MODULES.length} complete, ${failedCount} failed`);
         
         // If all modules are complete and none failed, mark pipeline as complete
         if (completedCount === MASTER_MODULES.length && failedCount === 0) {
           console.log('🎉 All modules complete! Triggering pipeline completion...');
           handlePipelineComplete();
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
    
    // Calculate next scheduled run
    calculateNextScheduledRun();
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
        runMasterPipeline(false);
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
    
    return {
      total: totalModules,
      completed: completedModules,
      failed: failedModules,
      running: runningModules,
      pending: totalModules - completedModules - failedModules - runningModules,
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

  const getModuleIcon = (state) => {
    switch (state) {
      case 'complete':
        return <CheckCircle className="h-4 w-4 text-green-600" />;
      case 'running':
        return <Loader className="h-4 w-4 text-blue-600 animate-spin" />;
      case 'failed':
        return <AlertTriangle className="h-4 w-4 text-red-600" />;
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
          description: 'Waiting for next scheduled run',
          color: 'border-gray-300 bg-gray-50 dark:bg-gray-800'
        };
    }
  };

  const status = getStatusDisplay();

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
              
              <button
                onClick={() => runMasterPipeline(true)}
                disabled={pipelineStatus === 'running'}
                className={`inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md transition-colors ${
                  pipelineStatus === 'running'
                    ? 'bg-gray-200 dark:bg-gray-700 text-gray-500 dark:text-gray-400 cursor-not-allowed'
                    : 'text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500'
                }`}
              >
                <Play className="mr-2 h-5 w-5" />
                {pipelineStatus === 'running' ? 'Running...' : 'Run Pipeline'}
              </button>
            </div>
          </div>

          {/* Status Information Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
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

            {/* Next Scheduled Run */}
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <div className="flex items-center space-x-3">
                <Clock className="h-8 w-8 text-blue-600" />
                <div>
                  <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                    Next Scheduled Run
                  </h3>
                  <p className="text-lg font-semibold text-gray-900 dark:text-gray-100 mt-1">
                    9:00 AM (Jerusalem)
                  </p>
                  <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                    {countdown || 'Calculating...'}
                  </p>
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
                            : 'text-gray-700 dark:text-gray-300'
                        }`}>
                          {module.name}
                        </p>
                      </div>
                      <div className={`text-xs px-2 py-1 rounded-full ${
                        state === 'complete'
                          ? 'bg-green-100 dark:bg-green-900/40 text-green-800 dark:text-green-200'
                          : state === 'failed'
                          ? 'bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-200'
                          : state === 'running'
                          ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-200'
                          : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                      }`}>
                        {state === 'pending' ? 'Pending' : 
                         state === 'running' ? 'Running' :
                         state === 'complete' ? 'Done' :
                         state === 'failed' ? 'Failed' : state}
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