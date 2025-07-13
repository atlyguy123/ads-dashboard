import React, { useState, useEffect, useCallback, useRef } from 'react';
import { RefreshCw, ArrowLeft } from 'lucide-react';
import { io } from 'socket.io-client';
import { apiRequest } from '../config/api';

const DataPipelinePage = () => {
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState(null);
  const [statusMessage, setStatusMessage] = useState('');
  const [messageType, setMessageType] = useState('');
  const socketRef = useRef(null);

  // Initialize socket connection
  useEffect(() => {
    const socketUrl = process.env.REACT_APP_API_URL || '';
    socketRef.current = io(socketUrl);
    
    socketRef.current.on('connect', () => {
      console.log('Connected to server');
    });
    
    socketRef.current.on('status_update', (data) => {
      handleStatusUpdate(data);
    });

    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);

  // Load pipelines on mount
  useEffect(() => {
    loadPipelines();
  }, []);

  const loadPipelines = async () => {
    try {
      const response = await apiRequest('/api/pipelines');
      const pipelineData = await response.json();
      setPipelines(pipelineData);
      
      // Try to restore previously selected pipeline
      restoreSelectedPipeline(pipelineData);
    } catch (error) {
      console.error('Error loading pipelines:', error);
    }
  };

  const refreshPipelines = async () => {
    try {
      await apiRequest('/api/refresh', { method: 'POST' });
      await loadPipelines();
      showMessage('Pipelines refreshed successfully', 'success');
    } catch (error) {
      showMessage('Error refreshing pipelines', 'error');
    }
  };

  const selectPipeline = (pipeline, fromClick = false) => {
    setSelectedPipeline(pipeline);
    
    // Save selection to localStorage
    localStorage.setItem('selectedPipeline', pipeline.name);
    console.log(`Selected and saved pipeline: ${pipeline.name}`);
  };

  const restoreSelectedPipeline = (pipelineData) => {
    const savedPipelineName = localStorage.getItem('selectedPipeline');
    if (savedPipelineName) {
      const savedPipeline = pipelineData.find(p => p.name === savedPipelineName);
      if (savedPipeline) {
        setSelectedPipeline(savedPipeline);
        console.log(`Restored selection: ${savedPipelineName}`);
        
        setTimeout(() => {
          showMessage(`Restored previous selection: ${savedPipelineName}`, 'success');
        }, 500);
      } else {
        localStorage.removeItem('selectedPipeline');
        console.log(`Pipeline '${savedPipelineName}' no longer exists, cleared selection`);
      }
    }
  };

  const runPipeline = async (pipelineName) => {
    try {
      const response = await apiRequest(`/api/run/${pipelineName}`, { method: 'POST' });
      const result = await response.json();
      
      if (result.success) {
        showMessage('Pipeline started successfully', 'success');
      } else {
        showMessage(`Error: ${result.message}`, 'error');
      }
    } catch (error) {
      showMessage('Error starting pipeline', 'error');
    }
  };

  const runStep = async (pipelineName, stepId) => {
    try {
      const response = await apiRequest(`/api/run/${pipelineName}/${stepId}`, { method: 'POST' });
      const result = await response.json();
      
      if (result.success) {
        showMessage(`Step '${stepId}' started successfully`, 'success');
      } else {
        showMessage(`Error: ${result.message}`, 'error');
      }
    } catch (error) {
      showMessage('Error starting step', 'error');
    }
  };

  const markTested = async (pipelineName, stepId, tested) => {
    try {
      const response = await apiRequest(`/api/mark_tested/${pipelineName}/${stepId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tested })
      });
      
      const result = await response.json();
      
      if (result.success) {
        // Update local state
        setSelectedPipeline(prev => {
          if (!prev) return prev;
          const updatedPipeline = { ...prev };
          const step = updatedPipeline.steps.find(s => s.id === stepId);
          if (step) step.tested = tested;
          return updatedPipeline;
        });
        
        showMessage(result.message, 'success');
      } else {
        showMessage(`Error: ${result.message}`, 'error');
      }
    } catch (error) {
      showMessage('Error updating step status', 'error');
    }
  };

  const cancelStep = async (pipelineName, stepId) => {
    console.log(`🛑 CLIENT: Attempting to cancel step '${stepId}' in pipeline '${pipelineName}'`);
    
    try {
      const response = await apiRequest(`/api/cancel/${pipelineName}/${stepId}`, {
        method: 'POST'
      });
      
      const result = await response.json();
      console.log(`🛑 CLIENT: Cancel response:`, result);
      
      if (result.success) {
        showMessage(result.message, 'success');
        console.log(`✅ CLIENT: Successfully cancelled step '${stepId}'`);
      } else {
        showMessage(`Cancel failed: ${result.message}`, 'error');
        console.log(`❌ CLIENT: Failed to cancel step '${stepId}': ${result.message}`);
      }
      
      // Refresh the pipeline view to get updated status
      console.log(`🔄 CLIENT: Refreshing pipeline view after cancel attempt`);
      loadPipelines();
    } catch (error) {
      console.error(`❌ CLIENT: Error cancelling step '${stepId}':`, error);
      showMessage(`Error cancelling step: ${error.message}`, 'error');
    }
  };

  const resetAllSteps = async (pipelineName) => {
    console.log(`🔄 RESET ALL: Resetting all steps in pipeline '${pipelineName}'`);
    
    if (!window.confirm(`Are you sure you want to reset ALL steps in pipeline '${pipelineName}'?\n\nThis will:\n• Cancel any running steps\n• Reset all step statuses to pending\n• Clear all progress and error states\n\nThis action cannot be undone.`)) {
      return;
    }
    
    try {
      const response = await apiRequest(`/api/reset-all/${pipelineName}`, {
        method: 'POST'
      });
      
      const result = await response.json();
      console.log(`🔄 RESET ALL: Reset response:`, result);
      
      if (result.success) {
        showMessage(result.message, 'success');
        console.log(`✅ RESET ALL: Successfully reset all steps in pipeline '${pipelineName}'`);
        loadPipelines();
      } else {
        showMessage(`Reset all failed: ${result.message}`, 'error');
        console.log(`❌ RESET ALL: Failed to reset all steps in pipeline '${pipelineName}': ${result.message}`);
      }
    } catch (error) {
      console.error(`❌ RESET ALL: Error resetting all steps in pipeline '${pipelineName}':`, error);
      showMessage(`Error resetting all steps: ${error.message}`, 'error');
    }
  };

  const resetStep = async (pipelineName, stepId) => {
    console.log(`🔄 RESET STEP: Resetting step '${stepId}' in pipeline '${pipelineName}'`);
    
    try {
      const response = await apiRequest(`/api/reset/${pipelineName}/${stepId}`, {
        method: 'POST'
      });
      
      const result = await response.json();
      console.log(`🔄 RESET STEP: Reset response:`, result);
      
      if (result.success) {
        showMessage(result.message, 'success');
        console.log(`✅ RESET STEP: Successfully reset step '${stepId}'`);
        loadPipelines();
      } else {
        showMessage(`Reset failed: ${result.message}`, 'error');
        console.log(`❌ RESET STEP: Failed to reset step '${stepId}': ${result.message}`);
      }
    } catch (error) {
      console.error(`❌ RESET STEP: Error resetting step '${stepId}':`, error);
      showMessage(`Error resetting step: ${error.message}`, 'error');
    }
  };

  const handleStatusUpdate = (data) => {
    console.log('📡 DEBUG PAGE: WebSocket status update received:', data);
    
    // Update the pipeline list to reflect status changes
    setPipelines(prev => {
      const updated = prev.map(pipeline => {
        if (pipeline.name === data.pipeline) {
          const updatedPipeline = { ...pipeline };
          if (!updatedPipeline.status) {
            updatedPipeline.status = {};
          }
          updatedPipeline.status[data.step] = {
            status: data.status,
            timestamp: data.timestamp,
            error_message: data.error_message
          };
          return updatedPipeline;
        }
        return pipeline;
      });
      return updated;
    });
    
    // Also update the selected pipeline if it matches
    if (selectedPipeline && selectedPipeline.name === data.pipeline) {
      setSelectedPipeline(prev => {
        if (!prev) return prev;
        const updatedPipeline = { ...prev };
        if (!updatedPipeline.status) {
          updatedPipeline.status = {};
        }
        updatedPipeline.status[data.step] = {
          status: data.status,
          timestamp: data.timestamp,
          error_message: data.error_message
        };
        return updatedPipeline;
      });
    }
  };

  const showMessage = (message, type) => {
    setStatusMessage(message);
    setMessageType(type);
    
    setTimeout(() => {
      setStatusMessage('');
      setMessageType('');
    }, 5000);
  };

  const renderStep = (step, status) => {
    const stepStatus = status.status || 'pending';
    const tested = step.tested;
    
    let icon = '○';
    let statusText = '';
    let statusClass = '';
    
    if (stepStatus === 'running') {
      icon = '●';
      statusText = 'RUNNING...';
      statusClass = 'text-blue-600 dark:text-blue-400 font-semibold';
    } else if (stepStatus === 'success') {
      icon = '✓';
      statusText = 'SUCCESS';
      statusClass = 'text-green-600 dark:text-green-400 font-semibold';
    } else if (stepStatus === 'failed') {
      icon = '✗';
      statusText = 'FAILED';
      statusClass = 'text-red-600 dark:text-red-400 font-semibold';
    } else if (stepStatus === 'cancelled') {
      icon = '🛑';
      statusText = 'CANCELLED';
      statusClass = 'text-orange-600 dark:text-orange-400 font-semibold';
    }

    let stepClass = 'bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-lg p-4 transition-all hover:shadow-md ';
    if (stepStatus === 'pending') stepClass += 'border-l-4 border-l-gray-300 dark:border-l-gray-500';
    else if (stepStatus === 'running') stepClass += 'border-l-4 border-l-blue-500 bg-blue-50/50 dark:bg-blue-900/10';
    else if (stepStatus === 'success') stepClass += 'border-l-4 border-l-green-500 bg-green-50/50 dark:bg-green-900/10';
    else if (stepStatus === 'failed') stepClass += 'border-l-4 border-l-red-500 bg-red-50/50 dark:bg-red-900/10';
    else if (stepStatus === 'cancelled') stepClass += 'border-l-4 border-l-orange-500 bg-orange-50/50 dark:bg-orange-900/10';

    return (
      <div key={step.id} className={stepClass}>
        <div className="flex items-start space-x-4">
          <div className={`w-6 h-6 rounded-full flex items-center justify-center text-sm font-bold text-white mt-1 ${
            stepStatus === 'pending' ? 'bg-gray-400 dark:bg-gray-500' :
            stepStatus === 'running' ? 'bg-blue-500 animate-pulse' :
            stepStatus === 'success' ? 'bg-green-500' :
            stepStatus === 'failed' ? 'bg-red-500' :
            stepStatus === 'cancelled' ? 'bg-orange-500' : 'bg-gray-400'
          }`}>
            {icon}
          </div>
          
          <div className="flex-1 min-w-0">
            <div className="flex items-center space-x-2 mb-1">
              <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                {step.name || step.id}
              </h4>
              {statusText && (
                <span className={`text-xs px-2 py-1 rounded-full font-medium ${
                  stepStatus === 'running' ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' :
                  stepStatus === 'success' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' :
                  stepStatus === 'failed' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300' :
                  stepStatus === 'cancelled' ? 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300' :
                  'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300'
                }`}>
                  {statusText}
                </span>
              )}
            </div>
            
            <div className="text-xs text-gray-600 dark:text-gray-400 font-mono mb-2 bg-gray-50 dark:bg-gray-800 px-2 py-1 rounded">
              {step.file}
            </div>
            
            {status.error_message && (
              <div className="text-xs text-red-700 dark:text-red-300 mt-2 p-3 bg-red-50 dark:bg-red-900/20 rounded-md border border-red-200 dark:border-red-800">
                <strong>Error:</strong> {status.error_message}
              </div>
            )}
            
            {stepStatus === 'success' && status.timestamp && (
              <div className="text-xs text-green-600 dark:text-green-400 mt-2">
                Completed: {new Date(status.timestamp).toLocaleTimeString()}
              </div>
            )}
          </div>
          
          <div className="flex flex-wrap gap-2 mt-1">
            <button
              onClick={() => markTested(selectedPipeline.name, step.id, !tested)}
              className={`inline-flex items-center px-3 py-1 text-xs font-medium rounded-md border transition-colors ${
                tested 
                  ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-300 dark:border-green-700 hover:bg-green-200 dark:hover:bg-green-900/50' 
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 border-gray-300 dark:border-gray-600 hover:bg-gray-200 dark:hover:bg-gray-600'
              }`}
            >
              {tested ? '✓ Tested' : 'Mark Tested'}
            </button>
            
            <button
              onClick={() => runStep(selectedPipeline.name, step.id)}
              disabled={stepStatus === 'running'}
              className={`inline-flex items-center px-3 py-1 text-xs font-medium rounded-md border transition-colors ${
                stepStatus === 'running' 
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 border-gray-300 dark:border-gray-600 cursor-not-allowed' 
                  : 'bg-blue-600 text-white border-blue-600 hover:bg-blue-700 hover:border-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500'
              }`}
            >
              {stepStatus === 'running' ? '⏳ Running...' : '▶️ Run'}
            </button>
            
            {stepStatus === 'running' && (
              <button
                onClick={() => cancelStep(selectedPipeline.name, step.id)}
                className="inline-flex items-center px-3 py-1 text-xs font-medium rounded-md border bg-red-600 text-white border-red-600 hover:bg-red-700 hover:border-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 transition-colors"
              >
                🛑 Cancel
              </button>
            )}
            
            {(stepStatus === 'cancelled' || stepStatus === 'failed' || stepStatus === 'success') && (
              <button
                onClick={() => resetStep(selectedPipeline.name, step.id)}
                className="inline-flex items-center px-3 py-1 text-xs font-medium rounded-md border bg-gray-600 text-white border-gray-600 hover:bg-gray-700 hover:border-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-500 transition-colors"
              >
                🔄 Reset
              </button>
            )}
          </div>
        </div>
      </div>
    );
  };

  const allTested = selectedPipeline ? selectedPipeline.steps.every(step => step.tested) : false;

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
                Manage and execute data processing pipelines
              </p>
            </div>
            
            <div className="flex items-center space-x-4">
              <button
                onClick={refreshPipelines}
                className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Refresh
              </button>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Sidebar */}
          <div className="lg:col-span-1">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="p-6 border-b border-gray-200 dark:border-gray-700">
                <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Available Pipelines</h2>
              </div>
              <div className="p-4 space-y-2 max-h-96 overflow-y-auto">
                {pipelines.map(pipeline => (
                  <div
                    key={pipeline.name}
                    onClick={() => selectPipeline(pipeline, true)}
                    className={`p-3 rounded-lg border cursor-pointer transition-all ${
                      selectedPipeline?.name === pipeline.name
                        ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-900 dark:text-blue-100 border-blue-200 dark:border-blue-800'
                        : 'bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 hover:border-blue-300 dark:hover:border-blue-600'
                    }`}
                  >
                    <div className="font-medium text-sm mb-1">{pipeline.name}</div>
                    <div className={`text-xs ${
                      selectedPipeline?.name === pipeline.name 
                        ? 'text-blue-600 dark:text-blue-300' 
                        : 'text-gray-500 dark:text-gray-400'
                    }`}>
                      {pipeline.description}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Main Content */}
          <div className="lg:col-span-3">
            {!selectedPipeline ? (
              <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-8 text-center">
                <div className="text-gray-400 dark:text-gray-500 mb-4">
                  <ArrowLeft className="mx-auto h-12 w-12" />
                </div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
                  Select a pipeline to view details
                </h3>
                <p className="text-gray-600 dark:text-gray-400">
                  Choose a pipeline from the sidebar to see its steps and run it.
                </p>
              </div>
            ) : (
              <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                {/* Pipeline Header */}
                <div className="p-6 border-b border-gray-200 dark:border-gray-700">
                  <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-2">
                    {selectedPipeline.name}
                  </h2>
                  <p className="text-gray-600 dark:text-gray-400 mb-4">
                    {selectedPipeline.description}
                  </p>
                  
                  <div className="flex flex-wrap gap-3">
                    <button
                      onClick={() => runPipeline(selectedPipeline.name)}
                      disabled={!allTested}
                      className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md ${
                        allTested
                          ? 'text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500'
                          : 'text-gray-500 bg-gray-200 dark:bg-gray-600 dark:text-gray-400 cursor-not-allowed'
                      }`}
                    >
                      Run Pipeline
                    </button>
                    
                    <button
                      onClick={() => resetAllSteps(selectedPipeline.name)}
                      className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-gray-600 hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-500"
                      title="Reset all steps to pending status"
                    >
                      🔄 Reset All
                    </button>
                    
                    {!allTested && (
                      <div className="flex items-center text-orange-600 dark:text-orange-400 text-sm">
                        <span>All steps must be tested before running</span>
                      </div>
                    )}
                  </div>
                </div>
                
                {/* Pipeline Steps */}
                <div className="p-6 space-y-4">
                  {selectedPipeline.steps.map(step => 
                    renderStep(step, selectedPipeline.status[step.id] || {})
                  )}
                </div>
                
                {/* Status Message */}
                {statusMessage && (
                  <div className="p-6 border-t border-gray-200 dark:border-gray-700">
                    <div className={`p-4 rounded-lg ${
                      messageType === 'success' 
                        ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-800' 
                        : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
                    }`}>
                      {statusMessage}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataPipelinePage; 