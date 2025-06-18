import React, { useState, useEffect } from 'react';
import { 
  Database, 
  CheckCircle, 
  XCircle,
  Loader,
  Clock
} from 'lucide-react';
import { analyticsPipelineApi } from '../../services/analyticsPipelineApi';

const AnalyticsPipelineControls = () => {
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [hasError, setHasError] = useState(false);

  // Fetch initial status and poll for updates
  useEffect(() => {
    const loadStatus = async () => {
      try {
        const status = await analyticsPipelineApi.getAnalyticsPipelineStatus();
        setPipelineStatus(status);
        setHasError(false);
      } catch (error) {
        console.error('Failed to load analytics pipeline status:', error);
        setHasError(true);
      }
    };

    loadStatus();
    
    // Poll every 30 seconds
    const interval = setInterval(loadStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const getStatusDisplay = () => {
    if (hasError) {
      return { 
        icon: <XCircle className="h-4 w-4 text-red-500" />, 
        text: 'Connection Error', 
        color: 'text-red-500' 
      };
    }
    
    if (!pipelineStatus) {
      return { 
        icon: <Loader className="h-4 w-4 text-gray-500 animate-spin" />, 
        text: 'Loading...', 
        color: 'text-gray-500' 
      };
    }
    
    const currentRun = pipelineStatus.current_run;
    if (currentRun?.is_running) {
      return { 
        icon: <Loader className="h-4 w-4 text-blue-500 animate-spin" />, 
        text: 'Processing Data', 
        color: 'text-blue-500' 
      };
    }
    
    if (currentRun?.completed_at) {
      const completedTime = new Date(currentRun.completed_at);
      const now = new Date();
      const hoursAgo = Math.floor((now - completedTime) / (1000 * 60 * 60));
      
      return { 
        icon: <CheckCircle className="h-4 w-4 text-green-500" />, 
        text: `Updated ${hoursAgo}h ago`, 
        color: 'text-green-500' 
      };
    }
    
    return { 
      icon: <Database className="h-4 w-4 text-gray-500" />, 
      text: 'Ready', 
      color: 'text-gray-500' 
    };
  };

  const status = getStatusDisplay();

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-4">
      <div className="flex items-center space-x-3">
        <div className="flex items-center space-x-2">
          {status.icon}
          <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
            Analytics Pipeline
          </span>
        </div>
        <div className="flex items-center space-x-2">
          <span className={`text-sm ${status.color}`}>
            {status.text}
          </span>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsPipelineControls; 