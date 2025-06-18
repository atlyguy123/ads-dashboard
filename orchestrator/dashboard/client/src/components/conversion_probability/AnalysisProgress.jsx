import React from 'react';
import { RefreshCw } from 'lucide-react';

const AnalysisProgress = ({ 
  analysisProgress, 
  isAnalyzing 
}) => {
  if (!isAnalyzing || !analysisProgress) return null;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
      <div className="p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
          Step 3: Running Conversion Analysis
        </h3>
        <p className="text-gray-600 dark:text-gray-400 mb-4">
          Calculating conversion rates for each user segment. This process analyzes {analysisProgress.total_combinations?.toLocaleString() || 0} different combinations of user properties.
        </p>
        
        {/* Main Progress Bar */}
        <div className="mb-6">
          <div className="flex justify-between text-sm text-gray-600 dark:text-gray-400 mb-2">
            <span>Overall Progress</span>
            <span>{analysisProgress.percentage_complete?.toFixed(1) || 0}% Complete</span>
          </div>
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3">
            <div 
              className="bg-indigo-600 h-3 rounded-full transition-all duration-300 relative"
              style={{ width: `${analysisProgress.percentage_complete || 0}%` }}
            >
              <div className="absolute inset-0 bg-indigo-400 rounded-full animate-pulse opacity-50"></div>
            </div>
          </div>
        </div>
        
        {/* Progress Statistics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
            <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Segments Processed</div>
            <div className="text-xl font-bold text-blue-600 dark:text-blue-400">
              {analysisProgress.completed_combinations?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-blue-600 dark:text-blue-400">
              of {analysisProgress.total_combinations?.toLocaleString() || 0} total
            </div>
          </div>
          
          <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
            <div className="text-sm text-green-800 dark:text-green-300 font-medium">Valid Groups Found</div>
            <div className="text-xl font-bold text-green-600 dark:text-green-400">
              {analysisProgress.valid_cohorts_found?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-green-600 dark:text-green-400">
              Groups with enough users for analysis
            </div>
          </div>
          
          <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg">
            <div className="text-sm text-red-800 dark:text-red-300 font-medium">Skipped Groups</div>
            <div className="text-xl font-bold text-red-600 dark:text-red-400">
              {analysisProgress.invalid_cohorts_found?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-red-600 dark:text-red-400">
              Groups with insufficient data
            </div>
          </div>
          
          <div className="p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
            <div className="text-sm text-yellow-800 dark:text-yellow-300 font-medium">Expanded Groups</div>
            <div className="text-xl font-bold text-yellow-600 dark:text-yellow-400">
              {analysisProgress.cohorts_with_dropped_properties?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-yellow-600 dark:text-yellow-400">
              Groups broadened to include more users
            </div>
          </div>
        </div>

        {/* Processing Details */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div className="p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
            <div className="text-sm text-purple-800 dark:text-purple-300 font-medium">Processing Rate</div>
            <div className="text-lg font-bold text-purple-600 dark:text-purple-400">
              {analysisProgress.combinations_per_minute ? analysisProgress.combinations_per_minute.toFixed(1) : '0'} per minute
            </div>
            <div className="text-xs text-purple-600 dark:text-purple-400">
              Current analysis speed
            </div>
          </div>
          
          <div className="p-3 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg">
            <div className="text-sm text-indigo-800 dark:text-indigo-300 font-medium">Current Step</div>
            <div className="text-sm font-medium text-indigo-600 dark:text-indigo-400">
              {analysisProgress.current_step || 'Initializing...'}
            </div>
            <div className="text-xs text-indigo-600 dark:text-indigo-400">
              What's happening now
            </div>
          </div>
        </div>
        
        {/* Current Combination Being Processed */}
        {analysisProgress.current_combination && (
          <div className="p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
            <div className="text-sm text-gray-800 dark:text-gray-300 font-medium mb-2 flex items-center">
              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
              Currently Processing
            </div>
            <div className="text-sm text-gray-600 dark:text-gray-400">
              <span className="font-medium">Product:</span> {analysisProgress.current_combination.product_id || 'N/A'} • 
              <span className="font-medium ml-2">Price:</span> {analysisProgress.current_combination.price_bucket || 'N/A'} • 
              <span className="font-medium ml-2">Location:</span> {analysisProgress.current_combination.country || 'Any'} / {analysisProgress.current_combination.region || 'Any'} • 
              <span className="font-medium ml-2">Store:</span> {analysisProgress.current_combination.app_store || 'Any'}
            </div>
          </div>
        )}

        {/* Time Estimates */}
        {analysisProgress.estimated_time_remaining && (
          <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
            <div className="text-sm text-blue-800 dark:text-blue-300">
              <strong>Estimated time remaining:</strong> {Math.ceil(analysisProgress.estimated_time_remaining)} minutes
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AnalysisProgress; 