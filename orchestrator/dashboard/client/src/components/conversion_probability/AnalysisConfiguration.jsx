import React from 'react';
import { Search, BarChart3, RefreshCw, Calendar, Users, AlertTriangle, Zap } from 'lucide-react';
import { timeframeOptions } from './utils/conversionUtils';

const AnalysisConfiguration = ({
  config,
  onConfigChange,
  onAnalyzeProperties,
  onStartAnalysis,
  onRunNewHierarchicalAnalysis,
  isAnalyzingProperties,
  isAnalyzing,
  isRunningNewAnalysis,
  propertyAnalysis
}) => {
  const currentTimeframeOption = timeframeOptions.find(option => option.value === config.timeframe) || timeframeOptions[0];

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
      <div className="p-6">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-2">
          Step 1: Configure Analysis Parameters
        </h2>
        <p className="text-gray-600 dark:text-gray-400 mb-6">
          Set the time period and minimum group size for your analysis. These settings determine which data to analyze and how to group users.
        </p>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              <Calendar className="inline h-4 w-4 mr-1" />
              Time Period
            </label>
            <select
              value={config.timeframe}
              onChange={(e) => onConfigChange({ ...config, timeframe: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
            >
              {timeframeOptions.map(option => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              {currentTimeframeOption.description}
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              <Users className="inline h-4 w-4 mr-1" />
              Minimum Group Size
            </label>
            <input
              type="number"
              min="10"
              max="1000"
              value={config.min_cohort_size}
              onChange={(e) => onConfigChange({ ...config, min_cohort_size: parseInt(e.target.value) || 50 })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Minimum number of users required in each group for reliable statistics
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              <BarChart3 className="inline h-4 w-4 mr-1" />
              Minimum Price Samples
            </label>
            <input
              type="number"
              min="10"
              max="1000"
              value={config.min_price_samples}
              onChange={(e) => onConfigChange({ ...config, min_price_samples: parseInt(e.target.value) || 100 })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Minimum price samples required per product for reliable price bucketing
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-1 gap-6 mb-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              <RefreshCw className="inline h-4 w-4 mr-1" />
              Force Recalculation
            </label>
            <div className="flex items-center mt-2">
              <input
                type="checkbox"
                checked={config.force_recalculate}
                onChange={(e) => onConfigChange({ ...config, force_recalculate: e.target.checked })}
                className="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
              />
              <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                Recalculate even if cached results exist
              </span>
            </div>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Check this to ignore previously calculated results and run fresh analysis
            </p>
          </div>
        </div>

        <div className="mb-6 p-4 border-2 border-emerald-200 dark:border-emerald-700 rounded-lg bg-emerald-50 dark:bg-emerald-900/20">
          <h3 className="text-lg font-semibold text-emerald-800 dark:text-emerald-200 mb-2 flex items-center">
            <Zap className="h-5 w-5 mr-2" />
            NEW: Enhanced Hierarchical Analysis
          </h3>
          <p className="text-sm text-emerald-700 dark:text-emerald-300 mb-4">
            Try our new 5-stage hierarchical pipeline with improved rollup methodology, better validation, and more accurate results.
          </p>
          
          <button
            onClick={onRunNewHierarchicalAnalysis}
            disabled={isRunningNewAnalysis || isAnalyzing || isAnalyzingProperties}
            className="inline-flex items-center px-8 py-3 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-emerald-600 hover:bg-emerald-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isRunningNewAnalysis ? (
              <RefreshCw className="animate-spin h-5 w-5 mr-2" />
            ) : (
              <Zap className="h-5 w-5 mr-2" />
            )}
            {isRunningNewAnalysis ? 'Running New Analysis...' : 'Run New Conversion Analysis'}
          </button>
          
          <div className="mt-3 text-xs text-emerald-600 dark:text-emerald-400">
            ✨ Features: 5-stage validation • Tree-based rollup • Levels 3-6 only • Enhanced accuracy scoring
          </div>
        </div>

        <div className="mb-4 p-4 border border-gray-200 dark:border-gray-700 rounded-lg">
          <h3 className="text-md font-medium text-gray-700 dark:text-gray-300 mb-3">
            Legacy Analysis System
          </h3>
          
          <div className="flex gap-3">
            <button
              onClick={onAnalyzeProperties}
              disabled={isAnalyzingProperties || isRunningNewAnalysis}
              className="inline-flex items-center px-6 py-3 border border-indigo-300 dark:border-indigo-600 rounded-md shadow-sm text-sm font-medium text-indigo-700 dark:text-indigo-300 bg-indigo-50 dark:bg-indigo-900/30 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isAnalyzingProperties ? (
                <RefreshCw className="animate-spin h-5 w-5 mr-2" />
              ) : (
                <Search className="h-5 w-5 mr-2" />
              )}
              {isAnalyzingProperties ? 'Discovering...' : 'Discover Data Structure'}
            </button>
            
            <button
              onClick={onStartAnalysis}
              disabled={isAnalyzing || isAnalyzingProperties || isRunningNewAnalysis}
              className="inline-flex items-center px-6 py-3 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isAnalyzing ? (
                <RefreshCw className="animate-spin h-5 w-5 mr-2" />
              ) : (
                <BarChart3 className="h-5 w-5 mr-2" />
              )}
              {isAnalyzing ? 'Analyzing...' : 'Run Legacy Analysis'}
            </button>
          </div>
        </div>
        
        {!propertyAnalysis && (
          <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
            <div className="flex items-center">
              <AlertTriangle className="h-5 w-5 text-blue-600 dark:text-blue-400 mr-2" />
              <p className="text-sm text-blue-800 dark:text-blue-200">
                <strong>Recommendation:</strong> Try the new hierarchical analysis above for better results, or use the legacy system below. You can optionally "Discover Data Structure" first to preview what will be analyzed.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AnalysisConfiguration; 