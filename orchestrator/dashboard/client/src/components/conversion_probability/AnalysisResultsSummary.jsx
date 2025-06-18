import React from 'react';
import { CheckCircle, AlertTriangle, Users, TrendingUp } from 'lucide-react';
import { formatDate } from './utils/conversionUtils';

const AnalysisResultsSummary = ({ analysisResults }) => {
  if (!analysisResults) return null;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
      <div className="p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
          Step 3: Conversion Analysis Complete
        </h3>
        <p className="text-gray-600 dark:text-gray-400 mb-4">
          Analysis finished! We found conversion patterns across {analysisResults.summary?.valid_cohorts?.toLocaleString() || 0} user segments. Use the search below to explore specific segments and their conversion rates.
        </p>
        
        {/* Summary Statistics */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
            <div className="text-sm text-green-800 dark:text-green-300 font-medium">Successful Analyses</div>
            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
              {analysisResults.summary?.valid_cohorts?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-green-600 dark:text-green-400">
              User segments with reliable conversion data
            </div>
          </div>
          
          <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg">
            <div className="text-sm text-red-800 dark:text-red-300 font-medium">Insufficient Data</div>
            <div className="text-2xl font-bold text-red-600 dark:text-red-400">
              {analysisResults.summary?.invalid_cohorts?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-red-600 dark:text-red-400">
              Segments with too few users
            </div>
          </div>
          
          <div className="p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
            <div className="text-sm text-yellow-800 dark:text-yellow-300 font-medium">Expanded Segments</div>
            <div className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
              {analysisResults.summary?.cohorts_with_dropped_properties?.toLocaleString() || 0}
            </div>
            <div className="text-xs text-yellow-600 dark:text-yellow-400">
              Segments broadened to include more users
            </div>
          </div>
          
          <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
            <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Analysis Date</div>
            <div className="text-lg font-semibold text-blue-600 dark:text-blue-400">
              {analysisResults.last_updated ? formatDate(analysisResults.last_updated) : 'N/A'}
            </div>
            <div className="text-xs text-blue-600 dark:text-blue-400">
              When this analysis was completed
            </div>
          </div>
        </div>
        
        {/* Analysis Quality Indicators */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div className="p-4 bg-gradient-to-r from-green-50 to-blue-50 dark:from-green-900/20 dark:to-blue-900/20 rounded-lg border border-green-200 dark:border-green-800">
            <div className="flex items-center mb-2">
              <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400 mr-2" />
              <h4 className="text-sm font-semibold text-green-900 dark:text-green-200">Analysis Quality</h4>
            </div>
            <div className="text-sm text-green-800 dark:text-green-300 space-y-1">
              <div>
                <strong>Success Rate:</strong> {analysisResults.summary?.valid_cohorts && analysisResults.summary?.total_combinations_attempted ? 
                  ((analysisResults.summary.valid_cohorts / analysisResults.summary.total_combinations_attempted) * 100).toFixed(1) : 0}% of segments produced reliable results
              </div>
              <div>
                <strong>Data Coverage:</strong> {analysisResults.summary?.total_users_analyzed?.toLocaleString() || 0} users analyzed across all segments
              </div>
            </div>
          </div>
          
          <div className="p-4 bg-gradient-to-r from-purple-50 to-indigo-50 dark:from-purple-900/20 dark:to-indigo-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
            <div className="flex items-center mb-2">
              <TrendingUp className="h-5 w-5 text-purple-600 dark:text-purple-400 mr-2" />
              <h4 className="text-sm font-semibold text-purple-900 dark:text-purple-200">Statistical Reliability</h4>
            </div>
            <div className="text-sm text-purple-800 dark:text-purple-300 space-y-1">
              <div>
                <strong>Confidence Intervals:</strong> Available for all valid segments
              </div>
              <div>
                <strong>Minimum Cohort Size:</strong> {analysisResults.analysis_parameters?.min_cohort_size || 'N/A'} users per segment
              </div>
            </div>
          </div>
        </div>

        {/* Analysis Warnings Summary */}
        {analysisResults.warnings && analysisResults.warnings.length > 0 && (
          <div className="p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg mb-4">
            <div className="flex items-center mb-2">
              <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400 mr-2" />
              <h4 className="text-sm font-semibold text-yellow-900 dark:text-yellow-200">Analysis Warnings</h4>
            </div>
            <div className="text-sm text-yellow-800 dark:text-yellow-300">
              <div className="mb-2">
                <strong>{analysisResults.warnings.length} warning{analysisResults.warnings.length !== 1 ? 's' : ''} occurred during analysis:</strong>
              </div>
              <ul className="space-y-1 list-disc list-inside">
                {analysisResults.warnings.slice(0, 3).map((warning, index) => (
                  <li key={index}>{warning.message || warning}</li>
                ))}
                {analysisResults.warnings.length > 3 && (
                  <li className="text-yellow-600 dark:text-yellow-400">
                    ... and {analysisResults.warnings.length - 3} more warnings
                  </li>
                )}
              </ul>
            </div>
          </div>
        )}

        {/* Next Steps Guide */}
        <div className="p-4 bg-indigo-50 dark:bg-indigo-900/20 border border-indigo-200 dark:border-indigo-800 rounded-lg">
          <div className="flex items-center mb-2">
            <Users className="h-5 w-5 text-indigo-600 dark:text-indigo-400 mr-2" />
            <h4 className="text-sm font-semibold text-indigo-900 dark:text-indigo-200">Next Steps</h4>
          </div>
          <div className="text-sm text-indigo-800 dark:text-indigo-300 space-y-1">
            <div>✓ Use the search filters below to explore specific products, regions, or price ranges</div>
            <div>✓ Look for segments with high conversion rates and large user counts for optimization opportunities</div>
            <div>✓ Pay attention to confidence intervals - segments with wider intervals may need more data</div>
            <div>✓ Click "Rollup Details" on expanded segments to understand how they were optimized</div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnalysisResultsSummary; 