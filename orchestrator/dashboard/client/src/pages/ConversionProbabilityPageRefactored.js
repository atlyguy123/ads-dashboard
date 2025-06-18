import React from 'react';
import { BarChart3, Info, AlertTriangle } from 'lucide-react';

// Import our new components from the proper client/src directory structure
import AlertMessages from '../components/conversion_probability/AlertMessages';
import AnalysisConfiguration from '../components/conversion_probability/AnalysisConfiguration';
import PropertyAnalysisResults from '../components/conversion_probability/PropertyAnalysisResults';
import AnalysisProgress from '../components/conversion_probability/AnalysisProgress';
import AnalysisResultsSummary from '../components/conversion_probability/AnalysisResultsSummary';
import AnalysisResultsHierarchy from '../components/conversion_probability/AnalysisResultsHierarchy';

// Import the custom hook from the proper client/src directory structure
import { useConversionProbability } from '../hooks/useConversionProbability';

const ConversionProbabilityPageRefactored = () => {
  const {
    // State
    config,
    analysisProgress,
    analysisResults,
    isAnalyzing,
    isRunningNewAnalysis,
    propertyAnalysis,
    isAnalyzingProperties,
    error,
    success,
    expandedSections,

    // Actions
    setConfig,
    setError,
    setSuccess,
    analyzeProperties,
    startAnalysis,
    runNewHierarchicalAnalysis,
    toggleSection
  } = useConversionProbability();

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Conversion Probability Analysis
        </h1>
        <p className="text-gray-600 dark:text-gray-400 mb-4">
          Analyze conversion probabilities for different user cohorts based on product, price, region, country, and app store properties.
        </p>
        
        {/* Workflow Guide */}
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
            <Info className="h-4 w-4 mr-2" />
            How This Analysis Works
          </h3>
          <div className="text-sm text-blue-800 dark:text-blue-300 space-y-1">
            <p><strong>NEW:</strong> Try "Run New Conversion Analysis" for enhanced 5-stage hierarchical analysis</p>
            <p><strong>Legacy:</strong> Use "Discover Data Structure" to preview segments, then "Run Legacy Analysis"</p>
            <p><strong>Results:</strong> Review conversion rates in the hierarchical view with rollup information</p>
          </div>
        </div>
      </div>

      {/* Alert Messages */}
      <AlertMessages
        error={error}
        success={success}
        onClearError={() => setError(null)}
        onClearSuccess={() => setSuccess(null)}
      />

      {/* Getting Started Guide - shown when no analysis has been run */}
      {!propertyAnalysis && !analysisResults && !isRunningNewAnalysis && (
        <div className="bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-900/20 dark:to-blue-900/20 border border-indigo-200 dark:border-indigo-800 rounded-lg p-6 mb-6">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <div className="flex items-center justify-center h-12 w-12 rounded-md bg-indigo-500 text-white">
                <BarChart3 className="h-6 w-6" />
              </div>
            </div>
            <div className="ml-4 flex-1">
              <h3 className="text-lg font-semibold text-indigo-900 dark:text-indigo-200 mb-2">
                Welcome to Conversion Probability Analysis
              </h3>
              <p className="text-indigo-800 dark:text-indigo-300 mb-4">
                This powerful tool analyzes your user data to identify conversion patterns across different user segments. 
                You'll discover how conversion rates vary by product, price range, geographic location, and app store.
              </p>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                <div className="p-3 bg-white dark:bg-indigo-900/30 rounded-lg border border-indigo-200 dark:border-indigo-700">
                  <h4 className="font-medium text-indigo-900 dark:text-indigo-200 mb-2">New Enhanced Analysis:</h4>
                  <ul className="text-sm text-indigo-800 dark:text-indigo-300 space-y-1">
                    <li>• 5-stage validation pipeline</li>
                    <li>• Tree-based rollup methodology</li>
                    <li>• Levels 3-6 only (Price → Region)</li>
                    <li>• Enhanced accuracy scoring</li>
                  </ul>
                </div>
                
                <div className="p-3 bg-white dark:bg-indigo-900/30 rounded-lg border border-indigo-200 dark:border-indigo-700">
                  <h4 className="font-medium text-indigo-900 dark:text-indigo-200 mb-2">What You'll Discover:</h4>
                  <ul className="text-sm text-indigo-800 dark:text-indigo-300 space-y-1">
                    <li>• Trial-to-paid conversion rates by segment</li>
                    <li>• Refund patterns across different user groups</li>
                    <li>• Geographic and pricing insights</li>
                    <li>• Statistical confidence for all metrics</li>
                  </ul>
                </div>
              </div>
              
              <div className="flex items-center p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
                <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400 mr-2 flex-shrink-0" />
                <div className="text-sm text-yellow-800 dark:text-yellow-200">
                  <strong>Ready to start?</strong> Configure your analysis parameters below, then click "Run New Conversion Analysis" for the best results, or try the legacy system if you need the old methodology.
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Step 1: Analysis Configuration */}
      <AnalysisConfiguration
        config={config}
        onConfigChange={setConfig}
        onAnalyzeProperties={analyzeProperties}
        onStartAnalysis={startAnalysis}
        onRunNewHierarchicalAnalysis={runNewHierarchicalAnalysis}
        isAnalyzingProperties={isAnalyzingProperties}
        isAnalyzing={isAnalyzing}
        isRunningNewAnalysis={isRunningNewAnalysis}
        propertyAnalysis={propertyAnalysis}
      />

      {/* Step 2: Property Analysis Results (Optional Preview) */}
      <PropertyAnalysisResults
        propertyAnalysis={propertyAnalysis}
        expandedSections={expandedSections}
        onToggleSection={toggleSection}
        config={config}
      />

      {/* Step 3A: Analysis Progress (shown while analysis is running) */}
      <AnalysisProgress
        analysisProgress={analysisProgress}
        isAnalyzing={isAnalyzing}
      />

      {/* Step 3B: Analysis Results Summary (shown when analysis is complete) */}
      <AnalysisResultsSummary
        analysisResults={analysisResults}
      />

      {/* Step 4: Analysis Results Hierarchy (replaces search functionality) */}
      <AnalysisResultsHierarchy
        analysisResults={analysisResults}
        expandedSections={expandedSections}
        onToggleSection={toggleSection}
      />
    </div>
  );
};

export default ConversionProbabilityPageRefactored; 