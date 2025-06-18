import React, { useState, useEffect } from 'react';
import PipelineStageViewer from '../cohort-pipeline/components/PipelineStageViewer';
import ProductFilterConfig from '../cohort-pipeline/components/ProductFilterConfig';
import CohortFilteringStats from '../cohort-pipeline/components/CohortFilteringStats';
import LifecycleRatesTable from '../cohort-pipeline/components/LifecycleRatesTable';
import ARPUDisplay from '../cohort-pipeline/components/ARPUDisplay';
import EventTimelineTable from '../cohort-pipeline/components/EventTimelineTable';
import RevenueCharts from '../cohort-pipeline/components/RevenueCharts';
import UserTimelineViewer from '../cohort-pipeline/components/UserTimelineViewer';
import DebugDataViewer from '../cohort-pipeline/components/DebugDataViewer';
import { usePipelineData } from '../cohort-pipeline/hooks/usePipelineData';
import { useDebugMode } from '../cohort-pipeline/hooks/useDebugMode';

const CohortPipelineDebugPage = () => {
    // Local storage key for saving state
    const STORAGE_KEY = 'cohort_pipeline_analysis_params';
    
    // Default analysis parameters
    const defaultAnalysisParams = {
        date_from: '2025-01-01',
        date_to: '2025-01-31',
        optional_filters: [], // New unified filter system
        // Keep legacy fields for backward compatibility
        primary_user_filter: {
            property_name: '',  // Empty means no primary filter - all users in time frame are acceptable
            property_values: []  // Empty array means no filtering by user properties
        },
        secondary_filters: [],
        config: {
            product_filter: {
                include_patterns: ['gluten'],  // Default to gluten products
                exclude_patterns: [],  // No exclusions by default
                specific_product_ids: ['prod_R7oYuL3bSUecnr']  // Specific product ID
            },
            lifecycle: {
                trial_window_days: 7,
                cancellation_window_days: 30,
                smoothing_enabled: true
            },
            timeline: {
                include_estimates: true,
                include_confidence_intervals: false
            },
            detailed_events: {
                user_id: ''  // For looking at specific user events
            }
        }
    };

    // Function to load saved parameters from localStorage
    const loadSavedParams = () => {
        try {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (saved) {
                const parsedParams = JSON.parse(saved);
                // Merge with defaults to ensure all required fields exist
                return {
                    ...defaultAnalysisParams,
                    ...parsedParams,
                    optional_filters: parsedParams.optional_filters || [], // Ensure optional_filters exists
                    secondary_filters: parsedParams.secondary_filters || [],
                    config: {
                        ...defaultAnalysisParams.config,
                        ...parsedParams.config,
                        product_filter: {
                            ...defaultAnalysisParams.config.product_filter,
                            ...parsedParams.config?.product_filter
                        },
                        lifecycle: {
                            ...defaultAnalysisParams.config.lifecycle,
                            ...parsedParams.config?.lifecycle
                        },
                        timeline: {
                            ...defaultAnalysisParams.config.timeline,
                            ...parsedParams.config?.timeline
                        },
                        detailed_events: {
                            ...defaultAnalysisParams.config.detailed_events,
                            ...parsedParams.config?.detailed_events
                        }
                    }
                };
            }
        } catch (error) {
            console.warn('Failed to load saved analysis parameters:', error);
        }
        return defaultAnalysisParams;
    };

    // Function to save parameters to localStorage
    const saveParams = (params) => {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(params));
        } catch (error) {
            console.warn('Failed to save analysis parameters:', error);
        }
    };

    // State for analysis parameters - initialize with saved values
    const [analysisParams, setAnalysisParams] = useState(loadSavedParams);
    const [saveIndicator, setSaveIndicator] = useState(false);
    const [showSavedData, setShowSavedData] = useState(false);

    // Save parameters whenever they change
    useEffect(() => {
        saveParams(analysisParams);
        // Show save indicator briefly
        setSaveIndicator(true);
        const timer = setTimeout(() => setSaveIndicator(false), 1000);
        return () => clearTimeout(timer);
    }, [analysisParams]);

    // Custom hooks for data management
    const { 
        pipelineData, 
        loading, 
        error, 
        runAnalysis,
        runStageAnalysis 
    } = usePipelineData();
    
    const { 
        debugMode, 
        debugStage, 
        setDebugMode, 
        setDebugStage 
    } = useDebugMode();

    const handleParameterChange = (newParams) => {
        setAnalysisParams(newParams);
    };

    const handleRunAnalysis = () => {
        if (debugMode && debugStage) {
            runStageAnalysis(analysisParams, debugStage);
        } else {
            runAnalysis(analysisParams);
        }
    };

    // Function to reset to defaults
    const handleResetToDefaults = () => {
        setAnalysisParams(defaultAnalysisParams);
        localStorage.removeItem(STORAGE_KEY);
    };

    // Function to clear all saved data
    const handleClearAllSavedData = () => {
        if (window.confirm('Are you sure you want to clear all saved settings and debug state? This cannot be undone.')) {
            localStorage.removeItem(STORAGE_KEY);
            localStorage.removeItem('cohort_pipeline_debug_mode');
            setAnalysisParams(defaultAnalysisParams);
            // Reset debug mode as well
            setDebugMode(false);
            setDebugStage(null);
        }
    };

    // Function to export current settings
    const handleExportSettings = () => {
        const exportData = {
            analysisParams,
            debugState: {
                debugMode,
                debugStage
            },
            exportedAt: new Date().toISOString(),
            version: '1.0'
        };
        
        const dataStr = JSON.stringify(exportData, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `cohort_analysis_settings_${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    };

    // Function to import settings
    const handleImportSettings = (event) => {
        const file = event.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const importedData = JSON.parse(e.target.result);
                    
                    // Handle both old format (just params) and new format (with debug state)
                    if (importedData.analysisParams) {
                        // New format
                        const mergedParams = {
                            ...defaultAnalysisParams,
                            ...importedData.analysisParams,
                            secondary_filters: importedData.analysisParams.secondary_filters || [],
                            config: {
                                ...defaultAnalysisParams.config,
                                ...importedData.analysisParams.config
                            }
                        };
                        setAnalysisParams(mergedParams);
                        
                        // Import debug state if available
                        if (importedData.debugState) {
                            setDebugMode(importedData.debugState.debugMode || false);
                            setDebugStage(importedData.debugState.debugStage || null);
                        }
                    } else {
                        // Old format - just analysis params
                        const mergedParams = {
                            ...defaultAnalysisParams,
                            ...importedData,
                            secondary_filters: importedData.secondary_filters || [],
                            config: {
                                ...defaultAnalysisParams.config,
                                ...importedData.config
                            }
                        };
                        setAnalysisParams(mergedParams);
                    }
                    
                    alert('Settings imported successfully!');
                } catch (error) {
                    alert('Failed to import settings: Invalid JSON file');
                }
            };
            reader.readAsText(file);
        }
        // Reset the input
        event.target.value = '';
    };

    return (
        <div className="cohort-pipeline-page min-h-screen bg-gray-50 dark:bg-gray-900">
            <div className="container mx-auto px-4 py-6">
                {/* Page Header */}
                <div className="mb-8">
                    <div className="flex items-center justify-between">
                        <div>
                            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
                                Cohort Pipeline Debug & Analysis
                            </h1>
                            <p className="text-gray-600 dark:text-gray-400">
                                Advanced cohort analysis with modular pipeline architecture and debug capabilities
                            </p>
                        </div>
                        
                        {/* Settings Management */}
                        <div className="flex items-center space-x-2">
                            {/* Save indicator */}
                            {saveIndicator && (
                                <div className="flex items-center space-x-1 text-green-600 dark:text-green-400 text-sm">
                                    <span>💾</span>
                                    <span>Saved</span>
                                </div>
                            )}
                            
                            <button
                                onClick={handleResetToDefaults}
                                className="px-3 py-2 text-sm bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded-lg transition-colors"
                                title="Reset all settings to defaults"
                            >
                                Reset to Defaults
                            </button>
                            
                            <button
                                onClick={handleClearAllSavedData}
                                className="px-3 py-2 text-sm bg-red-200 hover:bg-red-300 dark:bg-red-700 dark:hover:bg-red-600 text-red-700 dark:text-red-300 rounded-lg transition-colors"
                                title="Clear all saved data including debug state"
                            >
                                Clear All Data
                            </button>
                            
                            <button
                                onClick={handleExportSettings}
                                className="px-3 py-2 text-sm bg-blue-200 hover:bg-blue-300 dark:bg-blue-700 dark:hover:bg-blue-600 text-blue-700 dark:text-blue-300 rounded-lg transition-colors"
                                title="Export current settings to JSON file"
                            >
                                Export Settings
                            </button>
                            
                            <label className="px-3 py-2 text-sm bg-green-200 hover:bg-green-300 dark:bg-green-700 dark:hover:bg-green-600 text-green-700 dark:text-green-300 rounded-lg transition-colors cursor-pointer">
                                Import Settings
                                <input
                                    type="file"
                                    accept=".json"
                                    onChange={handleImportSettings}
                                    className="hidden"
                                />
                            </label>
                        </div>
                    </div>
                    
                    {/* Auto-save indicator */}
                    <div className="mt-2 flex items-center justify-between">
                        <div className="text-xs text-gray-500 dark:text-gray-400">
                            💾 Settings are automatically saved and will persist across page reloads
                        </div>
                        
                        <button
                            onClick={() => setShowSavedData(!showSavedData)}
                            className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
                        >
                            {showSavedData ? 'Hide' : 'Show'} saved data
                        </button>
                    </div>
                    
                    {/* Saved Data Viewer */}
                    {showSavedData && (
                        <div className="mt-4 bg-gray-100 dark:bg-gray-800 rounded-lg p-4 border">
                            <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-2">
                                Currently Saved Data
                            </h4>
                            <div className="space-y-2 text-xs">
                                <div>
                                    <span className="font-medium text-gray-700 dark:text-gray-300">Analysis Parameters:</span>
                                    <div className="mt-1 bg-white dark:bg-gray-900 rounded p-2 overflow-auto max-h-32">
                                        <pre className="text-gray-600 dark:text-gray-400">
                                            {JSON.stringify(analysisParams, null, 2)}
                                        </pre>
                                    </div>
                                </div>
                                <div>
                                    <span className="font-medium text-gray-700 dark:text-gray-300">Debug State:</span>
                                    <div className="mt-1 bg-white dark:bg-gray-900 rounded p-2">
                                        <pre className="text-gray-600 dark:text-gray-400">
                                            {JSON.stringify({
                                                debugMode,
                                                debugStage,
                                                lastSaved: new Date().toLocaleString()
                                            }, null, 2)}
                                        </pre>
                                    </div>
                                </div>
                                <div className="text-gray-500 dark:text-gray-400">
                                    Storage keys: <code>cohort_pipeline_analysis_params</code>, <code>cohort_pipeline_debug_mode</code>
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                {/* Debug Controls Section */}
                <div className="mb-8">
                    <PipelineStageViewer
                        debugMode={debugMode}
                        debugStage={debugStage}
                        onDebugModeChange={setDebugMode}
                        onDebugStageChange={setDebugStage}
                        onRunAnalysis={handleRunAnalysis}
                        loading={loading}
                        error={error}
                    />
                </div>

                {/* Configuration Section */}
                <div className="mb-8">
                    <ProductFilterConfig
                        config={analysisParams.config}
                        analysisParams={analysisParams}
                        onConfigChange={handleParameterChange}
                    />
                </div>

                {/* Loading State */}
                {loading && (
                    <div className="flex justify-center items-center py-12">
                        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
                        <span className="ml-3 text-lg text-gray-600 dark:text-gray-400">
                            Running analysis...
                        </span>
                    </div>
                )}

                {/* Error State */}
                {error && (
                    <div className="mb-8 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                        <h3 className="text-lg font-semibold text-red-800 dark:text-red-200 mb-2">
                            Analysis Error
                        </h3>
                        <p className="text-red-600 dark:text-red-300">{error}</p>
                    </div>
                )}

                {/* Results Sections - All on one page */}
                {pipelineData && !loading && (
                    <div className="space-y-8">
                        {/* Cohort Filtering Statistics Section - NEW */}
                        <CohortFilteringStats data={pipelineData} />

                        {/* ARPU Section */}
                        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                ARPU Analysis
                            </h2>
                            <ARPUDisplay data={pipelineData} />
                        </div>

                        {/* Lifecycle Rates Section */}
                        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                Lifecycle Rates
                            </h2>
                            <LifecycleRatesTable data={pipelineData} />
                        </div>

                        {/* Event Timeline Section */}
                        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                Event Timeline
                            </h2>
                            <EventTimelineTable data={pipelineData} />
                        </div>

                        {/* User Timeline Section */}
                        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                Individual User Timeline
                            </h2>
                            <UserTimelineViewer data={pipelineData} />
                        </div>

                        {/* Revenue Charts Section */}
                        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                Revenue Visualizations
                            </h2>
                            <RevenueCharts data={pipelineData} />
                        </div>

                        {/* Debug Data Section */}
                        {debugMode && (
                            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                                <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                    Debug Data
                                </h2>
                                <DebugDataViewer 
                                    data={pipelineData} 
                                    debugStage={debugStage}
                                />
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
};

export default CohortPipelineDebugPage; 