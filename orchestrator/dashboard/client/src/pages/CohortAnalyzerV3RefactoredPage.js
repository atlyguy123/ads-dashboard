import React, { useState, useEffect } from 'react';
import ProductFilterConfig from '../cohort-pipeline/components/ProductFilterConfig';
import PipelineStageIndicator from '../cohort-pipeline/components/PipelineStageIndicator';
import DebugDataViewer from '../cohort-pipeline/components/DebugDataViewer';
import CohortFilteringStats from '../cohort-pipeline/components/CohortFilteringStats';
import Stage2UserMatchingResults from '../cohort-pipeline/components/Stage2UserMatchingResults';
import Stage3RevenueResultsV3 from '../cohort-pipeline/components/Stage3RevenueResultsV3';
import { useDebugMode } from '../cohort-pipeline/hooks/useDebugMode';
import { api } from '../services/api';
import { Zap, TrendingUp, Target, Users, BarChart3, Settings, RefreshCcw, Download, Upload } from 'lucide-react';

const CohortAnalyzerV3RefactoredPage = () => {
    // Local storage key for saving state - different from old page
    const STORAGE_KEY = 'cohort_v3_refactored_analysis_params';
    
    // Default analysis parameters - matching the working old page format
    const defaultAnalysisParams = {
        date_from: '2025-01-01',
        date_to: '2025-01-31',
        optional_filters: [],
        primary_user_filter: {
            property_name: '',
            property_values: []
        },
        secondary_filters: [],
        config: {
            product_filter: {
                include_patterns: ['gluten'],
                exclude_patterns: [],
                specific_product_ids: ['prod_R7oYuL3bSUecnr']
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
                user_id: ''
            }
        }
    };

    // Function to load saved parameters from localStorage
    const loadSavedParams = () => {
        try {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (saved) {
                const parsedParams = JSON.parse(saved);
                return {
                    ...defaultAnalysisParams,
                    ...parsedParams,
                    optional_filters: parsedParams.optional_filters || [],
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

    // Save parameters whenever they change
    useEffect(() => {
        saveParams(analysisParams);
        setSaveIndicator(true);
        const timer = setTimeout(() => setSaveIndicator(false), 1000);
        return () => clearTimeout(timer);
    }, [analysisParams]);

    // Use the SAME hooks as the working old page
    const { 
        debugMode, 
        debugStage, 
        setDebugMode, 
        setDebugStage 
    } = useDebugMode();

    // V3-specific state management (isolated from V2)
    const [pipelineData, setPipelineData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    
    // V3-specific API calls
    const runV3Analysis = async (params) => {
        setLoading(true);
        setError(null);
        try {
            console.log('[V3-Refactored] Running full V3 refactored analysis with params:', params);
            
            // Add warning for potentially large responses
            const dateRange = new Date(params.date_to) - new Date(params.date_from);
            const daySpan = dateRange / (1000 * 60 * 60 * 24);
            
            if (daySpan > 35) {
                console.warn(`[V3-Refactored] Large date range detected (${daySpan} days). Response may be very large.`);
            }
            
            const result = await api.analyzeCohortDataV3Refactored(params);
            
            // Check response size and warn if large
            const responseSize = JSON.stringify(result).length;
            console.log(`[V3-Refactored] Response size: ${(responseSize / (1024 * 1024)).toFixed(2)} MB`);
            
            if (responseSize > 50 * 1024 * 1024) { // 50MB threshold
                console.warn('[V3-Refactored] Large response detected. Consider using debug mode to analyze stages individually.');
                setError('Response is very large (>50MB). Consider using debug mode to analyze stages individually, or reduce the date range.');
                return;
            }
            
            console.log('[V3-Refactored] Full analysis result:', result);
            setPipelineData(result);
        } catch (err) {
            console.error('[V3-Refactored] Full analysis failed:', err);
            
            // Enhanced error handling for large responses
            if (err.message && (err.message.includes('out of memory') || err.message.includes('Maximum call stack'))) {
                setError('Analysis failed due to large dataset size. Please use debug mode to analyze stages individually or reduce the date range.');
            } else if (err.code === 'ERR_INSUFFICIENT_RESOURCES') {
                setError('Browser ran out of memory processing the large response. Please use debug mode or reduce the date range.');
            } else {
                setError(err.message || 'V3 refactored analysis failed');
            }
        } finally {
            setLoading(false);
        }
    };
    
    const runV3StageAnalysis = async (params, stage) => {
        setLoading(true);
        setError(null);
        try {
            console.log(`[V3-Refactored] ENTERING runV3StageAnalysis for stage: ${stage}`);
            console.log(`[V3-Refactored] Params being passed:`, params);
            console.log(`[V3-Refactored] About to call api.runV3RefactoredStageAnalysis...`);
            
            const result = await api.runV3RefactoredStageAnalysis(params, stage);
            
            // Check response size
            const responseSize = JSON.stringify(result).length;
            console.log(`[V3-Refactored] Stage ${stage} response size: ${(responseSize / (1024 * 1024)).toFixed(2)} MB`);
            
            console.log(`[V3-Refactored] RECEIVED result from api.runV3RefactoredStageAnalysis:`, result);
            setPipelineData(result);
        } catch (err) {
            console.error(`[V3-Refactored] Stage ${stage} analysis failed:`, err);
            
            // Enhanced error handling
            if (err.message && (err.message.includes('out of memory') || err.message.includes('Maximum call stack'))) {
                setError(`Stage ${stage} analysis failed due to large dataset size. Please reduce the date range.`);
            } else {
                setError(err.message || `V3 refactored stage ${stage} analysis failed`);
            }
        } finally {
            setLoading(false);
        }
    };

    const handleParameterChange = (newParams) => {
        setAnalysisParams(newParams);
    };

    const handleRunAnalysis = () => {
        if (debugMode && debugStage) {
            console.log('[V3-Refactored DEBUG] Running stage analysis:', debugStage);
            runV3StageAnalysis(analysisParams, debugStage);
        } else {
            console.log('[V3-Refactored DEBUG] Running full analysis');
            runV3Analysis(analysisParams);
        }
    };

    const handleResetToDefaults = () => {
        setAnalysisParams(defaultAnalysisParams);
    };

    const handleClearAllSavedData = () => {
        localStorage.removeItem(STORAGE_KEY);
        setAnalysisParams(defaultAnalysisParams);
    };

    const handleExportSettings = () => {
        const dataStr = JSON.stringify(analysisParams, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
        const exportFileDefaultName = `cohort-v3-refactored-config-${new Date().toISOString().split('T')[0]}.json`;
        
        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();
    };

    const handleImportSettings = (event) => {
        const file = event.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const importedParams = JSON.parse(e.target.result);
                    const mergedParams = {
                        ...defaultAnalysisParams,
                        ...importedParams,
                        config: {
                            ...defaultAnalysisParams.config,
                            ...importedParams.config
                        }
                    };
                    setAnalysisParams(mergedParams);
                } catch (error) {
                    console.error('Failed to import settings:', error);
                    // Note: setError is now handled by usePipelineData hook, not available here
                }
            };
            reader.readAsText(file);
        }
        event.target.value = '';
    };

    // Get stage-specific results for display (V3 Refactored format)
    const getStageResults = (stageId) => {
        // Handle both direct format (pipelineData.stage_results) and nested format (pipelineData.data.stage_results)
        const stageResults = pipelineData?.stage_results || pipelineData?.data?.stage_results;
        if (!stageResults) return null;
        
        return stageResults[stageId] || null;
    };

    // Check stage progress (V3 Refactored format)
    // Handle both direct format (pipelineData.stage_results) and nested format (pipelineData.data.stage_results)
    const stageResults = pipelineData?.stage_results || pipelineData?.data?.stage_results;
    
    const stageProgress = {
        stage1: !!(stageResults?.stage1 && !stageResults.stage1.error),
        stage2: !!(stageResults?.stage2 && !stageResults.stage2.error),
        stage3: !!(stageResults?.stage3 && !stageResults.stage3.error)
    };

    // Debug: Log the actual data structure we're receiving
    useEffect(() => {
        if (pipelineData) {
            console.log('🔍 [DEBUG] pipelineData structure:', pipelineData);
            console.log('🔍 [DEBUG] pipelineData.stage_results (direct):', pipelineData.stage_results);
            console.log('🔍 [DEBUG] pipelineData.data.stage_results (nested):', pipelineData.data?.stage_results);
            console.log('🔍 [DEBUG] computed stageResults:', stageResults);
            console.log('🔍 [DEBUG] stageProgress:', stageProgress);
            console.log('🔍 [DEBUG] Keys in pipelineData:', Object.keys(pipelineData));
            
            // Check specific stage results
            if (stageResults) {
                console.log('🔍 [DEBUG] Stage results keys:', Object.keys(stageResults));
                console.log('🔍 [DEBUG] Stage 1 data:', stageResults.stage1);
                console.log('🔍 [DEBUG] Stage 2 data:', stageResults.stage2);
                console.log('🔍 [DEBUG] Stage 3 data:', stageResults.stage3);
            }
            
            // Check for alternative data locations
            if (pipelineData.data) {
                console.log('🔍 [DEBUG] Data keys:', Object.keys(pipelineData.data));
            }
        } else {
            console.log('🔍 [DEBUG] No pipelineData available');
        }
    }, [pipelineData, stageResults, stageProgress]);

    return (
        <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
            <div className="container mx-auto px-4 py-8">
                {/* Header - matching CohortPipelineDebugPage.js style */}
                <div className="mb-8">
                    <div className="flex items-center justify-between">
                        <div>
                            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
                                Cohort Pipeline V3 - Refactored Architecture
                            </h1>
                            <p className="text-gray-600 dark:text-gray-400">
                                3-stage conversion probability-based cohort analysis with segment matching and revenue timeline generation
                            </p>
                        </div>
                        
                        {/* Settings Management - matching CohortPipelineDebugPage.js */}
                        <div className="flex items-center space-x-2">
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
                    
                    {/* Auto-save indicator - matching CohortPipelineDebugPage.js */}
                    <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                        💾 Settings are automatically saved and will persist across page reloads
                    </div>
                </div>

                {/* Debug Controls Section - matching CohortPipelineDebugPage.js approach */}
                <div className="mb-8">
                    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
                        <div className="flex items-center justify-between mb-6">
                            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                                V3 Refactored Pipeline Controls
                            </h2>
                            
                            {/* Debug Mode Toggle */}
                            <div className="flex items-center space-x-4">
                                <label className="flex items-center space-x-2">
                                    <input
                                        type="checkbox"
                                        checked={debugMode}
                                        onChange={(e) => setDebugMode(e.target.checked)}
                                        className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500 dark:focus:ring-blue-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                                    />
                                    <span className="text-sm font-medium text-gray-900 dark:text-gray-300">
                                        Debug Mode
                                    </span>
                                </label>
                                
                                {debugMode && (
                                    <select
                                        value={debugStage || ''}
                                        onChange={(e) => setDebugStage(e.target.value || null)}
                                        className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                                    >
                                        <option value="">Select stage...</option>
                                        <option value="stage1">Stage 1: User Properties Input</option>
                                        <option value="stage2">Stage 2: Segment Matching</option>
                                        <option value="stage3">Stage 3: Revenue Timeline</option>
                                    </select>
                                )}
                                
                                <button
                                    onClick={handleRunAnalysis}
                                    disabled={loading}
                                    className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                                        loading
                                            ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                                            : 'bg-blue-600 hover:bg-blue-700 text-white'
                                    }`}
                                >
                                    {loading ? 'Running...' : 'Run Analysis'}
                                </button>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Configuration Section */}
                <div className="mb-8">
                    <ProductFilterConfig
                        config={analysisParams.config}
                        analysisParams={analysisParams}
                        onConfigChange={handleParameterChange}
                    />
                    
                    {/* Large Date Range Warning */}
                    {(() => {
                        const dateRange = new Date(analysisParams.date_to) - new Date(analysisParams.date_from);
                        const daySpan = dateRange / (1000 * 60 * 60 * 24);
                        
                        if (daySpan > 35) {
                            return (
                                <div className="mt-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
                                    <div className="flex items-start gap-3">
                                        <div className="text-yellow-600 dark:text-yellow-400">⚠️</div>
                                        <div>
                                            <h3 className="text-yellow-800 dark:text-yellow-200 font-semibold mb-2">Large Date Range Detected</h3>
                                            <p className="text-yellow-700 dark:text-yellow-300 text-sm mb-2">
                                                You've selected a {Math.round(daySpan)} day range. This may result in a very large response ({'>'}500MB) that could crash your browser.
                                            </p>
                                            <div className="text-yellow-700 dark:text-yellow-300 text-sm">
                                                <strong>Recommendations:</strong>
                                                <ul className="list-disc list-inside mt-1 space-y-1">
                                                    <li>Use <strong>Debug Mode</strong> to analyze stages individually</li>
                                                    <li>Reduce your date range to 30 days or less</li>
                                                    <li>Consider analyzing individual months separately</li>
                                                </ul>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            );
                        }
                        return null;
                    })()}
                </div>

                {/* Loading State */}
                {loading && (
                    <div className="flex justify-center items-center py-12">
                        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
                        <div className="ml-3">
                            <div className="text-lg text-gray-600 dark:text-gray-400">
                                Running V3 refactored analysis...
                            </div>
                            {(() => {
                                const dateRange = new Date(analysisParams.date_to) - new Date(analysisParams.date_from);
                                const daySpan = dateRange / (1000 * 60 * 60 * 24);
                                
                                if (daySpan > 35) {
                                    return (
                                        <div className="text-sm text-yellow-600 dark:text-yellow-400 mt-1">
                                            ⚠️ Large date range detected - this may take several minutes and could fail due to browser memory limits
                                        </div>
                                    );
                                }
                                return null;
                            })()}
                        </div>
                    </div>
                )}

                {/* Results Display */}
                <div className="space-y-6">
                    {/* Error Display */}
                    {error && (
                        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                            <h3 className="text-red-800 dark:text-red-200 font-semibold mb-2">Analysis Error</h3>
                            <p className="text-red-700 dark:text-red-300">{error}</p>
                        </div>
                    )}

                    {/* Debug Information */}
                    {pipelineData && (
                        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                            <h3 className="text-blue-800 dark:text-blue-200 font-semibold mb-2">Debug: V3 Refactored API Data Structure</h3>
                            <div className="text-sm text-blue-700 dark:text-blue-300 space-y-1">
                                <div>Has pipelineData: {pipelineData ? '✅' : '❌'}</div>
                                <div>Has stage_results (direct): {pipelineData?.stage_results ? '✅' : '❌'}</div>
                                <div>Has stage_results (nested): {pipelineData?.data?.stage_results ? '✅' : '❌'}</div>
                                <div>Stage 1 Complete: {stageProgress.stage1 ? '✅' : '❌'}</div>
                                <div>Stage 2 Complete: {stageProgress.stage2 ? '✅' : '❌'}</div>
                                <div>Stage 3 Complete: {stageProgress.stage3 ? '✅' : '❌'}</div>
                                {/* ENHANCED: Show stage errors if they exist */}
                                {stageResults?.stage1?.error && (
                                    <div className="text-red-600 dark:text-red-400">Stage 1 Error: {stageResults.stage1.error}</div>
                                )}
                                {stageResults?.stage2?.error && (
                                    <div className="text-red-600 dark:text-red-400">Stage 2 Error: {stageResults.stage2.error}</div>
                                )}
                                {stageResults?.stage3?.error && (
                                    <div className="text-red-600 dark:text-red-400">Stage 3 Error: {stageResults.stage3.error}</div>
                                )}
                                <div>Main data keys: {pipelineData ? Object.keys(pipelineData).join(', ') : 'None'}</div>
                                {stageResults && (
                                    <div>Stage results keys: {Object.keys(stageResults).join(', ')}</div>
                                )}
                                {stageResults?.stage1 && (
                                    <div>Stage 1 users: {stageResults.stage1.total_users || stageResults.stage1.cohort_size || 'N/A'}</div>
                                )}
                                {stageResults?.stage2 && (
                                    <div>Stage 2 matches: {stageResults.stage2.matching_stats?.successful_matches || 0}/{stageResults.stage2.matching_stats?.total_users || 0}</div>
                                )}
                            </div>
                        </div>
                    )}

                    {/* Progressive Results Display */}
                    {pipelineData && (
                        <>
                            {/* Stage 1: User Properties Input Results */}
                            {stageProgress.stage1 && (
                                <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                                    <div className="flex items-center gap-2 mb-4">
                                        <Zap size={24} className="text-blue-600 dark:text-blue-400" />
                                        <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                                            Stage 1: User Properties Input - Complete
                                        </h2>
                                    </div>
                                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                        <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                                            <h3 className="font-semibold text-blue-900 dark:text-blue-200">Total Users Found</h3>
                                            <p className="text-2xl font-bold text-blue-600 dark:text-blue-300">
                                                {pipelineData.stage_results?.stage1?.total_users || 0}
                                            </p>
                                        </div>
                                        <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
                                            <h3 className="font-semibold text-green-900 dark:text-green-200">Date Range</h3>
                                            <p className="text-sm font-medium text-green-600 dark:text-green-300">
                                                {analysisParams.date_from} to {analysisParams.date_to}
                                            </p>
                                        </div>
                                        <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                                            <h3 className="font-semibold text-purple-900 dark:text-purple-200">Stage Status</h3>
                                            <p className="text-sm font-medium text-purple-600 dark:text-purple-300">
                                                ✅ User Cohort Extracted
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* Stage 2: User-to-Segment Matching Results */}
                            {stageProgress.stage2 && (
                                <Stage2UserMatchingResults data={pipelineData} />
                            )}

                            {/* Stage 3: Revenue Timeline Generation Results */}
                            {stageProgress.stage3 && (
                                <Stage3RevenueResultsV3 data={pipelineData} />
                            )}
                        </>
                    )}

                    {/* Debug Data Viewer */}
                    {debugMode && pipelineData && (
                        <DebugDataViewer 
                            data={pipelineData} 
                            stage={debugStage}
                            title="V3 Refactored Pipeline Debug Data"
                        />
                    )}
                </div>
            </div>
        </div>
    );
};

export default CohortAnalyzerV3RefactoredPage; 