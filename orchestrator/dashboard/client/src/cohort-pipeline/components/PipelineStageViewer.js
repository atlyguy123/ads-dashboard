import React from 'react';

const PipelineStageViewer = ({
    debugMode,
    debugStage,
    onDebugModeChange,
    onDebugStageChange,
    onRunAnalysis,
    loading,
    error
}) => {
    const stages = [
        {
            id: 'stage1',
            name: 'Cohort Identification',
            description: 'Take ad set/campaign ID and return corresponding user cohort with properties',
            order: 1,
        },
        {
            id: 'stage2',
            name: 'User-to-Segment Matching',
            description: 'Connect users to conversion probabilities using pre-calculated segment data',
            order: 2,
        },
        {
            id: 'stage3',
            name: 'Revenue Timeline Generation',
            description: 'Generate revenue timelines using conversion rates and refund rates',
            order: 3,
        },
    ];

    const currentStageInfo = debugStage ? stages.find(s => s.id === debugStage) : null;

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <div className="flex items-center justify-between mb-6">
                <div>
                    <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                        V3 Refactored Pipeline Controls
                    </h2>
                    <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                        3-stage conversion probability-based cohort analysis
                    </p>
                </div>
                
                {/* Debug Mode Toggle */}
                <div className="flex items-center space-x-4">
                    <label className="flex items-center space-x-2">
                        <input
                            type="checkbox"
                            checked={debugMode}
                            onChange={(e) => onDebugModeChange(e.target.checked)}
                            className="w-4 h-4 text-purple-600 bg-gray-100 border-gray-300 rounded focus:ring-purple-500 dark:focus:ring-purple-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                        />
                        <span className="text-sm font-medium text-gray-900 dark:text-gray-300">
                            Debug Mode
                        </span>
                    </label>
                    
                    <button
                        onClick={onRunAnalysis}
                        disabled={loading}
                        className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                            loading
                                ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                                : 'bg-purple-600 hover:bg-purple-700 text-white'
                        }`}
                    >
                        {loading ? 'Running V3 Analysis...' : 'Run V3 Analysis'}
                    </button>
                </div>
            </div>

            {/* Debug Mode Controls */}
            {debugMode && (
                <div className="space-y-4">
                    {/* Stage Selection */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                            Select V3 Pipeline Stage
                        </label>
                        <select
                            value={debugStage || ''}
                            onChange={(e) => onDebugStageChange(e.target.value || null)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-purple-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                        >
                            <option value="">Run complete V3 pipeline...</option>
                            {stages.map(stage => (
                                <option key={stage.id} value={stage.id}>
                                    {stage.order}. {stage.name}
                                </option>
                            ))}
                        </select>
                    </div>

                    {/* Current Stage Info */}
                    {currentStageInfo && (
                        <div className="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
                            <h3 className="font-semibold text-purple-900 dark:text-purple-200 mb-2">
                                Stage {currentStageInfo.order}: {currentStageInfo.name}
                            </h3>
                            <p className="text-purple-700 dark:text-purple-300 text-sm">
                                {currentStageInfo.description}
                            </p>
                        </div>
                    )}

                    {/* Pipeline Progress Visualization */}
                    <div className="mt-6">
                        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                            V3 Refactored Pipeline Progress
                        </h3>
                        <div className="space-y-2">
                            {stages.map((stage, index) => {
                                const isActive = debugStage === stage.id;
                                const isCompleted = debugStage && stages.find(s => s.id === debugStage)?.order > stage.order;
                                
                                return (
                                    <div
                                        key={stage.id}
                                        className={`flex items-center p-3 rounded-lg border transition-colors ${
                                            isActive
                                                ? 'bg-purple-100 border-purple-300 dark:bg-purple-900/30 dark:border-purple-600'
                                                : isCompleted
                                                ? 'bg-green-100 border-green-300 dark:bg-green-900/30 dark:border-green-600'
                                                : 'bg-gray-50 border-gray-200 dark:bg-gray-700 dark:border-gray-600'
                                        }`}
                                    >
                                        {/* Stage Number */}
                                        <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold mr-3 ${
                                            isActive
                                                ? 'bg-purple-600 text-white'
                                                : isCompleted
                                                ? 'bg-green-600 text-white'
                                                : 'bg-gray-300 text-gray-600 dark:bg-gray-600 dark:text-gray-300'
                                        }`}>
                                            {isCompleted ? '✓' : stage.order}
                                        </div>
                                        
                                        {/* Stage Info */}
                                        <div className="flex-1">
                                            <h4 className={`font-medium ${
                                                isActive
                                                    ? 'text-purple-900 dark:text-purple-200'
                                                    : isCompleted
                                                    ? 'text-green-900 dark:text-green-200'
                                                    : 'text-gray-700 dark:text-gray-300'
                                            }`}>
                                                {stage.name}
                                            </h4>
                                            <p className={`text-sm ${
                                                isActive
                                                    ? 'text-purple-700 dark:text-purple-300'
                                                    : isCompleted
                                                    ? 'text-green-700 dark:text-green-300'
                                                    : 'text-gray-500 dark:text-gray-400'
                                            }`}>
                                                {stage.description}
                                            </p>
                                        </div>
                                        
                                        {/* Status Indicator */}
                                        <div className="ml-3">
                                            {isActive && (
                                                <span className="px-2 py-1 text-xs font-medium bg-purple-600 text-white rounded-full">
                                                    Current
                                                </span>
                                            )}
                                            {isCompleted && (
                                                <span className="px-2 py-1 text-xs font-medium bg-green-600 text-white rounded-full">
                                                    Complete
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                </div>
            )}

            {/* Non-Debug Mode Info */}
            {!debugMode && (
                <div className="bg-gradient-to-r from-purple-50 to-blue-50 dark:from-purple-900/20 dark:to-blue-900/20 rounded-lg p-4 border border-purple-200 dark:border-purple-700">
                    <div className="flex items-center gap-2 mb-2">
                        <span className="text-purple-600 dark:text-purple-400">🚀</span>
                        <h4 className="font-medium text-purple-900 dark:text-purple-200">V3 Refactored Pipeline</h4>
                    </div>
                    <p className="text-purple-700 dark:text-purple-300 text-sm">
                        Enable debug mode to run individual pipeline stages and view intermediate results.
                        In normal mode, the complete V3 refactored analysis pipeline will be executed using 
                        conversion probability-based revenue estimation.
                    </p>
                </div>
            )}

            {/* Error Display */}
            {error && (
                <div className="mt-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                    <h4 className="font-medium text-red-900 dark:text-red-200 mb-2">
                        Pipeline Error
                    </h4>
                    <p className="text-red-700 dark:text-red-300 text-sm">
                        {error}
                    </p>
                </div>
            )}
        </div>
    );
};

export default PipelineStageViewer; 