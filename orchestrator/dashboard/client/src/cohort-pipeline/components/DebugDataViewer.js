import React, { useState } from 'react';

const DebugDataViewer = ({ data, debugStage }) => {
    const [selectedSection, setSelectedSection] = useState('summary');
    const [expandedSections, setExpandedSections] = useState(new Set(['summary']));

    if (!data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No debug data available. Run an analysis in debug mode to see results.
                </p>
            </div>
        );
    }

    const toggleSection = (section) => {
        const newExpanded = new Set(expandedSections);
        if (newExpanded.has(section)) {
            newExpanded.delete(section);
        } else {
            newExpanded.add(section);
        }
        setExpandedSections(newExpanded);
    };

    const formatJSON = (obj, maxDepth = 3, currentDepth = 0) => {
        if (currentDepth >= maxDepth) {
            return typeof obj === 'object' ? '[Object]' : String(obj);
        }
        
        try {
            return JSON.stringify(obj, null, 2);
        } catch (e) {
            return String(obj);
        }
    };

    const getSectionData = (sectionKey) => {
        const sections = {
            summary: {
                title: 'Analysis Summary',
                data: {
                    success: data.success,
                    message: data.message,
                    timestamp: data.timestamp,
                    execution_metadata: data.execution_metadata,
                    debug_stage: debugStage
                }
            },
            cohort_summary: {
                title: 'Cohort Summary',
                data: data.data?.cohort_summary
            },
            arpu_data: {
                title: 'ARPU Data',
                data: data.data?.arpu_data
            },
            lifecycle_rates: {
                title: 'Lifecycle Rates',
                data: data.data?.lifecycle_rates
            },
            timeline_data: {
                title: 'Timeline Data',
                data: data.data?.timeline_data
            },
            raw_response: {
                title: 'Raw API Response',
                data: data
            }
        };

        return sections[sectionKey] || { title: 'Unknown', data: null };
    };

    const availableSections = [
        'summary',
        'cohort_summary',
        'arpu_data',
        'lifecycle_rates',
        'timeline_data',
        'raw_response'
    ].filter(section => {
        const sectionData = getSectionData(section);
        return sectionData.data !== null && sectionData.data !== undefined;
    });

    const DataSection = ({ sectionKey, title, data: sectionData, isExpanded }) => {
        if (!sectionData) return null;

        const hasData = sectionData && (
            typeof sectionData === 'object' ? Object.keys(sectionData).length > 0 : true
        );

        return (
            <div className="border border-gray-200 dark:border-gray-700 rounded-lg">
                <button
                    onClick={() => toggleSection(sectionKey)}
                    className="w-full px-4 py-3 text-left bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-t-lg flex items-center justify-between"
                >
                    <div className="flex items-center space-x-3">
                        <span className="font-medium text-gray-900 dark:text-white">{title}</span>
                        {hasData && (
                            <span className="text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 px-2 py-1 rounded">
                                {typeof sectionData === 'object' && sectionData !== null ? 
                                    `${Object.keys(sectionData).length} keys` : 
                                    'Data available'
                                }
                            </span>
                        )}
                    </div>
                    <span className="text-gray-400">
                        {isExpanded ? '▼' : '▶'}
                    </span>
                </button>
                
                {isExpanded && (
                    <div className="p-4 border-t border-gray-200 dark:border-gray-700">
                        {hasData ? (
                            <div className="bg-gray-900 text-green-400 p-4 rounded-lg overflow-auto max-h-96">
                                <pre className="text-sm whitespace-pre-wrap">
                                    {formatJSON(sectionData)}
                                </pre>
                            </div>
                        ) : (
                            <div className="text-center py-4 text-gray-500 dark:text-gray-400">
                                No data available for this section
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="space-y-6">
            {/* Debug Mode Info */}
            <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
                <div className="flex items-center space-x-2">
                    <span className="text-yellow-600 dark:text-yellow-400">🐛</span>
                    <h3 className="text-lg font-semibold text-yellow-800 dark:text-yellow-200">
                        Debug Mode Active
                    </h3>
                </div>
                <div className="mt-2 text-sm text-yellow-700 dark:text-yellow-300">
                    {debugStage ? (
                        <>Showing debug data for stage: <strong>{debugStage}</strong></>
                    ) : (
                        'Showing complete analysis debug data'
                    )}
                </div>
            </div>

            {/* Section Navigation */}
            <div className="flex flex-wrap gap-2">
                {availableSections.map(section => {
                    const sectionInfo = getSectionData(section);
                    const isActive = selectedSection === section;
                    
                    return (
                        <button
                            key={section}
                            onClick={() => setSelectedSection(section)}
                            className={`px-3 py-2 text-sm rounded-lg transition-colors ${
                                isActive
                                    ? 'bg-blue-600 text-white'
                                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
                            }`}
                        >
                            {sectionInfo.title}
                        </button>
                    );
                })}
            </div>

            {/* Data Sections */}
            <div className="space-y-4">
                {availableSections.map(sectionKey => {
                    const sectionInfo = getSectionData(sectionKey);
                    const isExpanded = expandedSections.has(sectionKey);
                    
                    return (
                        <DataSection
                            key={sectionKey}
                            sectionKey={sectionKey}
                            title={sectionInfo.title}
                            data={sectionInfo.data}
                            isExpanded={isExpanded}
                        />
                    );
                })}
            </div>

            {/* Quick Actions */}
            <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
                    Debug Actions
                </h3>
                <div className="flex flex-wrap gap-2">
                    <button
                        onClick={() => setExpandedSections(new Set(availableSections))}
                        className="px-3 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                    >
                        Expand All
                    </button>
                    <button
                        onClick={() => setExpandedSections(new Set())}
                        className="px-3 py-2 text-sm bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
                    >
                        Collapse All
                    </button>
                    <button
                        onClick={() => {
                            const dataStr = JSON.stringify(data, null, 2);
                            navigator.clipboard.writeText(dataStr);
                        }}
                        className="px-3 py-2 text-sm bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
                    >
                        Copy to Clipboard
                    </button>
                    <button
                        onClick={() => {
                            const dataStr = JSON.stringify(data, null, 2);
                            const blob = new Blob([dataStr], { type: 'application/json' });
                            const url = URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.href = url;
                            a.download = `debug-data-${debugStage || 'full'}-${new Date().toISOString()}.json`;
                            a.click();
                            URL.revokeObjectURL(url);
                        }}
                        className="px-3 py-2 text-sm bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors"
                    >
                        Download JSON
                    </button>
                </div>
            </div>

            {/* Performance Info */}
            {data.execution_metadata && (
                <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                    <h3 className="text-lg font-semibold text-blue-900 dark:text-blue-200 mb-3">
                        Performance Metrics
                    </h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div>
                            <div className="text-blue-700 dark:text-blue-300 font-medium">Duration</div>
                            <div className="text-blue-600 dark:text-blue-400">
                                {data.execution_metadata.duration_seconds?.toFixed(2)}s
                            </div>
                        </div>
                        <div>
                            <div className="text-blue-700 dark:text-blue-300 font-medium">Pipeline Version</div>
                            <div className="text-blue-600 dark:text-blue-400">
                                {data.execution_metadata.pipeline_version || 'N/A'}
                            </div>
                        </div>
                        <div>
                            <div className="text-blue-700 dark:text-blue-300 font-medium">Analysis Type</div>
                            <div className="text-blue-600 dark:text-blue-400">
                                {data.execution_metadata.analysis_type || 'full'}
                            </div>
                        </div>
                        <div>
                            <div className="text-blue-700 dark:text-blue-300 font-medium">Start Time</div>
                            <div className="text-blue-600 dark:text-blue-400">
                                {data.execution_metadata.start_time ? 
                                    new Date(data.execution_metadata.start_time).toLocaleTimeString() : 'N/A'
                                }
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default DebugDataViewer; 