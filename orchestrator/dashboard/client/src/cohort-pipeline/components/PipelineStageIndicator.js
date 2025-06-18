import React from 'react';
import { CheckCircle, AlertCircle, Clock, Users, Target, TrendingUp, ArrowRight } from 'lucide-react';

const PipelineStageIndicator = ({ data, loading = false, error = null }) => {
    // Extract stage results from pipeline data
    const stageResults = data?.stage_results || {};
    const summary = data?.summary || {};
    const pipelineVersion = data?.pipeline_version;
    
    // Define stages configuration
    const stages = [
        {
            id: 'stage1',
            name: 'User Properties Input',
            description: 'Collect users and their properties from the database',
            icon: Users,
            key_metrics: ['cohort_size', 'events_count']
        },
        {
            id: 'stage2', 
            name: 'Segment Matching',
            description: 'Match users to conversion probability segments',
            icon: Target,
            key_metrics: ['successful_matches', 'rollups_applied']
        },
        {
            id: 'stage3',
            name: 'Revenue Timeline',
            description: 'Generate revenue timelines with conversion estimates',
            icon: TrendingUp,
            key_metrics: ['users_with_timelines', 'total_estimated_revenue']
        }
    ];

    // Determine stage status
    const getStageStatus = (stageId) => {
        const result = stageResults[stageId];
        
        if (!result) return 'pending';
        if (result.error) return 'error';
        if (loading && stageId === 'stage3') return 'running'; // Assume last stage is running if loading
        return 'completed';
    };

    // Get stage metrics display
    const getStageMetrics = (stageId, keyMetrics) => {
        const result = stageResults[stageId];
        if (!result) return [];

        const metrics = [];
        
        if (stageId === 'stage1') {
            metrics.push({
                label: 'Users Found',
                value: result.cohort_size || 0,
                format: 'number'
            });
            metrics.push({
                label: 'Events Collected',
                value: result.events_count || 0,
                format: 'number'
            });
        } else if (stageId === 'stage2') {
            const stats = result.matching_stats || {};
            metrics.push({
                label: 'Successful Matches',
                value: stats.successful_matches || 0,
                format: 'number'
            });
            metrics.push({
                label: 'Success Rate',
                value: stats.success_rate || 0,
                format: 'percentage'
            });
            if (stats.rollups_applied) {
                metrics.push({
                    label: 'Rollups Applied',
                    value: stats.rollups_applied,
                    format: 'number'
                });
            }
        } else if (stageId === 'stage3') {
            metrics.push({
                label: 'Timeline Users',
                value: result.users_with_timelines || 0,
                format: 'number'
            });
            metrics.push({
                label: 'Estimated Revenue',
                value: result.aggregate_stats?.total_estimated_revenue || 0,
                format: 'currency'
            });
            if (result.aggregate_stats?.total_actual_revenue > 0) {
                metrics.push({
                    label: 'Actual Revenue',
                    value: result.aggregate_stats.total_actual_revenue,
                    format: 'currency'
                });
            }
        }

        return metrics;
    };

    // Format metric values
    const formatMetricValue = (value, format) => {
        switch (format) {
            case 'number':
                return value.toLocaleString();
            case 'percentage':
                return `${(value * 100).toFixed(1)}%`;
            case 'currency':
                return `$${value.toFixed(2)}`;
            default:
                return value;
        }
    };

    // Get status styling
    const getStatusStyling = (status) => {
        switch (status) {
            case 'completed':
                return {
                    icon: CheckCircle,
                    iconColor: 'text-green-500',
                    bgColor: 'bg-green-50 dark:bg-green-900/30 border-green-200 dark:border-green-700',
                    textColor: 'text-green-800 dark:text-green-200'
                };
            case 'running':
                return {
                    icon: Clock,
                    iconColor: 'text-blue-500 animate-pulse',
                    bgColor: 'bg-blue-50 dark:bg-blue-900/30 border-blue-200 dark:border-blue-700',
                    textColor: 'text-blue-800 dark:text-blue-200'
                };
            case 'error':
                return {
                    icon: AlertCircle,
                    iconColor: 'text-red-500',
                    bgColor: 'bg-red-50 dark:bg-red-900/30 border-red-200 dark:border-red-700',
                    textColor: 'text-red-800 dark:text-red-200'
                };
            default: // pending
                return {
                    icon: Clock,
                    iconColor: 'text-gray-400',
                    bgColor: 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700',
                    textColor: 'text-gray-600 dark:text-gray-400'
                };
        }
    };

    if (error) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                <div className="flex items-center">
                    <AlertCircle className="h-5 w-5 text-red-500 mr-2" />
                    <h3 className="text-lg font-semibold text-red-800 dark:text-red-200">
                        Pipeline Error
                    </h3>
                </div>
                <p className="text-red-600 dark:text-red-300 mt-2">{error}</p>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            {/* Pipeline Header */}
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                            Cohort Analysis Pipeline 
                            {pipelineVersion && (
                                <span className="ml-2 text-sm text-gray-600 dark:text-gray-400">
                                    v{pipelineVersion}
                                </span>
                            )}
                        </h2>
                        <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                            3-Stage Conversion Probability Based Analysis
                        </p>
                    </div>
                    
                    {summary.pipeline_success !== undefined && (
                        <div className="flex items-center">
                            {summary.pipeline_success ? (
                                <>
                                    <CheckCircle className="h-5 w-5 text-green-500 mr-2" />
                                    <span className="text-green-600 dark:text-green-400 font-medium">Complete</span>
                                </>
                            ) : (
                                <>
                                    <AlertCircle className="h-5 w-5 text-red-500 mr-2" />
                                    <span className="text-red-600 dark:text-red-400 font-medium">Failed</span>
                                </>
                            )}
                        </div>
                    )}
                </div>
            </div>

            {/* Summary Stats */}
            {summary && Object.keys(summary).length > 0 && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                    <div className="text-center">
                        <div className="text-2xl font-bold text-gray-900 dark:text-white">
                            {summary.total_users_analyzed || 0}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">Users Analyzed</div>
                    </div>
                    <div className="text-center">
                        <div className="text-2xl font-bold text-gray-900 dark:text-white">
                            {summary.users_successfully_matched || 0}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">Successfully Matched</div>
                    </div>
                    <div className="text-center">
                        <div className="text-2xl font-bold text-gray-900 dark:text-white">
                            {summary.users_with_timelines || 0}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">Timeline Generated</div>
                    </div>
                    <div className="text-center">
                        <div className="text-2xl font-bold text-gray-900 dark:text-white">
                            ${(summary.total_estimated_revenue || 0).toFixed(2)}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">Est. Revenue</div>
                    </div>
                </div>
            )}

            {/* Stage Progress */}
            <div className="flex flex-col lg:flex-row lg:items-stretch gap-4">
                {stages.map((stage, index) => {
                    const status = getStageStatus(stage.id);
                    const styling = getStatusStyling(status);
                    const metrics = getStageMetrics(stage.id, stage.key_metrics);
                    const stageResult = stageResults[stage.id];
                    const StageIcon = stage.icon;
                    const StatusIcon = styling.icon;

                    return (
                        <React.Fragment key={stage.id}>
                            <div className={`border rounded-lg p-4 flex-1 ${styling.bgColor}`}>
                                {/* Stage Header */}
                                <div className="flex items-center justify-between mb-3">
                                    <div className="flex items-center">
                                        <div className="flex items-center justify-center w-8 h-8 rounded-full bg-white dark:bg-gray-700 mr-3">
                                            <StageIcon className="h-4 w-4 text-gray-600 dark:text-gray-300" />
                                        </div>
                                        <div>
                                            <h3 className={`font-semibold ${styling.textColor}`}>
                                                {stage.name}
                                            </h3>
                                            <p className="text-xs text-gray-600 dark:text-gray-400">
                                                {stage.description}
                                            </p>
                                        </div>
                                    </div>
                                    <StatusIcon className={`h-5 w-5 ${styling.iconColor}`} />
                                </div>

                                {/* Status Message */}
                                <div className="mb-3">
                                    {status === 'completed' && (
                                        <span className="text-sm text-green-600 dark:text-green-400">
                                            ✓ Stage completed successfully
                                        </span>
                                    )}
                                    {status === 'running' && (
                                        <span className="text-sm text-blue-600 dark:text-blue-400">
                                            ⏳ Stage in progress...
                                        </span>
                                    )}
                                    {status === 'error' && (
                                        <span className="text-sm text-red-600 dark:text-red-400">
                                            ❌ {stageResult?.error || 'Stage failed'}
                                        </span>
                                    )}
                                    {status === 'pending' && (
                                        <span className="text-sm text-gray-500 dark:text-gray-400">
                                            ⏸️ Waiting to start
                                        </span>
                                    )}
                                </div>

                                {/* Metrics */}
                                {metrics.length > 0 && (
                                    <div className="space-y-2">
                                        {metrics.map((metric, metricIndex) => (
                                            <div key={metricIndex} className="flex justify-between text-sm">
                                                <span className="text-gray-600 dark:text-gray-400">
                                                    {metric.label}:
                                                </span>
                                                <span className={`font-medium ${styling.textColor}`}>
                                                    {formatMetricValue(metric.value, metric.format)}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                )}

                                {/* Execution Time */}
                                {stageResult && data?.execution_metadata?.execution_time_seconds && (
                                    <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                                        <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                                            <span>Execution time:</span>
                                            <span>{(data.execution_metadata.execution_time_seconds).toFixed(2)}s</span>
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Arrow between stages */}
                            {index < stages.length - 1 && (
                                <div className="flex items-center justify-center lg:px-2">
                                    <ArrowRight className="h-5 w-5 text-gray-400 transform lg:rotate-0 rotate-90" />
                                </div>
                            )}
                        </React.Fragment>
                    );
                })}
            </div>

            {/* Analysis Metadata */}
            {data?.conversion_analysis_metadata && (
                <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg p-4">
                    <h4 className="text-sm font-semibold text-blue-800 dark:text-blue-200 mb-2">
                        Conversion Analysis Data
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                        <div>
                            <span className="text-blue-600 dark:text-blue-400">Analysis ID:</span>
                            <div className="font-medium text-blue-800 dark:text-blue-200">
                                {data.conversion_analysis_metadata.analysis_id?.slice(0, 16)}...
                            </div>
                        </div>
                        <div>
                            <span className="text-blue-600 dark:text-blue-400">Total Segments:</span>
                            <div className="font-medium text-blue-800 dark:text-blue-200">
                                {data.conversion_analysis_metadata.total_segments || 'Unknown'}
                            </div>
                        </div>
                        <div>
                            <span className="text-blue-600 dark:text-blue-400">Created:</span>
                            <div className="font-medium text-blue-800 dark:text-blue-200">
                                {data.conversion_analysis_metadata.created_at ? 
                                    new Date(data.conversion_analysis_metadata.created_at).toLocaleDateString() : 
                                    'Unknown'
                                }
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default PipelineStageIndicator; 