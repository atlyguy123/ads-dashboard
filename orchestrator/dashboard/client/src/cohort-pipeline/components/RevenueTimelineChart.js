import React, { useState, useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine, Area, ComposedChart } from 'recharts';
import { Calendar, TrendingUp, DollarSign, Users, Activity, Eye, EyeOff } from 'lucide-react';

const RevenueTimelineChart = ({ data, loading = false, error = null }) => {
    const [selectedUser, setSelectedUser] = useState(null);
    const [viewMode, setViewMode] = useState('individual'); // 'individual', 'aggregate'
    const [showEstimated, setShowEstimated] = useState(true);
    const [showActual, setShowActual] = useState(true);

    // Extract stage 3 data from pipeline results
    const stage3Data = data?.stage_results?.stage3;
    const timelineResults = stage3Data?.timeline_results || {};
    const userTimelines = timelineResults?.user_timelines || {};
    const aggregateStats = timelineResults?.aggregate_stats || {};

    // Process timeline data for visualization
    const { chartData, userList, aggregateData } = useMemo(() => {
        const users = Object.entries(userTimelines);
        
        if (!users.length) {
            return { chartData: [], userList: [], aggregateData: [] };
        }

        // Create user list for selection
        const userList = users.map(([userId, timeline]) => ({
            userId,
            timeline,
            outcomeKnown: timeline.outcome_known,
            outcomeType: timeline.outcome_type,
            totalEstimatedRevenue: timeline.total_estimated_revenue,
            totalActualRevenue: timeline.total_actual_revenue,
            accuracyScore: timeline.segment_info?.accuracy_score || 'unknown'
        }));

        // Prepare individual user chart data
        const selectedTimeline = selectedUser ? userTimelines[selectedUser] : users[0][1];
        const chartData = selectedTimeline?.timeline_points?.map(point => ({
            date: point.date,
            estimated_revenue: point.revenue_type === 'estimated' ? point.revenue_amount : null,
            actual_revenue: point.revenue_type === 'actual' ? point.revenue_amount : null,
            conversion_probability: point.conversion_probability,
            refund_probability: point.refund_probability,
            event_type: point.event_type,
            notes: point.notes
        })) || [];

        // Prepare aggregate data (simplified - would need more complex aggregation in real implementation)
        const aggregateData = [];
        
        return { chartData, userList, aggregateData };
    }, [userTimelines, selectedUser]);

    const formatCurrency = (value) => {
        if (value === null || value === undefined) return null;
        return `$${value.toFixed(2)}`;
    };

    const formatPercentage = (value) => {
        return `${(value * 100).toFixed(1)}%`;
    };

    const getOutcomeColor = (outcomeType) => {
        const colors = {
            'converted': 'text-green-600 dark:text-green-400 bg-green-50 dark:bg-green-900/30',
            'cancelled': 'text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/30',
            'pending': 'text-yellow-600 dark:text-yellow-400 bg-yellow-50 dark:bg-yellow-900/30'
        };
        return colors[outcomeType] || 'text-gray-600 dark:text-gray-400 bg-gray-50 dark:bg-gray-900/30';
    };

    const getAccuracyColor = (accuracy) => {
        const colors = {
            'very_high': 'text-emerald-600',
            'high': 'text-blue-600',
            'medium': 'text-yellow-600',
            'low': 'text-orange-600',
            'very_low': 'text-red-600'
        };
        return colors[accuracy] || 'text-gray-600';
    };

    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            const data = payload[0]?.payload;
            
            return (
                <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-3 shadow-lg">
                    <p className="font-semibold text-gray-900 dark:text-white mb-2">{label}</p>
                    
                    {payload.map((entry, index) => (
                        <div key={index} className="text-sm">
                            <span style={{ color: entry.color }}>
                                {entry.name === 'estimated_revenue' ? 'Estimated' : 'Actual'} Revenue: {formatCurrency(entry.value)}
                            </span>
                        </div>
                    ))}
                    
                    <div className="mt-2 text-xs text-gray-600 dark:text-gray-400 border-t pt-2">
                        <div>Conversion Probability: {formatPercentage(data?.conversion_probability || 0)}</div>
                        <div>Refund Probability: {formatPercentage(data?.refund_probability || 0)}</div>
                        {data?.event_type && (
                            <div className="mt-1 font-medium text-blue-600 dark:text-blue-400">
                                Event: {data.event_type.replace('_', ' ')}
                            </div>
                        )}
                        {data?.notes && (
                            <div className="mt-1 text-gray-500 dark:text-gray-400">
                                {data.notes}
                            </div>
                        )}
                    </div>
                </div>
            );
        }
        return null;
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500"></div>
                <span className="ml-3 text-gray-600 dark:text-gray-400">Loading revenue timeline...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-red-800 dark:text-red-200 mb-2">
                    Revenue Timeline Error
                </h3>
                <p className="text-red-600 dark:text-red-300">{error}</p>
            </div>
        );
    }

    if (!userList.length) {
        return (
            <div className="text-center py-8 text-gray-600 dark:text-gray-400">
                <TrendingUp className="h-12 w-12 mx-auto mb-3 opacity-50" />
                <p>No revenue timeline data available</p>
            </div>
        );
    }

    return (
        <div className="space-y-6">
            {/* Summary Statistics */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {aggregateStats.total_users || 0}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Total Users</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {formatCurrency(aggregateStats.total_estimated_revenue || 0)}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Total Estimated Revenue</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {formatCurrency(aggregateStats.total_actual_revenue || 0)}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Total Actual Revenue</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {aggregateStats.users_with_outcomes || 0}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Known Outcomes</div>
                </div>
            </div>

            {/* Controls */}
            <div className="flex flex-wrap gap-4 items-center justify-between p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                <div className="flex items-center space-x-4">
                    <div className="flex items-center space-x-2">
                        <Users className="h-4 w-4 text-gray-600 dark:text-gray-400" />
                        <select
                            value={selectedUser || ''}
                            onChange={(e) => setSelectedUser(e.target.value || null)}
                            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                        >
                            <option value="">Select User</option>
                            {userList.map((user) => (
                                <option key={user.userId} value={user.userId}>
                                    {user.userId.slice(0, 8)}... - {user.outcomeType} ({user.accuracyScore})
                                </option>
                            ))}
                        </select>
                    </div>
                </div>

                <div className="flex items-center space-x-4">
                    <div className="flex items-center space-x-2">
                        <button
                            onClick={() => setShowEstimated(!showEstimated)}
                            className={`flex items-center space-x-1 px-3 py-2 rounded-md text-sm ${
                                showEstimated 
                                    ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' 
                                    : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                            }`}
                        >
                            {showEstimated ? <Eye className="h-4 w-4" /> : <EyeOff className="h-4 w-4" />}
                            <span>Estimated</span>
                        </button>
                        
                        <button
                            onClick={() => setShowActual(!showActual)}
                            className={`flex items-center space-x-1 px-3 py-2 rounded-md text-sm ${
                                showActual 
                                    ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' 
                                    : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
                            }`}
                        >
                            {showActual ? <Eye className="h-4 w-4" /> : <EyeOff className="h-4 w-4" />}
                            <span>Actual</span>
                        </button>
                    </div>
                </div>
            </div>

            {/* Selected User Details */}
            {selectedUser && userTimelines[selectedUser] && (
                <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-2 flex items-center">
                                <Users className="h-4 w-4 mr-1" />
                                User Info
                            </h4>
                            <div className="space-y-1 text-sm">
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">User ID:</span>
                                    <span className="font-medium text-gray-900 dark:text-white">
                                        {selectedUser.slice(0, 12)}...
                                    </span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Outcome:</span>
                                    <span className={`px-2 py-1 rounded text-xs font-medium ${getOutcomeColor(userTimelines[selectedUser].outcome_type)}`}>
                                        {userTimelines[selectedUser].outcome_type || 'pending'}
                                    </span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Accuracy:</span>
                                    <span className={`font-medium ${getAccuracyColor(userTimelines[selectedUser].segment_info?.accuracy_score)}`}>
                                        {userTimelines[selectedUser].segment_info?.accuracy_score?.replace('_', ' ') || 'unknown'}
                                    </span>
                                </div>
                            </div>
                        </div>

                        <div>
                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-2 flex items-center">
                                <TrendingUp className="h-4 w-4 mr-1" />
                                Revenue Summary
                            </h4>
                            <div className="space-y-1 text-sm">
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Estimated:</span>
                                    <span className="font-medium text-blue-600 dark:text-blue-400">
                                        {formatCurrency(userTimelines[selectedUser].total_estimated_revenue)}
                                    </span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Actual:</span>
                                    <span className="font-medium text-green-600 dark:text-green-400">
                                        {formatCurrency(userTimelines[selectedUser].total_actual_revenue)}
                                    </span>
                                </div>
                            </div>
                        </div>

                        <div>
                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-2 flex items-center">
                                <Activity className="h-4 w-4 mr-1" />
                                Segment Info
                            </h4>
                            <div className="space-y-1 text-sm">
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Segment ID:</span>
                                    <span className="font-medium text-gray-900 dark:text-white">
                                        {userTimelines[selectedUser].segment_info?.segment_id?.slice(0, 8)}...
                                    </span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-gray-600 dark:text-gray-400">Rollup Applied:</span>
                                    <span className="font-medium text-gray-900 dark:text-white">
                                        {userTimelines[selectedUser].segment_info?.rollup_applied ? 'Yes' : 'No'}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Revenue Timeline Chart */}
            <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-6">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <Calendar className="h-5 w-5 mr-2" />
                    Revenue Timeline
                    {selectedUser && (
                        <span className="ml-2 text-sm text-gray-600 dark:text-gray-400">
                            - {selectedUser.slice(0, 8)}...
                        </span>
                    )}
                </h3>
                
                <div className="h-96">
                    <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e5e5e5" />
                            <XAxis 
                                dataKey="date" 
                                stroke="#6b7280"
                                tick={{ fontSize: 12 }}
                                angle={-45}
                                textAnchor="end"
                                height={80}
                            />
                            <YAxis 
                                stroke="#6b7280"
                                tick={{ fontSize: 12 }}
                                tickFormatter={formatCurrency}
                            />
                            <Tooltip content={<CustomTooltip />} />
                            <Legend />
                            
                            {showEstimated && (
                                <Line
                                    type="monotone"
                                    dataKey="estimated_revenue"
                                    stroke="#3b82f6"
                                    strokeWidth={2}
                                    strokeDasharray="5 5"
                                    dot={{ fill: '#3b82f6', strokeWidth: 2, r: 4 }}
                                    connectNulls={false}
                                    name="Estimated Revenue"
                                />
                            )}
                            
                            {showActual && (
                                <Line
                                    type="monotone"
                                    dataKey="actual_revenue"
                                    stroke="#10b981"
                                    strokeWidth={3}
                                    dot={{ fill: '#10b981', strokeWidth: 2, r: 4 }}
                                    connectNulls={false}
                                    name="Actual Revenue"
                                />
                            )}

                            {/* Add reference lines for key events */}
                            {chartData.map((point, index) => {
                                if (point.event_type === 'trial_start') {
                                    return (
                                        <ReferenceLine 
                                            key={`trial-start-${index}`}
                                            x={point.date} 
                                            stroke="#8b5cf6" 
                                            strokeDasharray="2 2"
                                            label={{ value: "Trial Start", position: "topLeft" }}
                                        />
                                    );
                                }
                                if (point.event_type === 'trial_convert') {
                                    return (
                                        <ReferenceLine 
                                            key={`trial-convert-${index}`}
                                            x={point.date} 
                                            stroke="#10b981" 
                                            strokeDasharray="2 2"
                                            label={{ value: "Converted", position: "topLeft" }}
                                        />
                                    );
                                }
                                if (point.event_type === 'trial_cancel') {
                                    return (
                                        <ReferenceLine 
                                            key={`trial-cancel-${index}`}
                                            x={point.date} 
                                            stroke="#ef4444" 
                                            strokeDasharray="2 2"
                                            label={{ value: "Cancelled", position: "topLeft" }}
                                        />
                                    );
                                }
                                return null;
                            })}
                        </ComposedChart>
                    </ResponsiveContainer>
                </div>

                {/* Chart Legend */}
                <div className="mt-4 flex flex-wrap gap-4 text-sm">
                    <div className="flex items-center space-x-2">
                        <div className="w-4 h-0.5 bg-blue-500 border-dashed border-b-2 border-blue-500"></div>
                        <span className="text-gray-600 dark:text-gray-400">Estimated Revenue (based on conversion probability)</span>
                    </div>
                    <div className="flex items-center space-x-2">
                        <div className="w-4 h-0.5 bg-green-500"></div>
                        <span className="text-gray-600 dark:text-gray-400">Actual Revenue (known outcomes)</span>
                    </div>
                    <div className="flex items-center space-x-2">
                        <div className="w-4 h-0.5 bg-purple-500 border-dashed border-b-2 border-purple-500"></div>
                        <span className="text-gray-600 dark:text-gray-400">Key Events</span>
                    </div>
                </div>
            </div>

            {/* Revenue Formula Explanation */}
            <div className="bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg p-4">
                <h4 className="text-sm font-semibold text-blue-800 dark:text-blue-200 mb-2 flex items-center">
                    <DollarSign className="h-4 w-4 mr-1" />
                    Revenue Estimation Formula
                </h4>
                <div className="text-sm text-blue-700 dark:text-blue-300 space-y-1">
                    <div><strong>Pre-outcome:</strong> estimated_revenue = conversion_probability × price × (1 - refund_rate)</div>
                    <div><strong>Post-conversion:</strong> actual_revenue = price × (1 - refund_rate)</div>
                    <div><strong>Post-cancellation:</strong> actual_revenue = 0</div>
                </div>
            </div>
        </div>
    );
};

export default RevenueTimelineChart; 