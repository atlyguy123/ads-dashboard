import React, { useState, useMemo } from 'react';
import { User, Globe, TrendingUp, DollarSign, Target, Calendar, Activity, BarChart3, List } from 'lucide-react';

const TimelineTable = ({ data, selectedUserId = null }) => {
    const [userSearchTerm, setUserSearchTerm] = useState('');
    const [viewMode, setViewMode] = useState('summary'); // 'summary' or 'timeline'
    const [selectedUser, setSelectedUser] = useState(null);
    
    // Debug logging
    console.log('📊 TimelineTable Debug: Received data:', data);
    console.log('📊 TimelineTable Debug: Data structure check:', {
        hasData: !!data,
        hasStageResults: !!data?.stage_results,
        hasStage3: !!data?.stage_results?.stage3,
        hasTimelineResults: !!data?.stage_results?.stage3?.timeline_results,
        hasUserTimelines: !!data?.stage_results?.stage3?.timeline_results?.user_timelines,
        dataKeys: data ? Object.keys(data) : []
    });
    
    // Handle multiple API response formats for data access
    // V3 Refactored Pipeline: data.stage_results.stage3.timeline_results
    // Legacy formats: data.timeline_results or data.data.timeline_results
    let timelineResults = {};
    
    if (data?.stage_results?.stage3?.timeline_results) {
        // V3 Refactored Pipeline format
        timelineResults = data.stage_results.stage3.timeline_results;
        console.log('📊 TimelineTable Debug: Using V3 Refactored format');
    } else if (data?.data?.stage_results?.stage3?.timeline_results) {
        // V3 Refactored Pipeline format (nested under data)
        timelineResults = data.data.stage_results.stage3.timeline_results;
        console.log('📊 TimelineTable Debug: Using V3 Refactored format (nested)');
    } else if (data?.timeline_results) {
        // Direct timeline_results format
        timelineResults = data.timeline_results;
        console.log('📊 TimelineTable Debug: Using direct timeline_results format');
    } else if (data?.data?.timeline_results) {
        // Nested timeline_results format
        timelineResults = data.data.timeline_results;
        console.log('📊 TimelineTable Debug: Using nested timeline_results format');
    }
    
    const userTimelines = timelineResults.user_timelines || {};
    const backendUserTimelinesEvents = timelineResults.user_timelines_events || {};
    
    console.log('📊 TimelineTable Debug: Extracted data:', {
        timelineResultsKeys: Object.keys(timelineResults),
        userTimelinesCount: Object.keys(userTimelines).length,
        backendUserTimelinesEventsCount: Object.keys(backendUserTimelinesEvents).length
    });

    // Helper function to format numbers
    const formatNumber = (num) => {
        if (typeof num !== 'number') return '0';
        return num.toLocaleString();
    };

    // Helper function to format currency
    const formatCurrency = (num) => {
        if (typeof num !== 'number') return '$0';
        return '$' + num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    // Process user list for display
    const userList = useMemo(() => {
        const usersSource = backendUserTimelinesEvents && Object.keys(backendUserTimelinesEvents).length > 0 
            ? backendUserTimelinesEvents 
            : userTimelines;
        
        const users = Object.entries(usersSource).map(([userId, timelineData]) => {
            let outcomeType = 'pending';
            let estimatedRevenue = 0;
            let actualRevenue = 0;
            let accuracyScore = 'unknown';
            
            // Handle V3 Refactored Pipeline data structure
            if (timelineData.summary) {
                // New consolidated format from backend fix
                outcomeType = timelineData.summary.outcome_type || 'pending';
                estimatedRevenue = timelineData.summary.total_estimated_revenue || 0;
                actualRevenue = timelineData.summary.total_actual_revenue || 0;
            } else if (timelineData.outcome_type !== undefined) {
                // Original format (user#product composite keys)
                outcomeType = timelineData.outcome_type || 'pending';
                estimatedRevenue = timelineData.total_estimated_revenue || 0;
                actualRevenue = timelineData.total_actual_revenue || 0;
            }
            
            // Extract accuracy score from multiple possible locations in V3 data
            if (timelineData.segment_info) {
                if (timelineData.segment_info.accuracy_score) {
                    accuracyScore = timelineData.segment_info.accuracy_score;
                } else if (timelineData.segment_info.score) {
                    accuracyScore = timelineData.segment_info.score;
                }
            } else if (timelineData.accuracy_score) {
                accuracyScore = timelineData.accuracy_score;
            } else if (timelineData.accuracy_classification) {
                accuracyScore = timelineData.accuracy_classification;
            }
            
            // Debug log for accuracy score extraction
            console.log(`📊 User ${userId} accuracy debug:`, {
                segment_info: timelineData.segment_info,
                accuracy_score: timelineData.accuracy_score,
                accuracy_classification: timelineData.accuracy_classification,
                final_score: accuracyScore
            });
            
            // NEW: Extract refunds and calculate net revenue
            let totalRefunds = 0;
            let netRevenue = 0;
            
            if (timelineData.summary) {
                // New consolidated format
                totalRefunds = timelineData.summary.total_refunds || 0;
                netRevenue = timelineData.summary.net_revenue || (actualRevenue - totalRefunds);
            } else if (timelineData.total_refunds !== undefined) {
                // Original format
                totalRefunds = timelineData.total_refunds || 0;
                netRevenue = timelineData.net_revenue || (actualRevenue - totalRefunds);
            } else {
                // Fallback: calculate from actual revenue
                netRevenue = actualRevenue;
            }
            
            return {
                userId,
                timeline: timelineData,
                displayName: `${userId.substring(0, 8)}...`,
                fullId: userId,
                outcomeType,
                estimatedRevenue,
                actualRevenue,
                accuracyScore,
                // Show actual revenue if known, otherwise estimated
                displayRevenue: actualRevenue > 0 ? actualRevenue : estimatedRevenue,
                revenueType: actualRevenue > 0 ? 'actual' : 'estimated',
                netRevenue
            };
        });
        
        // Filter based on search term
        if (userSearchTerm) {
            return users.filter(user => 
                user.userId.toLowerCase().includes(userSearchTerm.toLowerCase()) ||
                user.outcomeType.toLowerCase().includes(userSearchTerm.toLowerCase()) ||
                user.accuracyScore.toLowerCase().includes(userSearchTerm.toLowerCase())
            );
        }
        
        return users;
    }, [userTimelines, backendUserTimelinesEvents, userSearchTerm]);

    // Get accuracy color
    const getAccuracyColor = (accuracy) => {
        switch (accuracy.toLowerCase()) {
            case 'high':
                return 'text-green-600 bg-green-100 dark:text-green-400 dark:bg-green-900/30';
            case 'medium':
                return 'text-yellow-600 bg-yellow-100 dark:text-yellow-400 dark:bg-yellow-900/30';
            case 'low':
                return 'text-red-600 bg-red-100 dark:text-red-400 dark:bg-red-900/30';
            default:
                return 'text-gray-600 bg-gray-100 dark:text-gray-400 dark:bg-gray-900/30';
        }
    };

    // Get outcome color
    const getOutcomeColor = (outcome) => {
        switch (outcome.toLowerCase()) {
            case 'conversion':
                return 'text-green-600 bg-green-100 dark:text-green-400 dark:bg-green-900/30';
            case 'refund':
                return 'text-red-600 bg-red-100 dark:text-red-400 dark:bg-red-900/30';
            default:
                return 'text-blue-600 bg-blue-100 dark:text-blue-400 dark:bg-blue-900/30';
        }
    };

    // Extract timeline events for detailed view
    const getTimelineEvents = () => {
        const events = [];
        
        // Extract events from user_timelines_events (V3 format)
        Object.entries(backendUserTimelinesEvents).forEach(([userId, userData]) => {
            if (userData.events && Array.isArray(userData.events)) {
                userData.events.forEach(event => {
                    events.push({
                        ...event,
                        userId,
                        userDisplayName: `${userId.substring(0, 8)}...`
                    });
                });
            }
        });
        
        // Also extract from user_timelines if available
        Object.entries(userTimelines).forEach(([userId, userData]) => {
            if (userData.timeline_points && Array.isArray(userData.timeline_points)) {
                userData.timeline_points.forEach(point => {
                    if (point.event_type) { // Only include actual events
                        events.push({
                            date: point.date,
                            event_type: point.event_type,
                            revenue_amount: point.revenue_amount,
                            revenue_type: point.revenue_type,
                            notes: point.notes,
                            userId,
                            userDisplayName: `${userId.substring(0, 8)}...`
                        });
                    }
                });
            }
        });
        
        // Sort events by date
        return events.sort((a, b) => {
            const dateA = new Date(a.date || a.timestamp);
            const dateB = new Date(b.date || b.timestamp);
            return dateA - dateB;
        });
    };

    const timelineEvents = useMemo(() => getTimelineEvents(), [userTimelines, backendUserTimelinesEvents]);

    if (!data || (Object.keys(userTimelines).length === 0 && Object.keys(backendUserTimelinesEvents).length === 0)) {
        console.log('📊 TimelineTable Debug: Showing no data message because:', {
            noData: !data,
            noUserTimelines: Object.keys(userTimelines).length === 0,
            noBackendEvents: Object.keys(backendUserTimelinesEvents).length === 0,
            userTimelinesCount: Object.keys(userTimelines).length,
            backendUserTimelinesEventsCount: Object.keys(backendUserTimelinesEvents).length,
            timelineResultsKeys: Object.keys(timelineResults)
        });
        
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No timeline data available.
                </p>
                <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">
                    Debug: {Object.keys(userTimelines).length} user timelines, {Object.keys(backendUserTimelinesEvents).length} events
                </p>
            </div>
        );
    }

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg">
            {/* View Toggle and Search Controls */}
            <div className="p-4 border-b border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between gap-4 mb-4">
                    <div className="flex items-center gap-2">
                        <button
                            onClick={() => setViewMode('summary')}
                            className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                                viewMode === 'summary'
                                    ? 'bg-blue-600 text-white'
                                    : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                            }`}
                        >
                            <BarChart3 size={16} />
                            Summary View
                        </button>
                        <button
                            onClick={() => setViewMode('timeline')}
                            className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                                viewMode === 'timeline'
                                    ? 'bg-blue-600 text-white'
                                    : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                            }`}
                        >
                            <Activity size={16} />
                            Timeline View
                        </button>
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">
                        {viewMode === 'summary' ? `${userList.length} users` : `${timelineEvents.length} events`}
                    </div>
                </div>
                
                <div className="flex items-center gap-4">
                    <div className="flex-1">
                        <input
                            type="text"
                            placeholder={viewMode === 'summary' ? "Search by User ID, outcome, or accuracy..." : "Search events by user ID or event type..."}
                            value={userSearchTerm}
                            onChange={(e) => setUserSearchTerm(e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                        />
                    </div>
                </div>
            </div>

            {/* Content Area */}
            <div className="overflow-x-auto">
                {viewMode === 'summary' ? (
                    // Summary Table
                    <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                        <thead className="bg-gray-50 dark:bg-gray-800">
                            <tr>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    User ID
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    Revenue
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    Revenue Type
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    Net Revenue
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    Outcome
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                                    Accuracy Score
                                </th>
                            </tr>
                        </thead>
                        <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
                            {userList.map((user) => (
                                <tr 
                                    key={user.userId}
                                    className={`hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer ${
                                        selectedUserId === user.userId ? 'bg-blue-50 dark:bg-blue-900/20' : ''
                                    }`}
                                    onClick={() => setSelectedUser(user)}
                                >
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <div className="flex items-center">
                                            <User size={16} className="text-gray-400 mr-2" />
                                            <div>
                                                <div className="text-sm font-medium text-gray-900 dark:text-white">
                                                    {user.displayName}
                                                </div>
                                                <div className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                                                    {user.fullId}
                                                </div>
                                            </div>
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <div className="text-sm font-medium text-gray-900 dark:text-white">
                                            {formatCurrency(user.displayRevenue)}
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${
                                            user.revenueType === 'actual' 
                                                ? 'text-green-600 bg-green-100 dark:text-green-400 dark:bg-green-900/30'
                                                : 'text-blue-600 bg-blue-100 dark:text-blue-400 dark:bg-blue-900/30'
                                        }`}>
                                            {user.revenueType === 'actual' ? 'Confirmed' : 'Estimated'}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <div className="text-sm font-medium text-gray-900 dark:text-white">
                                            {formatCurrency(user.netRevenue)}
                                        </div>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${getOutcomeColor(user.outcomeType)}`}>
                                            {user.outcomeType}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap">
                                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${getAccuracyColor(user.accuracyScore)}`}>
                                            {String(user.accuracyScore).toUpperCase()}
                                        </span>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                ) : (
                    // Timeline View
                    <div className="p-4">
                        <div className="space-y-4">
                            {timelineEvents
                                .filter(event => 
                                    !userSearchTerm || 
                                    event.userId.toLowerCase().includes(userSearchTerm.toLowerCase()) ||
                                    (event.event_type || event.event_name || '').toLowerCase().includes(userSearchTerm.toLowerCase())
                                )
                                .map((event, index) => (
                                <div key={index} className="flex items-start gap-4 p-4 border border-gray-200 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800">
                                    <div className="flex-shrink-0 w-2 h-2 bg-blue-600 rounded-full mt-2"></div>
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-3">
                                                <Calendar size={16} className="text-gray-400" />
                                                <span className="text-sm font-medium text-gray-900 dark:text-white">
                                                    {new Date(event.date || event.timestamp).toLocaleDateString()}
                                                </span>
                                                <span className="text-xs text-gray-500 dark:text-gray-400">
                                                    {new Date(event.date || event.timestamp).toLocaleTimeString()}
                                                </span>
                                            </div>
                                            <div className="flex items-center gap-2">
                                                <User size={14} className="text-gray-400" />
                                                <span className="text-sm text-gray-600 dark:text-gray-400">
                                                    {event.userDisplayName}
                                                </span>
                                            </div>
                                        </div>
                                        <div className="mt-2">
                                            <div className="flex items-center gap-2">
                                                <Activity size={16} className="text-blue-600" />
                                                <span className="font-medium text-gray-900 dark:text-white">
                                                    {event.event_type || event.event_name}
                                                </span>
                                                {event.revenue_amount > 0 && (
                                                    <span className={`ml-2 px-2 py-1 text-xs font-medium rounded-full ${
                                                        event.revenue_type === 'actual' 
                                                            ? 'text-green-600 bg-green-100 dark:text-green-400 dark:bg-green-900/30'
                                                            : 'text-blue-600 bg-blue-100 dark:text-blue-400 dark:bg-blue-900/30'
                                                    }`}>
                                                        {formatCurrency(event.revenue_amount)}
                                                    </span>
                                                )}
                                            </div>
                                            {event.notes && (
                                                <p className="mt-1 text-sm text-gray-600 dark:text-gray-400">
                                                    {event.notes}
                                                </p>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                        
                        {timelineEvents.length === 0 && (
                            <div className="text-center py-8">
                                <Activity size={48} className="mx-auto text-gray-400 mb-4" />
                                <p className="text-gray-600 dark:text-gray-400 mb-2">
                                    No timeline events found
                                </p>
                                <p className="text-sm text-gray-500 dark:text-gray-500">
                                    Events will appear here when timeline data is available
                                </p>
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* Summary Footer - only show in summary view */}
            {viewMode === 'summary' && (
                <div className="px-6 py-4 bg-gray-50 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700">
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <div className="text-center">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                {userList.length}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">Total Users</div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                                {formatCurrency(userList.reduce((sum, user) => sum + (user.revenueType === 'actual' ? user.displayRevenue : 0), 0))}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">Confirmed Revenue</div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                                {formatCurrency(userList.reduce((sum, user) => sum + (user.revenueType === 'estimated' ? user.displayRevenue : 0), 0))}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">Estimated Revenue</div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                                {formatCurrency(userList.reduce((sum, user) => sum + user.displayRevenue, 0))}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">Total Revenue</div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default TimelineTable; 