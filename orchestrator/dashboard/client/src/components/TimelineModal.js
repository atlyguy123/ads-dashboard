import React from 'react';
import { X, Clock, TrendingUp, Users, DollarSign, User, Filter } from 'lucide-react';
import EventTimelineTableV3 from '../cohort-pipeline/components/EventTimelineTableV3';

const TimelineModal = ({ isOpen, onClose, data, rowData }) => {
    // NEW: Add state for user selection
    const [selectedUserId, setSelectedUserId] = React.useState('');

    if (!isOpen) return null;

    // Helper function to format currency
    const formatCurrency = (num) => {
        if (typeof num !== 'number') return '$0';
        return '$' + num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    // Get row identifier for display
    const getRowIdentifier = () => {
        if (!rowData) return 'Unknown';
        
        // Check what level this row is (campaign, adset, or ad)
        if (rowData.ad_name || rowData.name) {
            return {
                type: 'Ad',
                name: rowData.ad_name || rowData.name,
                id: rowData.ad_id
            };
        } else if (rowData.adset_name) {
            return {
                type: 'Ad Set',
                name: rowData.adset_name,
                id: rowData.adset_id
            };
        } else if (rowData.campaign_name) {
            return {
                type: 'Campaign',
                name: rowData.campaign_name,
                id: rowData.campaign_id
            };
        }
        
        return {
            type: 'Item',
            name: 'Unknown',
            id: 'Unknown'
        };
    };

    // NEW: Extract available users for selection
    const getAvailableUsers = () => {
        if (!data) {
            return [];
        }

        // Handle V3 Refactored Pipeline format
        let timelineResults = null;
        if (data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.stage_results.stage3.timeline_results;
        } else if (data?.data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.data.stage_results.stage3.timeline_results;
        } else if (data?.timeline_results) {
            timelineResults = data.timeline_results;
        } else if (data?.data?.timeline_results) {
            timelineResults = data.data.timeline_results;
        }

        if (!timelineResults) {
            return [];
        }

        const users = new Set();

        // Extract users from user_daily_metrics (most accurate)
        if (timelineResults.user_daily_metrics) {
            Object.keys(timelineResults.user_daily_metrics).forEach(userId => {
                users.add(userId);
            });
        }

        // Extract users from user_timelines
        if (timelineResults.user_timelines) {
            Object.keys(timelineResults.user_timelines).forEach(userId => {
                users.add(userId);
            });
        }

        // Extract users from user_timelines_events
        if (timelineResults.user_timelines_events) {
            Object.keys(timelineResults.user_timelines_events).forEach(userId => {
                users.add(userId);
            });
        }

        return Array.from(users).sort();
    };

    // Get summary stats from timeline data
    const getSummaryStats = () => {
        if (!data) {
            return {
                totalUsers: 0,
                totalActualRevenue: 0,
                totalEstimatedRevenue: 0
            };
        }

        // Handle V3 Refactored Pipeline format
        let timelineResults = null;
        if (data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.stage_results.stage3.timeline_results;
        } else if (data?.data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.data.stage_results.stage3.timeline_results;
        } else if (data?.timeline_results) {
            timelineResults = data.timeline_results;
        } else if (data?.data?.timeline_results) {
            timelineResults = data.data.timeline_results;
        }

        if (!timelineResults) {
            return {
                totalUsers: 0,
                totalActualRevenue: 0,
                totalEstimatedRevenue: 0
            };
        }

        const aggregateStats = timelineResults.aggregate_stats || {};
        
        // Get estimated revenue from the last day of the timeline
        const timelineData = timelineResults.timeline_data || {};
        const dates = Object.keys(timelineData).sort();
        const lastDate = dates[dates.length - 1];
        const lastDayData = timelineData[lastDate] || {};
        const lastDayEstimatedRevenue = lastDayData.estimated_revenue || 0;
        
        return {
            totalUsers: Object.keys(timelineResults.user_timelines || {}).length,
            totalActualRevenue: aggregateStats.total_actual_revenue || 0,
            totalEstimatedRevenue: lastDayEstimatedRevenue
        };
    };

    // Prepare data for EventTimelineTableV3 component
    const prepareTimelineData = () => {
        if (!data) return null;

        // Handle V3 Refactored Pipeline format - extract timeline results
        let timelineResults = null;
        if (data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.stage_results.stage3.timeline_results;
        } else if (data?.data?.stage_results?.stage3?.timeline_results) {
            timelineResults = data.data.stage_results.stage3.timeline_results;
        } else if (data?.timeline_results) {
            timelineResults = data.timeline_results;
        } else if (data?.data?.timeline_results) {
            timelineResults = data.data.timeline_results;
        }

        if (!timelineResults) return null;

        const dates = Object.keys(timelineResults.timeline_data || {}).sort();
        const dailyMetrics = timelineResults.timeline_data || {};
        
        // Calculate cumulative metrics from daily data on the frontend
        const cumulativeMetrics = {};
        let cumulativeTotals = {
            trial_started: 0,
            trial_pending: 0,
            trial_ended: 0,
            trial_cancelled: 0,
            trial_converted: 0,
            initial_purchase: 0,
            subscription_cancelled: 0,
            refund_count: 0,
            revenue: 0,
            refund: 0,
            revenue_net: 0
        };

        // Calculate cumulative values for each date
        dates.forEach(date => {
            const dayData = dailyMetrics[date] || {};
            
            // Add daily values to cumulative totals
            cumulativeTotals.trial_started += dayData.trial_started || 0;
            cumulativeTotals.trial_pending += dayData.trial_pending || 0;
            cumulativeTotals.trial_ended += dayData.trial_ended || 0;
            cumulativeTotals.trial_cancelled += dayData.trial_cancelled || 0;
            cumulativeTotals.trial_converted += dayData.trial_converted || 0;
            cumulativeTotals.initial_purchase += dayData.initial_purchase || 0;
            cumulativeTotals.subscription_cancelled += dayData.subscription_cancelled || 0;
            cumulativeTotals.refund_count += dayData.refund_count || 0;
            cumulativeTotals.revenue += dayData.revenue || 0;
            cumulativeTotals.refund += dayData.refund || 0;
            cumulativeTotals.revenue_net += dayData.revenue_net || 0;

            // Store cumulative values for this date with the field names EventTimelineTableV3 expects
            cumulativeMetrics[date] = {
                cumulative_trial_started: cumulativeTotals.trial_started,
                cumulative_trial_pending: cumulativeTotals.trial_pending,
                cumulative_trial_ended: cumulativeTotals.trial_ended,
                cumulative_trial_cancelled: cumulativeTotals.trial_cancelled,
                cumulative_trial_converted: cumulativeTotals.trial_converted,
                cumulative_initial_purchase: cumulativeTotals.initial_purchase,
                cumulative_subscription_cancelled: cumulativeTotals.subscription_cancelled,
                cumulative_refund_count: cumulativeTotals.refund_count,
                cumulative_revenue: cumulativeTotals.revenue,
                cumulative_refund: cumulativeTotals.refund,
                cumulative_revenue_net: cumulativeTotals.revenue_net,
                
                // For state-based metrics, cumulative should show current daily value (not accumulated)
                // These represent current state, not cumulative totals
                cumulative_users: dayData.daily_users || 0, // Current active users
                estimated_revenue: dayData.estimated_revenue || 0, // Current estimated revenue
                subscription_active: dayData.subscription_active || 0 // Current active subscriptions
            };
        });

        // Transform timeline results into the format expected by EventTimelineTableV3
        const transformedData = {
            data: {
                dates: dates,
                daily_metrics: dailyMetrics,
                cumulative_metrics: cumulativeMetrics,
                user_timelines: timelineResults.user_timelines || {},
                user_daily_metrics: timelineResults.user_daily_metrics || {}
            }
        };

        console.log('🔍 [TimelineModal] Timeline data received:', timelineResults.timeline_data);
        console.log('🔍 [TimelineModal] Sample daily data:', dates[0], dailyMetrics[dates[0]]);
        console.log('🔍 [TimelineModal] Calculated cumulative data:', dates[0], cumulativeMetrics[dates[0]]);
        console.log('🔍 [TimelineModal] Prepared data for EventTimelineTableV3:', transformedData);
        return transformedData;
    };

    const rowIdentifier = getRowIdentifier();
    const summaryStats = getSummaryStats();
    const timelineTableData = prepareTimelineData();
    const availableUsers = getAvailableUsers();

    // NEW: Helper function to get display name for user
    const getUserDisplayName = (userId) => {
        if (!userId) return 'All Users (Aggregate)';
        
        // Extract the readable part from user ID
        if (userId.includes('#')) {
            // Format: user#product, show just the user part with truncation
            const userPart = userId.split('#')[0];
            return userPart.length > 20 ? `${userPart.substring(0, 20)}...` : userPart;
        }
        
        // Regular user ID, truncate if too long
        return userId.length > 25 ? `${userId.substring(0, 25)}...` : userId;
    };

    return (
        <div className="fixed inset-0 z-50 overflow-y-auto">
            <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
                {/* Background overlay */}
                <div 
                    className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" 
                    onClick={onClose}
                ></div>

                {/* Modal positioning */}
                <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

                {/* Modal content - Made wider and taller to accommodate the table */}
                <div className="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-2 sm:align-middle sm:max-w-[98vw] sm:w-full sm:h-[95vh]">
                    {/* Header */}
                    <div className="bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 px-6 py-4 border-b border-gray-200 dark:border-gray-700">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <TrendingUp size={24} className="text-blue-600 dark:text-blue-400" />
                                <div>
                                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                                        Revenue Timeline Analysis
                                    </h3>
                                    <p className="text-sm text-gray-600 dark:text-gray-400">
                                        {rowIdentifier.type}: {rowIdentifier.name}
                                    </p>
                                </div>
                            </div>
                            <button
                                onClick={onClose}
                                className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                            >
                                <X size={24} />
                            </button>
                        </div>
                    </div>

                    {/* NEW: User Selection Controls */}
                    <div className="px-6 py-4 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
                        <div className="flex items-center gap-4 flex-wrap">
                            <div className="flex items-center gap-2">
                                <Filter size={16} className="text-gray-600 dark:text-gray-400" />
                                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Filters:</span>
                            </div>
                            
                            {/* User Selector */}
                            <div className="flex items-center gap-2">
                                <User size={16} className="text-blue-600 dark:text-blue-400" />
                                <label className="text-sm text-gray-600 dark:text-gray-400">User:</label>
                                <select
                                    value={selectedUserId}
                                    onChange={(e) => setSelectedUserId(e.target.value)}
                                    className="px-3 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm min-w-[200px]"
                                >
                                    <option value="">All Users (Aggregate)</option>
                                    {availableUsers.map(userId => (
                                        <option key={userId} value={userId}>
                                            {getUserDisplayName(userId)}
                                        </option>
                                    ))}
                                </select>
                            </div>

                            {/* Clear Filters Button */}
                            {selectedUserId && (
                                <button
                                    onClick={() => setSelectedUserId('')}
                                    className="px-3 py-1 text-sm bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-300 dark:hover:bg-gray-500 transition-colors"
                                >
                                    Clear Filters
                                </button>
                            )}
                        </div>

                        {/* Selection Status */}
                        {selectedUserId && (
                            <div className="mt-2 text-sm text-blue-600 dark:text-blue-400">
                                <strong>Showing:</strong>
                                <span> User: {getUserDisplayName(selectedUserId)}</span>
                            </div>
                        )}
                    </div>

                    {/* Summary Cards - Simplified */}
                    <div className="px-6 py-3 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            <div className="p-3 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                                <div className="flex items-center gap-2 mb-1">
                                    <Users size={16} className="text-blue-600 dark:text-blue-400" />
                                    <h4 className="text-sm font-semibold text-gray-900 dark:text-white">Users</h4>
                                </div>
                                <p className="text-xl font-bold text-blue-600 dark:text-blue-400">
                                    {selectedUserId ? '1' : summaryStats.totalUsers}
                                </p>
                            </div>

                            <div className="p-3 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                                <div className="flex items-center gap-2 mb-1">
                                    <DollarSign size={16} className="text-green-600 dark:text-green-400" />
                                    <h4 className="text-sm font-semibold text-gray-900 dark:text-white">Actual Revenue</h4>
                                </div>
                                <p className="text-xl font-bold text-green-600 dark:text-green-400">
                                    {formatCurrency(summaryStats.totalActualRevenue)}
                                </p>
                            </div>

                            <div className="p-3 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                                <div className="flex items-center gap-2 mb-1">
                                    <TrendingUp size={16} className="text-purple-600 dark:text-purple-400" />
                                    <h4 className="text-sm font-semibold text-gray-900 dark:text-white">Estimated Revenue</h4>
                                </div>
                                <p className="text-xl font-bold text-purple-600 dark:text-purple-400">
                                    {formatCurrency(summaryStats.totalEstimatedRevenue)}
                                </p>
                            </div>
                        </div>
                    </div>

                    {/* EventTimelineTableV3 - Main Content */}
                    <div className="flex-1 overflow-y-auto" style={{ height: 'calc(95vh - 280px)' }}>
                        {timelineTableData ? (
                            <EventTimelineTableV3 
                                data={timelineTableData}
                                selectedUserId={selectedUserId}
                            />
                        ) : (
                            <div className="text-center py-8">
                                <Clock size={48} className="mx-auto text-gray-400 mb-4" />
                                <p className="text-gray-600 dark:text-gray-400 mb-2">
                                    No timeline data available
                                </p>
                                <p className="text-sm text-gray-500 dark:text-gray-500">
                                    Run the pipeline analysis to generate timeline data
                                </p>
                            </div>
                        )}
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-3 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
                        <div className="flex justify-end">
                            <button
                                onClick={onClose}
                                className="px-4 py-2 bg-gray-300 dark:bg-gray-600 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-400 dark:hover:bg-gray-500 transition-colors"
                            >
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default TimelineModal; 