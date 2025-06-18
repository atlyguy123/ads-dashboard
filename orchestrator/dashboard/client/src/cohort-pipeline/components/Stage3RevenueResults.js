import React, { useState, useMemo } from 'react';
import { TrendingUp, Users, DollarSign, Clock, Activity, AlertCircle, Database, Calculator, Target, User, Globe } from 'lucide-react';
import EventTimelineTable from './EventTimelineTable';

const Stage3RevenueResults = ({ data }) => {
    const [selectedUser, setSelectedUser] = useState(''); // Empty string means aggregate view
    const [userSearchTerm, setUserSearchTerm] = useState('');
    
    // Handle multiple API response formats for data access
    let stage3Data = null;
    
    if (data?.stage_results?.stage3) {
        // Direct format: pipelineData.stage_results.stage3
        stage3Data = data.stage_results.stage3;
    } else if (data?.data?.stage_results?.stage3) {
        // Nested format: pipelineData.data.stage_results.stage3
        stage3Data = data.data.stage_results.stage3;
    }

    const timelineResults = stage3Data?.timeline_results || {};
    const aggregateStats = timelineResults.aggregate_stats || {};
    const userTimelines = timelineResults.user_timelines || {};
    
    // NEW: Backend now provides EventTimelineTable format data directly
    const backendTimelineData = timelineResults.timeline_data || {};
    const backendUserDailyMetrics = timelineResults.user_daily_metrics || {};
    const backendUserTimelinesEvents = timelineResults.user_timelines_events || {};
    
    // Process user list for dropdown with search filtering
    const userList = useMemo(() => {
        const users = Object.entries(userTimelines).map(([userId, timeline]) => ({
            userId,
            timeline,
            displayName: `${userId.substring(0, 8)}... (${timeline.outcome_type || 'pending'})`,
            fullId: userId,
            outcomeType: timeline.outcome_type || 'pending',
            estimatedRevenue: timeline.total_estimated_revenue || 0,
            actualRevenue: timeline.total_actual_revenue || 0
        }));
        
        // Filter based on search term
        if (userSearchTerm) {
            return users.filter(user => 
                user.userId.toLowerCase().includes(userSearchTerm.toLowerCase()) ||
                user.outcomeType.toLowerCase().includes(userSearchTerm.toLowerCase())
            );
        }
        
        return users;
    }, [userTimelines, userSearchTerm]);

    if (!stage3Data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No Stage 3 data available. Run analysis to see revenue timeline results.
                </p>
            </div>
        );
    }

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

    // Helper function to format percentages
    const formatPercentage = (num, total) => {
        if (!total || typeof num !== 'number') return '0.0%';
        return ((num / total) * 100).toFixed(1) + '%';
    };
    
    // Get the appropriate data for EventTimelineTable - backend provides it directly now
    const getTimelineTableData = () => {
        if (selectedUser && backendUserDailyMetrics[selectedUser]) {
            // Individual user view - create data structure for single user
            const userDates = Object.keys(backendUserDailyMetrics[selectedUser]).sort();
            const userDailyData = {};
            const userCumulativeData = {};
            
            // Build daily metrics for selected user only
            userDates.forEach(date => {
                userDailyData[date] = backendUserDailyMetrics[selectedUser][date];
            });
            
            // Calculate cumulative metrics for individual user
            let cumulativeCounters = {
                trial_started: 0,
                trial_ended: 0,
                trial_converted: 0,
                trial_cancelled: 0,
                initial_purchase: 0,
                subscription_cancelled: 0,
                revenue: 0,
                refund: 0,
                refund_count: 0,
                estimated_revenue: 0
            };
            
            userDates.forEach(date => {
                const dayData = userDailyData[date];
                
                // Update cumulative counters
                Object.keys(cumulativeCounters).forEach(key => {
                    if (key === 'trial_pending' || key === 'subscription_active') {
                        // State-based metrics use current value, not cumulative
                        userCumulativeData[date] = userCumulativeData[date] || {};
                        userCumulativeData[date][`cumulative_${key}`] = dayData[key] || 0;
                    } else {
                        cumulativeCounters[key] += (dayData[key] || 0);
                        userCumulativeData[date] = userCumulativeData[date] || {};
                        userCumulativeData[date][`cumulative_${key}`] = cumulativeCounters[key];
                    }
                });
                
                // Add net revenue
                userCumulativeData[date]['cumulative_revenue_net'] = cumulativeCounters['revenue'] - cumulativeCounters['refund'];
            });
            
            return {
                data: {
                    dates: userDates,
                    daily_metrics: userDailyData,
                    cumulative_metrics: userCumulativeData,
                    user_timelines: backendUserTimelinesEvents,
                    user_daily_metrics: { [selectedUser]: backendUserDailyMetrics[selectedUser] },
                    summary_stats: {
                        total_users: 1,
                        total_estimated_revenue: userTimelines[selectedUser]?.total_estimated_revenue || 0,
                        total_actual_revenue: userTimelines[selectedUser]?.total_actual_revenue || 0
                    }
                }
            };
        } else {
            // Aggregate view - use backend data directly
            return {
                data: {
                    dates: Object.keys(backendTimelineData).sort(),
                    daily_metrics: backendTimelineData,
                    cumulative_metrics: {}, // Will be calculated by EventTimelineTable
                    user_timelines: backendUserTimelinesEvents,
                    user_daily_metrics: backendUserDailyMetrics,
                    summary_stats: aggregateStats
                }
            };
        }
    };

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <div className="flex items-center gap-2 mb-6">
                <TrendingUp size={24} className="text-purple-600 dark:text-purple-400" />
                <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                    Stage 3: Revenue Timeline Generation Results
                </h2>
                <div className="ml-auto flex items-center gap-1 text-purple-600 dark:text-purple-400 text-sm">
                    <span>✓</span>
                    <span>Revenue Estimation & Timeline Projection</span>
                </div>
            </div>

            {/* User Selection Controls */}
            <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                <div className="flex items-center gap-2 mb-3">
                    <User size={20} className="text-blue-600 dark:text-blue-400" />
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">View Selection</h3>
                </div>
                
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {/* User Dropdown */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                            Select User (or leave empty for aggregate)
                        </label>
                        <div className="space-y-2">
                            <input
                                type="text"
                                placeholder="Search by User ID or outcome..."
                                value={userSearchTerm}
                                onChange={(e) => setUserSearchTerm(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                            />
                            <select
                                value={selectedUser}
                                onChange={(e) => setSelectedUser(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                            >
                                <option value="">🌍 Aggregate View (All Users)</option>
                                {userList.map(user => (
                                    <option key={user.userId} value={user.userId}>
                                        👤 {user.displayName} - Est: {formatCurrency(user.estimatedRevenue)}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </div>
                    
                    {/* Current Selection Info */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                            Current View
                        </label>
                        <div className="p-3 bg-white dark:bg-gray-800 rounded-md border border-gray-200 dark:border-gray-700">
                            {selectedUser ? (
                                <div>
                                    <div className="flex items-center gap-2 mb-1">
                                        <User size={16} className="text-blue-600 dark:text-blue-400" />
                                        <span className="font-medium text-gray-900 dark:text-white">Individual User</span>
                                    </div>
                                    <div className="text-sm text-gray-600 dark:text-gray-400 break-all">
                                        ID: {selectedUser}
                                    </div>
                                    <div className="text-sm text-gray-600 dark:text-gray-400">
                                        Status: {userTimelines[selectedUser]?.outcome_type || 'pending'}
                                    </div>
                                </div>
                            ) : (
                                <div>
                                    <div className="flex items-center gap-2 mb-1">
                                        <Globe size={16} className="text-green-600 dark:text-green-400" />
                                        <span className="font-medium text-gray-900 dark:text-white">Aggregate View</span>
                                    </div>
                                    <div className="text-sm text-gray-600 dark:text-gray-400">
                                        Showing data for all {Object.keys(userTimelines).length} users
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* Revenue Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Users size={20} className="text-purple-600 dark:text-purple-400" />
                        <h3 className="font-semibold text-purple-900 dark:text-purple-200">Users with Timelines</h3>
                    </div>
                    <p className="text-2xl font-bold text-purple-600 dark:text-purple-300">
                        {selectedUser ? '1' : formatNumber(stage3Data.users_with_timelines || 0)}
                    </p>
                    <p className="text-sm text-purple-700 dark:text-purple-400">
                        {selectedUser ? 'Individual view' : 'Revenue projections generated'}
                    </p>
                </div>

                <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
                    <div className="flex items-center gap-2 mb-2">
                        <DollarSign size={20} className="text-green-600 dark:text-green-400" />
                        <h3 className="font-semibold text-green-900 dark:text-green-200">Estimated Revenue</h3>
                    </div>
                    <p className="text-2xl font-bold text-green-600 dark:text-green-300">
                        {selectedUser ? 
                            formatCurrency(userTimelines[selectedUser]?.total_estimated_revenue || 0) :
                            formatCurrency(aggregateStats.total_estimated_revenue || 0)
                        }
                    </p>
                    <p className="text-sm text-green-700 dark:text-green-400">
                        Based on conversion probabilities
                    </p>
                </div>

                <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Activity size={20} className="text-blue-600 dark:text-blue-400" />
                        <h3 className="font-semibold text-blue-900 dark:text-blue-200">Actual Revenue</h3>
                    </div>
                    <p className="text-2xl font-bold text-blue-600 dark:text-blue-300">
                        {selectedUser ? 
                            formatCurrency(userTimelines[selectedUser]?.total_actual_revenue || 0) :
                            formatCurrency(aggregateStats.total_actual_revenue || 0)
                        }
                    </p>
                    <p className="text-sm text-blue-700 dark:text-blue-400">
                        From confirmed outcomes
                    </p>
                </div>

                <div className="p-4 bg-orange-50 dark:bg-orange-900/20 rounded-lg border border-orange-200 dark:border-orange-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Clock size={20} className="text-orange-600 dark:text-orange-400" />
                        <h3 className="font-semibold text-orange-900 dark:text-orange-200">Timeline End</h3>
                    </div>
                    <p className="text-lg font-bold text-orange-600 dark:text-orange-300">
                        {stage3Data.timeline_end_date || 'N/A'}
                    </p>
                    <p className="text-sm text-orange-700 dark:text-orange-400">
                        Projection end date
                    </p>
                </div>
            </div>

            {/* Revenue Formula Explanation */}
            <div className="mb-6">
                <div className="p-4 bg-gradient-to-r from-green-50 to-blue-50 dark:from-green-900/20 dark:to-blue-900/20 rounded-lg border border-green-200 dark:border-green-800">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
                        <Calculator size={20} className="text-green-600 dark:text-green-400" />
                        Revenue Calculation Using Event State Tracker
                    </h3>
                    
                    <div className="bg-white dark:bg-gray-800 rounded-lg p-4 font-mono text-sm mb-4">
                        <div className="space-y-3">
                            <div className="text-center">
                                <div className="text-lg font-bold text-gray-900 dark:text-white mb-2">
                                    🔵 Pre-Outcome (Trial Pending)
                                </div>
                                <div className="text-base text-blue-600 dark:text-blue-400">
                                    estimated_revenue = conversion_probability × (1 - refund_rate) × price
                                </div>
                            </div>
                            
                            <div className="text-center">
                                <div className="text-lg font-bold text-gray-900 dark:text-white mb-2">
                                    🟢 Post-Conversion (Subscription Active)
                                </div>
                                <div className="text-base text-green-600 dark:text-green-400">
                                    revenue = price × (1 - current_refund_rate)
                                </div>
                                <div className="text-xs text-gray-500 dark:text-gray-400">
                                    current_refund_rate degrades linearly to 0% over 30 days
                                </div>
                            </div>
                            
                            <div className="text-center">
                                <div className="text-lg font-bold text-gray-900 dark:text-white mb-2">
                                    🔴 Post-Cancellation (Subscription Cancelled)
                                </div>
                                <div className="text-base text-red-600 dark:text-red-400">
                                    revenue = $0.00
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="text-sm text-gray-600 dark:text-gray-400">
                        <strong>📈 State Tracking:</strong> 
                        • Backend uses event_state_tracker.py for proper event state management
                        • Trial states: started → pending → ended (converted/cancelled)
                        • Subscription states: active → cancelled  
                        • Revenue tracking includes actual event revenue and refunds
                    </div>
                </div>
            </div>

            {/* User & Segment Details - Full Width */}
            <div className="mb-6 p-4 bg-gradient-to-r from-purple-50 to-indigo-50 dark:from-purple-900/20 dark:to-indigo-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                    <Target size={20} className="text-purple-600 dark:text-purple-400" />
                    User & Segment Details
                </h3>
                
                {selectedUser ? (
                    (() => {
                        const userTimeline = userTimelines[selectedUser];
                        const segmentInfo = userTimeline?.segment_info || {};
                        const userProperties = userTimeline?.user_properties || {};
                        const segmentProperties = segmentInfo.segment_properties || {};
                        const segmentMetrics = segmentInfo.segment_metrics || {};
                        
                        // Check if enhanced rollup data is available (new feature)
                        const hasEnhancedData = segmentInfo.has_enhanced_rollup_data === true;
                        
                        // Use enhanced data if available, otherwise fall back to existing behavior
                        const originalSegmentProperties = hasEnhancedData 
                            ? (segmentInfo.original_segment_properties || segmentProperties)
                            : segmentProperties;
                        const targetSegmentProperties = hasEnhancedData 
                            ? (segmentInfo.target_segment_properties || segmentProperties)
                            : segmentProperties;
                        const rollupChain = hasEnhancedData ? (segmentInfo.rollup_chain || []) : [];
                        
                        // DEBUG: Log the actual structure
                        console.log('🔍 [STAGE3 DEBUG] Complete segmentInfo structure:', segmentInfo);
                        console.log('🔍 [STAGE3 DEBUG] hasEnhancedData:', hasEnhancedData);
                        console.log('🔍 [STAGE3 DEBUG] originalSegmentProperties:', originalSegmentProperties);
                        console.log('🔍 [STAGE3 DEBUG] targetSegmentProperties:', targetSegmentProperties);
                        console.log('🔍 [STAGE3 DEBUG] rollupChain:', rollupChain);
                        console.log('🔍 [STAGE3 DEBUG] Available keys in segmentInfo:', Object.keys(segmentInfo));
                        
                        // Determine if rollup was applied and get appropriate segment IDs
                        const hasRollup = segmentInfo.match_type === 'rollup' && segmentInfo.rollup_steps > 0;
                        const matchedSegmentId = segmentInfo.matched_segment_id || segmentInfo.segment_id;
                        const finalSegmentId = segmentInfo.final_segment_id || segmentInfo.segment_id;
                        
                        // Extract user count from segment metrics
                        const userCount = segmentMetrics.trial_started_count || segmentMetrics.sample_size || 'Unknown';
                        
                        // Level is the rollup steps
                        const level = segmentInfo.rollup_steps || 0;
                        
                        return (
                            <div className="space-y-4">
                                <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
                                    {/* Column 1: User ID + User Properties */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        <div className="font-mono text-xs mb-3 text-blue-300 break-all">
                                            {selectedUser}
                                        </div>
                                        <div className="text-xs text-gray-400 mb-3">
                                            Short: {selectedUser.substring(0, 8)}...
                                        </div>
                                        <div className="space-y-1 text-xs">
                                            <div><span className="text-gray-400">Product:</span> {userProperties.product_id || 'N/A'}</div>
                                            <div><span className="text-gray-400">Store:</span> {userProperties.app_store || 'N/A'}</div>
                                            <div><span className="text-gray-400">Price:</span> {userProperties.price_bucket || 'N/A'}</div>
                                            <div><span className="text-gray-400">Country:</span> {userProperties.country || 'N/A'}</div>
                                            <div><span className="text-gray-400">Economic Tier:</span> {userProperties.economic_tier || 'N/A'}</div>
                                            <div><span className="text-gray-400">Region:</span> {userProperties.region || 'N/A'}</div>
                                        </div>
                                    </div>

                                    {/* Column 2: Matched Segment (Initial) - Uses enhanced data when available */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        <div className="font-medium text-blue-300 mb-2 text-xs">
                                            ID: {matchedSegmentId ? matchedSegmentId.substring(0, 8) + '...' : 'Unknown'}
                                        </div>
                                        <div className="space-y-1 text-xs">
                                            <div><span className="text-gray-400">Product:</span> {originalSegmentProperties.product_id || 'N/A'}</div>
                                            <div><span className="text-gray-400">Store:</span> {originalSegmentProperties.app_store || 'N/A'}</div>
                                            <div><span className="text-gray-400">Price:</span> {originalSegmentProperties.price_bucket || 'N/A'}</div>
                                            <div><span className="text-gray-400">Country:</span> {originalSegmentProperties.country || 'N/A'}</div>
                                            <div><span className="text-gray-400">Economic Tier:</span> {originalSegmentProperties.economic_tier || 'N/A'}</div>
                                            <div><span className="text-gray-400">Region:</span> {originalSegmentProperties.region || 'N/A'}</div>
                                            <div><span className="text-gray-400">Level:</span> {hasEnhancedData && rollupChain.length > 0 ? (rollupChain[0].level || 'N/A') : level}</div>
                                            <div><span className="text-gray-400">Users:</span> {hasEnhancedData && rollupChain.length > 0 ? (rollupChain[0].cohort_size || 'N/A') : userCount}</div>
                                        </div>
                                    </div>

                                    {/* Column 3: Target Segment (If Rollup) - Uses enhanced data when available */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        {hasRollup ? (
                                            <div>
                                                <div className="font-medium text-green-300 mb-2 text-xs">
                                                    ID: {finalSegmentId ? finalSegmentId.substring(0, 8) + '...' : 'Unknown'}
                                                </div>
                                                <div className="space-y-1 text-xs">
                                                    <div><span className="text-gray-400">Product:</span> {targetSegmentProperties.product_id || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Store:</span> {targetSegmentProperties.app_store || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Price:</span> {targetSegmentProperties.price_bucket || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Country:</span> {targetSegmentProperties.country || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Economic Tier:</span> {targetSegmentProperties.economic_tier || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Region:</span> {targetSegmentProperties.region || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Level:</span> {hasEnhancedData && rollupChain.length > 0 ? (rollupChain[rollupChain.length - 1].level || 'N/A') : level}</div>
                                                    <div><span className="text-gray-400">Users:</span> {hasEnhancedData && rollupChain.length > 0 ? (rollupChain[rollupChain.length - 1].cohort_size || 'N/A') : userCount}</div>
                                                </div>
                                            </div>
                                        ) : (
                                            <div className="text-center text-green-300 font-medium text-xs py-8">
                                                Direct match - no rollup needed
                                            </div>
                                        )}
                                    </div>

                                    {/* Column 4: 4 Key Metrics */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        <div className="space-y-2 text-xs">
                                            <div>
                                                <span className="text-gray-400">Trial Conv.:</span> 
                                                <span className="ml-1 text-green-300">
                                                    {((segmentMetrics.trial_conversion_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((segmentMetrics.trial_converted_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Init. Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((segmentMetrics.initial_purchase_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Renewal Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((segmentMetrics.renewal_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Sample size:</span> 
                                                <span className="ml-1 text-blue-300">
                                                    {userCount}
                                                </span>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Column 5: Prediction Accuracy */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        <div className="text-center">
                                            <div className={`
                                                px-3 py-2 rounded-full text-xs font-medium
                                                ${segmentInfo.accuracy_score === 'high' ? 'bg-green-600 text-white' : 
                                                  segmentInfo.accuracy_score === 'medium' ? 'bg-yellow-600 text-white' : 
                                                  'bg-red-600 text-white'}
                                            `}>
                                                {(segmentInfo.accuracy_score || 'Unknown').toUpperCase()}
                                            </div>
                                            <div className="text-xs text-gray-400 mt-2">
                                                Prediction Accuracy
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        );
                    })()
                ) : (
                    <div className="text-gray-600 dark:text-gray-400 italic text-center py-8">
                        Select an individual user to view segment details
                    </div>
                )}
            </div>

            {/* Event Timeline Table - Uses backend data directly */}
            {stage3Data && (
                <div className="mb-6">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                        {selectedUser ? `Revenue Timeline for User ${selectedUser.substring(0, 16)}...` : 'Aggregate Revenue Timeline'}
                    </h3>
                    <EventTimelineTable 
                        data={getTimelineTableData()} 
                        selectedUserId={selectedUser}
                        selectedProductId={selectedUser ? userTimelines[selectedUser]?.user_properties?.product_id : null}
                    />
                </div>
            )}

            {/* Stage 3 Error Display */}
            {stage3Data.error && (
                <div className="text-center py-8 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
                    <AlertCircle size={48} className="mx-auto text-red-400 mb-4" />
                    <p className="text-red-600 dark:text-red-400 mb-2">
                        Stage 3 Error: {stage3Data.error}
                    </p>
                </div>
            )}

            {/* No Data Message */}
            {Object.keys(userTimelines).length === 0 && !stage3Data.error && (
                <div className="text-center py-8 bg-gray-50 dark:bg-gray-700 rounded-lg">
                    <TrendingUp size={48} className="mx-auto text-gray-400 mb-4" />
                    <p className="text-gray-600 dark:text-gray-400 mb-2">
                        No revenue timeline data available
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-500">
                        Run Stage 3 analysis to see revenue timeline projections
                    </p>
                </div>
            )}
        </div>
    );
};

export default Stage3RevenueResults; 