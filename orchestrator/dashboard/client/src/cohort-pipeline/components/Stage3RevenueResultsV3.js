import React, { useState, useMemo } from 'react';
import { TrendingUp, Users, DollarSign, Clock, Activity, AlertCircle, Database, Calculator, Target, User, Globe, Package } from 'lucide-react';
import EventTimelineTableV3 from './EventTimelineTableV3';

const Stage3RevenueResultsV3 = ({ data }) => {
    const [selectedUser, setSelectedUser] = useState(''); // Empty string means aggregate view
    const [userSearchTerm, setUserSearchTerm] = useState('');
    const [selectedProductId, setSelectedProductId] = useState(''); // NEW: Product selector state
    
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
    
    // 🔍 FRONTEND TIMELINE DEBUG: Log the data received from backend
    if (Object.keys(timelineResults).length > 0) {
        console.log('🔍 [FRONTEND TIMELINE DEBUG] Data received from backend:');
        console.log('🔍   - timeline_data dates:', backendTimelineData ? Object.keys(backendTimelineData).length : 0);
        console.log('🔍   - user_daily_metrics users:', backendUserDailyMetrics ? Object.keys(backendUserDailyMetrics).length : 0);
        console.log('🔍   - user_timelines_events users:', backendUserTimelinesEvents ? Object.keys(backendUserTimelinesEvents).length : 0);
        
        // ***** SEND THIS TO AI - DEBUG LOG #1 *****
        console.log('***** SEND THIS TO AI - DEBUG LOG #1 *****');
        console.log('ISSUE 2 DEBUG - Specific User Daily Metrics Check:');
        const debugUserId = '$device:07BDA330-D882-45B2-946E-A2E81B011718';
        console.log('Target User ID:', debugUserId);
        console.log('User found in backendUserDailyMetrics:', debugUserId in backendUserDailyMetrics);
        
        if (backendUserDailyMetrics[debugUserId]) {
            const userData = backendUserDailyMetrics[debugUserId];
            console.log('Available dates for user:', Object.keys(userData));
            
            const may27Data = userData['2025-05-27'];
            console.log('May 27th data exists:', may27Data ? 'YES' : 'NO');
            
            if (may27Data) {
                console.log('May 27th COMPLETE data:', may27Data);
                console.log('trial_started:', may27Data.trial_started);
                console.log('trial_ended:', may27Data.trial_ended, '← Should be 1');
                console.log('trial_cancelled:', may27Data.trial_cancelled, '← Should be 1');
                console.log('trial_converted:', may27Data.trial_converted);
                console.log('trial_pending:', may27Data.trial_pending);
            }
        } else {
            console.log('User NOT FOUND in backendUserDailyMetrics!');
            console.log('Available users (first 5):', Object.keys(backendUserDailyMetrics).slice(0, 5));
        }
        console.log('***** END DEBUG LOG #1 *****');
        
        // ***** SEND THIS TO AI - DEBUG LOG #2 *****
        console.log('***** SEND THIS TO AI - DEBUG LOG #2 *****');
        console.log('ISSUE 1 DEBUG - Product Selector Data Check:');
        console.log('User found in backendUserTimelinesEvents:', debugUserId in backendUserTimelinesEvents);
        
        if (backendUserTimelinesEvents[debugUserId]) {
            const userData = backendUserTimelinesEvents[debugUserId];
            console.log('User timeline events structure keys:', Object.keys(userData));
            console.log('Has summary:', 'summary' in userData);
            
            if (userData.summary) {
                console.log('Summary keys:', Object.keys(userData.summary));
                console.log('Has product_summaries:', 'product_summaries' in userData.summary);
                
                if (userData.summary.product_summaries) {
                    console.log('Product summaries keys:', Object.keys(userData.summary.product_summaries));
                    console.log('Number of products:', Object.keys(userData.summary.product_summaries).length);
                    console.log('COMPLETE product_summaries data:', userData.summary.product_summaries);
                } else {
                    console.log('ERROR: product_summaries is missing from summary!');
                }
            } else {
                console.log('ERROR: summary is missing from user timeline events!');
            }
        } else {
            console.log('User NOT FOUND in backendUserTimelinesEvents!');
            console.log('Available users in backendUserTimelinesEvents (first 5):', Object.keys(backendUserTimelinesEvents).slice(0, 5));
        }
        console.log('***** END DEBUG LOG #2 *****');
        
        // ***** SEND THIS TO AI - DEBUG LOG #3 *****
        console.log('***** SEND THIS TO AI - DEBUG LOG #3 *****');
        console.log('GENERAL DATA STRUCTURE CHECK:');
        console.log('backendTimelineData exists:', !!backendTimelineData);
        console.log('backendUserDailyMetrics exists:', !!backendUserDailyMetrics);
        console.log('backendUserTimelinesEvents exists:', !!backendUserTimelinesEvents);
        console.log('Number of users in backendUserDailyMetrics:', Object.keys(backendUserDailyMetrics).length);
        console.log('Number of users in backendUserTimelinesEvents:', Object.keys(backendUserTimelinesEvents).length);
        
        if (Object.keys(backendUserDailyMetrics).length > 0) {
            const firstUser = Object.keys(backendUserDailyMetrics)[0];
            const firstUserData = backendUserDailyMetrics[firstUser];
            const firstDate = Object.keys(firstUserData)[0];
            console.log('Sample user daily metrics structure:', {
                userId: firstUser,
                date: firstDate,
                metrics: firstUserData[firstDate]
            });
        }
        console.log('***** END DEBUG LOG #3 *****');
        
        if (backendTimelineData && Object.keys(backendTimelineData).length > 0) {
            const sampleDate = Object.keys(backendTimelineData)[0];
            const sampleData = backendTimelineData[sampleDate];
            console.log(`🔍   - Sample timeline_data[${sampleDate}]:`);
            console.log('🔍     - trial_started:', sampleData.trial_started || 0);
            console.log('🔍     - trial_converted:', sampleData.trial_converted || 0);
            console.log('🔍     - revenue:', sampleData.revenue || 0);
            console.log('🔍     - estimated_revenue:', sampleData.estimated_revenue || 0);
        }
        
        if (backendUserDailyMetrics && Object.keys(backendUserDailyMetrics).length > 0) {
            const sampleUser = Object.keys(backendUserDailyMetrics)[0];
            const sampleUserData = backendUserDailyMetrics[sampleUser];
            console.log(`🔍   - Sample user_daily_metrics[${sampleUser}]:`);
            console.log('🔍     - Dates available:', Object.keys(sampleUserData).length);
            if (Object.keys(sampleUserData).length > 0) {
                const sampleDate = Object.keys(sampleUserData)[0];
                const sampleDayData = sampleUserData[sampleDate];
                console.log(`🔍     - Sample date[${sampleDate}]:`);
                console.log('🔍       - trial_started:', sampleDayData.trial_started || 0);
                console.log('🔍       - trial_converted:', sampleDayData.trial_converted || 0);
                console.log('🔍       - revenue:', sampleDayData.revenue || 0);
                console.log('🔍       - estimated_revenue:', sampleDayData.estimated_revenue || 0);
            }
        }
        
        if (backendUserTimelinesEvents && Object.keys(backendUserTimelinesEvents).length > 0) {
            const sampleEventsUser = Object.keys(backendUserTimelinesEvents)[0];
            const sampleEventsData = backendUserTimelinesEvents[sampleEventsUser];
            console.log(`🔍   - Sample user_timelines_events[${sampleEventsUser}]:`);
            console.log('🔍     - Structure keys:', Object.keys(sampleEventsData));
            console.log('🔍     - Events count:', sampleEventsData.events ? sampleEventsData.events.length : 0);
            console.log('🔍     - Products count:', sampleEventsData.products ? Object.keys(sampleEventsData.products).length : 0);
            if (sampleEventsData.summary) {
                console.log('🔍     - Summary total_estimated_revenue:', sampleEventsData.summary.total_estimated_revenue || 0);
                console.log('🔍     - Summary total_actual_revenue:', sampleEventsData.summary.total_actual_revenue || 0);
            }
        }
    }
    
    // NEW: Detect users with multiple products
    const usersWithMultipleProducts = useMemo(() => {
        const multiProductUsers = {};
        
        Object.entries(backendUserTimelinesEvents).forEach(([userId, userData]) => {
            if (userData.summary && userData.summary.product_summaries) {
                const productIds = Object.keys(userData.summary.product_summaries);
                if (productIds.length > 1) {
                    multiProductUsers[userId] = productIds;
                }
            }
        });
        
        return multiProductUsers;
    }, [backendUserTimelinesEvents]);
    
    // NEW: Get available products for selected user
    const availableProductsForUser = useMemo(() => {
        if (!selectedUser || !backendUserTimelinesEvents[selectedUser]) {
            return [];
        }
        
        const userData = backendUserTimelinesEvents[selectedUser];
        if (userData.summary && userData.summary.product_summaries) {
            return Object.keys(userData.summary.product_summaries);
        }
        
        return [];
    }, [selectedUser, backendUserTimelinesEvents]);
    
    // NEW: Check if current user has multiple products
    const selectedUserHasMultipleProducts = availableProductsForUser.length > 1;
    
    // Process user list for dropdown with search filtering
    const userList = useMemo(() => {
        // FIXED: Use user_timelines_events for user list, which has consolidated user data
        const usersSource = backendUserTimelinesEvents && Object.keys(backendUserTimelinesEvents).length > 0 
            ? backendUserTimelinesEvents 
            : userTimelines;
        
        const users = Object.entries(usersSource).map(([userId, timelineData]) => {
            // Handle both old and new data formats
            let outcomeType = 'pending';
            let estimatedRevenue = 0;
            let actualRevenue = 0;
            
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
            
            // NEW: Add indicator for multi-product users
            const hasMultipleProducts = usersWithMultipleProducts[userId] ? true : false;
            const productIndicator = hasMultipleProducts ? ' 📦' : '';
            
            return {
            userId,
                timeline: timelineData,
                displayName: `${userId.substring(0, 8)}... (${outcomeType})${productIndicator}`,
            fullId: userId,
                outcomeType,
                estimatedRevenue,
                actualRevenue,
                hasMultipleProducts
            };
        });
        
        // Filter based on search term
        if (userSearchTerm) {
            return users.filter(user => 
                user.userId.toLowerCase().includes(userSearchTerm.toLowerCase()) ||
                user.outcomeType.toLowerCase().includes(userSearchTerm.toLowerCase())
            );
        }
        
        return users;
    }, [userTimelines, backendUserTimelinesEvents, userSearchTerm, usersWithMultipleProducts]);

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
            
            const individualResult = {
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
            
            return individualResult;
        }
        
        // Aggregate view - calculate cumulative metrics from daily metrics
        const aggregateDates = Object.keys(backendTimelineData).sort();
        const aggregateCumulativeMetrics = {};
        
        // Initialize cumulative counters for aggregate view
        let cumulativeCounters = {
            trial_started: 0,
            trial_ended: 0,
            trial_converted: 0,
            trial_cancelled: 0,
            initial_purchase: 0,
            subscription_cancelled: 0,
            revenue: 0,
            refund: 0,
            refund_count: 0
        };
        
        // Calculate cumulative metrics for each date
        aggregateDates.forEach(date => {
            const dayData = backendTimelineData[date] || {};
            
            // Update cumulative counters (for event-based metrics)
            Object.keys(cumulativeCounters).forEach(key => {
                if (key in dayData && typeof dayData[key] === 'number') {
                    cumulativeCounters[key] += dayData[key];
                }
            });
            
            // Store cumulative data for this date
            aggregateCumulativeMetrics[date] = {
                // State-based metrics use current daily value (not cumulative)
                trial_pending: dayData.trial_pending || 0,
                subscription_active: dayData.subscription_active || 0,
                estimated_revenue: dayData.estimated_revenue || 0,
                
                // Event-based metrics use cumulative values
                cumulative_trial_started: cumulativeCounters.trial_started,
                cumulative_trial_ended: cumulativeCounters.trial_ended,
                cumulative_trial_converted: cumulativeCounters.trial_converted,
                cumulative_trial_cancelled: cumulativeCounters.trial_cancelled,
                cumulative_initial_purchase: cumulativeCounters.initial_purchase,
                cumulative_subscription_cancelled: cumulativeCounters.subscription_cancelled,
                cumulative_revenue: cumulativeCounters.revenue,
                cumulative_refund: cumulativeCounters.refund,
                cumulative_refund_count: cumulativeCounters.refund_count,
                cumulative_revenue_net: cumulativeCounters.revenue - cumulativeCounters.refund
            };
        });
        
        const aggregateResult = {
            data: {
                dates: aggregateDates,
                daily_metrics: backendTimelineData,
                cumulative_metrics: aggregateCumulativeMetrics,
                user_timelines: backendUserTimelinesEvents,
                user_daily_metrics: backendUserDailyMetrics,
                summary_stats: aggregateStats
            }
        };
        
        return aggregateResult;
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
                                onChange={(e) => {
                                    setSelectedUser(e.target.value);
                                    setSelectedProductId(''); // Reset product selection when user changes
                                }}
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
                        
                        {/* NEW: Product Selector - Only show when user has multiple products */}
                        {selectedUser && selectedUserHasMultipleProducts && (
                            <div className="mt-3 p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
                                <div className="flex items-center gap-2 mb-2">
                                    <Package size={16} className="text-yellow-600 dark:text-yellow-400" />
                                    <span className="text-sm font-medium text-yellow-800 dark:text-yellow-200">
                                        Multiple Products Detected
                                    </span>
                                </div>
                                <select
                                    value={selectedProductId}
                                    onChange={(e) => setSelectedProductId(e.target.value)}
                                    className="w-full px-3 py-2 border border-yellow-300 dark:border-yellow-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                                >
                                    <option value="">📊 All Products (Combined)</option>
                                    {availableProductsForUser.map(productId => (
                                        <option key={productId} value={productId}>
                                            📦 {productId}
                                        </option>
                                    ))}
                                </select>
                                <p className="text-xs text-yellow-700 dark:text-yellow-300 mt-1">
                                    This user has trials for {availableProductsForUser.length} different products. 
                                    Select a specific product or view combined data.
                                </p>
                            </div>
                        )}
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
                                    {selectedUserHasMultipleProducts && (
                                        <div className="text-sm text-orange-600 dark:text-orange-400">
                                            Products: {availableProductsForUser.length} ({selectedProductId || 'All'})
                                        </div>
                                    )}
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
                                    <div className="text-sm text-blue-600 dark:text-blue-400">
                                        Multi-product users: {Object.keys(usersWithMultipleProducts).length}
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
                        // --- START: REPLACEMENT LOGIC ---
                        
                        // Get the complete, consolidated data for the selected user
                        const userEventsData = backendUserTimelinesEvents[selectedUser];

                        // If there's no data for the selected user, show an error state.
                        if (!userEventsData) {
                            return <div className="text-red-500 text-center">Error: No timeline event data found for user {selectedUser}.</div>;
                        }

                        // Extract segment info and properties directly from the new, correct data structure.
                        const segmentInfo = userEventsData.segment_info || {};
                        const userProperties = segmentInfo.user_properties || {};
                        
                        // Extract metrics directly from the segment info
                        const effectiveSegmentMetrics = segmentInfo.conversion_metrics || {};

                        // The rest of your display logic can now use these variables directly.
                        // For example, to get the matched segment ID:
                        const matchedSegmentId = segmentInfo.matched_segment_id || segmentInfo.segment_id;
                        // To get the final segment ID after rollup:
                        const finalSegmentId = segmentInfo.final_viable_segment_id || segmentInfo.segment_id;

                        // To check if a rollup occurred:
                        const hasActualRollup = matchedSegmentId && finalSegmentId && matchedSegmentId !== finalSegmentId;

                        // Extract segment properties from the correct sources
                        let matchedSegmentProperties = userProperties;
                        let finalSegmentProperties = userProperties;
                        let matchedLevel = 0;
                        let matchedUserCount = effectiveSegmentMetrics.sample_size || effectiveSegmentMetrics.cohort_size || 'Unknown';
                        let finalLevel = 0;
                        let finalUserCount = matchedUserCount;

                        // --- END: REPLACEMENT LOGIC ---
                        
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
                                            <div><span className="text-gray-400">Product:</span> {matchedSegmentProperties.product_id || 'N/A'}</div>
                                            <div><span className="text-gray-400">Store:</span> {matchedSegmentProperties.app_store || 'N/A'}</div>
                                            <div><span className="text-gray-400">Price:</span> {matchedSegmentProperties.price_bucket || 'N/A'}</div>
                                            <div><span className="text-gray-400">Country:</span> {matchedSegmentProperties.country || 'N/A'}</div>
                                            <div><span className="text-gray-400">Economic Tier:</span> {matchedSegmentProperties.economic_tier || 'N/A'}</div>
                                            <div><span className="text-gray-400">Region:</span> {matchedSegmentProperties.region || 'N/A'}</div>
                                            <div><span className="text-gray-400">Level:</span> {matchedLevel}</div>
                                            <div><span className="text-gray-400">Users:</span> {matchedUserCount}</div>
                                        </div>
                                    </div>

                                    {/* Column 3: Target Segment (Final Segment) - Always show if we have segment data */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        {hasActualRollup ? (
                                            <div>
                                                <div className="font-medium text-green-300 mb-2 text-xs">
                                                    ID: {finalSegmentId ? finalSegmentId.substring(0, 8) + '...' : 'Unknown'}
                                                </div>
                                                <div className="space-y-1 text-xs">
                                                    <div><span className="text-gray-400">Product:</span> {finalSegmentProperties.product_id || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Store:</span> {finalSegmentProperties.app_store || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Price:</span> {finalSegmentProperties.price_bucket || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Country:</span> {finalSegmentProperties.country || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Economic Tier:</span> {finalSegmentProperties.economic_tier || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Region:</span> {finalSegmentProperties.region || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Level:</span> {finalLevel}</div>
                                                    <div><span className="text-gray-400">Users:</span> {finalUserCount}</div>
                                                </div>
                                            </div>
                                        ) : (
                                            <div>
                                                <div className="font-medium text-blue-300 mb-2 text-xs">
                                                    ID: {finalSegmentId ? finalSegmentId.substring(0, 8) + '...' : 'Same as matched'}
                                                </div>
                                                <div className="space-y-1 text-xs">
                                                    <div><span className="text-gray-400">Product:</span> {finalSegmentProperties.product_id || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Store:</span> {finalSegmentProperties.app_store || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Price:</span> {finalSegmentProperties.price_bucket || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Country:</span> {finalSegmentProperties.country || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Economic Tier:</span> {finalSegmentProperties.economic_tier || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Region:</span> {finalSegmentProperties.region || 'N/A'}</div>
                                                    <div><span className="text-gray-400">Level:</span> {finalLevel}</div>
                                                    <div><span className="text-gray-400">Users:</span> {finalUserCount}</div>
                                                </div>
                                                <div className="text-xs text-gray-500 mt-2 italic">
                                                    Direct match - no rollup applied
                                                </div>
                                            </div>
                                        )}
                                    </div>

                                    {/* Column 4: 4 Key Metrics */}
                                    <div className="bg-gray-800 text-white p-4 rounded-lg text-sm">
                                        <div className="space-y-2 text-xs">
                                            <div>
                                                <span className="text-gray-400">Trial Conv.:</span> 
                                                <span className="ml-1 text-green-300">
                                                    {((effectiveSegmentMetrics.trial_conversion_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((effectiveSegmentMetrics.trial_converted_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Init. Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((effectiveSegmentMetrics.initial_purchase_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Renewal Refund:</span> 
                                                <span className="ml-1 text-red-300">
                                                    {((effectiveSegmentMetrics.renewal_to_refund_rate || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                            <div>
                                                <span className="text-gray-400">Sample size:</span> 
                                                <span className="ml-1 text-blue-300">
                                                    {finalUserCount}
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
                        {selectedUser && selectedProductId && (
                            <span className="text-sm text-gray-500 dark:text-gray-400 ml-2">
                                (Product: {selectedProductId})
                            </span>
                        )}
                    </h3>
                    <EventTimelineTableV3 
                        data={getTimelineTableData()} 
                        selectedUserId={selectedUser}
                        selectedProductId={selectedProductId || (selectedUser ? userTimelines[selectedUser]?.user_properties?.product_id : null)}
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

export default Stage3RevenueResultsV3; 