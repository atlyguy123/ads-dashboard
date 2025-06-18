import React, { useState, useMemo } from 'react';

// Estimated Revenue Tooltip Component
const EstimatedRevenueTooltip = ({ data, position, onClose, selectedUserId, selectedProductId }) => {
    const [isHovered, setIsHovered] = React.useState(false);
    const closeTimeoutRef = React.useRef(null);
    
    if (!data) return null;
    
    // Add safety checks for data structure
    const safeValue = data.value || 0;
    const safeBreakdown = data.breakdown || {};
    const safeComponents = safeBreakdown.components || {};
    const safeFormula = safeBreakdown.formula || 'No formula available';
    const safeCalculation = safeBreakdown.calculation || 'No calculation available';
    
    const handleMouseEnter = () => {
        setIsHovered(true);
        // Clear any pending close timeout
        if (closeTimeoutRef.current) {
            clearTimeout(closeTimeoutRef.current);
            closeTimeoutRef.current = null;
        }
    };
    
    const handleMouseLeave = () => {
        setIsHovered(false);
        // Set a timeout to close the tooltip
        closeTimeoutRef.current = setTimeout(() => {
            onClose();
        }, 300); // Longer delay to prevent accidental closing
    };
    
    return (
        <div 
            className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg p-4 max-w-md pointer-events-auto"
            style={{
                // Use the position calculated in the mouse event handler directly
                left: position.x,
                top: position.y,
                pointerEvents: 'auto'
            }}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
        >
            <div className="flex justify-between items-start mb-3">
                <h4 className="font-semibold text-gray-900 dark:text-white">
                    Estimated Revenue: ${safeValue.toFixed(2)}
                </h4>
                <button 
                    onClick={onClose}
                    className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 ml-2"
                >
                    ×
                </button>
            </div>
            
            <div className="space-y-3">
                {/* NEW: Refund Rate Ratios - Show prominently at the top */}
                {(() => {
                    const trialConverted = safeBreakdown.trial_converted || {};
                    const initialPurchase = safeBreakdown.initial_purchase || {};
                    
                    const showTrialRatio = trialConverted.actual_revenue > 0;
                    const showInitialRatio = initialPurchase.actual_revenue > 0;
                    
                    if (!showTrialRatio && !showInitialRatio) return null;
                    
                    return (
                        <div className="bg-blue-50 dark:bg-blue-900 p-3 rounded-lg border border-blue-200 dark:border-blue-700">
                            <div className="text-sm font-medium text-blue-800 dark:text-blue-200 mb-2">
                                🎯 Refund Rate Analysis
                            </div>
                            
                            {showTrialRatio && (
                                <div className="mb-2">
                                    <div className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                        Trial Conversion Refund Rate:
                                    </div>
                                    <div className="text-sm font-bold text-blue-900 dark:text-blue-100">
                                        {(() => {
                                            const revenue = trialConverted.actual_revenue || 0;
                                            const estimatedRefunds = trialConverted.estimated_refunds || 0;
                                            const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                            return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                        })()}
                                    </div>
                                    <div className="text-xs text-blue-600 dark:text-blue-400">
                                        ${(trialConverted.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(trialConverted.actual_revenue || 0).toFixed(2)} revenue
                                    </div>
                                </div>
                            )}
                            
                            {showInitialRatio && (
                                <div>
                                    <div className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                        Initial Purchase Refund Rate:
                                    </div>
                                    <div className="text-sm font-bold text-blue-900 dark:text-blue-100">
                                        {(() => {
                                            const revenue = initialPurchase.actual_revenue || 0;
                                            const estimatedRefunds = initialPurchase.estimated_refunds || 0;
                                            const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                            return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                        })()}
                                    </div>
                                    <div className="text-xs text-blue-600 dark:text-blue-400">
                                        ${(initialPurchase.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(initialPurchase.actual_revenue || 0).toFixed(2)} revenue
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })()}
                
                {/* Formula */}
                <div>
                    <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Formula:
                    </div>
                    <div className="text-xs text-gray-600 dark:text-gray-400 font-mono bg-gray-50 dark:bg-gray-700 p-2 rounded">
                        {safeFormula}
                    </div>
                </div>
                
                {/* Calculation */}
                <div>
                    <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Calculation:
                    </div>
                    <div className="text-xs text-gray-600 dark:text-gray-400 font-mono bg-gray-50 dark:bg-gray-700 p-2 rounded">
                        {safeCalculation}
                    </div>
                </div>
                
                {/* Components */}
                <div>
                    <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                        Components:
                    </div>
                    <div className="space-y-1">
                        {(() => {
                            // Use explicit ordering from backend if available
                            const componentsOrder = safeBreakdown.components_order;
                            const components = safeComponents;
                            
                            if (componentsOrder && Array.isArray(componentsOrder)) {
                                // Use backend's explicit ordering
                                return componentsOrder.map((key) => {
                                    const value = components[key];
                                    if (value === undefined) return null;
                                    
                                    return (
                                        <div key={key} className="flex justify-between text-xs">
                                            <span className="text-gray-600 dark:text-gray-400">{key}:</span>
                                            <span className="text-gray-900 dark:text-white font-medium">
                                                {typeof value === 'string' ? value : 
                                                 typeof value === 'number' ? (key.toLowerCase().includes('revenue') || key.toLowerCase().includes('arpu') ? 
                                                                              `$${value.toFixed(2)}` : value.toString()) : 
                                                 value || 'N/A'}
                                            </span>
                                        </div>
                                    );
                                }).filter(Boolean);
                            } else {
                                // Fallback to object iteration (original behavior)
                                return Object.entries(components).map(([key, value]) => (
                                    <div key={key} className="flex justify-between text-xs">
                                        <span className="text-gray-600 dark:text-gray-400">{key}:</span>
                                        <span className="text-gray-900 dark:text-white font-medium">
                                            {typeof value === 'string' ? value : 
                                             typeof value === 'number' ? (key.toLowerCase().includes('revenue') || key.toLowerCase().includes('arpu') ? 
                                                                          `$${value.toFixed(2)}` : value.toString()) : 
                                             value || 'N/A'}
                                        </span>
                                    </div>
                                ));
                            }
                        })()}
                        
                        {(() => {
                            const componentsOrder = safeBreakdown.components_order;
                            const components = safeComponents;
                            
                            // Show fallback message if no components
                            if ((!componentsOrder || !Array.isArray(componentsOrder) || componentsOrder.length === 0) && 
                                Object.keys(components).length === 0) {
                                return (
                                    <div className="text-xs text-gray-500 dark:text-gray-400">
                                        No component data available
                                    </div>
                                );
                            }
                            return null;
                        })()}
                    </div>
                </div>
                
                {/* Context for single user */}
                {(selectedUserId || selectedProductId) && (
                    <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                        <div className="text-xs text-blue-600 dark:text-blue-400">
                            <strong>Note:</strong> This calculation is specific to the filtered view
                            {selectedUserId && ` for user ${selectedUserId}`}
                            {selectedProductId && ` for product ${selectedProductId}`}.
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

const EventTimelineTable = ({ data, selectedUserId = '', selectedProductId = '' }) => {
    const [showCumulative, setShowCumulative] = useState(false);
    
    // Add state for tooltip with debounce
    const [tooltipData, setTooltipData] = useState(null);
    const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
    const tooltipTimeoutRef = React.useRef(null);
    
    // FILTER VARIABLES: Now received as props from parent Stage3RevenueResults
    // These are passed from the parent component that handles user selection
    // selectedUserId and selectedProductId are now props instead of hardcoded empty strings

    // Cleanup tooltip timeout on unmount
    React.useEffect(() => {
        return () => {
            if (tooltipTimeoutRef.current) {
                clearTimeout(tooltipTimeoutRef.current);
            }
        };
    }, []);

    // Debug logging
    console.log('[DEBUG] EventTimelineTable received data:', data);
    console.log('[DEBUG] Data structure check:', {
        hasData: !!data,
        hasDataData: !!data?.data,
        hasDailyMetrics: !!data?.data?.daily_metrics,
        hasTimelineData: !!data?.data?.timeline_data,
        hasStructuredFormat: !!data?.data?.structured_format,
        hasTimelineAnalysis: !!data?.data?.structured_format?.timeline_analysis,
        dataKeys: data ? Object.keys(data) : [],
        dataDataKeys: data?.data ? Object.keys(data.data) : []
    });
    
    // NEW: Additional debugging for data.data structure
    console.log('[DEBUG] Raw data.data structure:', data?.data);
    console.log('[DEBUG] Available keys in data.data:', data?.data ? Object.keys(data.data) : []);
    if (data?.data?.daily_metrics) {
        console.log('[DEBUG] daily_metrics type:', typeof data.data.daily_metrics);
        console.log('[DEBUG] daily_metrics sample:', data.data.daily_metrics);
    }

    // Handle multiple data formats: unified pipeline, legacy API, and direct timeline API
    let timelineData;
    let dates = [];
    let dailyMetrics = {};
    let cumulativeMetrics = {};
    let userTimelines = {};
    let userDailyMetrics = {};
    
    // Check if we have valid data first, but don't return early yet
    const hasValidData = !!(data?.data?.daily_metrics || data?.data?.timeline_data || data?.data?.structured_format?.timeline_analysis || data?.data);
    
    if (hasValidData) {
        if (data.data.structured_format?.timeline_analysis) {
            // Unified pipeline format: timeline data is in structured_format.timeline_analysis
            timelineData = data.data.structured_format.timeline_analysis;
            dates = timelineData.dates || [];
            dailyMetrics = timelineData.daily_metrics || {};
            cumulativeMetrics = timelineData.cumulative_metrics || {};
            userTimelines = timelineData.user_timelines || {};
            userDailyMetrics = timelineData.user_daily_metrics || {};
            console.log('[DEBUG] Using unified pipeline format. Timeline data:', timelineData);
            console.log('[DEBUG] Dates:', dates);
            console.log('[DEBUG] Daily metrics keys:', Object.keys(dailyMetrics));
            console.log('[DEBUG] User timelines available:', Object.keys(userTimelines).length);
            console.log('[DEBUG] User daily metrics available:', Object.keys(userDailyMetrics).length);
        } else if (data.data.timeline_data && data.data.timeline_data.daily_metrics) {
            // Direct timeline API format: timeline_data contains the TimelineData structure
            timelineData = data.data.timeline_data;
            dates = timelineData.dates || [];
            dailyMetrics = timelineData.daily_metrics || {};
            cumulativeMetrics = timelineData.cumulative_metrics || {};
            userTimelines = timelineData.user_timelines || {};
            userDailyMetrics = timelineData.user_daily_metrics || {};
        } else if (data.data.daily_metrics && typeof data.data.daily_metrics === 'object' && data.data.daily_metrics.dates) {
            // Legacy format: daily_metrics contains the entire legacy result
            timelineData = data.data.daily_metrics;
            dates = timelineData.dates || [];
            // Convert legacy format to expected structure
            if (timelineData.event_rows) {
                // Transform legacy event_rows format to daily_metrics format
                dates.forEach(date => {
                    dailyMetrics[date] = {};
                    Object.keys(timelineData.event_rows).forEach(metric => {
                        dailyMetrics[date][metric] = timelineData.event_rows[metric][date] || 0;
                    });
                });
            }
            // Legacy format doesn't have user daily metrics
            userDailyMetrics = {};
        } else if (data.data.daily_metrics) {
            // New format: daily_metrics is a direct mapping
            timelineData = data.data;
            dates = timelineData.dates || [];
            dailyMetrics = timelineData.daily_metrics || {};
            cumulativeMetrics = timelineData.cumulative_metrics || {};
            userTimelines = timelineData.user_timelines || {};
            userDailyMetrics = timelineData.user_daily_metrics || {};
        }
    }

    // Extract available users and products for filtering
    const { availableUsers, availableProducts } = useMemo(() => {
        const users = new Set();
        const products = new Set();
        
        // PRIORITY 1: Extract from user daily metrics (most accurate)
        Object.keys(userDailyMetrics).forEach(userId => {
            users.add(userId);
        });
        
        // PRIORITY 2: Extract from user timelines if available
        Object.keys(userTimelines).forEach(userId => {
            users.add(userId);
            const userTimeline = userTimelines[userId];
            if (userTimeline.events) {
                userTimeline.events.forEach(event => {
                    if (event.product_id) {
                        products.add(event.product_id);
                    }
                });
            }
            if (userTimeline.summary?.product_summaries) {
                Object.keys(userTimeline.summary.product_summaries).forEach(productId => {
                    products.add(productId);
                });
            }
        });
        
        // PRIORITY 3: Extract from daily metrics breakdown if available
        Object.values(dailyMetrics).forEach(dayData => {
            if (dayData.estimated_revenue_breakdown?.user_breakdowns) {
                Object.keys(dayData.estimated_revenue_breakdown.user_breakdowns).forEach(userId => {
                    users.add(userId);
                    const userBreakdown = dayData.estimated_revenue_breakdown.user_breakdowns[userId];
                    if (userBreakdown.product_breakdowns) {
                        Object.keys(userBreakdown.product_breakdowns).forEach(productId => {
                            products.add(productId);
                        });
                    }
                });
            }
        });
        
        return {
            availableUsers: Array.from(users).sort(),
            availableProducts: Array.from(products).sort()
        };
    }, [userDailyMetrics, userTimelines, dailyMetrics]);

    // Filter data based on selected user/product
    const filteredData = useMemo(() => {
        if (!selectedUserId && !selectedProductId) {
            return { dailyMetrics, cumulativeMetrics };
        }

        // CRITICAL: Use backend's pre-calculated user daily metrics directly
        // No business logic recalculation in frontend - pure display layer
        
        const filteredDaily = {};
        const filteredCumulative = {};

        if (selectedUserId && !selectedProductId) {
            // Filter by user only - use backend's user daily metrics directly
            const userMetrics = userDailyMetrics[selectedUserId];
            
            if (userMetrics) {
                // CRITICAL FIX: Calculate cumulative values for single user view
                // Backend provides daily user metrics, but we need to calculate cumulative on frontend
                let cumulativeValues = {
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
                
                dates.forEach(date => {
                    const userDayMetrics = userMetrics[date];
                    if (userDayMetrics) {
                        // Use backend's pre-calculated daily metrics directly
                        filteredDaily[date] = { 
                            ...userDayMetrics,
                            // CRITICAL FIX: Calculate revenue_net since backend doesn't provide it for individual users
                            revenue_net: (userDayMetrics.revenue || 0) - (userDayMetrics.refund || 0)
                        };
                        
                        // Calculate cumulative values by summing daily values
                        cumulativeValues.trial_started += userDayMetrics.trial_started || 0;
                        cumulativeValues.trial_ended += userDayMetrics.trial_ended || 0;
                        cumulativeValues.trial_converted += userDayMetrics.trial_converted || 0;
                        cumulativeValues.trial_cancelled += userDayMetrics.trial_cancelled || 0;
                        cumulativeValues.initial_purchase += userDayMetrics.initial_purchase || 0;
                        cumulativeValues.subscription_cancelled += userDayMetrics.subscription_cancelled || 0;
                        cumulativeValues.revenue += userDayMetrics.revenue || 0;
                        cumulativeValues.refund += userDayMetrics.refund || 0;
                        cumulativeValues.refund_count += userDayMetrics.refund_count || 0;
                        
                        // Create cumulative metrics with proper field names
                        filteredCumulative[date] = {
                            // Copy daily values for state-based metrics (these don't accumulate)
                            trial_pending: userDayMetrics.trial_pending || 0,
                            subscription_active: userDayMetrics.subscription_active || 0,
                            estimated_revenue: userDayMetrics.estimated_revenue || 0,
                            
                            // Add cumulative values with proper field names expected by getCumulativeValue functions
                            cumulative_trial_started: cumulativeValues.trial_started,
                            cumulative_trial_ended: cumulativeValues.trial_ended,
                            cumulative_trial_converted: cumulativeValues.trial_converted,
                            cumulative_trial_cancelled: cumulativeValues.trial_cancelled,
                            cumulative_initial_purchase: cumulativeValues.initial_purchase,
                            cumulative_subscription_cancelled: cumulativeValues.subscription_cancelled,
                            cumulative_revenue: cumulativeValues.revenue,
                            cumulative_refund: cumulativeValues.refund,
                            cumulative_refund_count: cumulativeValues.refund_count,
                            cumulative_revenue_net: cumulativeValues.revenue - cumulativeValues.refund
                        };
                    } else {
                        // No data for this user on this date
                        filteredDaily[date] = {
                            trial_started: 0,
                            trial_pending: 0,
                            trial_ended: 0,
                            trial_converted: 0,
                            trial_cancelled: 0,
                            initial_purchase: 0,
                            subscription_active: 0,
                            subscription_cancelled: 0,
                            revenue: 0,
                            refund: 0,
                            refund_count: 0,
                            estimated_revenue: 0,
                            revenue_net: 0
                        };
                        
                        // For cumulative, maintain previous cumulative values (no change on days with no activity)
                        filteredCumulative[date] = {
                            // State-based metrics remain 0
                            trial_pending: 0,
                            subscription_active: 0,
                            estimated_revenue: 0,
                            
                            // Cumulative values remain at previous levels
                            cumulative_trial_started: cumulativeValues.trial_started,
                            cumulative_trial_ended: cumulativeValues.trial_ended,
                            cumulative_trial_converted: cumulativeValues.trial_converted,
                            cumulative_trial_cancelled: cumulativeValues.trial_cancelled,
                            cumulative_initial_purchase: cumulativeValues.initial_purchase,
                            cumulative_subscription_cancelled: cumulativeValues.subscription_cancelled,
                            cumulative_revenue: cumulativeValues.revenue,
                            cumulative_refund: cumulativeValues.refund,
                            cumulative_refund_count: cumulativeValues.refund_count,
                            cumulative_revenue_net: cumulativeValues.revenue - cumulativeValues.refund
                        };
                    }
                });
            } else {
                // User not found in user daily metrics
                dates.forEach(date => {
                    filteredDaily[date] = {
                        trial_started: 0,
                        trial_pending: 0,
                        trial_ended: 0,
                        trial_converted: 0,
                        trial_cancelled: 0,
                        initial_purchase: 0,
                        subscription_active: 0,
                        subscription_cancelled: 0,
                        revenue: 0,
                        refund: 0,
                        refund_count: 0,
                        estimated_revenue: 0,
                        revenue_net: 0
                    };
                    
                    filteredCumulative[date] = {
                        trial_pending: 0,
                        subscription_active: 0,
                        estimated_revenue: 0,
                        cumulative_trial_started: 0,
                        cumulative_trial_ended: 0,
                        cumulative_trial_converted: 0,
                        cumulative_trial_cancelled: 0,
                        cumulative_initial_purchase: 0,
                        cumulative_subscription_cancelled: 0,
                        cumulative_revenue: 0,
                        cumulative_refund: 0,
                        cumulative_refund_count: 0,
                        cumulative_revenue_net: 0
                    };
                });
            }
        } else if (selectedProductId && !selectedUserId) {
            // Filter by product only - aggregate all users for this product
            // TODO: Backend should provide product-specific aggregations directly
            // For now, show original aggregate data with a note
            dates.forEach(date => {
                filteredDaily[date] = { ...dailyMetrics[date] };
                filteredCumulative[date] = { ...cumulativeMetrics[date] };
            });
        } else if (selectedUserId && selectedProductId) {
            // Filter by both user and product
            // TODO: Backend should provide user-product specific metrics
            // For now, use user metrics (which aggregate across all products for that user)
            const userMetrics = userDailyMetrics[selectedUserId];
            
            if (userMetrics) {
                // CRITICAL FIX: Calculate cumulative values for single user view (same logic as user-only)
                let cumulativeValues = {
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
                
                dates.forEach(date => {
                    const userDayMetrics = userMetrics[date];
                    if (userDayMetrics) {
                        // Use backend's pre-calculated metrics directly
                        // Note: This shows all products for the user, not just the selected product
                        filteredDaily[date] = { 
                            ...userDayMetrics,
                            // CRITICAL FIX: Calculate revenue_net since backend doesn't provide it for individual users
                            revenue_net: (userDayMetrics.revenue || 0) - (userDayMetrics.refund || 0)
                        };
                        
                        // Calculate cumulative values by summing daily values
                        cumulativeValues.trial_started += userDayMetrics.trial_started || 0;
                        cumulativeValues.trial_ended += userDayMetrics.trial_ended || 0;
                        cumulativeValues.trial_converted += userDayMetrics.trial_converted || 0;
                        cumulativeValues.trial_cancelled += userDayMetrics.trial_cancelled || 0;
                        cumulativeValues.initial_purchase += userDayMetrics.initial_purchase || 0;
                        cumulativeValues.subscription_cancelled += userDayMetrics.subscription_cancelled || 0;
                        cumulativeValues.revenue += userDayMetrics.revenue || 0;
                        cumulativeValues.refund += userDayMetrics.refund || 0;
                        cumulativeValues.refund_count += userDayMetrics.refund_count || 0;
                        
                        // Create cumulative metrics with proper field names
                        filteredCumulative[date] = {
                            // Copy daily values for state-based metrics (these don't accumulate)
                            trial_pending: userDayMetrics.trial_pending || 0,
                            subscription_active: userDayMetrics.subscription_active || 0,
                            estimated_revenue: userDayMetrics.estimated_revenue || 0,
                            
                            // Add cumulative values with proper field names expected by getCumulativeValue functions
                            cumulative_trial_started: cumulativeValues.trial_started,
                            cumulative_trial_ended: cumulativeValues.trial_ended,
                            cumulative_trial_converted: cumulativeValues.trial_converted,
                            cumulative_trial_cancelled: cumulativeValues.trial_cancelled,
                            cumulative_initial_purchase: cumulativeValues.initial_purchase,
                            cumulative_subscription_cancelled: cumulativeValues.subscription_cancelled,
                            cumulative_revenue: cumulativeValues.revenue,
                            cumulative_refund: cumulativeValues.refund,
                            cumulative_refund_count: cumulativeValues.refund_count,
                            cumulative_revenue_net: cumulativeValues.revenue - cumulativeValues.refund
                        };
                    } else {
                        filteredDaily[date] = {
                            trial_started: 0,
                            trial_pending: 0,
                            trial_ended: 0,
                            trial_converted: 0,
                            trial_cancelled: 0,
                            initial_purchase: 0,
                            subscription_active: 0,
                            subscription_cancelled: 0,
                            revenue: 0,
                            refund: 0,
                            refund_count: 0,
                            estimated_revenue: 0,
                            revenue_net: 0
                        };
                        
                        // For cumulative, maintain previous cumulative values (no change on days with no activity)
                        filteredCumulative[date] = {
                            // State-based metrics remain 0
                            trial_pending: 0,
                            subscription_active: 0,
                            estimated_revenue: 0,
                            
                            // Cumulative values remain at previous levels
                            cumulative_trial_started: cumulativeValues.trial_started,
                            cumulative_trial_ended: cumulativeValues.trial_ended,
                            cumulative_trial_converted: cumulativeValues.trial_converted,
                            cumulative_trial_cancelled: cumulativeValues.trial_cancelled,
                            cumulative_initial_purchase: cumulativeValues.initial_purchase,
                            cumulative_subscription_cancelled: cumulativeValues.subscription_cancelled,
                            cumulative_revenue: cumulativeValues.revenue,
                            cumulative_refund: cumulativeValues.refund,
                            cumulative_refund_count: cumulativeValues.refund_count,
                            cumulative_revenue_net: cumulativeValues.revenue - cumulativeValues.refund
                        };
                    }
                });
            } else {
                dates.forEach(date => {
                    filteredDaily[date] = {
                        trial_started: 0,
                        trial_pending: 0,
                        trial_ended: 0,
                        trial_converted: 0,
                        trial_cancelled: 0,
                        initial_purchase: 0,
                        subscription_active: 0,
                        subscription_cancelled: 0,
                        revenue: 0,
                        refund: 0,
                        refund_count: 0,
                        estimated_revenue: 0,
                        revenue_net: 0
                    };
                    
                    filteredCumulative[date] = {
                        trial_pending: 0,
                        subscription_active: 0,
                        estimated_revenue: 0,
                        cumulative_trial_started: 0,
                        cumulative_trial_ended: 0,
                        cumulative_trial_converted: 0,
                        cumulative_trial_cancelled: 0,
                        cumulative_initial_purchase: 0,
                        cumulative_subscription_cancelled: 0,
                        cumulative_revenue: 0,
                        cumulative_refund: 0,
                        cumulative_refund_count: 0,
                        cumulative_revenue_net: 0
                    };
                });
            }
        }

        return { dailyMetrics: filteredDaily, cumulativeMetrics: filteredCumulative };
    }, [selectedUserId, selectedProductId, dailyMetrics, cumulativeMetrics, userDailyMetrics, dates]);

    // NOW we can do early returns after all hooks are called
    if (!hasValidData) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No timeline data available. Run an analysis to see results.
                </p>
            </div>
        );
    }

    const formatCurrency = (value) => {
        if (value === null || value === undefined) return '$0';
        const rounded = Math.round(Number(value));
        return `$${rounded.toLocaleString()}`;
    };

    const formatNumber = (value) => {
        if (value === null || value === undefined) return '0';
        return Number(value).toLocaleString();
    };

    // Function to generate tooltip data for estimated revenue
    const getEstimatedRevenueTooltipData = (date, value) => {
        // CRITICAL: Only show tooltip for individual user view, NEVER for aggregate
        if (!selectedUserId) {
            return null;
        }
        
        // Add safety check for value parameter
        const safeValue = typeof value === 'number' ? value : 0;
        
        if (safeValue === 0) {
            return null; // Don't show tooltip for $0 values
        }
        
        console.log(`[DEBUG] getEstimatedRevenueTooltipData called with:`, {
            date,
            originalValue: value,
            safeValue,
            valueType: typeof value,
            selectedUserId,
            selectedProductId
        });
        
        // SIMPLIFIED: Use pre-computed tooltip data from backend
        // No more business logic calculations on frontend - just display what backend sends
        
        // Get the user's daily metrics for this date
        const userMetrics = userDailyMetrics[selectedUserId];
        if (!userMetrics || !userMetrics[date]) {
            console.warn(`[WARN] No user metrics found for user ${selectedUserId} on date ${date}`);
            return null;
        }
        
        const dayMetrics = userMetrics[date];
        const tooltipData = dayMetrics.tooltip_data;
        
        console.log(`[DEBUG] Backend tooltip data for ${selectedUserId} on ${date}:`, {
            hasTooltipData: !!tooltipData,
            tooltipData: tooltipData,
            dayMetrics: dayMetrics
        });
        
        if (!tooltipData) {
            console.warn(`[WARN] No tooltip data found for user ${selectedUserId} on date ${date}`);
            return null;
        }
        
        // Return the pre-computed tooltip data from backend
        return {
            value: safeValue,
            breakdown: tooltipData.breakdown,
            result: `$${safeValue.toFixed(2)}`
        };
    };

    // Define all metrics with their categories and data extraction logic
    const metrics = [
        // Daily Absolute Values
        {
            key: 'daily_trial_started',
            label: 'Trial Started',
            category: 'daily_absolute',
            categoryLabel: 'Daily Absolute Values',
            getValue: (date) => filteredData.dailyMetrics[date]?.trial_started || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_trial_started || 0,
            format: formatNumber
        },
        {
            key: 'daily_trial_pending',
            label: 'Trial Pending',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.trial_pending || 0,
            // CRITICAL FIX: Trial pending is a state-based metric, cumulative should show same as daily
            getCumulativeValue: (date) => filteredData.dailyMetrics[date]?.trial_pending || 0,
            format: formatNumber
        },
        {
            key: 'daily_trial_ended',
            label: 'Trial Ended',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.trial_ended || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_trial_ended || 0,
            format: formatNumber
        },
        {
            key: 'daily_trial_cancelled',
            label: 'Trial Cancelled',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.trial_cancelled || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_trial_cancelled || 0,
            format: formatNumber
        },
        {
            key: 'daily_trial_converted',
            label: 'Trial Converted',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.trial_converted || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_trial_converted || 0,
            format: formatNumber
        },
        {
            key: 'daily_initial_purchase',
            label: 'Initial Purchase',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.initial_purchase || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_initial_purchase || 0,
            format: formatNumber
        },
        {
            key: 'daily_subscription_active',
            label: 'Subscription Active',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.subscription_active || 0,
            // CRITICAL FIX: Subscription active is a state-based metric, cumulative should show same as daily
            getCumulativeValue: (date) => filteredData.dailyMetrics[date]?.subscription_active || 0,
            format: formatNumber
        },
        {
            key: 'daily_subscription_cancelled',
            label: 'Subscription Cancelled',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.subscription_cancelled || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_subscription_cancelled || 0,
            format: formatNumber
        },
        {
            key: 'daily_users',
            label: 'Daily Users',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.daily_users || 0,
            getCumulativeValue: (date) => filteredData.dailyMetrics[date]?.cumulative_users || 0,
            format: formatNumber
        },
        {
            key: 'daily_refund_count',
            label: 'Refunds',
            category: 'daily_absolute',
            getValue: (date) => filteredData.dailyMetrics[date]?.refund_count || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_refund_count || 0,
            format: formatNumber
        },
        // Revenue Values
        {
            key: 'daily_revenue',
            label: 'Revenue',
            category: 'revenue',
            categoryLabel: 'Revenue Values',
            getValue: (date) => filteredData.dailyMetrics[date]?.revenue || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_revenue || 0,
            format: formatCurrency
        },
        {
            key: 'daily_refund',
            label: 'Refunds',
            category: 'revenue',
            getValue: (date) => filteredData.dailyMetrics[date]?.refund || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_refund || 0,
            format: formatCurrency
        },
        {
            key: 'daily_revenue_net',
            label: 'Net Revenue',
            category: 'revenue',
            getValue: (date) => filteredData.dailyMetrics[date]?.revenue_net || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_revenue_net || 0,
            format: formatCurrency
        },
        {
            key: 'daily_estimated_revenue',
            label: 'Estimated Revenue',
            category: 'revenue',
            isEstimatedRevenue: true,
            getValue: (date) => filteredData.dailyMetrics[date]?.estimated_revenue || 0,
            // CRITICAL FIX: Estimated revenue is a state-based metric, cumulative should show same as daily
            getCumulativeValue: (date) => filteredData.dailyMetrics[date]?.estimated_revenue || 0,
            format: formatCurrency,
            getTooltipData: (date) => {
                const value = showCumulative 
                    ? (filteredData.dailyMetrics[date]?.estimated_revenue || 0)  // Same as daily for state-based
                    : (filteredData.dailyMetrics[date]?.estimated_revenue || 0);
                return getEstimatedRevenueTooltipData(date, value);
            }
        }
    ];

    // Group metrics by category
    const groupedMetrics = metrics.reduce((acc, metric) => {
        if (!acc[metric.category]) {
            acc[metric.category] = {
                label: metric.categoryLabel || metric.category,
                metrics: []
            };
        }
        acc[metric.category].metrics.push(metric);
        return acc;
    }, {});

    return (
        <>
            <div className="mb-8 bg-white dark:bg-gray-700 p-6 rounded-lg shadow-md">
                <div className="flex justify-between items-center mb-4">
                    <h2 className="text-2xl font-bold text-gray-800 dark:text-white">Daily Event & Revenue Timeline</h2>
                    <div className="flex gap-2">
                        <button
                            onClick={() => setShowCumulative(!showCumulative)}
                            className={`px-3 py-1 text-sm rounded-full ${
                                showCumulative 
                                    ? 'bg-blue-500 text-white' 
                                    : 'bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-white'
                            }`}
                        >
                            {showCumulative ? 'Show Daily' : 'Show Cumulative'}
                        </button>
                    </div>
                </div>
                
                {/* Table Container */}
                <div className="overflow-x-auto">
                    <table className="min-w-full text-sm border dark:border-gray-600">
                        <thead className="bg-gray-50 dark:bg-gray-700">
                            <tr>
                                <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white sticky left-0 bg-gray-50 dark:bg-gray-700 z-10 min-w-[200px]">
                                    Metric
                                </th>
                                {dates.map(date => (
                                    <th key={date} className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white whitespace-nowrap min-w-[80px]">
                                        {date}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody className="bg-white dark:bg-gray-800">
                            {Object.entries(groupedMetrics).map(([categoryKey, category]) => (
                                <React.Fragment key={categoryKey}>
                                    {/* Category Header */}
                                    <tr className="bg-gray-100 dark:bg-gray-600">
                                        <td colSpan={dates.length + 1} className="p-2 font-semibold text-gray-800 dark:text-white border-b dark:border-gray-600">
                                            {category.label}
                                        </td>
                                    </tr>
                                    {/* Metrics in this category */}
                                    {category.metrics.map((metric) => (
                                        <tr key={metric.key} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-white sticky left-0 bg-white dark:bg-gray-800 z-10">
                                                {metric.label}
                                            </td>
                                            {dates.map(date => {
                                                const value = showCumulative 
                                                    ? metric.getCumulativeValue(date)
                                                    : metric.getValue(date);
                                                return (
                                                    <td 
                                                        key={date} 
                                                        className={`p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white ${
                                                            metric.isEstimatedRevenue && selectedUserId ? 'cursor-help hover:bg-yellow-50 dark:hover:bg-yellow-900/20 relative' : ''
                                                        }`}
                                                        onMouseEnter={metric.isEstimatedRevenue && selectedUserId ? (e) => {
                                                            // Only show tooltip for single user views
                                                            if (!selectedUserId) return;
                                                            
                                                            // Clear any existing timeout
                                                            if (tooltipTimeoutRef.current) {
                                                                clearTimeout(tooltipTimeoutRef.current);
                                                                tooltipTimeoutRef.current = null;
                                                            }
                                                            
                                                            // Small delay to prevent tooltip from appearing on quick mouse movements
                                                            const newTimeout = setTimeout(() => {
                                                                const rect = e.target.getBoundingClientRect();
                                                                const tooltipData = getEstimatedRevenueTooltipData(date, value);
                                                                
                                                                if (tooltipData) {
                                                                    setTooltipData(tooltipData);
                                                                    
                                                                    // Smart positioning: check available space
                                                                    const tooltipWidth = 480; // Estimated tooltip width
                                                                    const tooltipHeight = 480; // Estimated tooltip height
                                                                    const spaceOnRight = window.innerWidth - rect.right;
                                                                    const spaceOnLeft = rect.left;
                                                                    
                                                                    let xPosition, yPosition;
                                                                    
                                                                    // Choose horizontal position based on available space
                                                                    if (spaceOnRight >= tooltipWidth + 20) {
                                                                        // Enough space on the right
                                                                        xPosition = rect.right + 10;
                                                                    } else if (spaceOnLeft >= tooltipWidth + 20) {
                                                                        // Not enough space on right, but enough on left
                                                                        xPosition = rect.left - tooltipWidth - 10;
                                                                    } else {
                                                                        // Not enough space on either side, center it
                                                                        xPosition = Math.max(10, (window.innerWidth - tooltipWidth) / 2);
                                                                    }
                                                                    
                                                                    // Vertical position: position so BOTTOM of tooltip aligns with cell center
                                                                    const cellCenterY = rect.top + (rect.height / 2);
                                                                    yPosition = Math.max(10, cellCenterY - tooltipHeight); // Subtract tooltip height
                                                                    
                                                                    // Ensure tooltip doesn't go below screen
                                                                    yPosition = Math.min(yPosition, window.innerHeight - tooltipHeight - 10);
                                                                    
                                                                    setTooltipPosition({ 
                                                                        x: xPosition,
                                                                        y: yPosition
                                                                    });
                                                                }
                                                            }, 150); // Shorter delay for better responsiveness
                                                            
                                                            tooltipTimeoutRef.current = newTimeout;
                                                        } : undefined}
                                                        onMouseLeave={metric.isEstimatedRevenue && selectedUserId ? () => {
                                                            // Only handle mouse leave for single user views
                                                            if (!selectedUserId) return;
                                                            
                                                            // Clear the show timeout if mouse leaves before tooltip appears
                                                            if (tooltipTimeoutRef.current) {
                                                                clearTimeout(tooltipTimeoutRef.current);
                                                                tooltipTimeoutRef.current = null;
                                                            }
                                                            
                                                            // Set a timeout to hide the tooltip, allowing time to move to tooltip
                                                            const hideTimeout = setTimeout(() => {
                                                                setTooltipData(null);
                                                            }, 100); // Shorter delay for quicker hiding
                                                            
                                                            tooltipTimeoutRef.current = hideTimeout;
                                                        } : undefined}
                                                    >
                                                        {metric.format(value)}
                                                    </td>
                                                );
                                            })}
                                        </tr>
                                    ))}
                                </React.Fragment>
                            ))}
                        </tbody>
                    </table>
                </div>

                {dates.length === 0 && (
                    <div className="text-center py-8">
                        <p className="text-gray-500 dark:text-gray-400">
                            No timeline data available. Run an analysis to see results.
                        </p>
                    </div>
                )}
            </div>
            
            {/* Estimated Revenue Tooltip */}
            <EstimatedRevenueTooltip
                data={tooltipData}
                position={tooltipPosition}
                onClose={() => setTooltipData(null)}
                selectedUserId={selectedUserId}
                selectedProductId={selectedProductId}
            />
        </>
    );
};

export default EventTimelineTable; 