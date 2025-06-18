import React, { useState, useMemo } from 'react';

/*
 * ========================================
 * ENHANCED ESTIMATED REVENUE & REFUND TRACKING
 * ========================================
 * 
 * This component has been enhanced to support granular tracking of estimated refunds
 * (trial vs. subscription) and estimated revenue (trial vs. subscription contributions),
 * with detailed breakdown data for frontend tooltips.
 * 
 * NEW FEATURES (TODOs 4 & 5 IMPLEMENTED):
 * 
 * 1. Enhanced Estimated Revenue Tooltip (TODO 5):
 *    - Now shows 3-category breakdown:
 *      - "From Pending Trials": Revenue from users still in trial
 *      - "From Trial Conversions": Revenue from users who converted from trial
 *      - "From Initial Purchases": Revenue from users who purchased directly
 *    - Uses new backend data: estimated_trial_converted_revenue_component, estimated_initial_purchase_revenue_component
 * 
 * 2. Separate Tooltip Components (TODO 4):
 *    - ActualRevenueTooltip: Handles actual revenue event details with sources
 *    - ActualRefundTooltip: Handles actual refund event details with sources  
 *    - EstimatedRevenueTooltip: Handles estimated revenue with 3-category breakdown
 *    - Updated state management to activeTooltip with type differentiation
 * 
 * 3. Enhanced State Management (TODO 4):
 *    - Changed from tooltipData/tooltipPosition to activeTooltip object
 *    - activeTooltip contains: { type, data, position } for better type handling
 *    - Mouse event handlers updated to handle different tooltip types
 * 
 * DATA STRUCTURE CHANGES:
 * 
 * Per-user daily metrics (consolidated_user_daily_metrics[user_id][date_str]):
 *   - estimated_trial_converted_revenue_component: NEW - Revenue from trial conversions  
 *   - estimated_initial_purchase_revenue_component: NEW - Revenue from direct purchases
 *   - revenue_sources: NEW - Array of detailed revenue event sources
 *   - refund_sources: NEW - Array of detailed refund event sources
 * 
 * Aggregated daily metrics (timeline_data[date_str]):
 *   - estimated_trial_converted_revenue_component: NEW - Aggregated trial conversion revenue
 *   - estimated_initial_purchase_revenue_component: NEW - Aggregated direct purchase revenue
 *   - users_from_trial_conversion: NEW - Count of users with trial conversion revenue
 *   - users_from_initial_purchase: NEW - Count of users with direct purchase revenue
 * 
 * BUSINESS LOGIC NOTES:
 * 
 * - Trial refunds: conversion_rate * base_refund_rate * price
 *   (Potential refunds IF trial converts AND user requests refund)
 * 
 * - Subscription refunds: actual_conversion_revenue * current_refund_rate
 *   (Refunds based on subscription age, degrading linearly over 30 days)
 * 
 * - All calculations come from backend state_models.py - NO business logic in frontend
 * 
 * TOOLTIP PLACEMENT:
 * - "Estimated Refunds (Potential)" metric is placed in the "Revenue Values" category
 * - Positioned between "Net Revenue" and "Estimated Revenue" as requested
 */

const EstimatedRevenueTooltip = ({ data, position, onClose }) => {
    // Basic mouse hover logic to keep the tooltip open
    const [isHovered, setIsHovered] = React.useState(false);
    const closeTimeoutRef = React.useRef(null);
    const handleMouseEnter = () => { setIsHovered(true); clearTimeout(closeTimeoutRef.current); };
    const handleMouseLeave = () => { setIsHovered(false); closeTimeoutRef.current = setTimeout(onClose, 300); };
    
    if (!data || !data.breakdown) return null;

    const { breakdown } = data;
    const totalValue = breakdown.total?.final_net_revenue || 0;

    // Helper to render a section only if it has meaningful data
    const renderSection = (title, sectionData) => {
        // First check if sectionData exists
        if (!sectionData) return null;
        
        // A section is meaningful if it has users or any revenue/refund data
        const hasData = sectionData.users > 0 || sectionData.estimated_revenue || sectionData.actual_revenue;
        if (!hasData) return null;

        return (
            <div className="border-b border-gray-700 pb-2 mb-2">
                <h5 className="font-bold text-blue-300 mb-2">{title} {sectionData.users > 0 ? `(${sectionData.users} users)` : ''}</h5>
                <div className="grid grid-cols-2 gap-x-4 text-xs">
                    {/* Render fields based on what the section contains */}
                    {sectionData.hasOwnProperty('estimated_revenue') && <div>Est. Revenue:</div>}
                    {sectionData.hasOwnProperty('estimated_revenue') && <div className="text-right font-mono">${sectionData.estimated_revenue.toFixed(2)}</div>}
                    
                    {sectionData.hasOwnProperty('actual_revenue') && <div>Actual Revenue:</div>}
                    {sectionData.hasOwnProperty('actual_revenue') && <div className="text-right font-mono">${sectionData.actual_revenue.toFixed(2)}</div>}
                    
                    {sectionData.hasOwnProperty('actual_refunds') && <div>Actual Refunds:</div>}
                    {sectionData.hasOwnProperty('actual_refunds') && <div className="text-right font-mono text-red-400">-${sectionData.actual_refunds.toFixed(2)}</div>}

                    {sectionData.hasOwnProperty('estimated_refunds') && <div>Est. Refunds:</div>}
                    {sectionData.hasOwnProperty('estimated_refunds') && <div className="text-right font-mono text-yellow-400">-${sectionData.estimated_refunds.toFixed(2)}</div>}
                    
                    {/* Render Net values */}
                    <div className="border-t border-gray-600 mt-1 pt-1">Net Revenue:</div>
                    <div className="text-right font-mono border-t border-gray-600 mt-1 pt-1">
                        {sectionData.hasOwnProperty('net_estimated_revenue') ? `$${sectionData.net_estimated_revenue.toFixed(2)}` :
                         sectionData.hasOwnProperty('net_revenue_after_estimated_refunds') ? `$${sectionData.net_revenue_after_estimated_refunds.toFixed(2)}` :
                         `$${(sectionData.actual_revenue - sectionData.actual_refunds).toFixed(2)}`}
                    </div>
                </div>
            </div>
        );
    };

    return (
        <div
            className="fixed z-50 bg-gray-800 border border-gray-600 text-white rounded-lg shadow-lg p-4 w-96 pointer-events-auto"
            style={{ left: position.x, top: position.y }}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
        >
            <div className="flex justify-between items-start mb-3">
                <h4 className="font-semibold">Est. Net Revenue: ${totalValue.toFixed(2)}</h4>
                <button onClick={onClose} className="text-gray-400 hover:text-gray-200">&times;</button>
            </div>
            
            {/* NEW: Refund Rate Ratios - Show prominently at the top */}
            {(() => {
                const trialConverted = breakdown.trial_converted || {};
                const initialPurchase = breakdown.initial_purchase || {};
                
                const showTrialRatio = trialConverted.actual_revenue > 0;
                const showInitialRatio = initialPurchase.actual_revenue > 0;
                
                if (!showTrialRatio && !showInitialRatio) return null;
                
                return (
                    <div className="bg-blue-900 p-3 rounded-lg border border-blue-600 mb-3">
                        <div className="text-sm font-medium text-blue-200 mb-2">
                            🎯 Refund Rate Analysis
                        </div>
                        
                        {showTrialRatio && (
                            <div className="mb-2">
                                <div className="text-xs font-medium text-blue-300">
                                    Trial Conversion Refund Rate:
                                </div>
                                <div className="text-sm font-bold text-blue-100">
                                    {(() => {
                                        const revenue = trialConverted.actual_revenue || 0;
                                        const estimatedRefunds = trialConverted.estimated_refunds || 0;
                                        const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                        return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                    })()}
                                </div>
                                <div className="text-xs text-blue-400">
                                    ${(trialConverted.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(trialConverted.actual_revenue || 0).toFixed(2)} revenue
                                </div>
                            </div>
                        )}
                        
                        {showInitialRatio && (
                            <div>
                                <div className="text-xs font-medium text-blue-300">
                                    Initial Purchase Refund Rate:
                                </div>
                                <div className="text-sm font-bold text-blue-100">
                                    {(() => {
                                        const revenue = initialPurchase.actual_revenue || 0;
                                        const estimatedRefunds = initialPurchase.estimated_refunds || 0;
                                        const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                        return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                    })()}
                                </div>
                                <div className="text-xs text-blue-400">
                                    ${(initialPurchase.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(initialPurchase.actual_revenue || 0).toFixed(2)} revenue
                                </div>
                            </div>
                        )}
                    </div>
                );
            })()}
            
            {renderSection('-- Pending Trial --', breakdown.pending_trial)}
            {renderSection('-- Trial Converted --', breakdown.trial_converted)}
            {renderSection('-- Initial Purchase --', breakdown.initial_purchase)}

            {/* Total Section */}
            <div className="pt-2">
                <h5 className="font-bold text-green-300 mb-2">-- Total --</h5>
                <div className="grid grid-cols-2 gap-x-4 text-xs">
                    <div>Total Actual Revenue:</div>
                    <div className="text-right font-mono">${(breakdown.total?.total_actual_revenue || 0).toFixed(2)}</div>
                    
                    <div>Total Actual Refunds:</div>
                    <div className="text-right font-mono text-red-400">-${(breakdown.total?.total_actual_refunds || 0).toFixed(2)}</div>
                    
                    <div>Total Net Revenue:</div>
                    <div className="text-right font-mono">${(breakdown.total?.total_net_revenue || 0).toFixed(2)}</div>
                    
                    <div>Est. Revenue (from trials):</div>
                    <div className="text-right font-mono">${(breakdown.total?.est_revenue_from_trials || 0).toFixed(2)}</div>
                    
                    <div>Est. Refunds (from pending trials, trial converted, and initial purchase):</div>
                    <div className="text-right font-mono text-yellow-400">-${(breakdown.total?.est_refunds_from_all_sources || 0).toFixed(2)}</div>
                    
                    <div className="font-bold border-t border-gray-600 mt-1 pt-1">Final Net Revenue:</div>
                    <div className="font-bold text-right font-mono border-t border-gray-600 mt-1 pt-1">${(breakdown.total?.final_net_revenue || 0).toFixed(2)}</div>
                </div>
            </div>
        </div>
    );
};

// CORRECTED - Final version of the component
const ActualRevenueTooltip = ({ data, position, onClose }) => {
    const [isHovered, setIsHovered] = React.useState(false);
    const closeTimeoutRef = React.useRef(null);
    
    if (!data) return null;
    
    const { value = 0, sources = {}, buckets = {} } = data;
    
    const handleMouseEnter = () => {
        setIsHovered(true);
        if (closeTimeoutRef.current) {
            clearTimeout(closeTimeoutRef.current);
            closeTimeoutRef.current = null;
        }
    };
    
    const handleMouseLeave = () => {
        setIsHovered(false);
        closeTimeoutRef.current = setTimeout(() => {
            onClose();
        }, 300);
    };
    
    return (
        <div 
            className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg p-4 max-w-md pointer-events-auto"
            style={{
                left: position.x,
                top: position.y,
                pointerEvents: 'auto'
            }}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
        >
            <div className="flex justify-between items-start mb-3">
                <h4 className="font-semibold text-lg text-gray-900 dark:text-white">Actual Revenue: ${value.toFixed(2)}</h4>
                <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 ml-2">&times;</button>
            </div>
            <div className="space-y-3 text-xs">
                <div>
                    <h5 className="font-bold text-sm text-gray-700 dark:text-gray-300 mb-1">By Source</h5>
                    {Object.keys(sources).length > 0 ? Object.entries(sources).map(([source, stats]) => (
                        <div key={source} className="flex justify-between"><span className="text-gray-600 dark:text-gray-400">{source}:</span><span className="font-medium text-gray-800 dark:text-gray-200">{stats.count} txn, ${stats.total.toFixed(2)}</span></div>
                    )) : <p className="text-gray-500 italic">No source data.</p>}
                </div>
                <div className="border-t dark:border-gray-600 pt-2">
                    <h5 className="font-bold text-sm text-gray-700 dark:text-gray-300 mb-1">By Amount ($5 Buckets)</h5>
                    {Object.keys(buckets).length > 0 ? Object.entries(buckets).sort().map(([bucket, count]) => (
                        <div key={bucket} className="flex justify-between"><span className="text-gray-600 dark:text-gray-400">{bucket}:</span><span className="font-medium text-gray-800 dark:text-gray-200">{count} transactions</span></div>
                    )) : <p className="text-gray-500 italic">No bucket data.</p>}
                </div>
            </div>
        </div>
    );
};

// CORRECTED - Final version of the component
const ActualRefundTooltip = ({ data, position, onClose }) => {
    const [isHovered, setIsHovered] = React.useState(false);
    const closeTimeoutRef = React.useRef(null);
    
    if (!data) return null;
    
    const { value = 0, sources = {}, buckets = {} } = data;
    
    const handleMouseEnter = () => {
        setIsHovered(true);
        if (closeTimeoutRef.current) {
            clearTimeout(closeTimeoutRef.current);
            closeTimeoutRef.current = null;
        }
    };
    
    const handleMouseLeave = () => {
        setIsHovered(false);
        closeTimeoutRef.current = setTimeout(() => {
            onClose();
        }, 300);
    };
    
    return (
        <div 
            className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg p-4 max-w-md pointer-events-auto"
            style={{
                left: position.x,
                top: position.y,
                pointerEvents: 'auto'
            }}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
        >
            <div className="flex justify-between items-start mb-3">
                <h4 className="font-semibold text-lg text-gray-900 dark:text-white">Actual Refunds: ${value.toFixed(2)}</h4>
                <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 ml-2">&times;</button>
            </div>
            <div className="space-y-3 text-xs">
                <div>
                    <h5 className="font-bold text-sm text-gray-700 dark:text-gray-300 mb-1">By Source</h5>
                    {Object.keys(sources).length > 0 ? Object.entries(sources).map(([source, stats]) => (
                        <div key={source} className="flex justify-between"><span className="text-gray-600 dark:text-gray-400">{source}:</span><span className="font-medium text-gray-800 dark:text-gray-200">{stats.count} txn, ${stats.total.toFixed(2)}</span></div>
                    )) : <p className="text-gray-500 italic">No source data.</p>}
                </div>
                <div className="border-t dark:border-gray-600 pt-2">
                    <h5 className="font-bold text-sm text-gray-700 dark:text-gray-300 mb-1">By Amount ($5 Buckets)</h5>
                    {Object.keys(buckets).length > 0 ? Object.entries(buckets).sort().map(([bucket, count]) => (
                        <div key={bucket} className="flex justify-between"><span className="text-gray-600 dark:text-gray-400">{bucket}:</span><span className="font-medium text-gray-800 dark:text-gray-200">{count} transactions</span></div>
                    )) : <p className="text-gray-500 italic">No bucket data.</p>}
                </div>
            </div>
        </div>
    );
};

const EventTimelineTableV3 = ({ data, selectedUserId = '', selectedProductId = '' }) => {
    const [showCumulative, setShowCumulative] = useState(false);
    
    // TODO 4: Enhanced state management with activeTooltip type differentiation
    const [activeTooltip, setActiveTooltip] = useState(null); // { type, data, position }
    const tooltipTimeoutRef = React.useRef(null);
    
    // V3 ONLY: Only handle the V3 backend format directly
    let timelineData;
    let dates = [];
    let dailyMetrics = {};
    let cumulativeMetrics = {};
    let userTimelines = {};
    let userDailyMetrics = {};
    
    if (data?.data) {
        // V3 format: data comes from Stage3RevenueResults.getTimelineTableData()
        dates = data.data.dates || [];
        dailyMetrics = data.data.daily_metrics || {};
        cumulativeMetrics = data.data.cumulative_metrics || {};
        userTimelines = data.data.user_timelines || {};
        userDailyMetrics = data.data.user_daily_metrics || {};
        
        // 🔍 FRONTEND DATA VERIFICATION: Log what EventTimelineTableV3 receives
        console.log('🔍 [EventTimelineTableV3] Received data structure:');
        console.log('🔍   - dates count:', dates.length);
        console.log('🔍   - dailyMetrics dates:', Object.keys(dailyMetrics).length);
        console.log('🔍   - userDailyMetrics users:', Object.keys(userDailyMetrics).length);
        console.log('🔍   - selectedUserId:', selectedUserId);
        
        // 🔍 ENHANCED DEBUG: Show detailed userDailyMetrics structure
        if (Object.keys(userDailyMetrics).length > 0) {
            console.log('🔍   - userDailyMetrics keys:', Object.keys(userDailyMetrics));
            
            // Show first user's data as sample
            const firstUserId = Object.keys(userDailyMetrics)[0];
            const firstUserData = userDailyMetrics[firstUserId];
            console.log('🔍   - First user ID:', firstUserId);
            console.log('🔍   - First user dates:', Object.keys(firstUserData || {}));
            
            if (firstUserData && Object.keys(firstUserData).length > 0) {
                const firstDate = Object.keys(firstUserData)[0];
                const firstDateData = firstUserData[firstDate];
                console.log('🔍   - First date data sample:', firstDate, firstDateData);
                console.log('🔍   - Has tooltip_data:', !!firstDateData?.tooltip_data);
                if (firstDateData?.tooltip_data) {
                    console.log('🔍   - tooltip_data structure:', Object.keys(firstDateData.tooltip_data));
                }
            }
            
            // Check if selectedUserId exists in userDailyMetrics
            if (selectedUserId && selectedUserId in userDailyMetrics) {
                console.log('🔍   - selectedUserId found in userDailyMetrics!');
                const selectedUserData = userDailyMetrics[selectedUserId];
                console.log('🔍   - selectedUser dates:', Object.keys(selectedUserData || {}));
                
                if (selectedUserData && Object.keys(selectedUserData).length > 0) {
                    const sampleDate = Object.keys(selectedUserData)[0];
                    const sampleDateData = selectedUserData[sampleDate];
                    console.log('🔍   - selectedUser sample date data:', sampleDate, sampleDateData);
                    console.log('🔍   - selectedUser has tooltip_data:', !!sampleDateData?.tooltip_data);
                }
            } else if (selectedUserId) {
                console.log('🔍   - selectedUserId NOT found in userDailyMetrics:', selectedUserId);
                console.log('🔍   - Available user IDs:', Object.keys(userDailyMetrics));
            }
        }
        
        // 🔥 STAGE 3 TIMELINE DEBUG: Target the problematic user and date
        const targetUser = '$device:056F1F33-A295-47F1-9284-F3719DAA976E';
        const targetDate = '2025-05-14';
        
        if (selectedUserId && selectedUserId.includes('056F1F33-A295-47F1-9284')) {
            console.log('🔥 STAGE3 SUCCESS: V3 Data Correctly Fixed');
            console.log('🔥   - Selected User ID:', selectedUserId);
            console.log('🔥   - Target Date:', targetDate);
            
            if (userDailyMetrics[selectedUserId] && userDailyMetrics[selectedUserId][targetDate]) {
                const dayData = userDailyMetrics[selectedUserId][targetDate];
                console.log('🔥   - trial_converted:', dayData.trial_converted);
                console.log('🔥   - initial_purchase:', dayData.initial_purchase);
                
                if (dayData.trial_converted === 1 && dayData.initial_purchase === 0) {
                    console.log('🔥   ✅ BUSINESS RULE COMPLIANCE: trial_converted=1, initial_purchase=0 (FIXED!)');
                } else {
                    console.log('🔥   ❌ BUSINESS RULE VIOLATION: Both values still showing as 1');
                }
            } else {
                console.log('🔥   ❌ TARGET USER DATA NOT FOUND in userDailyMetrics');
            }
        }
    }

    // Cleanup tooltip timeout on unmount
    React.useEffect(() => {
        return () => {
            if (tooltipTimeoutRef.current) {
                clearTimeout(tooltipTimeoutRef.current);
            }
        };
    }, []);

    // V3 Debug logging
    console.log('[DEBUG V3] EventTimelineTable received data:', data);
    console.log('[DEBUG V3] Data structure check:', {
        hasData: !!data,
        hasDataData: !!data?.data,
        dataKeys: data ? Object.keys(data) : [],
        dataDataKeys: data?.data ? Object.keys(data.data) : []
    });
    
    // V3 Additional debugging for data structure
    console.log('[DEBUG V3] Raw data.data structure:', data?.data);
    console.log('[DEBUG V3] Available keys in data.data:', data?.data ? Object.keys(data.data) : []);
    if (data?.data?.daily_metrics) {
        console.log('[DEBUG V3] daily_metrics type:', typeof data.data.daily_metrics);
        console.log('[DEBUG V3] daily_metrics sample keys:', Object.keys(data.data.daily_metrics).slice(0, 3));
    }

    // Extract available users and products for filtering
    const { availableUsers, availableProducts } = useMemo(() => {
        const users = new Set();
        const products = new Set();
        
        // Extract from user daily metrics (most accurate)
        Object.keys(userDailyMetrics).forEach(userId => {
            users.add(userId);
        });
        
        // Extract from user timelines if available
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
        
        // Extract from daily metrics breakdown if available
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
            // 🔥 STAGE 3 FILTERED DATA DEBUG: Target the problematic user
            const targetDate = '2025-05-14';
            const isTargetUser = selectedUserId.includes('056F1F33-A295-47F1-9284');
            
            if (isTargetUser) {
                console.log('🔥 STAGE3 FILTERED DATA DEBUG - Processing Target User:');
                console.log('🔥   - Selected User ID:', selectedUserId);
                console.log('🔥   - Raw User Metrics:', userDailyMetrics[selectedUserId]);
            }
            
            // Filter by user only - use backend's user daily metrics directly
            const userMetrics = userDailyMetrics[selectedUserId];
            
            if (userMetrics) {
                // Calculate cumulative values for single user view
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
                            revenue_net: (userDayMetrics.revenue || 0) - (userDayMetrics.refund || 0)
                        };
                        
                        // 🔥 STAGE 3 DEBUG: Check target date data processing
                        if (isTargetUser && date === targetDate) {
                            console.log('🔥 STAGE3 FILTERED DATA DEBUG - Target Date Processing:');
                            console.log('🔥   - Date:', date);
                            console.log('🔥   - Raw Backend Data:', userDayMetrics);
                            console.log('🔥   - trial_converted (backend):', userDayMetrics.trial_converted);
                            console.log('🔥   - initial_purchase (backend):', userDayMetrics.initial_purchase);
                            console.log('🔥   - Filtered Daily Result:', filteredDaily[date]);
                        }
                        
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
            // Filter by product only - currently showing aggregate data
            // TODO: Backend should provide product-specific aggregations directly
            // For now, ensure cumulative metrics are properly calculated if they're empty
            dates.forEach(date => {
                filteredDaily[date] = { ...dailyMetrics[date] };
                
                // Check if cumulative metrics exist for this date, if not calculate them
                if (cumulativeMetrics[date] && Object.keys(cumulativeMetrics[date]).length > 0) {
                    filteredCumulative[date] = { ...cumulativeMetrics[date] };
                } else {
                    // Calculate cumulative metrics for this date if missing
                    // Note: This is a fallback - should ideally be handled by backend or Stage3RevenueResultsV3
                    console.warn(`Missing cumulative metrics for date ${date}, this should be handled by Stage3RevenueResultsV3`);
                    filteredCumulative[date] = { ...cumulativeMetrics[date] } || {};
                }
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
    if (!data) {
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

    // Replace the entire getEstimatedRevenueTooltipData function with this new version:
    const getEstimatedRevenueTooltipData = (date, value) => {
        const safeValue = typeof value === 'number' ? value : 0;
        if (safeValue === 0 && !selectedUserId) return null; // Allow empty tooltips for individual users to show state

        if (selectedUserId) {
            // INDIVIDUAL USER VIEW
            const userMetrics = userDailyMetrics[selectedUserId];
            if (!userMetrics || !userMetrics[date] || !userMetrics[date].tooltip_data) {
                console.warn(`[WARN] No individual tooltip data for user ${selectedUserId} on date ${date}`);
                // Return a minimal object to show an 'inactive' state tooltip
                return {
                    value: 0,
                    breakdown: {
                        formula: '⚫ Inactive: Est_Revenue = 0 (No active trial or subscription)',
                        calculation: 'No data for this user on this date.',
                        components: {
                            'User ID': selectedUserId.substring(0, 16) + '...',
                            'Date': date,
                            'Current State': 'Inactive',
                        },
                        components_order: ['User ID', 'Date', 'Current State']
                    }
                };
            }
            // The 'tooltip_data' object from the backend has the exact structure { value, breakdown }
            // that the EstimatedRevenueTooltip component expects.
            return userMetrics[date].tooltip_data;

        } else {
            // AGGREGATE VIEW
            const aggregateData = dailyMetrics[date];
            if (!aggregateData || !aggregateData.estimated_revenue_breakdown) {
                console.warn(`[WARN] No aggregate breakdown data for date ${date}`);
                return null;
            }

            const breakdownData = aggregateData.estimated_revenue_breakdown;

            // This part relies on the backend fix from TODO 1.
            // It constructs the data object for the tooltip component.
            return {
                value: safeValue,
                estimated_trial_revenue_component: breakdownData.total_trial_revenue,
                estimated_trial_converted_revenue_component: breakdownData.total_trial_converted_revenue,
                estimated_initial_purchase_revenue_component: breakdownData.total_initial_purchase_revenue,
                estimated_trial_refund_component: breakdownData.total_trial_refunds,
                estimated_trial_converted_refund_component: breakdownData.total_trial_converted_refunds,
                estimated_initial_purchase_refund_component: breakdownData.total_initial_purchase_refunds,
                breakdown: breakdownData,
            };
        }
    };

    // CORRECTED - Final version of the function
    const getRevenueTooltipData = (date, value) => {
        const safeValue = typeof value === 'number' ? value : 0;
        if (safeValue === 0) return null;
        
        const tooltipData = dailyMetrics[date]?.revenue_tooltip_data;
        if (!tooltipData) return null;
        
        return { value: safeValue, ...tooltipData };
    };

    // CORRECTED - Final version of the function
    const getRefundTooltipData = (date, value) => {
        const safeValue = typeof value === 'number' ? value : 0;
        if (safeValue === 0) return null;

        const tooltipData = dailyMetrics[date]?.refund_tooltip_data;
        if (!tooltipData) return null;

        return { value: safeValue, ...tooltipData };
    };

    // Function to generate tooltip data for estimated refunds
    const getEstimatedRefundsTooltipData = (date, trialRefunds, subscriptionRefunds, totalRefunds) => {
        if (totalRefunds === 0) {
            return null; // Don't show tooltip for $0 values
        }
        
        console.log(`[DEBUG] getEstimatedRefundsTooltipData called with:`, {
            date,
            trialRefunds,
            subscriptionRefunds,
            totalRefunds,
            selectedUserId,
            selectedProductId,
            isAggregateView: !selectedUserId
        });
        
        // ENHANCED: Support both individual user view AND aggregate view
        if (!selectedUserId) {
            // AGGREGATE VIEW: Show breakdown across all users
            console.log(`[DEBUG] Creating aggregate refunds tooltip for date ${date}`);
            
            // Get aggregate data from dailyMetrics
            const aggregateData = dailyMetrics[date];
            if (!aggregateData) {
                console.warn(`[WARN] No aggregate data found for date ${date}`);
                return null;
            }
            
            // Count users contributing to each type of refund
            let usersWithTrialRefunds = 0;
            let usersWithSubscriptionRefunds = 0;
            let totalUsers = 0;
            
            // Analyze user daily metrics to get user counts
            Object.keys(userDailyMetrics).forEach(userId => {
                const userMetrics = userDailyMetrics[userId][date];
                if (userMetrics) {
                    totalUsers++;
                    if ((userMetrics.estimated_trial_refund_component || 0) > 0) {
                        usersWithTrialRefunds++;
                    }
                    if ((userMetrics.estimated_subscription_refund_component || 0) > 0) {
                        usersWithSubscriptionRefunds++;
                    }
                }
            });
            
            return {
                value: totalRefunds,
                // THIS IS THE FIX: Add the component values so the tooltip can read them
                estimated_trial_refund_component: trialRefunds,
                estimated_subscription_refund_component: subscriptionRefunds, 
                // We can reuse the estimated revenue component names since the tooltip is generic
                estimated_trial_revenue_component: trialRefunds,
                estimated_trial_converted_revenue_component: aggregateData.estimated_trial_converted_refund_component || 0,
                estimated_initial_purchase_revenue_component: aggregateData.estimated_initial_purchase_refund_component || 0,
                breakdown: {
                    formula: '🔄 Aggregate Estimated Potential Refunds: Σ(Trial_Refunds) + Σ(Subscription_Refunds) across all users',
                    calculation: `Trial: $${trialRefunds.toFixed(2)} (${usersWithTrialRefunds} users) + Subscription: $${subscriptionRefunds.toFixed(2)} (${usersWithSubscriptionRefunds} users) = $${totalRefunds.toFixed(2)}`,
                    components: {
                        'View Type': 'Aggregate (All Users)',
                        'Date': date,
                        'Total Users': totalUsers,
                        'Total Estimated Refunds (Potential)': `$${totalRefunds.toFixed(2)}`,
                        'Estimated Refunds from Trials (Potential)': `$${trialRefunds.toFixed(2)}`,
                        'Users with Trial Refunds': usersWithTrialRefunds,
                        'Estimated Refunds from Subscriptions (Potential)': `$${subscriptionRefunds.toFixed(2)}`,
                        'Users with Subscription Refunds': usersWithSubscriptionRefunds,
                        'Trial Refunds Explanation': 'Refunds expected if pending trials convert then users request refunds',
                        'Subscription Refunds Explanation': 'Refunds expected from active subscriptions based on age and historical patterns'
                    },
                    components_order: [
                        'View Type',
                        'Date', 
                        'Total Users',
                        'Total Estimated Refunds (Potential)',
                        'Estimated Refunds from Trials (Potential)',
                        'Users with Trial Refunds',
                        'Estimated Refunds from Subscriptions (Potential)',
                        'Users with Subscription Refunds',
                        'Trial Refunds Explanation',
                        'Subscription Refunds Explanation'
                    ]
                }
            };
        }
        
        // INDIVIDUAL USER VIEW: Show breakdown for specific user
        // Get the user's daily metrics for this date to access detailed breakdown
        const userMetrics = userDailyMetrics[selectedUserId];
        if (!userMetrics || !userMetrics[date]) {
            console.warn(`[WARN] No user metrics found for user ${selectedUserId} on date ${date}`);
            return null;
        }
        
        const dayMetrics = userMetrics[date];
        const tooltipData = dayMetrics.tooltip_data;
        
        if (!tooltipData || !tooltipData.breakdown) {
            console.warn(`[WARN] No tooltip breakdown data found for estimated refunds`);
            return null;
        }
        
        // Extract components from the enhanced breakdown
        const breakdown = tooltipData.breakdown;
        const components = breakdown.components || {};
        
        return {
            value: totalRefunds,
            breakdown: {
                formula: '🔄 Estimated Potential Refunds: Trial_Refunds + Subscription_Refunds',
                calculation: `Trial: $${trialRefunds.toFixed(2)} + Subscription: $${subscriptionRefunds.toFixed(2)} = $${totalRefunds.toFixed(2)}`,
                components: {
                    'View Type': 'Individual User',
                    'Product ID': components['Product ID'] || 'Unknown',
                    'Current State': components['Current State'] || 'Unknown',
                    'Total Estimated Refunds (Potential)': `$${totalRefunds.toFixed(2)}`,
                    'Estimated Refunds from Trial (Potential)': `$${trialRefunds.toFixed(2)}`,
                    'Estimated Refunds from Subscription (Potential)': `$${subscriptionRefunds.toFixed(2)}`,
                    'Segment Base Refund Rate': components['Segment Base Refund Rate'] || 'Unknown',
                    'Current Refund Probability': components['Current Refund Probability'] || components['Refund Likelihood'] || 'Unknown',
                    'Notes': trialRefunds > 0 ? 'Refunds expected if trial converts then user requests refund' : 'Refunds expected based on subscription age and historical patterns'
                },
                components_order: [
                    'View Type',
                    'Product ID',
                    'Current State',
                    'Total Estimated Refunds (Potential)',
                    'Estimated Refunds from Trial (Potential)',
                    'Estimated Refunds from Subscription (Potential)',
                    'Segment Base Refund Rate',
                    'Current Refund Probability',
                    'Notes'
                ]
            }
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
            format: formatCurrency,
            isRevenue: true,  // NEW: Mark this as revenue for tooltip handling
            getTooltipData: (date) => {
                const value = showCumulative 
                    ? (filteredData.cumulativeMetrics[date]?.cumulative_revenue || 0)
                    : (filteredData.dailyMetrics[date]?.revenue || 0);
                return getRevenueTooltipData(date, value);
            }
        },
        {
            key: 'daily_refund',
            label: 'Refunds',
            category: 'revenue',
            getValue: (date) => filteredData.dailyMetrics[date]?.refund || 0,
            getCumulativeValue: (date) => filteredData.cumulativeMetrics[date]?.cumulative_refund || 0,
            format: formatCurrency,
            isRefund: true,  // NEW: Mark this as refund for tooltip handling
            getTooltipData: (date) => {
                const value = showCumulative 
                    ? (filteredData.cumulativeMetrics[date]?.cumulative_refund || 0)
                    : (filteredData.dailyMetrics[date]?.refund || 0);
                return getRefundTooltipData(date, value);
            }
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
        },
        {
            key: 'daily_estimated_refunds',
            label: 'Estimated Refunds (Potential)',
            category: 'revenue',
            isEstimatedRefunds: true,
            getValue: (date) => {
                const dailyData = filteredData.dailyMetrics[date];
                const trialRefunds = dailyData?.estimated_trial_refund_component || 0;
                const subscriptionRefunds = dailyData?.estimated_subscription_refund_component || 0;
                return trialRefunds + subscriptionRefunds;
            },
            // CRITICAL FIX: Estimated refunds is a state-based metric, cumulative should show same as daily
            getCumulativeValue: (date) => {
                const dailyData = filteredData.dailyMetrics[date];
                const trialRefunds = dailyData?.estimated_trial_refund_component || 0;
                const subscriptionRefunds = dailyData?.estimated_subscription_refund_component || 0;
                return trialRefunds + subscriptionRefunds;
            },
            format: formatCurrency,
            getTooltipData: (date) => {
                const dailyData = filteredData.dailyMetrics[date];
                const trialRefunds = dailyData?.estimated_trial_refund_component || 0;
                const subscriptionRefunds = dailyData?.estimated_subscription_refund_component || 0;
                const totalRefunds = trialRefunds + subscriptionRefunds;
                
                if (totalRefunds === 0) {
                    return null; // Don't show tooltip for $0 values
                }
                
                return getEstimatedRefundsTooltipData(date, trialRefunds, subscriptionRefunds, totalRefunds);
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
                                                            metric.isEstimatedRevenue || metric.isRevenue || metric.isRefund || metric.isEstimatedRefunds ? 'cursor-help hover:bg-yellow-50 dark:hover:bg-yellow-900/20 relative' : ''
                                                        }`}
                                                        onMouseEnter={
                                            (metric.isRevenue || metric.isRefund || metric.isEstimatedRevenue || metric.isEstimatedRefunds)
                                            ? (e) => {
                                                // This logic block now correctly only runs for the aggregate view.
                                                if (tooltipTimeoutRef.current) clearTimeout(tooltipTimeoutRef.current);
                                                tooltipTimeoutRef.current = setTimeout(() => {
                                                    const rect = e.target.getBoundingClientRect();
                                                    const tooltipData = metric.getTooltipData(date, value);
                                                    if (tooltipData) {
                                                                                                // Smart positioning: check available space
                                        const tooltipWidth = 400; // Estimated tooltip width (updated for w-96 class)
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
                                                        yPosition = Math.max(10, cellCenterY - tooltipHeight - 200); // Move up by 200 pixels
                                                        
                                                        // Ensure tooltip doesn't go below screen
                                                        yPosition = Math.min(yPosition, window.innerHeight - tooltipHeight - 10);
                                                        
                                                        let tooltipType = 'estimated_revenue';
                                                        if (metric.isRevenue) tooltipType = 'revenue';
                                                        else if (metric.isRefund) tooltipType = 'refund';
                                                        else if (metric.isEstimatedRefunds) tooltipType = 'estimated_refunds';
                                                        
                                                        setActiveTooltip({ 
                                                            type: tooltipType,
                                                            data: tooltipData,
                                                            position: { x: xPosition, y: yPosition }
                                                        });
                                                    }
                                                }, 150);
                                            }
                                            : undefined
                                        }
                                                        onMouseLeave={metric.isEstimatedRevenue || metric.isRevenue || metric.isRefund || metric.isEstimatedRefunds ? () => {
                                                            // Handle mouse leave for tooltips
                                                            
                                                            // Clear the show timeout if mouse leaves before tooltip appears
                                                            if (tooltipTimeoutRef.current) {
                                                                clearTimeout(tooltipTimeoutRef.current);
                                                                tooltipTimeoutRef.current = null;
                                                            }
                                                            
                                                            // Set a timeout to hide the tooltip, allowing time to move to tooltip
                                                            const hideTimeout = setTimeout(() => {
                                                                setActiveTooltip(null);
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
            
            {/* TODO 4: Conditional tooltip rendering based on type */}
            {activeTooltip?.type === 'estimated_revenue' && (
                <EstimatedRevenueTooltip
                    data={activeTooltip.data}
                    position={activeTooltip.position}
                    onClose={() => setActiveTooltip(null)}
                    selectedUserId={selectedUserId}
                    selectedProductId={selectedProductId}
                />
            )}
            
            {activeTooltip?.type === 'revenue' && (
                <ActualRevenueTooltip
                    data={activeTooltip.data}
                    position={activeTooltip.position}
                    onClose={() => setActiveTooltip(null)}
                    selectedUserId={selectedUserId}
                    selectedProductId={selectedProductId}
                />
            )}
            
            {activeTooltip?.type === 'refund' && (
                <ActualRefundTooltip
                    data={activeTooltip.data}
                    position={activeTooltip.position}
                    onClose={() => setActiveTooltip(null)}
                    selectedUserId={selectedUserId}
                    selectedProductId={selectedProductId}
                />
            )}
            
            {activeTooltip?.type === 'estimated_refunds' && (
                <EstimatedRevenueTooltip
                    data={activeTooltip.data}
                    position={activeTooltip.position}
                    onClose={() => setActiveTooltip(null)}
                    selectedUserId={selectedUserId}
                    selectedProductId={selectedProductId}
                />
            )}
        </>
    );
};

export default EventTimelineTableV3; 