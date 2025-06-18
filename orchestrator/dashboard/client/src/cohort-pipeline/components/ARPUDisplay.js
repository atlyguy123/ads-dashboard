import React, { useState } from 'react';

const ARPUDisplay = ({ data }) => {
    const [showCalculationDetails, setShowCalculationDetails] = useState(false);

    // Comprehensive debugging
    console.log('ARPUDisplay received data:', data);
    console.log('Data structure:', {
        hasData: !!data,
        hasDataData: !!data?.data,
        dataKeys: data ? Object.keys(data) : [],
        dataDataKeys: data?.data ? Object.keys(data.data) : []
    });

    // COMPLETELY REWRITTEN: More robust data extraction
    let arpuData = null;
    let cohortSummary = null;
    let extendedCohortInfo = null;
    let baseVsLookbackBreakdown = null;

    // Primary extraction from structured format
    if (data?.data?.structured_format?.arpu_analysis) {
        console.log('Using structured_format.arpu_analysis');
        arpuData = data.data.structured_format.arpu_analysis;
        
        // Get cohort breakdown
        extendedCohortInfo = data.data.structured_format.cohort_analysis?.cohort_breakdown;
        cohortSummary = data.data.structured_format?.cohort_data?.cleaned_cohort?.metadata;
        
        console.log('Structured format data:', {
            arpuData,
            extendedCohortInfo,
            cohortSummary
        });
    } 
    // Secondary extraction from legacy format
    else if (data?.data?.legacy_format) {
        console.log('Using legacy_format');
        const legacyData = data.data.legacy_format;
        
        // Extract ARPU data from legacy format
        arpuData = {
            cohort_wide: {
                arpu: legacyData.arpu || 0,
                total_revenue: 0,
                paying_users: 0,
                net_revenue: 0
            },
            per_product: {},
            metadata: {
                calculation_method: 'legacy_format',
                message: 'Data from legacy format',
                lookback_days: legacyData.metadata?.lookback_days_used || 0
            }
        };
        
        // Convert legacy per-product ARPU to new format
        if (legacyData.arpu_per_product) {
            Object.entries(legacyData.arpu_per_product).forEach(([productId, arpu]) => {
                arpuData.per_product[productId] = {
                    arpu: arpu,
                    total_revenue: 0,
                    paying_users: 0,
                    event_count: 0,
                    net_revenue: 0
                };
            });
        }
        
        // Extract extended cohort info
        extendedCohortInfo = {
            base_users: legacyData.base_users || 0,
            lookback_users: legacyData.lookback_users || 0,
            total_users: legacyData.comprehensive_cohort_size || 0,
            base_percentage: 0,
            lookback_percentage: 0
        };
        
        if (extendedCohortInfo.total_users > 0) {
            extendedCohortInfo.base_percentage = (extendedCohortInfo.base_users / extendedCohortInfo.total_users) * 100;
            extendedCohortInfo.lookback_percentage = (extendedCohortInfo.lookback_users / extendedCohortInfo.total_users) * 100;
        }
        
        cohortSummary = {
            user_count: legacyData.cohort_size || 0,
            event_count: legacyData.total_events || 0
        };
        
        console.log('Legacy format data:', {
            arpuData,
            extendedCohortInfo,
            cohortSummary,
            legacyData
        });
    }
    // Tertiary extraction from direct structured format
    else if (data?.structured_format?.arpu_analysis) {
        console.log('Using direct structured_format.arpu_analysis');
        arpuData = data.structured_format.arpu_analysis;
        extendedCohortInfo = data.structured_format.cohort_analysis?.cohort_breakdown;
        cohortSummary = data.structured_format?.cohort_data?.cleaned_cohort?.metadata;
    }
    // Quaternary extraction from direct legacy format
    else if (data?.legacy_format) {
        console.log('Using direct legacy_format');
        const legacyData = data.legacy_format;
        
        arpuData = {
            cohort_wide: {
                arpu: legacyData.arpu || 0,
                total_revenue: 0,
                paying_users: 0,
                net_revenue: 0
            },
            per_product: {},
            metadata: {
                calculation_method: 'legacy_format',
                message: 'Data from legacy format',
                lookback_days: legacyData.metadata?.lookback_days_used || 0
            }
        };
        
        if (legacyData.arpu_per_product) {
            Object.entries(legacyData.arpu_per_product).forEach(([productId, arpu]) => {
                arpuData.per_product[productId] = {
                    arpu: arpu,
                    total_revenue: 0,
                    paying_users: 0,
                    event_count: 0,
                    net_revenue: 0
                };
            });
        }
        
        extendedCohortInfo = {
            base_users: legacyData.base_users || 0,
            lookback_users: legacyData.lookback_users || 0,
            total_users: legacyData.comprehensive_cohort_size || 0,
            base_percentage: 0,
            lookback_percentage: 0
        };
        
        if (extendedCohortInfo.total_users > 0) {
            extendedCohortInfo.base_percentage = (extendedCohortInfo.base_users / extendedCohortInfo.total_users) * 100;
            extendedCohortInfo.lookback_percentage = (extendedCohortInfo.lookback_users / extendedCohortInfo.total_users) * 100;
        }
        
        cohortSummary = {
            user_count: legacyData.cohort_size || 0,
            event_count: legacyData.total_events || 0
        };
    }
    // Final fallback from regular arpu_data
    else if (data?.data?.arpu_data) {
        console.log('Using regular arpu_data');
        arpuData = data.data.arpu_data;
        cohortSummary = data.data.cohort_summary;
    }

    console.log('FINAL EXTRACTED ARPU DATA:', {
        arpuData,
        extendedCohortInfo,
        cohortSummary,
        hasArpuData: !!arpuData
    });

    if (!arpuData) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No ARPU data available. Run an analysis to see results.
                </p>
                <div className="mt-4 text-xs text-gray-400">
                    Debug: {JSON.stringify({
                        hasData: !!data,
                        hasDataData: !!data?.data,
                        dataKeys: data ? Object.keys(data) : [],
                        dataDataKeys: data?.data ? Object.keys(data.data) : []
                    })}
                </div>
            </div>
        );
    }

    const cohortWide = arpuData.cohort_wide || {};
    const perProduct = arpuData.per_product || {};
    const metadata = arpuData.metadata || {};

    // Extract base vs lookback breakdown information
    const baseRevenue = metadata.base_revenue || 0;
    const lookbackRevenue = metadata.lookback_revenue || 0;
    const totalRevenue = baseRevenue + lookbackRevenue || cohortWide.total_revenue || 0;
    const baseContributionPct = metadata.base_contribution_pct || 0;
    const lookbackContributionPct = metadata.lookback_contribution_pct || 0;
    
    // Get lookback days
    const lookbackDaysUsed = metadata.lookback_days_used || 
                            metadata.lookback_days || 
                            (data?.data?.structured_format?.metadata?.lookback_days_used) ||
                            (data?.structured_format?.metadata?.lookback_days_used) ||
                            0;

    const formatCurrency = (value) => {
        if (value === null || value === undefined || isNaN(value)) return '$0.00';
        return `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    const formatNumber = (value) => {
        if (value === null || value === undefined || isNaN(value)) return '0';
        return Number(value).toLocaleString();
    };

    const formatPercentage = (value) => {
        if (value === null || value === undefined || isNaN(value)) return '0.0%';
        return `${Number(value).toFixed(1)}%`;
    };

    // Sort products by ARPU (highest first)
    const sortedProducts = Object.entries(perProduct).sort(([,a], [,b]) => (b.arpu || 0) - (a.arpu || 0));

    // NEW: Check for products with zero ARPU that might be hidden
    const zeroArpuProducts = Object.entries(perProduct).filter(([,productData]) => (productData.arpu || 0) === 0);
    const hasHiddenProducts = zeroArpuProducts.length > 0;

    // NEW: Calculate separate ARPU for base and lookback periods
    const calculateSeparateARPU = () => {
        const baseUsers = (metadata.base_non_refunded_users || 0);
        const lookbackUsers = (metadata.lookback_non_refunded_users || 0);
        const baseRevenue = (metadata.base_revenue || 0);
        const lookbackRevenue = (metadata.lookback_revenue || 0);
        const baseRefunds = (metadata.base_refunds || 0);
        const lookbackRefunds = (metadata.lookback_refunds || 0);
        
        // Calculate NET revenue for each period (gross revenue - refunds)
        const baseNetRevenue = baseRevenue - baseRefunds;
        const lookbackNetRevenue = lookbackRevenue - lookbackRefunds;
        
        return {
            baseARPU: baseUsers > 0 ? baseNetRevenue / baseUsers : 0,
            lookbackARPU: lookbackUsers > 0 ? lookbackNetRevenue / lookbackUsers : 0,
            hasData: baseUsers > 0 || lookbackUsers > 0,
            baseNetRevenue,
            lookbackNetRevenue
        };
    };

    const separateARPU = calculateSeparateARPU();

    return (
        <div className="space-y-6">
            {/* Header with Key Metrics */}
            <div className="mb-4">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
                    Cohort-Wide ARPU Analysis
                </h3>
                
                {/* NEW: Extended Timeline Note */}
                {(extendedCohortInfo?.base_users > 0 && extendedCohortInfo?.lookback_users > 0) && (
                    <div className="mb-4 p-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg border border-amber-200 dark:border-amber-800">
                        <div className="text-sm text-amber-800 dark:text-amber-200">
                            <strong>Extended Timeline Analysis:</strong> This ARPU calculation includes data from an extended timeline.
                            <div className="mt-1 text-xs">
                                Total Users: {extendedCohortInfo?.total_users.toLocaleString()} ({extendedCohortInfo?.base_users.toLocaleString()} base period + {extendedCohortInfo?.lookback_users.toLocaleString()} lookback period)
                                {lookbackDaysUsed > 0 && ` • Lookback: ${lookbackDaysUsed} days`}
                            </div>
                        </div>
                    </div>
                )}

                {/* NEW: Separate Base vs Lookback ARPU Quick View */}
                {separateARPU.hasData && (
                    <div className="mb-4 p-3 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg border border-indigo-200 dark:border-indigo-800">
                        <div className="text-sm font-medium text-indigo-800 dark:text-indigo-200 mb-2">
                            Quick ARPU Comparison by Period (Net Revenue ÷ Non-Refunded Users):
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="text-center">
                                <div className="text-lg font-bold text-indigo-600 dark:text-indigo-400">
                                    {formatCurrency(separateARPU.baseARPU)}
                                </div>
                                <div className="text-xs text-indigo-700 dark:text-indigo-300">
                                    Base Period ARPU
                                </div>
                                <div className="text-xs text-indigo-600 dark:text-indigo-400">
                                    {formatCurrency(separateARPU.baseNetRevenue)} ÷ {formatNumber(metadata.base_non_refunded_users || 0)} users
                                </div>
                            </div>
                            <div className="text-center">
                                <div className="text-lg font-bold text-indigo-600 dark:text-indigo-400">
                                    {formatCurrency(separateARPU.lookbackARPU)}
                                </div>
                                <div className="text-xs text-indigo-700 dark:text-indigo-300">
                                    Lookback Period ARPU
                                </div>
                                <div className="text-xs text-indigo-600 dark:text-indigo-400">
                                    {formatCurrency(separateARPU.lookbackNetRevenue)} ÷ {formatNumber(metadata.lookback_non_refunded_users || 0)} users
                                </div>
                            </div>
                        </div>
                    </div>
                )}
                
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                    <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                        <div className="text-3xl font-bold text-blue-600 dark:text-blue-400">
                            {formatCurrency(cohortWide.arpu)}
                        </div>
                        <div className="text-sm text-blue-700 dark:text-blue-300">
                            ARPU (Net Revenue ÷ Non-Refunded Users)
                        </div>
                        <div className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                            {formatNumber((metadata.base_non_refunded_users || 0) + (metadata.lookback_non_refunded_users || 0) || cohortWide.paying_users)} users, {formatCurrency(cohortWide.net_revenue || cohortWide.total_revenue)} net revenue
                        </div>
                        
                        {/* Enhanced: Comprehensive Base vs Lookback breakdown */}
                        {(baseRevenue > 0 || lookbackRevenue > 0) && totalRevenue > 0 && (
                            <div className="mt-3 pt-3 border-t border-blue-200 dark:border-blue-700">
                                <div className="text-xs font-medium text-blue-800 dark:text-blue-300 mb-2">
                                    Revenue Timeline Breakdown:
                                </div>
                                <div className="space-y-1">
                                    <div className="flex justify-between text-xs">
                                        <span className="text-blue-500 dark:text-blue-500">Base Period:</span>
                                        <span className="text-blue-600 dark:text-blue-400">
                                            {formatCurrency(baseRevenue)} ({baseContributionPct.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div className="flex justify-between text-xs">
                                        <span className="text-blue-500 dark:text-blue-500">Lookback Period:</span>
                                        <span className="text-blue-600 dark:text-blue-400">
                                            {formatCurrency(lookbackRevenue)} ({lookbackContributionPct.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div className="flex justify-between text-xs font-medium border-t border-blue-200 dark:border-blue-700 pt-1">
                                        <span className="text-blue-600 dark:text-blue-400">Total:</span>
                                        <span className="text-blue-700 dark:text-blue-300">
                                            {formatCurrency(totalRevenue)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                    
                    <div className="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                        <div className="text-2xl font-semibold text-gray-700 dark:text-gray-300">
                            {formatCurrency(cohortWide.total_revenue || totalRevenue)}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">
                            Total Revenue
                        </div>
                        {cohortWide.total_refunds > 0 && (
                            <div className="text-xs text-gray-600 dark:text-gray-400 mt-1">
                                {formatCurrency(cohortWide.total_refunds)} refunded
                            </div>
                        )}
                        
                        {/* Enhanced: Detailed revenue breakdown with period-specific refunds */}
                        {(baseRevenue > 0 || lookbackRevenue > 0) && (
                            <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                                <div className="text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
                                    Revenue Sources:
                                </div>
                                <div className="space-y-1">
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Base Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatCurrency(baseRevenue)}
                                        </span>
                                    </div>
                                    {/* Base period refunds */}
                                    {(metadata.base_refunds > 0) && (
                                        <div className="flex justify-between text-xs ml-2">
                                            <span className="text-red-500 dark:text-red-400">Base Refunds:</span>
                                            <span className="text-red-600 dark:text-red-400">
                                                -{formatCurrency(metadata.base_refunds)}
                                            </span>
                                        </div>
                                    )}
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Lookback Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatCurrency(lookbackRevenue)}
                                        </span>
                                    </div>
                                    {/* Lookback period refunds */}
                                    {(metadata.lookback_refunds > 0) && (
                                        <div className="flex justify-between text-xs ml-2">
                                            <span className="text-red-500 dark:text-red-400">Lookback Refunds:</span>
                                            <span className="text-red-600 dark:text-red-400">
                                                -{formatCurrency(metadata.lookback_refunds)}
                                            </span>
                                        </div>
                                    )}
                                    {/* Fallback: Total refunds if period-specific not available */}
                                    {cohortWide.total_refunds > 0 && !(metadata.base_refunds > 0) && !(metadata.lookback_refunds > 0) && (
                                        <div className="flex justify-between text-xs text-red-600 dark:text-red-400">
                                            <span>Total Refunds:</span>
                                            <span>-{formatCurrency(cohortWide.total_refunds)}</span>
                                        </div>
                                    )}
                                    <div className="flex justify-between text-xs font-medium border-t border-gray-200 dark:border-gray-600 pt-1">
                                        <span className="text-gray-600 dark:text-gray-400">Net Revenue:</span>
                                        <span className="text-gray-700 dark:text-gray-300">
                                            {formatCurrency(cohortWide.net_revenue || totalRevenue)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                    
                    <div className="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                        <div className="text-2xl font-semibold text-gray-700 dark:text-gray-300">
                            {formatNumber(cohortWide.total_paying_users || cohortWide.paying_users)}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">
                            Total Paying Users
                        </div>
                        {cohortWide.refund_users > 0 && (
                            <div className="text-xs text-gray-600 dark:text-gray-400 mt-1">
                                {formatNumber(cohortWide.refund_users)} refunded ({((cohortWide.refund_users / (cohortWide.total_paying_users || cohortWide.paying_users)) * 100).toFixed(1)}%)
                            </div>
                        )}
                        
                        {/* Enhanced: User timeline breakdown with refund information */}
                        {((metadata.base_paying_users || 0) > 0 || (metadata.lookback_paying_users || 0) > 0) && (
                            <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                                <div className="text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
                                    User Timeline Distribution:
                                </div>
                                <div className="space-y-1">
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Base Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatNumber(metadata.base_paying_users || 0)} paying
                                        </span>
                                    </div>
                                    {(metadata.base_refund_users || 0) > 0 && (
                                        <div className="flex justify-between text-xs ml-2">
                                            <span className="text-red-500 dark:text-red-400">Base Refunds:</span>
                                            <span className="text-red-600 dark:text-red-400">
                                                {formatNumber(metadata.base_refund_users)} users ({((metadata.base_refund_users / metadata.base_paying_users) * 100).toFixed(1)}%)
                                            </span>
                                        </div>
                                    )}
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Lookback Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatNumber(metadata.lookback_paying_users || 0)} paying
                                        </span>
                                    </div>
                                    {(metadata.lookback_refund_users || 0) > 0 && (
                                        <div className="flex justify-between text-xs ml-2">
                                            <span className="text-red-500 dark:text-red-400">Lookback Refunds:</span>
                                            <span className="text-red-600 dark:text-red-400">
                                                {formatNumber(metadata.lookback_refund_users)} users ({((metadata.lookback_refund_users / metadata.lookback_paying_users) * 100).toFixed(1)}%)
                                            </span>
                                        </div>
                                    )}
                                    <div className="flex justify-between text-xs font-medium border-t border-gray-200 dark:border-gray-600 pt-1">
                                        <span className="text-gray-600 dark:text-gray-400">Non-Refunded Users:</span>
                                        <span className="text-gray-700 dark:text-gray-300">
                                            {formatNumber((metadata.base_non_refunded_users || 0) + (metadata.lookback_non_refunded_users || 0))}
                                        </span>
                                    </div>
                                    {lookbackDaysUsed > 0 && (
                                        <div className="text-xs text-gray-500 dark:text-gray-500 mt-1">
                                            Lookback: {lookbackDaysUsed} days
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}
                        
                        {/* Fallback: Extended cohort info if metadata not available */}
                        {!((metadata.base_paying_users || 0) > 0 || (metadata.lookback_paying_users || 0) > 0) && 
                         (extendedCohortInfo?.base_users > 0 && extendedCohortInfo?.lookback_users > 0) && (
                            <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                                <div className="text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
                                    User Timeline Distribution:
                                </div>
                                <div className="space-y-1">
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Base Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatNumber(extendedCohortInfo.base_users)} ({extendedCohortInfo.base_percentage.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div className="flex justify-between text-xs">
                                        <span className="text-gray-500 dark:text-gray-500">Lookback Period:</span>
                                        <span className="text-gray-600 dark:text-gray-400">
                                            {formatNumber(extendedCohortInfo.lookback_users)} ({extendedCohortInfo.lookback_percentage.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div className="flex justify-between text-xs font-medium border-t border-gray-200 dark:border-gray-600 pt-1">
                                        <span className="text-gray-600 dark:text-gray-400">Total Extended:</span>
                                        <span className="text-gray-700 dark:text-gray-300">
                                            {formatNumber(extendedCohortInfo.total_users)}
                                        </span>
                                    </div>
                                    {lookbackDaysUsed > 0 && (
                                        <div className="text-xs text-gray-500 dark:text-gray-500 mt-1">
                                            Lookback: {lookbackDaysUsed} days
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                </div>

                {/* Method Badge */}
                {metadata.calculation_method && (
                    <div className="flex items-center space-x-2 mb-4">
                        <span className={`px-3 py-1 text-sm rounded-full ${
                            metadata.calculation_method === 'standard' 
                                ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300'
                                : metadata.calculation_method === 'extended_lookback'
                                ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300'
                                : 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300'
                        }`}>
                            {metadata.calculation_method === 'standard' ? 'Standard Calculation - No Lookback Used' : 
                             metadata.calculation_method === 'extended_lookback' ? `Extended Lookback - ${metadata.lookback_days} Days` : 'Fallback Method - Insufficient Data'}
                        </span>
                        {metadata.lookback_days > 0 && metadata.calculation_method === 'extended_lookback' && (
                            <span className="px-3 py-1 text-sm bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300 rounded-full">
                                Extended by {metadata.lookback_days} days for sufficient sample size
                            </span>
                        )}
                    </div>
                )}
            </div>

            {/* Per-Product ARPU Breakdown */}
            {sortedProducts.length > 0 && (
                <div>
                    <div className="flex items-center justify-between mb-4">
                        <h4 className="text-lg font-semibold text-gray-900 dark:text-white">
                            ARPU by Product ({sortedProducts.length} products{hasHiddenProducts ? ` + ${zeroArpuProducts.length} with $0 ARPU` : ''})
                        </h4>
                        <button
                            onClick={() => setShowCalculationDetails(!showCalculationDetails)}
                            className="text-sm text-blue-600 dark:text-blue-400 hover:underline"
                        >
                            {showCalculationDetails ? 'Hide' : 'Show'} calculation details
                        </button>
                    </div>

                    {/* NEW: Warning about hidden zero ARPU products */}
                    {hasHiddenProducts && (
                        <div className="mb-4 p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
                            <div className="text-sm text-yellow-800 dark:text-yellow-200">
                                <strong>Note:</strong> {zeroArpuProducts.length} product{zeroArpuProducts.length > 1 ? 's' : ''} with $0.00 ARPU {zeroArpuProducts.length > 1 ? 'are' : 'is'} not displayed above.
                                <div className="text-xs mt-1">
                                    {zeroArpuProducts.length > 5 
                                        ? `${zeroArpuProducts.slice(0, 5).map(([id]) => id).join(', ')} and ${zeroArpuProducts.length - 5} more...`
                                        : zeroArpuProducts.map(([id]) => id).join(', ')
                                    }
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Product Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
                        {sortedProducts.filter(([,productData]) => (productData.arpu || 0) > 0).map(([productId, productData]) => (
                            <div
                                key={productId}
                                className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5 hover:shadow-lg transition-all duration-200"
                            >
                                {/* Product Name */}
                                <div className="mb-4">
                                    <h5 className="font-semibold text-gray-900 dark:text-white text-sm leading-tight" title={productId}>
                                        {productId.length > 45 ? `${productId.substring(0, 45)}...` : productId}
                                    </h5>
                                </div>

                                {/* Main ARPU Display */}
                                <div className="text-center mb-4 py-3 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-lg border border-blue-200 dark:border-blue-700">
                                    <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                                        {formatCurrency(productData.arpu)}
                                    </div>
                                    <div className="text-xs text-blue-700 dark:text-blue-300 font-medium">
                                        ARPU
                                    </div>
                                </div>

                                {/* NEW: Separate Base vs Lookback ARPU for this product */}
                                {(productData.base_revenue > 0 || productData.lookback_revenue > 0) && 
                                 (productData.base_non_refunded_users > 0 || productData.lookback_non_refunded_users > 0) && (
                                    <div className="mb-4 p-2 bg-indigo-50 dark:bg-indigo-900/20 rounded border border-indigo-200 dark:border-indigo-700">
                                        <div className="text-xs font-medium text-indigo-800 dark:text-indigo-300 mb-2">Period ARPU (Net Revenue ÷ Non-Refunded Users):</div>
                                        <div className="grid grid-cols-2 gap-2">
                                            <div className="text-center">
                                                <div className="text-sm font-bold text-indigo-600 dark:text-indigo-400">
                                                    {formatCurrency(productData.base_non_refunded_users > 0 ? (productData.base_revenue - (productData.base_refunds || 0)) / productData.base_non_refunded_users : 0)}
                                                </div>
                                                <div className="text-xs text-indigo-700 dark:text-indigo-300">Base</div>
                                                <div className="text-xs text-indigo-600 dark:text-indigo-400">
                                                    {formatCurrency((productData.base_revenue || 0) - (productData.base_refunds || 0))} ÷ {formatNumber(productData.base_non_refunded_users || 0)}
                                                </div>
                                            </div>
                                            <div className="text-center">
                                                <div className="text-sm font-bold text-indigo-600 dark:text-indigo-400">
                                                    {formatCurrency(productData.lookback_non_refunded_users > 0 ? (productData.lookback_revenue - (productData.lookback_refunds || 0)) / productData.lookback_non_refunded_users : 0)}
                                                </div>
                                                <div className="text-xs text-indigo-700 dark:text-indigo-300">Lookback</div>
                                                <div className="text-xs text-indigo-600 dark:text-indigo-400">
                                                    {formatCurrency((productData.lookback_revenue || 0) - (productData.lookback_refunds || 0))} ÷ {formatNumber(productData.lookback_non_refunded_users || 0)}
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                )}
                                
                                {/* Revenue and User Stats */}
                                <div className="space-y-3">
                                    <div className="flex justify-between items-center py-2 px-3 bg-gray-50 dark:bg-gray-700 rounded">
                                        <span className="text-xs text-gray-600 dark:text-gray-400">
                                            {productData.net_revenue ? 'Net Revenue' : 'Total Revenue'}
                                        </span>
                                        <span className="text-sm font-semibold text-gray-900 dark:text-white">
                                            {formatCurrency(productData.net_revenue || productData.total_revenue)}
                                            {productData.total_refunds > 0 && (
                                                <div className="text-xs text-gray-600 dark:text-gray-400 mt-1">
                                                    {formatCurrency(productData.total_revenue)} total - {formatCurrency(productData.total_refunds)} refunded
                                                </div>
                                            )}
                                        </span>
                                    </div>
                                    
                                    {/* Enhanced: Comprehensive Base vs Lookback revenue breakdown with period-specific refunds */}
                                    {(productData.base_revenue > 0 || productData.lookback_revenue > 0) && (
                                        <div className="py-2 px-3 bg-amber-50 dark:bg-amber-900/20 rounded border border-amber-200 dark:border-amber-700">
                                            <div className="text-xs font-medium text-amber-800 dark:text-amber-300 mb-2">Revenue Timeline:</div>
                                            <div className="space-y-1">
                                                <div className="flex justify-between text-xs">
                                                    <span className="text-amber-700 dark:text-amber-400">Base Period:</span>
                                                    <span className="text-amber-800 dark:text-amber-300 font-medium">
                                                        {formatCurrency(productData.base_revenue || 0)}
                                                    </span>
                                                </div>
                                                {/* NEW: Base period refunds for this product */}
                                                {productData.base_refunds > 0 && (
                                                    <div className="flex justify-between text-xs ml-2">
                                                        <span className="text-red-600 dark:text-red-400">Base Refunds:</span>
                                                        <span className="text-red-700 dark:text-red-400 font-medium">
                                                            -{formatCurrency(productData.base_refunds)}
                                                        </span>
                                                    </div>
                                                )}
                                                <div className="flex justify-between text-xs">
                                                    <span className="text-amber-700 dark:text-amber-400">Lookback Period:</span>
                                                    <span className="text-amber-800 dark:text-amber-300 font-medium">
                                                        {formatCurrency(productData.lookback_revenue || 0)}
                                                    </span>
                                                </div>
                                                {/* NEW: Lookback period refunds for this product */}
                                                {productData.lookback_refunds > 0 && (
                                                    <div className="flex justify-between text-xs ml-2">
                                                        <span className="text-red-600 dark:text-red-400">Lookback Refunds:</span>
                                                        <span className="text-red-700 dark:text-red-400 font-medium">
                                                            -{formatCurrency(productData.lookback_refunds)}
                                                        </span>
                                                    </div>
                                                )}
                                                {/* Net revenue calculation */}
                                                <div className="flex justify-between text-xs font-medium border-t border-amber-300 dark:border-amber-600 pt-1">
                                                    <span className="text-amber-800 dark:text-amber-300">Net Revenue:</span>
                                                    <span className="text-amber-900 dark:text-amber-200">
                                                        {formatCurrency(productData.net_revenue || (productData.base_revenue + productData.lookback_revenue - (productData.base_refunds || 0) - (productData.lookback_refunds || 0)))}
                                                    </span>
                                                </div>
                                                {(productData.base_revenue > 0 && productData.lookback_revenue > 0) && (
                                                    <div className="text-xs text-amber-600 dark:text-amber-400 mt-1 pt-1 border-t border-amber-300 dark:border-amber-600">
                                                        Base: {((productData.base_revenue / (productData.base_revenue + productData.lookback_revenue)) * 100).toFixed(1)}% • 
                                                        Lookback: {((productData.lookback_revenue / (productData.base_revenue + productData.lookback_revenue)) * 100).toFixed(1)}%
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    )}
                                    
                                    <div className="flex justify-between items-center py-2 px-3 bg-gray-50 dark:bg-gray-700 rounded">
                                        <span className="text-xs text-gray-600 dark:text-gray-400">Total Paying Users</span>
                                        <span className="text-sm font-semibold text-gray-900 dark:text-white">
                                            {formatNumber(productData.paying_users)}
                                            {productData.refund_users > 0 && (
                                                <span className="text-xs text-red-600 dark:text-red-400 ml-1">
                                                    ({formatNumber(productData.refund_users)} refunded - {((productData.refund_users / productData.paying_users) * 100).toFixed(1)}%)
                                                </span>
                                            )}
                                        </span>
                                    </div>
                                    
                                    {/* Non-refunded users breakdown */}
                                    {(productData.base_non_refunded_users > 0 || productData.lookback_non_refunded_users > 0) && (
                                        <div className="py-2 px-3 bg-purple-50 dark:bg-purple-900/20 rounded border border-purple-200 dark:border-purple-700">
                                            <div className="text-xs font-medium text-purple-800 dark:text-purple-300 mb-2">User Timeline (Used in ARPU):</div>
                                            <div className="space-y-1">
                                                <div className="flex justify-between text-xs">
                                                    <span className="text-purple-700 dark:text-purple-400">Base Period:</span>
                                                    <span className="text-purple-800 dark:text-purple-300 font-medium">
                                                        {formatNumber(productData.base_paying_users || 0)} paying
                                                    </span>
                                                </div>
                                                {(productData.base_refund_users || 0) > 0 && (
                                                    <div className="flex justify-between text-xs ml-2">
                                                        <span className="text-red-600 dark:text-red-400">Base Refunds:</span>
                                                        <span className="text-red-700 dark:text-red-400 font-medium">
                                                            -{formatNumber(productData.base_refund_users)} users ({((productData.base_refund_users / productData.base_paying_users) * 100).toFixed(1)}%)
                                                        </span>
                                                    </div>
                                                )}
                                                <div className="flex justify-between text-xs">
                                                    <span className="text-purple-700 dark:text-purple-400">Lookback Period:</span>
                                                    <span className="text-purple-800 dark:text-purple-300 font-medium">
                                                        {formatNumber(productData.lookback_paying_users || 0)} paying
                                                    </span>
                                                </div>
                                                {(productData.lookback_refund_users || 0) > 0 && (
                                                    <div className="flex justify-between text-xs ml-2">
                                                        <span className="text-red-600 dark:text-red-400">Lookback Refunds:</span>
                                                        <span className="text-red-700 dark:text-red-400 font-medium">
                                                            -{formatNumber(productData.lookback_refund_users)} users ({((productData.lookback_refund_users / productData.lookback_paying_users) * 100).toFixed(1)}%)
                                                        </span>
                                                    </div>
                                                )}
                                                <div className="flex justify-between text-xs font-medium border-t border-purple-300 dark:border-purple-600 pt-1">
                                                    <span className="text-purple-800 dark:text-purple-300">Non-Refunded Users:</span>
                                                    <span className="text-purple-900 dark:text-purple-200">
                                                        {formatNumber((productData.base_non_refunded_users || 0) + (productData.lookback_non_refunded_users || 0))}
                                                    </span>
                                                </div>
                                                {((productData.base_paying_users || 0) > 0 && (productData.lookback_paying_users || 0) > 0) && (
                                                    <div className="text-xs text-purple-600 dark:text-purple-400 mt-1 pt-1 border-t border-purple-300 dark:border-purple-600">
                                                        Base: {(((productData.base_paying_users || 0) / ((productData.base_paying_users || 0) + (productData.lookback_paying_users || 0))) * 100).toFixed(1)}% • 
                                                        Lookback: {(((productData.lookback_paying_users || 0) / ((productData.base_paying_users || 0) + (productData.lookback_paying_users || 0))) * 100).toFixed(1)}%
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Detailed Table View */}
                    {showCalculationDetails && (
                        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-4">
                            <h5 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
                                Calculation Details
                            </h5>
                            
                            {/* NEW: Calculation Method Note */}
                            <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                                <div className="text-sm text-blue-800 dark:text-blue-200">
                                    <strong>ARPU Calculation Method:</strong> All ARPU values use <strong>Net Revenue ÷ Non-Refunded Users</strong>
                                    <div className="text-xs mt-1 text-blue-700 dark:text-blue-300">
                                        • Base ARPU = (Base Revenue - Base Refunds) ÷ Base Non-Refunded Users<br/>
                                        • Lookback ARPU = (Lookback Revenue - Lookback Refunds) ÷ Lookback Non-Refunded Users<br/>
                                        • Combined ARPU = Total Net Revenue ÷ Total Non-Refunded Users
                                    </div>
                                </div>
                            </div>
                            
                            <div className="overflow-x-auto">
                                <table className="min-w-full text-sm">
                                    <thead className="bg-gray-100 dark:bg-gray-700">
                                        <tr>
                                            <th className="px-3 py-2 text-left text-gray-700 dark:text-gray-300">Product ID</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Total Revenue</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Base Revenue</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Lookback Revenue</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Base Refunds</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Lookback Refunds</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Net Revenue</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Total Paying Users</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Refund %</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Non-Refunded Users</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Base ARPU</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Lookback ARPU</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">Combined ARPU</th>
                                            <th className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">% of Net Revenue</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                                        {sortedProducts.map(([productId, productData]) => {
                                            const netRevenue = productData.net_revenue || productData.total_revenue;
                                            const cohortNetRevenue = cohortWide.net_revenue || cohortWide.total_revenue;
                                            const revenuePercentage = cohortNetRevenue > 0 
                                                ? (netRevenue / cohortNetRevenue) * 100 
                                                : 0;
                                            const nonRefundedUsers = (productData.base_non_refunded_users || 0) + (productData.lookback_non_refunded_users || 0) || productData.paying_users;
                                            const refundPercentage = productData.paying_users > 0 ? ((productData.refund_users || 0) / productData.paying_users) * 100 : 0;
                                            const baseARPU = productData.base_non_refunded_users > 0 ? (productData.base_revenue - (productData.base_refunds || 0)) / productData.base_non_refunded_users : 0;
                                            const lookbackARPU = productData.lookback_non_refunded_users > 0 ? (productData.lookback_revenue - (productData.lookback_refunds || 0)) / productData.lookback_non_refunded_users : 0;
                                            
                                            return (
                                                <tr key={productId} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                                                    <td className="px-3 py-2 text-gray-900 dark:text-white">
                                                        <div className="max-w-xs truncate" title={productId}>
                                                            {productId}
                                                        </div>
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatCurrency(productData.total_revenue)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatCurrency(productData.base_revenue || 0)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatCurrency(productData.lookback_revenue || 0)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-red-600 dark:text-red-400">
                                                        {formatCurrency(productData.base_refunds || 0)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-red-600 dark:text-red-400">
                                                        {formatCurrency(productData.lookback_refunds || 0)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatCurrency(productData.net_revenue || productData.total_revenue)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatNumber(productData.paying_users)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-red-600 dark:text-red-400">
                                                        {formatPercentage(refundPercentage)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-purple-600 dark:text-purple-400 font-medium">
                                                        {formatNumber(nonRefundedUsers)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-indigo-600 dark:text-indigo-400">
                                                        {formatCurrency(baseARPU)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-indigo-600 dark:text-indigo-400">
                                                        {formatCurrency(lookbackARPU)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right font-semibold text-green-600 dark:text-green-400">
                                                        {formatCurrency(productData.arpu)}
                                                    </td>
                                                    <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">
                                                        {formatPercentage(revenuePercentage)}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export default ARPUDisplay; 