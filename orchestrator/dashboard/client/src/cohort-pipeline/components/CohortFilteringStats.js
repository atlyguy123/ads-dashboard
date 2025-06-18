import React from 'react';
import { Filter, Users, Activity, TrendingDown, Clock, Target, Database, Split } from 'lucide-react';

const CohortFilteringStats = ({ data }) => {
    if (!data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No cohort data available. Run an analysis to see filtering statistics.
                </p>
            </div>
        );
    }

    // Extract data from the pipeline response with robust fallbacks
    const legacyFormat = data.legacy_format || data.data?.legacy_format;
    const structuredFormat = data.structured_format || data.data?.structured_format;
    const summary = data.summary || data.data?.summary;
    
    // ROBUST DATA EXTRACTION with clear priorities
    let filteringSteps = [];
    let baseCohortCount = 0;
    let finalCohortCount = 0;
    let totalEvents = 0;
    
    // Extended cohort information (base vs lookback breakdown)
    let comprehensiveCohortSize = 0;
    let baseUsers = 0;
    let lookbackUsers = 0;
    let lookbackDaysUsed = 60; // Default from unified pipeline
    let baseEvents = 0;
    let lookbackEvents = 0;
    let extendedCohortSize = 0; // Track initial extended cohort size
    
    // Priority 1: Extract from summary (most reliable for unified pipeline)
    if (summary) {
        extendedCohortSize = summary.extended_cohort_count || 0;
        finalCohortCount = summary.final_cohort_count || 0;
        comprehensiveCohortSize = finalCohortCount;
        baseUsers = summary.base_user_count || 0;
        lookbackUsers = summary.lookback_user_count || 0;
        totalEvents = summary.total_event_count || 0;
        baseEvents = summary.base_event_count || 0;
        lookbackEvents = summary.lookback_event_count || 0;
    }
    
    // Priority 2: Extract from legacy format (fallback)
    if (legacyFormat) {
        if (finalCohortCount === 0) finalCohortCount = legacyFormat.cohort_size || 0;
        if (comprehensiveCohortSize === 0) comprehensiveCohortSize = legacyFormat.comprehensive_cohort_size || finalCohortCount;
        if (baseUsers === 0) baseUsers = legacyFormat.base_users || 0;
        if (lookbackUsers === 0) lookbackUsers = legacyFormat.lookback_users || 0;
        if (extendedCohortSize === 0) extendedCohortSize = legacyFormat.extended_cohort_size || 0;
        if (totalEvents === 0) totalEvents = legacyFormat.total_events || legacyFormat.event_count || 0;
        if (baseEvents === 0) baseEvents = legacyFormat.base_events || 0;
        if (lookbackEvents === 0) lookbackEvents = legacyFormat.lookback_events || 0;
        
        // Extract lookback days
        if (legacyFormat.metadata?.lookback_days_used) {
            lookbackDaysUsed = legacyFormat.metadata.lookback_days_used;
        }
        
        // Extract filter stats from legacy format
        if (legacyFormat.filter_stats) {
            const fs = legacyFormat.filter_stats;
            filteringSteps = fs.filtering_steps || [];
            baseCohortCount = fs.base_cohort_count || fs.extended_cohort_size || 0;
            if (finalCohortCount === 0) finalCohortCount = fs.final_cohort_count || fs.final_cohort_size || 0;
            if (extendedCohortSize === 0) extendedCohortSize = fs.extended_cohort_size || 0;
        }
    }
    
    // Priority 3: Extract from structured format (unified pipeline data)
    if (structuredFormat?.unified_pipeline_data) {
        const unifiedData = structuredFormat.unified_pipeline_data;
        
        // Extract from unified cohort data
        if (unifiedData.unified_cohort) {
            const cohortData = unifiedData.unified_cohort;
            if (finalCohortCount === 0) finalCohortCount = cohortData.size || 0;
            if (comprehensiveCohortSize === 0) comprehensiveCohortSize = finalCohortCount;
            if (baseUsers === 0) baseUsers = cohortData.base_size || 0;
            if (lookbackUsers === 0) lookbackUsers = cohortData.lookback_size || 0;
            if (extendedCohortSize === 0) extendedCohortSize = cohortData.extended_size || 0;
            
            // Extract metadata
            if (cohortData.metadata) {
                if (lookbackDaysUsed === 60) lookbackDaysUsed = cohortData.metadata.lookback_days_used || 60;
                if (extendedCohortSize === 0) extendedCohortSize = cohortData.metadata.extended_cohort_size || 0;
                
                // Extract filter stats from unified cohort metadata
                if (cohortData.metadata.filter_stats && filteringSteps.length === 0) {
                    const fs = cohortData.metadata.filter_stats;
                    filteringSteps = fs.filtering_steps || [];
                    if (baseCohortCount === 0) baseCohortCount = fs.base_cohort_count || fs.extended_cohort_size || 0;
                    if (extendedCohortSize === 0) extendedCohortSize = fs.extended_cohort_size || 0;
                }
            }
        }
        
        // Extract from unified events data
        if (unifiedData.unified_events) {
            const eventData = unifiedData.unified_events;
            if (totalEvents === 0) totalEvents = eventData.size || 0;
            if (baseEvents === 0) baseEvents = eventData.base_size || 0;
            if (lookbackEvents === 0) lookbackEvents = eventData.lookback_size || 0;
        }
        
        // Extract from categorized data
        if (unifiedData.categorized_data) {
            const catData = unifiedData.categorized_data;
            if (finalCohortCount === 0) finalCohortCount = catData.total_cohort_size || 0;
            if (baseUsers === 0) baseUsers = catData.base_cohort_size || 0;
            if (totalEvents === 0) totalEvents = catData.total_events_count || 0;
            if (baseEvents === 0) baseEvents = catData.base_events_count || 0;
        }
    }
    
    // Fallback: Extract from old structured format
    if (structuredFormat?.cohort_data && !structuredFormat?.unified_pipeline_data) {
        const cohortBreakdown = structuredFormat.cohort_analysis?.cohort_breakdown;
        if (cohortBreakdown) {
            if (comprehensiveCohortSize === 0) comprehensiveCohortSize = cohortBreakdown.total_users || 0;
            if (baseUsers === 0) baseUsers = cohortBreakdown.base_users || 0;
            if (lookbackUsers === 0) lookbackUsers = cohortBreakdown.lookback_users || 0;
        }
        
        const eventData = structuredFormat.event_data;
        if (eventData) {
            if (baseEvents === 0) baseEvents = eventData.base_size || 0;
            if (lookbackEvents === 0) lookbackEvents = eventData.lookback_size || 0;
            if (totalEvents === 0) totalEvents = baseEvents + lookbackEvents;
        }
    }
    
    // VALIDATION: Ensure math consistency
    const mathCheck = {
        userMathCorrect: (baseUsers + lookbackUsers) === comprehensiveCohortSize,
        eventMathCorrect: (baseEvents + lookbackEvents) === totalEvents,
        basePriorityApplied: baseUsers > 0 && lookbackUsers >= 0,
        extendedToFinalFlow: extendedCohortSize >= comprehensiveCohortSize
    };
    
    // Calculate percentages
    const calculatePercentage = (count, total) => {
        if (!total) return 0;
        return ((count / total) * 100).toFixed(1);
    };

    // Get event type breakdown if available
    const eventTypeBreakdown = legacyFormat?.event_type_breakdown || {};
    const getEventTypeStats = () => {
        if (!eventTypeBreakdown || Object.keys(eventTypeBreakdown).length === 0) {
            return null;
        }
        
        return Object.entries(eventTypeBreakdown).map(([eventType, count]) => ({
            eventType,
            count,
            percentage: calculatePercentage(count, finalCohortCount)
        }));
    };
    const eventTypeStats = getEventTypeStats();

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <div className="flex items-center gap-2 mb-6">
                <Filter size={24} className="text-blue-600 dark:text-blue-400" />
                <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                    Unified Cohort Pipeline
                </h2>
                {/* Math validation indicator */}
                {mathCheck.userMathCorrect && mathCheck.basePriorityApplied && (
                    <div className="ml-auto flex items-center gap-1 text-green-600 dark:text-green-400 text-sm">
                        <span>✓</span>
                        <span>Three-Phase Unified Approach</span>
                    </div>
                )}
            </div>

            {/* NEW: Three-Phase Pipeline Overview */}
            <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
                    <Database size={20} className="text-blue-600 dark:text-blue-400" />
                    Unified Three-Phase Approach
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-blue-600 dark:text-blue-400 mb-1">Phase 1: Extended Collection</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            Collect maximum dataset (+{lookbackDaysUsed} days lookback)
                        </div>
                        {extendedCohortSize > 0 && (
                            <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Extended cohort: {extendedCohortSize.toLocaleString()} users
                            </div>
                        )}
                    </div>
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-green-600 dark:text-green-400 mb-1">Phase 2: Unified Filtering</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            Apply all filters to extended dataset
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            Filtered cohort: {comprehensiveCohortSize.toLocaleString()} users
                        </div>
                    </div>
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-purple-600 dark:text-purple-400 mb-1">Phase 3: Categorization</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            Split by timeline with base priority
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            Base: {baseUsers.toLocaleString()}, Lookback: {lookbackUsers.toLocaleString()}
                        </div>
                    </div>
                </div>
            </div>

            {/* High-level Summary Cards with Base/Lookback Breakdown */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                {/* Final Cohort Size Card */}
                <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Users size={20} className="text-blue-600 dark:text-blue-400" />
                        <h3 className="font-semibold text-blue-900 dark:text-blue-200">Final Cohort Size</h3>
                    </div>
                    <p className="text-2xl font-bold text-blue-600 dark:text-blue-300">
                        {comprehensiveCohortSize.toLocaleString()}
                    </p>
                    <p className="text-sm text-blue-700 dark:text-blue-400">
                        Users after unified filtering
                    </p>
                    
                    {/* Base vs Lookback Breakdown */}
                    {(baseUsers > 0 || lookbackUsers > 0) && (
                        <div className="mt-3 pt-3 border-t border-blue-200 dark:border-blue-700">
                            <div className="text-xs font-medium text-blue-800 dark:text-blue-300 mb-2">
                                <Split size={12} className="inline mr-1" />
                                Timeline Categorization:
                            </div>
                            <div className="space-y-1">
                                <div className="flex justify-between text-xs">
                                    <span className="text-blue-600 dark:text-blue-400">
                                        <Target size={12} className="inline mr-1" />
                                        Base Period:
                                    </span>
                                    <span className="font-medium text-blue-700 dark:text-blue-300">
                                        {baseUsers.toLocaleString()} ({calculatePercentage(baseUsers, comprehensiveCohortSize)}%)
                                    </span>
                                </div>
                                <div className="flex justify-between text-xs">
                                    <span className="text-blue-500 dark:text-blue-500">
                                        <Clock size={12} className="inline mr-1" />
                                        Lookback Only:
                                    </span>
                                    <span className="text-blue-600 dark:text-blue-400">
                                        {lookbackUsers.toLocaleString()} ({calculatePercentage(lookbackUsers, comprehensiveCohortSize)}%)
                                    </span>
                                </div>
                                <div className="flex justify-between text-xs font-medium border-t border-blue-200 dark:border-blue-700 pt-1">
                                    <span className="text-blue-700 dark:text-blue-300">Total:</span>
                                    <span className="text-blue-700 dark:text-blue-300">
                                        {(baseUsers + lookbackUsers).toLocaleString()}
                                        {mathCheck.userMathCorrect ? ' ✓' : ' ⚠️'}
                                    </span>
                                </div>
                                {lookbackDaysUsed > 0 && (
                                    <div className="text-xs text-blue-500 dark:text-blue-500 mt-1">
                                        Extended by {lookbackDaysUsed} days
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </div>

                {/* Events in Cohort Card with Base/Lookback Breakdown */}
                <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Activity size={20} className="text-green-600 dark:text-green-400" />
                        <h3 className="font-semibold text-green-900 dark:text-green-200">Events in Cohort</h3>
                    </div>
                    <p className="text-2xl font-bold text-green-600 dark:text-green-300">
                        {totalEvents > 0 ? totalEvents.toLocaleString() : 'N/A'}
                    </p>
                    <p className="text-sm text-green-700 dark:text-green-400">
                        Total events analyzed
                    </p>
                    
                    {/* Base vs Lookback Event Breakdown */}
                    {(baseEvents > 0 || lookbackEvents > 0) && (
                        <div className="mt-3 pt-3 border-t border-green-200 dark:border-green-700">
                            <div className="text-xs font-medium text-green-800 dark:text-green-300 mb-2">
                                <Split size={12} className="inline mr-1" />
                                Event Timeline Categorization:
                            </div>
                            <div className="space-y-1">
                                <div className="flex justify-between text-xs">
                                    <span className="text-green-600 dark:text-green-400">
                                        <Target size={12} className="inline mr-1" />
                                        Base Period:
                                    </span>
                                    <span className="font-medium text-green-700 dark:text-green-300">
                                        {baseEvents.toLocaleString()} ({calculatePercentage(baseEvents, totalEvents)}%)
                                    </span>
                                </div>
                                <div className="flex justify-between text-xs">
                                    <span className="text-green-500 dark:text-green-500">
                                        <Clock size={12} className="inline mr-1" />
                                        Lookback Only:
                                    </span>
                                    <span className="text-green-600 dark:text-green-400">
                                        {lookbackEvents.toLocaleString()} ({calculatePercentage(lookbackEvents, totalEvents)}%)
                                    </span>
                                </div>
                                <div className="flex justify-between text-xs font-medium border-t border-green-200 dark:border-green-700 pt-1">
                                    <span className="text-green-700 dark:text-green-300">Total:</span>
                                    <span className="text-green-700 dark:text-green-300">
                                        {(baseEvents + lookbackEvents).toLocaleString()}
                                        {mathCheck.eventMathCorrect ? ' ✓' : ' ⚠️'}
                                    </span>
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                {/* Filter Efficiency Card */}
                <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                    <div className="flex items-center gap-2 mb-2">
                        <TrendingDown size={20} className="text-purple-600 dark:text-purple-400" />
                        <h3 className="font-semibold text-purple-900 dark:text-purple-200">Filter Efficiency</h3>
                    </div>
                    <p className="text-2xl font-bold text-purple-600 dark:text-purple-300">
                        {extendedCohortSize > 0 ? calculatePercentage(comprehensiveCohortSize, extendedCohortSize) : '0'}%
                    </p>
                    <p className="text-sm text-purple-700 dark:text-purple-400">
                        Users retained from extended cohort
                    </p>
                    
                    {/* Filtering Summary */}
                    {extendedCohortSize > 0 && (
                        <div className="mt-3 pt-3 border-t border-purple-200 dark:border-purple-700">
                            <div className="space-y-1 text-xs">
                                <div className="flex justify-between">
                                    <span className="text-purple-600 dark:text-purple-400">Extended cohort:</span>
                                    <span className="text-purple-700 dark:text-purple-300">{extendedCohortSize.toLocaleString()}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span className="text-purple-600 dark:text-purple-400">Filtered out:</span>
                                    <span className="text-purple-700 dark:text-purple-300">{(extendedCohortSize - comprehensiveCohortSize).toLocaleString()}</span>
                                </div>
                                <div className="flex justify-between font-medium border-t border-purple-200 dark:border-purple-700 pt-1">
                                    <span className="text-purple-700 dark:text-purple-300">Final:</span>
                                    <span className="text-purple-700 dark:text-purple-300">{comprehensiveCohortSize.toLocaleString()}</span>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* NEW: Unified Pipeline Flow Visualization */}
            {filteringSteps && filteringSteps.length > 0 && (
                <div className="mb-6">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                        Unified Pipeline Flow
                    </h3>
                    
                    {/* Visual Flow showing Extended Collection → Filtering → Categorization */}
                    <div className="flex flex-col md:flex-row items-center justify-between mb-4 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg overflow-x-auto">
                        {/* Phase 1: Extended Collection */}
                        <div className="text-center min-w-0 flex-shrink-0">
                            <div className="text-sm text-blue-600 dark:text-blue-400 mb-1 font-medium">
                                Extended Collection
                            </div>
                            <div className="text-xl font-bold text-gray-900 dark:text-white mb-1">
                                {extendedCohortSize > 0 ? extendedCohortSize.toLocaleString() : 'N/A'}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">
                                Base + {lookbackDaysUsed} days
                            </div>
                        </div>
                        
                        <div className="hidden md:block text-gray-400 mx-2">→</div>
                        
                        {/* Phase 2: Filtering Steps */}
                        {filteringSteps.slice(1).map((step, index) => {
                            const usersRemaining = step.users_after || step.users_count || 0;
                            const isLastFilterStep = index === filteringSteps.slice(1).length - 1;
                            
                            return (
                                <React.Fragment key={index}>
                                    <div className="text-center min-w-0 flex-shrink-0">
                                        <div className="text-sm text-green-600 dark:text-green-400 mb-1">
                                            {step.step_name || step.step}
                                        </div>
                                        <div className="text-xl font-bold text-gray-900 dark:text-white mb-1">
                                            {usersRemaining.toLocaleString()}
                                        </div>
                                        <div className="text-xs text-gray-500 dark:text-gray-400">
                                            {extendedCohortSize > 0 ? 
                                                calculatePercentage(usersRemaining, extendedCohortSize) : '0'
                                            }% retained
                                        </div>
                                    </div>
                                    
                                    {!isLastFilterStep && (
                                        <div className="hidden md:block text-gray-400 mx-2">→</div>
                                    )}
                                </React.Fragment>
                            );
                        })}
                        
                        <div className="hidden md:block text-gray-400 mx-2">→</div>
                        
                        {/* Phase 3: Categorization */}
                        <div className="text-center min-w-0 flex-shrink-0">
                            <div className="text-sm text-purple-600 dark:text-purple-400 mb-1 font-medium">
                                Timeline Categorization
                            </div>
                            <div className="text-lg font-bold text-gray-900 dark:text-white mb-1">
                                {baseUsers.toLocaleString()} + {lookbackUsers.toLocaleString()}
                            </div>
                            <div className="text-xs space-y-0.5">
                                <div className="text-blue-600 dark:text-blue-400">
                                    Base: {calculatePercentage(baseUsers, comprehensiveCohortSize)}%
                                </div>
                                <div className="text-green-600 dark:text-green-400">
                                    Lookback: {calculatePercentage(lookbackUsers, comprehensiveCohortSize)}%
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Detailed Table with Updated Structure */}
                    <div className="overflow-x-auto">
                        <table className="min-w-full text-sm border dark:border-gray-600">
                            <thead className="bg-gray-50 dark:bg-gray-700">
                                <tr>
                                    <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                        Pipeline Step
                                    </th>
                                    <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                        Description
                                    </th>
                                    <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                        Users After Step
                                    </th>
                                    <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                        Users Filtered
                                    </th>
                                    <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                        % Retained
                                    </th>
                                    <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                        Phase
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="bg-white dark:bg-gray-800">
                                {filteringSteps.map((step, index) => {
                                    const usersRemaining = step.users_after || step.users_count || 0;
                                    const usersFiltered = step.users_filtered || 0;
                                    const stepName = step.step_name || step.step || '';
                                    
                                    // Determine phase based on step name and type
                                    let phaseLabel = 'Filtering';
                                    let phaseColor = 'text-green-600 dark:text-green-400';
                                    
                                    if (stepName.includes('extended_cohort_selection')) {
                                        phaseLabel = 'Extended Collection';
                                        phaseColor = 'text-blue-600 dark:text-blue-400';
                                    } else if (stepName === 'Timeline Split' || stepName.includes('timeline_split')) {
                                        phaseLabel = 'Categorization';
                                        phaseColor = 'text-purple-600 dark:text-purple-400';
                                    }
                                    // All other steps (primary_user_filter, secondary_filters, product_filtering, orphan_removal) are "Filtering"
                                    
                                    return (
                                        <tr key={index} className={index % 2 === 0 ? '' : 'bg-gray-50 dark:bg-gray-700'}>
                                            <td className="p-3 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">
                                                {step.step_name || step.step}
                                            </td>
                                            <td className="p-3 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {step.description}
                                            </td>
                                            <td className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300 font-medium">
                                                {usersRemaining.toLocaleString()}
                                            </td>
                                            <td className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                                {stepName.includes('extended_cohort_selection') ? 'N/A' : usersFiltered.toLocaleString()}
                                            </td>
                                            <td className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                                {extendedCohortSize > 0 ? calculatePercentage(usersRemaining, extendedCohortSize) : '0'}%
                                            </td>
                                            <td className={`p-3 border-b dark:border-gray-600 text-center font-medium ${phaseColor}`}>
                                                {phaseLabel}
                                            </td>
                                        </tr>
                                    );
                                })}
                                
                                {/* Add categorization breakdown row */}
                                {(baseUsers > 0 || lookbackUsers > 0) && (
                                    <tr className="bg-purple-50 dark:bg-purple-900/20">
                                        <td className="p-3 border-b dark:border-gray-600 font-medium text-purple-700 dark:text-purple-300">
                                            Timeline Split
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-purple-700 dark:text-purple-300">
                                            Base period: {baseUsers.toLocaleString()} users, Lookback only: {lookbackUsers.toLocaleString()} users
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center text-purple-700 dark:text-purple-300 font-medium">
                                            {(baseUsers + lookbackUsers).toLocaleString()}
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center text-purple-700 dark:text-purple-300">
                                            Split only
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center text-purple-700 dark:text-purple-300">
                                            100%
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center font-medium text-purple-600 dark:text-purple-400">
                                            Categorization
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/* Event Type Breakdown */}
            {eventTypeStats && (
                <div className="mb-6">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                        Unique Users by Event Type
                    </h3>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {eventTypeStats.map(({ eventType, count, percentage }) => (
                            <div key={eventType} className="p-4 bg-gray-50 dark:bg-gray-700 rounded-lg border">
                                <div className="flex items-center justify-between mb-2">
                                    <h4 className="font-medium text-gray-900 dark:text-white text-sm">
                                        {eventType.replace('RC ', '')}
                                    </h4>
                                    <span className="text-xs text-gray-500 dark:text-gray-400">
                                        {percentage}%
                                    </span>
                                </div>
                                <p className="text-xl font-bold text-gray-700 dark:text-gray-300">
                                    {count.toLocaleString()}
                                </p>
                                <div className="mt-2 bg-gray-200 dark:bg-gray-600 rounded-full h-2">
                                    <div 
                                        className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                                        style={{ width: `${Math.min(percentage, 100)}%` }}
                                    />
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* No Data Message */}
            {(!filteringSteps || filteringSteps.length === 0) && !eventTypeStats && (
                <div className="text-center py-8 bg-gray-50 dark:bg-gray-700 rounded-lg">
                    <Filter size={48} className="mx-auto text-gray-400 mb-4" />
                    <p className="text-gray-600 dark:text-gray-400 mb-2">
                        No detailed filtering statistics available
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-500">
                        Run a full analysis to see the complete unified pipeline breakdown
                    </p>
                </div>
            )}
        </div>
    );
};

export default CohortFilteringStats; 