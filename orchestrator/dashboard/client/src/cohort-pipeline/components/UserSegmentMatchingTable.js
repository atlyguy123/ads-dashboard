import React, { useState, useMemo } from 'react';
import { ChevronDown, ChevronRight, User, Target, TrendingUp, Info } from 'lucide-react';

const UserSegmentMatchingTable = ({ data, loading = false, error = null }) => {
    const [expandedUsers, setExpandedUsers] = useState(new Set());
    const [sortBy, setSortBy] = useState('conversion_rate');
    const [sortOrder, setSortOrder] = useState('desc');
    const [filterAccuracy, setFilterAccuracy] = useState('all');

    // Extract stage 2 data from pipeline results
    const stage2Data = data?.stage_results?.stage2;
    const usersWithSegments = stage2Data?.users_with_segments || [];
    const matchingStats = stage2Data?.matching_stats || {};

    // Process and sort user data
    const processedUsers = useMemo(() => {
        if (!usersWithSegments.length) return [];

        return usersWithSegments
            .filter(user => {
                if (!user.segment_match_result?.success) return false;
                if (filterAccuracy === 'all') return true;
                return user.segment_match_result.accuracy_score === filterAccuracy;
            })
            .map(user => {
                const result = user.segment_match_result;
                return {
                    ...user,
                    conversion_rate: result.conversion_metrics?.trial_conversion_rate || 0,
                    refund_rate: result.conversion_metrics?.trial_converted_to_refund_rate || 0,
                    cohort_size: result.conversion_metrics?.trial_started_count || 0,
                    accuracy_score: result.accuracy_score || 'unknown'
                };
            })
            .sort((a, b) => {
                const aVal = a[sortBy] || 0;
                const bVal = b[sortBy] || 0;
                return sortOrder === 'desc' ? bVal - aVal : aVal - bVal;
            });
    }, [usersWithSegments, sortBy, sortOrder, filterAccuracy]);

    const toggleUserExpansion = (userId) => {
        const newExpanded = new Set(expandedUsers);
        if (newExpanded.has(userId)) {
            newExpanded.delete(userId);
        } else {
            newExpanded.add(userId);
        }
        setExpandedUsers(newExpanded);
    };

    const getAccuracyColor = (accuracy) => {
        const colors = {
            'very_high': 'text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-900/30',
            'high': 'text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/30',
            'medium': 'text-yellow-600 dark:text-yellow-400 bg-yellow-50 dark:bg-yellow-900/30',
            'low': 'text-orange-600 dark:text-orange-400 bg-orange-50 dark:bg-orange-900/30',
            'very_low': 'text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/30'
        };
        return colors[accuracy] || 'text-gray-600 dark:text-gray-400 bg-gray-50 dark:bg-gray-900/30';
    };

    const formatPercentage = (value) => {
        return `${(value * 100).toFixed(1)}%`;
    };

    const formatCurrency = (value) => {
        return `$${value.toFixed(2)}`;
    };

    const PropertyDisplay = ({ label, value, className = "" }) => (
        <div className={`flex justify-between text-sm ${className}`}>
            <span className="text-gray-600 dark:text-gray-400">{label}:</span>
            <span className="font-medium text-gray-900 dark:text-white">{value || 'N/A'}</span>
        </div>
    );

    const RollupChainDisplay = ({ rollupChain }) => {
        if (!rollupChain || rollupChain.length <= 1) return null;

        return (
            <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/30 rounded-lg border border-blue-200 dark:border-blue-700">
                <h5 className="text-sm font-semibold text-blue-800 dark:text-blue-200 mb-2 flex items-center">
                    <Target className="h-4 w-4 mr-1" />
                    Rollup Chain Applied
                </h5>
                <div className="space-y-2">
                    {rollupChain.map((segment, index) => (
                        <div key={index} className="flex items-center text-sm">
                            <div className={`w-2 h-2 rounded-full mr-2 ${
                                segment.is_viable ? 'bg-green-500' : 'bg-gray-400'
                            }`} />
                            <span className="text-gray-700 dark:text-gray-300">
                                Level {segment.level} - {segment.accuracy_score} accuracy
                                {index < rollupChain.length - 1 && (
                                    <ChevronRight className="inline h-3 w-3 mx-1" />
                                )}
                            </span>
                        </div>
                    ))}
                </div>
            </div>
        );
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500"></div>
                <span className="ml-3 text-gray-600 dark:text-gray-400">Loading segment matching results...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-red-800 dark:text-red-200 mb-2">
                    Segment Matching Error
                </h3>
                <p className="text-red-600 dark:text-red-300">{error}</p>
            </div>
        );
    }

    if (!processedUsers.length) {
        return (
            <div className="text-center py-8 text-gray-600 dark:text-gray-400">
                <User className="h-12 w-12 mx-auto mb-3 opacity-50" />
                <p>No users successfully matched to segments</p>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            {/* Summary Statistics */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {matchingStats.successful_matches || 0}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Successful Matches</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {formatPercentage(matchingStats.success_rate || 0)}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Success Rate</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {matchingStats.rollups_applied || 0}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Rollups Applied</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                        {formatPercentage(matchingStats.rollup_rate || 0)}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">Rollup Rate</div>
                </div>
            </div>

            {/* Filters and Sorting */}
            <div className="flex flex-wrap gap-4 items-center justify-between p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                <div className="flex items-center space-x-4">
                    <select
                        value={filterAccuracy}
                        onChange={(e) => setFilterAccuracy(e.target.value)}
                        className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    >
                        <option value="all">All Accuracy Levels</option>
                        <option value="very_high">Very High</option>
                        <option value="high">High</option>
                        <option value="medium">Medium</option>
                        <option value="low">Low</option>
                    </select>
                </div>
                
                <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600 dark:text-gray-400">Sort by:</span>
                    <select
                        value={sortBy}
                        onChange={(e) => setSortBy(e.target.value)}
                        className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    >
                        <option value="conversion_rate">Conversion Rate</option>
                        <option value="refund_rate">Refund Rate</option>
                        <option value="cohort_size">Cohort Size</option>
                        <option value="accuracy_score">Accuracy</option>
                    </select>
                    <button
                        onClick={() => setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc')}
                        className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white hover:bg-gray-50 dark:hover:bg-gray-600"
                    >
                        {sortOrder === 'desc' ? '↓' : '↑'}
                    </button>
                </div>
            </div>

            {/* Users Table */}
            <div className="space-y-2">
                {processedUsers.map((user) => {
                    const isExpanded = expandedUsers.has(user.user_id);
                    const result = user.segment_match_result;
                    
                    return (
                        <div key={user.user_id} className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                            {/* User Summary Row */}
                            <div 
                                className="p-4 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer"
                                onClick={() => toggleUserExpansion(user.user_id)}
                            >
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center space-x-3">
                                        {isExpanded ? (
                                            <ChevronDown className="h-5 w-5 text-gray-400" />
                                        ) : (
                                            <ChevronRight className="h-5 w-5 text-gray-400" />
                                        )}
                                        <User className="h-5 w-5 text-blue-500" />
                                        <span className="font-medium text-gray-900 dark:text-white">
                                            {user.user_id.slice(0, 8)}...
                                        </span>
                                    </div>
                                    
                                    <div className="flex items-center space-x-4">
                                        <div className="text-right">
                                            <div className="text-sm font-semibold text-gray-900 dark:text-white">
                                                {formatPercentage(user.conversion_rate)}
                                            </div>
                                            <div className="text-xs text-gray-500 dark:text-gray-400">Conversion Rate</div>
                                        </div>
                                        
                                        <div className="text-right">
                                            <div className="text-sm font-semibold text-gray-900 dark:text-white">
                                                {formatPercentage(user.refund_rate)}
                                            </div>
                                            <div className="text-xs text-gray-500 dark:text-gray-400">Refund Rate</div>
                                        </div>
                                        
                                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${getAccuracyColor(user.accuracy_score)}`}>
                                            {user.accuracy_score.replace('_', ' ')}
                                        </span>
                                        
                                        {result.rollup_applied && (
                                            <div className="flex items-center text-blue-600 dark:text-blue-400">
                                                <Target className="h-4 w-4 mr-1" />
                                                <span className="text-xs">Rollup</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>

                            {/* Expanded Details */}
                            {isExpanded && (
                                <div className="border-t border-gray-200 dark:border-gray-700 p-4 bg-gray-50 dark:bg-gray-900">
                                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                                        {/* User Properties */}
                                        <div>
                                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                                                <User className="h-4 w-4 mr-1" />
                                                User Properties
                                            </h4>
                                            <div className="space-y-2 p-3 bg-white dark:bg-gray-800 rounded-lg">
                                                <PropertyDisplay label="Product ID" value={user.user_properties.product_id} />
                                                <PropertyDisplay label="App Store" value={user.user_properties.app_store} />
                                                <PropertyDisplay label="Price Bucket" value={user.user_properties.price_bucket} />
                                                <PropertyDisplay label="Economic Tier" value={user.user_properties.economic_tier} />
                                                <PropertyDisplay label="Country" value={user.user_properties.country} />
                                                <PropertyDisplay label="Region" value={user.user_properties.region} />
                                            </div>
                                        </div>

                                        {/* Matched Segment */}
                                        <div>
                                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                                                <Target className="h-4 w-4 mr-1" />
                                                Final Segment ({result.rollup_applied ? 'After Rollup' : 'Direct Match'})
                                            </h4>
                                            <div className="space-y-2 p-3 bg-white dark:bg-gray-800 rounded-lg">
                                                <PropertyDisplay 
                                                    label="Segment ID" 
                                                    value={`${result.final_viable_segment_id?.slice(0, 8)}...`} 
                                                />
                                                <PropertyDisplay 
                                                    label="Accuracy Score" 
                                                    value={<span className={`px-2 py-1 rounded text-xs ${getAccuracyColor(result.accuracy_score)}`}>
                                                        {result.accuracy_score.replace('_', ' ')}
                                                    </span>}
                                                />
                                                <PropertyDisplay 
                                                    label="Cohort Size" 
                                                    value={result.conversion_metrics?.trial_started_count} 
                                                />
                                            </div>
                                        </div>

                                        {/* Conversion Metrics */}
                                        <div>
                                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                                                <TrendingUp className="h-4 w-4 mr-1" />
                                                Conversion Probabilities
                                            </h4>
                                            <div className="space-y-2 p-3 bg-white dark:bg-gray-800 rounded-lg">
                                                <PropertyDisplay 
                                                    label="Trial Conversion" 
                                                    value={formatPercentage(result.conversion_metrics?.trial_conversion_rate || 0)} 
                                                />
                                                <PropertyDisplay 
                                                    label="Refund Rate" 
                                                    value={formatPercentage(result.conversion_metrics?.trial_converted_to_refund_rate || 0)} 
                                                />
                                                <PropertyDisplay 
                                                    label="Initial Purchase Refunds" 
                                                    value={formatPercentage(result.conversion_metrics?.initial_purchase_to_refund_rate || 0)} 
                                                />
                                                <PropertyDisplay 
                                                    label="Renewal Refunds" 
                                                    value={formatPercentage(result.conversion_metrics?.renewal_to_refund_rate || 0)} 
                                                />
                                                <PropertyDisplay 
                                                    label="Trial Conversions" 
                                                    value={`${result.conversion_metrics?.trial_converted_count || 0} / ${result.conversion_metrics?.trial_started_count || 0}`} 
                                                />
                                            </div>
                                        </div>
                                    </div>

                                    {/* Rollup Chain */}
                                    <RollupChainDisplay rollupChain={result.rollup_chain} />
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
};

export default UserSegmentMatchingTable; 