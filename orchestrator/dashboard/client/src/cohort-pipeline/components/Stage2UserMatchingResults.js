import React from 'react';
import { Target, Users, Activity, TrendingUp, CheckCircle, AlertCircle, Database, Star, Layers, Award, Info } from 'lucide-react';

const Stage2UserMatchingResults = ({ data }) => {
    // Handle multiple API response formats for data access
    let stage2Data = null;
    
    if (data?.stage_results?.stage2) {
        // Direct format: pipelineData.stage_results.stage2
        stage2Data = data.stage_results.stage2;
    } else if (data?.data?.stage_results?.stage2) {
        // Nested format: pipelineData.data.stage_results.stage2
        stage2Data = data.data.stage_results.stage2;
    }

    if (!stage2Data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No Stage 2 data available. Run analysis to see user segment matching results.
                </p>
            </div>
        );
    }

    const matchingStats = stage2Data.matching_stats || {};
    const usersWithSegments = stage2Data.users_with_segments || [];

    // Helper function to format numbers
    const formatNumber = (num) => {
        if (typeof num !== 'number') return '0';
        return num.toLocaleString();
    };

    // Helper function to format percentages
    const formatPercentage = (num, total) => {
        if (!total || typeof num !== 'number') return '0.0%';
        return ((num / total) * 100).toFixed(1) + '%';
    };

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <div className="flex items-center gap-2 mb-6">
                <Target size={24} className="text-green-600 dark:text-green-400" />
                <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                    Stage 2: User-to-Segment Matching Results
                </h2>
                <div className="ml-auto flex items-center gap-1 text-green-600 dark:text-green-400 text-sm">
                    <span>✓</span>
                    <span>Conversion Probability Segment Matching</span>
                </div>
            </div>

            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
                    <div className="flex items-center gap-2 mb-2">
                        <CheckCircle size={20} className="text-green-600 dark:text-green-400" />
                        <h3 className="font-semibold text-green-900 dark:text-green-200">Successful Matches</h3>
                    </div>
                    <p className="text-2xl font-bold text-green-600 dark:text-green-300">
                        {formatNumber(matchingStats.successful_matches || 0)}
                    </p>
                    <p className="text-sm text-green-700 dark:text-green-400">
                        {formatPercentage(matchingStats.successful_matches, matchingStats.total_users)} success rate
                    </p>
                </div>

                <div className="p-4 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
                    <div className="flex items-center gap-2 mb-2">
                        <AlertCircle size={20} className="text-red-600 dark:text-red-400" />
                        <h3 className="font-semibold text-red-900 dark:text-red-200">Failed Matches</h3>
                    </div>
                    <p className="text-2xl font-bold text-red-600 dark:text-red-300">
                        {formatNumber(matchingStats.failed_matches || 0)}
                    </p>
                    <p className="text-sm text-red-700 dark:text-red-400">
                        No viable segments found
                    </p>
                </div>

                <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Layers size={20} className="text-blue-600 dark:text-blue-400" />
                        <h3 className="font-semibold text-blue-900 dark:text-blue-200">Rollups Applied</h3>
                    </div>
                    <p className="text-2xl font-bold text-blue-600 dark:text-blue-300">
                        {formatNumber(matchingStats.rollups_applied || 0)}
                    </p>
                    <p className="text-sm text-blue-700 dark:text-blue-400">
                        {formatPercentage(matchingStats.rollups_applied, matchingStats.successful_matches)} rollup rate
                    </p>
                </div>

                <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                    <div className="flex items-center gap-2 mb-2">
                        <Award size={20} className="text-purple-600 dark:text-purple-400" />
                        <h3 className="font-semibold text-purple-900 dark:text-purple-200">Average Accuracy</h3>
                    </div>
                    <p className="text-2xl font-bold text-purple-600 dark:text-purple-300">
                        {Object.keys(matchingStats.accuracy_distribution || {}).length > 0 ? 
                            Object.keys(matchingStats.accuracy_distribution)[0] : 'N/A'}
                    </p>
                    <p className="text-sm text-purple-700 dark:text-purple-400">
                        Prediction quality score
                    </p>
                </div>
            </div>

            {/* Economic Tier Explanation */}
            <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
                    <Database size={20} className="text-blue-600 dark:text-blue-400" />
                    Economic Tier Classification System
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-green-600 dark:text-green-400 mb-1">Tier 1: High Income</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            US, CA, UK, AU, DE, FR, etc.
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            World Bank high-income classification
                        </div>
                    </div>
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-yellow-600 dark:text-yellow-400 mb-1">Tier 2: Medium Income</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            BR, MX, TR, RU, AR, etc.
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            World Bank upper-middle-income
                        </div>
                    </div>
                    <div className="p-3 bg-white dark:bg-gray-800 rounded border">
                        <div className="font-medium text-red-600 dark:text-red-400 mb-1">Tier 3: Low Income</div>
                        <div className="text-gray-700 dark:text-gray-300">
                            IN, PH, VN, ID, EG, etc.
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                            World Bank lower-middle & low-income
                        </div>
                    </div>
                </div>
            </div>

            {/* Individual User Matching Results */}
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                    Individual User Segment Matching Results
                </h3>
                
                {/* Explanation Section */}
                <div className="mb-4 p-4 bg-gradient-to-r from-yellow-50 to-orange-50 dark:from-yellow-900/20 dark:to-orange-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
                    <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
                        <Info size={16} className="text-yellow-600 dark:text-yellow-400" />
                        How to Read This Table
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-gray-700 dark:text-gray-300">
                        <div>
                            <div className="font-medium text-blue-600 dark:text-blue-400 mb-1">📊 4 Key Metrics Column:</div>
                            <ul className="text-xs space-y-1 ml-4">
                                <li><strong>Trial Conv:</strong> % who convert from trial to paid</li>
                                <li><strong>Refund:</strong> % who refund after trial conversion</li>
                                <li><strong>Init. Refund:</strong> % who refund initial purchase</li>
                                <li><strong>Renewal Refund:</strong> % who refund renewals</li>
                                <li><strong>Sample:</strong> Number of users in this segment</li>
                            </ul>
                        </div>
                        <div>
                            <div className="font-medium text-green-600 dark:text-green-400 mb-1">🎯 Match Type Column:</div>
                            <ul className="text-xs space-y-1 ml-4">
                                <li><strong>Direct:</strong> Exact match found at user's specific level</li>
                                <li><strong>Applied (X steps):</strong> Had to use more general segment</li>
                                <li>More steps = less specific match = lower accuracy</li>
                                <li>Rollup helps when exact match has too few users</li>
                            </ul>
                        </div>
                    </div>
                </div>
                
                <div className="overflow-x-auto">
                    <table className="min-w-full text-sm border dark:border-gray-600">
                        <thead className="bg-gray-50 dark:bg-gray-700">
                            <tr>
                                <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                    User Distinct ID
                                </th>
                                <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                    User Properties
                                </th>
                                <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                    Matched Segment
                                </th>
                                <th className="p-3 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">
                                    Target Segment (if rollup)
                                </th>
                                <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                    4 Key Metrics (Trial, Refunds)
                                </th>
                                <th className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">
                                    Prediction Accuracy
                                </th>
                            </tr>
                        </thead>
                        <tbody className="bg-white dark:bg-gray-800">
                            {usersWithSegments.slice(0, 20).map((user, index) => {
                                const match = user.segment_match_result || {};
                                const properties = user.user_properties || {};
                                
                                // Get rollup chain information for segment properties
                                const rollupChain = match.rollup_chain || [];
                                const initialSegment = rollupChain.length > 0 ? rollupChain[0] : null;
                                const finalSegment = rollupChain.length > 0 ? rollupChain[rollupChain.length - 1] : null;
                                const hasRollup = match.rollup_applied && rollupChain.length > 1;
                                
                                return (
                                    <tr key={user.user_id} className={index % 2 === 0 ? '' : 'bg-gray-50 dark:bg-gray-700'}>
                                        <td className="p-3 border-b dark:border-gray-600 font-mono text-xs text-gray-700 dark:text-gray-300">
                                            <div className="max-w-[200px] break-all">
                                                {user.user_id}
                                            </div>
                                            <div className="text-gray-500 dark:text-gray-400 text-xs mt-1">
                                                Short: {user.user_id.substring(0, 8)}...
                                            </div>
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                            <div className="space-y-1 text-xs">
                                                <div><strong>Product:</strong> {properties.product_id || 'N/A'}</div>
                                                <div><strong>Store:</strong> {properties.app_store || 'N/A'}</div>
                                                <div><strong>Price:</strong> {properties.price_bucket || 'N/A'}</div>
                                                <div><strong>Country:</strong> {properties.country || 'N/A'}</div>
                                                <div><strong>Economic Tier:</strong> {properties.economic_tier || 'N/A'}</div>
                                                <div><strong>Region:</strong> {properties.region || 'N/A'}</div>
                                            </div>
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                            {match.success && initialSegment ? (
                                                <div className="space-y-1 text-xs">
                                                    <div className="font-medium text-blue-600 dark:text-blue-400 mb-2">
                                                        ID: {match.matched_segment_id?.substring(0, 8) || 'Unknown'}...
                                                    </div>
                                                    <div><strong>Product:</strong> {initialSegment.properties?.product_id || 'N/A'}</div>
                                                    <div><strong>Store:</strong> {initialSegment.properties?.app_store || 'N/A'}</div>
                                                    <div><strong>Price:</strong> {initialSegment.properties?.price_bucket || 'N/A'}</div>
                                                    <div><strong>Country:</strong> {initialSegment.properties?.country || 'N/A'}</div>
                                                    <div><strong>Economic Tier:</strong> {initialSegment.properties?.economic_tier || 'N/A'}</div>
                                                    <div><strong>Region:</strong> {initialSegment.properties?.region || 'N/A'}</div>
                                                    <div className="pt-1 border-t border-gray-200 dark:border-gray-600">
                                                        <strong>Users:</strong> {initialSegment.cohort_size || 0}
                                                    </div>
                                                    <div><strong>Level:</strong> {initialSegment.level || 'N/A'}</div>
                                                </div>
                                            ) : match.success ? (
                                                <div className="space-y-1 text-xs">
                                                    <div className="font-medium text-blue-600 dark:text-blue-400 mb-2">
                                                        ID: {match.matched_segment_id?.substring(0, 8) || 'Unknown'}...
                                                    </div>
                                                    <div className="text-gray-500">Segment properties not available</div>
                                                    {match.conversion_metrics && (
                                                        <div className="pt-1 border-t border-gray-200 dark:border-gray-600">
                                                            <strong>Users:</strong> {match.conversion_metrics.trial_started_count || 0}
                                                        </div>
                                                    )}
                                                </div>
                                            ) : (
                                                <span className="text-red-600 dark:text-red-400 text-xs">No match found</span>
                                            )}
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                            {hasRollup && finalSegment ? (
                                                <div className="space-y-1 text-xs">
                                                    <div className="font-medium text-green-600 dark:text-green-400 mb-2">
                                                        ID: {match.final_viable_segment_id?.substring(0, 8) || 'Unknown'}...
                                                    </div>
                                                    <div><strong>Product:</strong> {finalSegment.properties?.product_id || 'N/A'}</div>
                                                    <div><strong>Store:</strong> {finalSegment.properties?.app_store || 'N/A'}</div>
                                                    <div><strong>Price:</strong> {finalSegment.properties?.price_bucket || 'N/A'}</div>
                                                    <div><strong>Country:</strong> {finalSegment.properties?.country || 'N/A'}</div>
                                                    <div><strong>Economic Tier:</strong> {finalSegment.properties?.economic_tier || 'N/A'}</div>
                                                    <div><strong>Region:</strong> {finalSegment.properties?.region || 'N/A'}</div>
                                                    <div className="pt-1 border-t border-gray-200 dark:border-gray-600">
                                                        <strong>Users:</strong> {finalSegment.cohort_size || 0}
                                                    </div>
                                                    <div><strong>Level:</strong> {finalSegment.level || 'N/A'}</div>
                                                </div>
                                            ) : (
                                                <div className="text-xs text-gray-400">
                                                    {match.success ? 'Direct match - no rollup needed' : ''}
                                                </div>
                                            )}
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                            {match.success ? (
                                                <div className="text-xs">
                                                    <div className="space-y-1">
                                                        <div>
                                                            <div className="font-bold text-blue-600 dark:text-blue-400 text-sm">
                                                                {((match.conversion_metrics?.trial_conversion_rate || 0) * 100).toFixed(1)}%
                                                            </div>
                                                            <div className="text-gray-500 dark:text-gray-400 text-xs">Trial Conv.</div>
                                                        </div>
                                                        <div>
                                                            <div className="font-medium text-orange-600 dark:text-orange-400 text-xs">
                                                                {((match.conversion_metrics?.trial_converted_to_refund_rate || 0) * 100).toFixed(1)}%
                                                            </div>
                                                            <div className="text-gray-500 dark:text-gray-400 text-xs">Refund</div>
                                                        </div>
                                                        <div>
                                                            <div className="font-medium text-red-600 dark:text-red-400 text-xs">
                                                                {((match.conversion_metrics?.initial_purchase_to_refund_rate || 0) * 100).toFixed(1)}%
                                                            </div>
                                                            <div className="text-gray-500 dark:text-gray-400 text-xs">Init. Refund</div>
                                                        </div>
                                                        <div>
                                                            <div className="font-medium text-purple-600 dark:text-purple-400 text-xs">
                                                                {((match.conversion_metrics?.renewal_to_refund_rate || 0) * 100).toFixed(1)}%
                                                            </div>
                                                            <div className="text-gray-500 dark:text-gray-400 text-xs">Renewal Refund</div>
                                                        </div>
                                                        <div className="pt-1 border-t border-gray-200 dark:border-gray-600">
                                                            <div className="text-gray-500 dark:text-gray-400 text-xs">
                                                                Sample: {match.conversion_metrics?.trial_started_count || 0}
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            ) : (
                                                <span className="text-gray-400 text-xs">-</span>
                                            )}
                                        </td>
                                        <td className="p-3 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                            {match.success ? (
                                                <div className="flex items-center justify-center gap-1">
                                                    <Star size={12} className="text-yellow-500" />
                                                    <span className={`text-xs font-medium px-2 py-1 rounded ${
                                                        match.accuracy_score === 'very_high' ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200' :
                                                        match.accuracy_score === 'high' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200' :
                                                        match.accuracy_score === 'medium' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200' :
                                                        match.accuracy_score === 'low' ? 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200' :
                                                        'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
                                                    }`}>
                                                        {match.accuracy_score || 'Unknown'}
                                                    </span>
                                                </div>
                                            ) : (
                                                <span className="text-gray-400 text-xs">-</span>
                                            )}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>

                {usersWithSegments.length > 20 && (
                    <div className="mt-4 text-center text-sm text-gray-600 dark:text-gray-400 bg-gray-50 dark:bg-gray-700 p-3 rounded">
                        Showing first 20 users of {usersWithSegments.length} total users
                    </div>
                )}
            </div>

            {/* Stage 2 Error Display */}
            {stage2Data.error && (
                <div className="text-center py-8 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
                    <AlertCircle size={48} className="mx-auto text-red-400 mb-4" />
                    <p className="text-red-600 dark:text-red-400 mb-2">
                        Stage 2 Error: {stage2Data.error}
                    </p>
                </div>
            )}

            {/* No Data Message */}
            {usersWithSegments.length === 0 && !stage2Data.error && (
                <div className="text-center py-8 bg-gray-50 dark:bg-gray-700 rounded-lg">
                    <Target size={48} className="mx-auto text-gray-400 mb-4" />
                    <p className="text-gray-600 dark:text-gray-400 mb-2">
                        No user segment matching data available
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-500">
                        Run Stage 2 analysis to see user-to-segment matching results
                    </p>
                </div>
            )}
        </div>
    );
};

export default Stage2UserMatchingResults; 