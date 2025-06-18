import React, { useState } from 'react';
import { ChevronDown, ChevronRight, BarChart3, Users, Target, Info, Eye, ArrowDown } from 'lucide-react';

const SearchResults = ({
  searchResults,
  expandedSections,
  onToggleSection,
  onOpenRollupModal
}) => {
  const [expandedSegments, setExpandedSegments] = useState({});
  
  if (!searchResults) return null;

  const toggleSegmentExpansion = (segmentId) => {
    setExpandedSegments(prev => ({
      ...prev,
      [segmentId]: !prev[segmentId]
    }));
  };

  // Create enhanced segment data with rollup information
  const enhancedSegments = searchResults.matches.map(result => {
    const rolledUpSegments = searchResults.rollup_information?.rolled_up_segments || [];
    const segmentId = result.cohort.segment_id;
    
    // Find segments that roll up to this target using rollup_target_segment_id
    const rollingUpSegments = rolledUpSegments.filter(segment => 
      segment.rollup_target_segment_id === segmentId
    );
    
    return {
      ...result,
      rollupCount: rollingUpSegments.length,
      rollingUpSegments: rollingUpSegments,
      totalRolledUpUsers: rollingUpSegments.reduce((sum, seg) => sum + (seg.original_user_count || 0), 0)
    };
  });

  // Sort segments by rollup count (descending) then by cohort size (descending)
  const sortedSegments = enhancedSegments.sort((a, b) => {
    if (a.rollupCount !== b.rollupCount) {
      return b.rollupCount - a.rollupCount; // Most rollups first
    }
    return b.cohort.cohort_size - a.cohort.cohort_size; // Larger cohorts first
  });

  const formatSegmentId = (id) => {
    return id ? id.slice(0, 8) : 'N/A';
  };

  const formatAppStore = (store) => {
    if (!store || store === 'Unknown') return 'Any Store';
    return store === 'app_store' ? 'App Store' : 
           store === 'play_store' ? 'Play Store' : 
           store === 'stripe' ? 'Stripe' : store;
  };

  // Helper to determine if a property is set (specific) or generalized (Any)
  const getPropertyDisplay = (value, propertyName) => {
    const isGeneralized = !value || value === 'Unknown' || 
                         (propertyName === 'region' && value === 'Unknown') ||
                         (propertyName === 'country' && value === 'Unknown') ||
                         (propertyName === 'app_store' && value === 'Unknown');
    
    if (isGeneralized) {
      return {
        value: `Any ${propertyName.charAt(0).toUpperCase() + propertyName.slice(1)}`,
        isSet: false
      };
    }
    
    return {
      value: value,
      isSet: true
    };
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
      <div className="p-6 border-b border-gray-200 dark:border-gray-700 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-gray-800 dark:to-gray-700">
        <div className="flex items-center justify-between w-full">
          <button
            onClick={() => onToggleSection('searchResults')}
            className="flex items-center text-left"
          >
            <div className="flex items-center">
              <BarChart3 className="h-6 w-6 text-blue-600 dark:text-blue-400 mr-3" />
              <div>
                <h3 className="text-xl font-bold text-gray-900 dark:text-white">
                  Conversion Analysis Results
                </h3>
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                  Viable segments ordered by rollup count (most consolidation first)
                </p>
              </div>
              <span className="ml-4 inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                <Users className="h-4 w-4 mr-1" />
                {searchResults.total_matches} segments
              </span>
            </div>
          </button>
          
          <div className="flex items-center space-x-3">
            {/* Rollup Summary Stats */}
            {(() => {
              const rollupInfo = searchResults?.rollup_information;
              const rollupCount = rollupInfo?.rolled_up_segments?.length || 0;
              const rollupPercentage = rollupInfo?.summary?.rollup_percentage || 0;
              
              if (rollupCount > 0) {
                return (
                  <div className="flex items-center space-x-3 text-sm">
                    <div className="text-center">
                      <div className="text-lg font-bold text-blue-600 dark:text-blue-400">{rollupCount}</div>
                      <div className="text-xs text-gray-500">Rolled Up</div>
                    </div>
                    <div className="text-center">
                      <div className="text-lg font-bold text-green-600 dark:text-green-400">{searchResults.total_matches}</div>
                      <div className="text-xs text-gray-500">Final Viable</div>
                    </div>
                    <div className="text-center">
                      <div className="text-lg font-bold text-purple-600 dark:text-purple-400">{rollupPercentage.toFixed(1)}%</div>
                      <div className="text-xs text-gray-500">Consolidation</div>
                    </div>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onOpenRollupModal(rollupInfo);
                      }}
                      className="inline-flex items-center px-4 py-2 border border-blue-300 dark:border-blue-600 rounded-lg text-sm font-medium text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors shadow-sm"
                    >
                      <Target className="h-4 w-4 mr-2" />
                      View All Rollups
                    </button>
                  </div>
                );
              }
              return null;
            })()}
            
            {expandedSections.searchResults ? (
              <ChevronDown className="h-5 w-5 text-gray-500" />
            ) : (
              <ChevronRight className="h-5 w-5 text-gray-500" />
            )}
          </div>
        </div>
      </div>
      
      {expandedSections.searchResults && (
        <div>
          {/* Instructions */}
          <div className="p-6 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/10 dark:to-purple-900/10 border-b border-gray-200 dark:border-gray-700">
            <h4 className="text-lg font-semibold text-blue-900 dark:text-blue-200 mb-3 flex items-center">
              <Info className="h-5 w-5 mr-2" />
              Understanding Rollup Results
            </h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-blue-800 dark:text-blue-300">
              <div className="space-y-2">
                <p><strong>Green Properties:</strong> Specific values (e.g., "US", "App Store")</p>
                <p><strong>Orange Properties:</strong> Generalized to "Any" for broader coverage</p>
                <p><strong>Rollup Count:</strong> Number of failed segments that use this target's metrics</p>
              </div>
              <div className="space-y-2">
                <p><strong>Ordering:</strong> Segments with most rollups shown first</p>
                <p><strong>Failed Segments:</strong> Had insufficient users, rolled up to viable targets</p>
                <p><strong>All Metrics:</strong> Trial conversion, refund rates by purchase type</p>
              </div>
            </div>
          </div>

          {/* Segments List */}
          <div className="divide-y divide-gray-200 dark:divide-gray-700">
            {sortedSegments.map((segment, index) => {
              const isExpanded = expandedSegments[segment.cohort.segment_id];
              
              // Determine property display
              const productDisplay = getPropertyDisplay(segment.cohort.product_id, 'product');
              const storeDisplay = getPropertyDisplay(segment.cohort.app_store, 'store');
              const priceDisplay = getPropertyDisplay(segment.cohort.price_bucket, 'price');
              const countryDisplay = getPropertyDisplay(segment.cohort.country, 'country');
              const regionDisplay = getPropertyDisplay(segment.cohort.region, 'region');
              
              return (
                <div key={segment.cohort.segment_id} className="p-6">
                  {/* Main Segment Info */}
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex-1">
                      <div className="flex items-center space-x-3 mb-3">
                        <div className="w-8 h-8 bg-blue-600 dark:bg-blue-500 rounded-full flex items-center justify-center text-white font-bold text-sm">
                          #{index + 1}
                        </div>
                        <div>
                          <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                            Viable Segment Target
                          </h3>
                          <div className="text-sm text-gray-600 dark:text-gray-400">
                            ID: {formatSegmentId(segment.cohort.segment_id)} • {segment.rollupCount} failed segments roll up to this
                          </div>
                        </div>
                      </div>

                      {/* Segment Properties */}
                      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 mb-4">
                        <div className="flex flex-col">
                          <span className="text-xs font-medium text-gray-500 uppercase mb-1">Product</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                            productDisplay.isSet 
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
                          }`}>
                            {productDisplay.value}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-xs font-medium text-gray-500 uppercase mb-1">Store</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                            storeDisplay.isSet 
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
                          }`}>
                            {formatAppStore(storeDisplay.value)}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-xs font-medium text-gray-500 uppercase mb-1">Price</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                            priceDisplay.isSet 
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
                          }`}>
                            {priceDisplay.value}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-xs font-medium text-gray-500 uppercase mb-1">Country</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                            countryDisplay.isSet 
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
                          }`}>
                            {countryDisplay.value}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-xs font-medium text-gray-500 uppercase mb-1">Region</span>
                          <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                            regionDisplay.isSet 
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200'
                          }`}>
                            {regionDisplay.value}
                          </span>
                        </div>
                      </div>

                      {/* All Conversion Metrics */}
                      <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
                        <div className="text-center">
                          <div className="text-lg font-bold text-blue-600 dark:text-blue-400">
                            {segment.cohort.cohort_size?.toLocaleString()}
                          </div>
                          <div className="text-xs text-gray-500">Users</div>
                        </div>
                        <div className="text-center">
                          <div className="text-lg font-bold text-green-600 dark:text-green-400">
                            {(segment.metrics.trial_conversion_rate * 100).toFixed(1)}%
                          </div>
                          <div className="text-xs text-gray-500">Trial→Paid</div>
                        </div>
                        <div className="text-center">
                          <div className="text-lg font-bold text-red-600 dark:text-red-400">
                            {(segment.metrics.trial_converted_to_refund_rate * 100).toFixed(1)}%
                          </div>
                          <div className="text-xs text-gray-500">Trial→Refund</div>
                        </div>
                        <div className="text-center">
                          <div className="text-lg font-bold text-orange-600 dark:text-orange-400">
                            {(segment.metrics.initial_purchase_to_refund_rate * 100).toFixed(1)}%
                          </div>
                          <div className="text-xs text-gray-500">Purchase→Refund</div>
                        </div>
                        <div className="text-center">
                          <div className="text-lg font-bold text-purple-600 dark:text-purple-400">
                            {(segment.metrics.renewal_to_refund_rate * 100).toFixed(1)}%
                          </div>
                          <div className="text-xs text-gray-500">Renewal→Refund</div>
                        </div>
                        <div className="text-center">
                          <div className="text-lg font-bold text-indigo-600 dark:text-indigo-400">
                            {segment.rollupCount}
                          </div>
                          <div className="text-xs text-gray-500">Rollups</div>
                        </div>
                      </div>
                    </div>
                    
                    {/* Expand Button */}
                    {segment.rollupCount > 0 && (
                      <div className="ml-4">
                        <button
                          onClick={() => toggleSegmentExpansion(segment.cohort.segment_id)}
                          className="inline-flex items-center px-4 py-2 border border-blue-300 dark:border-blue-600 rounded-lg text-sm font-medium text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors"
                        >
                          <Eye className="h-4 w-4 mr-2" />
                          {isExpanded ? 'Hide' : 'Show'} Failed Segments
                          {isExpanded ? <ChevronDown className="h-4 w-4 ml-1" /> : <ChevronRight className="h-4 w-4 ml-1" />}
                        </button>
                      </div>
                    )}
                  </div>

                  {/* Failed Segments List (when expanded) */}
                  {isExpanded && segment.rollupCount > 0 && (
                    <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-900/30 rounded-lg">
                      <h4 className="text-md font-bold text-gray-900 dark:text-white mb-4 flex items-center">
                        <ArrowDown className="h-4 w-4 mr-2 text-orange-600" />
                        Failed Segments That Roll Up To This Target ({segment.rollupCount})
                      </h4>
                      
                      <div className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                        These segments had insufficient users ({segment.totalRolledUpUsers.toLocaleString()} total original users) and now use the conversion metrics from the viable target above.
                      </div>
                      
                      <div className="space-y-3">
                        {segment.rollingUpSegments.map((failedSegment, segIndex) => (
                          <div key={segIndex} className="border border-red-200 dark:border-red-800 rounded-lg bg-red-50 dark:bg-red-900/10 p-3">
                            <div className="flex items-center justify-between">
                              <div className="flex-1">
                                <div className="flex items-center space-x-2 mb-2">
                                  <span className="text-xs font-mono bg-gray-200 dark:bg-gray-700 px-2 py-1 rounded">
                                    ID: {formatSegmentId(failedSegment.segment_id)}
                                  </span>
                                  <span className="text-sm font-medium text-red-600 dark:text-red-400">
                                    Failed: {(failedSegment.original_user_count || 0).toLocaleString()} users (insufficient)
                                  </span>
                                </div>
                                
                                <div className="text-sm text-gray-700 dark:text-gray-300">
                                  <span className="font-medium">{failedSegment.product_id}</span>
                                  <span className="mx-2 text-gray-400">|</span>
                                  <span>{formatAppStore(failedSegment.app_store)}</span>
                                  <span className="mx-2 text-gray-400">|</span>
                                  <span>{failedSegment.price_bucket}</span>
                                  <span className="mx-2 text-gray-400">|</span>
                                  <span>{failedSegment.country || 'Any Country'}</span>
                                  <span className="mx-2 text-gray-400">|</span>
                                  <span>{failedSegment.region || 'Any Region'}</span>
                                </div>
                                
                                {failedSegment.properties_dropped && failedSegment.properties_dropped.length > 0 && (
                                  <div className="mt-1 text-xs text-orange-600 dark:text-orange-400">
                                    Properties generalized during rollup: {failedSegment.properties_dropped.join(', ')}
                                  </div>
                                )}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default SearchResults; 