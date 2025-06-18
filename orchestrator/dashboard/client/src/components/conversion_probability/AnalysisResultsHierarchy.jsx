import React from 'react';
import { BarChart3, Package, Store, DollarSign, TrendingUp, Globe, MapPin, CheckCircle, AlertTriangle } from 'lucide-react';
import { formatPercentage, formatAppStore } from './utils/conversionUtils';

const AnalysisResultsHierarchy = ({ analysisResults, expandedSections, onToggleSection }) => {

  // Generate consistent colors for segment IDs based on hash
  const getSegmentIdColor = (segmentId) => {
    if (!segmentId) return '#6B7280';
    
    let hash = 0;
    for (let i = 0; i < segmentId.length; i++) {
      hash = segmentId.charCodeAt(i) + ((hash << 5) - hash);
    }
    
    const hue = Math.abs(hash) % 360;
    return `hsl(${hue}, 65%, 50%)`;
  };

  // Format segment ID to show first 4 and last 4 characters
  const formatSegmentIdShort = (segmentId) => {
    if (!segmentId || segmentId.length <= 8) return segmentId;
    return `${segmentId.substring(0, 4)}...${segmentId.substring(segmentId.length - 4)}`;
  };

  // Render segment ID badge with consistent color
  const renderSegmentIdBadge = (segmentId) => {
    if (!segmentId) return null;
    
    const color = getSegmentIdColor(segmentId);
    const shortId = formatSegmentIdShort(segmentId);
    
    return (
      <span 
        className="inline-flex items-center px-2 py-1 rounded text-xs font-mono mx-1"
        style={{ 
          backgroundColor: `${color}20`, 
          color: color,
          border: `1px solid ${color}40`
        }}
      >
        ID: {shortId}
      </span>
    );
  };

  if (!analysisResults) {
    return (
      <div className="text-center text-gray-500 dark:text-gray-400 py-8">
        No analysis results available
      </div>
    );
  }

  // Build hierarchy data with accurate statistics
  const hierarchyData = {};
  let totalSegmentsWithMetrics = 0;
  let totalRollupSegments = 0;
  
  if (analysisResults.conversion_probabilities) {
    Object.entries(analysisResults.conversion_probabilities).forEach(([segmentId, segmentData]) => {
      const cohort = segmentData.cohort || {};
      const metrics = segmentData.metrics || {};
      
      const productId = cohort.product_id;
      const appStore = cohort.app_store || 'Unknown';
      const priceRange = cohort.price_bucket;
      const economicTier = cohort.economic_tier || '';
      const country = cohort.country || '';
      const region = cohort.region || '';
      
      if (!productId || !priceRange) return;

      // Determine if this segment has actual metrics vs is rolled up
      const isRollup = cohort.properties_dropped && cohort.properties_dropped.length > 0;
      const hasMetrics = !isRollup;
      
      if (hasMetrics) totalSegmentsWithMetrics++;
      if (isRollup) totalRollupSegments++;

      // Determine hierarchy level based on what properties are specified
      let hierarchyLevel = 'region';
      if (!region) hierarchyLevel = 'country';
      if (!country) hierarchyLevel = 'tier';
      if (!economicTier) hierarchyLevel = 'price';
      
      // Store segment at its appropriate level - but ALL segments get stored somewhere
      const segmentKey = `${productId}|${appStore}|${priceRange}|${economicTier}|${country}|${region}`;
      const segmentInfo = {
        ...cohort,
        ...metrics,
        segment_id: cohort.segment_id || segmentId,
        is_viable: hasMetrics,
        trial_conversion_rate: metrics.trial_conversion_rate || 0,
        trial_converted_count: metrics.trial_converted_count || 0,
        trial_started_count: metrics.trial_started_count || 0,
        rollup_target_segment_id: cohort.rollup_target_segment_id,
        rollup_reason: cohort.rollup_reason,
        hierarchy_level: hierarchyLevel
      };

      // Build nested hierarchy with aggregated statistics
      if (!hierarchyData[productId]) {
        hierarchyData[productId] = { 
          stores: {}, 
          totalUsers: 0, 
          viableSegments: 0, 
          rollupSegments: 0 
        };
      }

      if (!hierarchyData[productId].stores[appStore]) {
        hierarchyData[productId].stores[appStore] = { 
          prices: {}, 
          totalUsers: 0, 
          viableSegments: 0, 
          rollupSegments: 0 
        };
      }
      
      if (!hierarchyData[productId].stores[appStore].prices[priceRange]) {
        hierarchyData[productId].stores[appStore].prices[priceRange] = { 
          tiers: {}, 
          totalUsers: 0, 
          viableSegments: 0, 
          rollupSegments: 0,
          segment: hierarchyLevel === 'price' ? segmentInfo : null
        };
      }

      if (!hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown']) {
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'] = { 
          countries: {}, 
          totalUsers: 0, 
          viableSegments: 0, 
          rollupSegments: 0,
          segment: hierarchyLevel === 'tier' ? segmentInfo : null
        };
      }

      if (!hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown']) {
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown'] = { 
          regions: {}, 
          totalUsers: 0, 
          viableSegments: 0, 
          rollupSegments: 0,
          segment: hierarchyLevel === 'country' ? segmentInfo : null
        };
      }

      // ALL region-level segments get stored, whether viable or not
      if (hierarchyLevel === 'region') {
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown'].regions[region || 'Unknown'] = segmentInfo;
      }

      // Aggregate user counts and statistics upward
      const userCount = cohort.cohort_size || 0;
      
      // Update counts at each level
      hierarchyData[productId].totalUsers += userCount;
      hierarchyData[productId].stores[appStore].totalUsers += userCount;
      hierarchyData[productId].stores[appStore].prices[priceRange].totalUsers += userCount;
      hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].totalUsers += userCount;
      hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown'].totalUsers += userCount;

      // Update statistics at each level
      if (hasMetrics) {
        hierarchyData[productId].viableSegments++;
        hierarchyData[productId].stores[appStore].viableSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].viableSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].viableSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown'].viableSegments++;
      }
      
      if (isRollup) {
        hierarchyData[productId].rollupSegments++;
        hierarchyData[productId].stores[appStore].rollupSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].rollupSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].rollupSegments++;
        hierarchyData[productId].stores[appStore].prices[priceRange].tiers[economicTier || 'Unknown'].countries[country || 'Unknown'].rollupSegments++;
      }
    });
  }

  // Render a single hierarchy line with proper indentation
  const renderHierarchyLine = (icon, label, userCount, viableCount, rollupCount, indentLevel = 0, segmentData = null) => {
    const indentStyle = { marginLeft: `${indentLevel * 24}px` };
    
    return (
      <div 
        className={`flex items-center justify-between py-1 px-3 hover:bg-gray-50 dark:hover:bg-gray-700/30 ${
          indentLevel % 2 === 0 ? 'bg-gray-25 dark:bg-gray-800/20' : 'bg-white dark:bg-gray-800'
        }`}
        style={indentStyle}
      >
        {/* Left side: Icon + Label + Segment Info */}
        <div className="flex items-center gap-2 min-w-0 flex-1">
          {icon}
          <span className="font-medium text-gray-900 dark:text-gray-100 truncate">
            {label}
          </span>
          
          {/* ALWAYS show segment ID FIRST if we have segment data */}
          {segmentData && segmentData.segment_id && (
            renderSegmentIdBadge(segmentData.segment_id)
          )}
          
          {/* Segment-specific information - metrics or rollup info */}
          {segmentData && (
            <>
              {segmentData.is_viable ? (
                <>
                  <span className="text-green-600 dark:text-green-400 text-sm font-medium">
                    🎯 {formatPercentage(segmentData.trial_conversion_rate)}
                  </span>
                  <span className="text-blue-600 dark:text-blue-400 text-sm">
                    → {segmentData.trial_converted_count || 0}/{segmentData.trial_started_count || 0}
                  </span>
                </>
              ) : (
                <>
                  {segmentData.rollup_target_segment_id && (
                    <span className="text-red-600 dark:text-red-400 text-sm">
                      → {renderSegmentIdBadge(segmentData.rollup_target_segment_id)}
                    </span>
                  )}
                  {segmentData.rollup_reason && (
                    <span className="text-red-500 dark:text-red-400 text-xs italic">
                      ({segmentData.rollup_reason})
                    </span>
                  )}
                  {segmentData.properties_dropped && segmentData.properties_dropped.length > 0 && (
                    <span className="text-orange-500 dark:text-orange-400 text-xs">
                      Dropped: [{segmentData.properties_dropped.join(', ')}]
                    </span>
                  )}
                  {segmentData.rollup_description && (
                    <span className="text-gray-500 dark:text-gray-400 text-xs">
                      {segmentData.rollup_description}
                    </span>
                  )}
                </>
              )}
            </>
          )}
        </div>

        {/* Right side: Statistics */}
        <div className="flex items-center gap-4 text-sm font-medium">
          <span className="text-gray-600 dark:text-gray-400">
            {userCount.toLocaleString()} users
          </span>
          
          {/* Hierarchy level statistics */}
          {viableCount !== undefined && rollupCount !== undefined && (
            <div className="flex items-center gap-2">
              {viableCount > 0 && (
                <span className="text-green-600 dark:text-green-400">
                  {viableCount} viable
                </span>
              )}
              {rollupCount > 0 && (
                <span className="text-orange-600 dark:text-orange-400">
                  {rollupCount} rollup
                </span>
              )}
            </div>
          )}
          
          {/* Final segment status */}
          {segmentData && (
            <div>
              {segmentData.is_viable ? (
                <span className="bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 px-2 py-1 rounded-full text-xs font-medium">
                  ✅ VIABLE
                </span>
              ) : (
                <span className="bg-red-100 dark:bg-red-900/30 text-red-800 dark:text-red-200 px-2 py-1 rounded-full text-xs font-medium">
                  ❌ ROLLUP
                </span>
              )}
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg">
      
      {/* Clean Header */}
      <div className="p-6 border-b border-gray-200 dark:border-gray-700 bg-gradient-to-r from-blue-600 to-purple-600 text-white">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <BarChart3 className="h-6 w-6" />
            <div>
              <h2 className="text-xl font-bold">Hierarchical Analysis Results</h2>
              <p className="text-blue-100 text-sm mt-1">
                Tree-based segment hierarchy with conversion metrics and rollup information
              </p>
            </div>
          </div>
          
          {/* Summary Statistics */}
          <div className="flex items-center gap-6 text-sm">
            <div className="text-center">
              <div className="text-2xl font-bold">{totalSegmentsWithMetrics}</div>
              <div className="text-blue-100">Viable Segments</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">{totalRollupSegments}</div>
              <div className="text-blue-100">Rollup Segments</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">
                {totalSegmentsWithMetrics + totalRollupSegments > 0 ? 
                  Math.round((totalSegmentsWithMetrics / (totalSegmentsWithMetrics + totalRollupSegments)) * 100) : 0}%
              </div>
              <div className="text-blue-100">Coverage</div>
            </div>
          </div>
        </div>
      </div>

      {/* Clean Tree Structure */}
      <div className="divide-y divide-gray-100 dark:divide-gray-700">
        {Object.entries(hierarchyData).map(([productId, productData]) => (
          <div key={productId}>
            
            {/* Product Level */}
            {renderHierarchyLine(
              <Package className="h-5 w-5 text-purple-600" />,
              `🏢 ${productId}`,
              productData.totalUsers,
              productData.viableSegments,
              productData.rollupSegments,
              0
            )}

            {/* Store Level */}
            {Object.entries(productData.stores).map(([appStore, storeData]) => (
              <div key={appStore}>
                {renderHierarchyLine(
                  <Store className="h-4 w-4 text-blue-600" />,
                  `📱 ${formatAppStore(appStore)}`,
                  storeData.totalUsers,
                  storeData.viableSegments,
                  storeData.rollupSegments,
                  1
                )}

                {/* Price Level */}
                {Object.entries(storeData.prices).map(([priceRange, priceData]) => (
                  <div key={priceRange}>
                    {renderHierarchyLine(
                      <DollarSign className="h-4 w-4 text-green-600" />,
                      `💰 ${priceRange}`,
                      priceData.totalUsers,
                      priceData.viableSegments,
                      priceData.rollupSegments,
                      2,
                      priceData.segment
                    )}

                    {/* Economic Tier Level */}
                    {Object.entries(priceData.tiers).map(([economicTier, tierData]) => (
                      <div key={economicTier}>
                        {renderHierarchyLine(
                          <TrendingUp className="h-4 w-4 text-yellow-600" />,
                          `🌍 ${economicTier}`,
                          tierData.totalUsers,
                          tierData.viableSegments,
                          tierData.rollupSegments,
                          3,
                          tierData.segment
                        )}

                        {/* Country Level */}
                        {Object.entries(tierData.countries).map(([country, countryData]) => (
                          <div key={country}>
                            {renderHierarchyLine(
                              <Globe className="h-4 w-4 text-orange-600" />,
                              `🏁 ${country}`,
                              countryData.totalUsers,
                              countryData.viableSegments,
                              countryData.rollupSegments,
                              4,
                              countryData.segment
                            )}

                            {/* Region Level - ALL Segments */}
                            {Object.entries(countryData.regions).map(([region, segment]) => {
                              // ALWAYS show the segment if it exists
                              if (!segment) return null;
                              
                              return renderHierarchyLine(
                                segment.is_viable ? 
                                  <CheckCircle className="h-4 w-4 text-green-500" /> : 
                                  <AlertTriangle className="h-4 w-4 text-red-500" />,
                                `📍 ${region}`,
                                segment.cohort_size || 0,
                                undefined,
                                undefined,
                                5,
                                segment
                              );
                            })}
                          </div>
                        ))}
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
};

export default AnalysisResultsHierarchy;