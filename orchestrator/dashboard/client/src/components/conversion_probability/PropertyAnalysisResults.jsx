import React, { useState } from 'react';
import { ChevronDown, ChevronRight, CheckCircle, Globe, Package, Info, TrendingUp } from 'lucide-react';
import { formatPercentage, buildHierarchicalStructure, getStatusIndicator, formatSegmentId, getSegmentIdBadge, getRollupTargetIdBadge, formatAppStore } from './utils/conversionUtils';

const PropertyAnalysisResults = ({
  propertyAnalysis,
  expandedSections,
  onToggleSection,
  config
}) => {
  const [expandedItems, setExpandedItems] = useState({});
  const [showFullDetails, setShowFullDetails] = useState(false);

  const toggleExpanded = (key) => {
    setExpandedItems(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  if (!propertyAnalysis) return null;

  // Build hierarchical structure - user_segments is an object with productId keys
  // We need to flatten all segments from all products into a single array
  const allSegments = [];
  if (propertyAnalysis.user_segments) {
    Object.values(propertyAnalysis.user_segments).forEach(productSegments => {
      if (Array.isArray(productSegments)) {
        allSegments.push(...productSegments);
      }
    });
  }
  
  const hierarchicalData = buildHierarchicalStructure(allSegments, config.min_cohort_size);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
      <div className="p-4 border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => onToggleSection('propertyAnalysis')}
          className="flex items-center justify-between w-full text-left"
        >
          <div className="flex items-center">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Step 2: Data Structure Discovery Results
            </h3>
            <span className="ml-3 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
              ✓ Complete
            </span>
          </div>
          {expandedSections.propertyAnalysis ? (
            <ChevronDown className="h-5 w-5 text-gray-500" />
          ) : (
            <ChevronRight className="h-5 w-5 text-gray-500" />
          )}
        </button>
      </div>
      
      {expandedSections.propertyAnalysis && (
        <div className="p-6">
          {/* Overview Summary */}
          <div className="mb-6 p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
            <h4 className="text-sm font-semibold text-green-900 dark:text-green-200 mb-2 flex items-center">
              <CheckCircle className="h-4 w-4 mr-2" />
              What This Analysis Discovered
            </h4>
            <p className="text-sm text-green-800 dark:text-green-300 mb-3">
              We analyzed your actual users and grouped them into meaningful segments. Here's what we found:
            </p>
            <ul className="text-sm text-green-800 dark:text-green-300 space-y-1 list-disc list-inside">
              <li><strong>{propertyAnalysis.products_analyzed} products</strong> with sufficient data for analysis (at least {propertyAnalysis.min_price_samples_used || 100} price samples each)</li>
              <li><strong>{propertyAnalysis.total_combinations.toLocaleString()} viable user segments</strong> based on actual user data</li>
              <li><strong>{propertyAnalysis.user_analysis?.total_users_analyzed?.toLocaleString() || 0} total users</strong> analyzed across all segments</li>
              <li><strong>Geographic coverage:</strong> {propertyAnalysis.user_analysis?.total_countries_found || 0} countries and {propertyAnalysis.user_analysis?.total_regions_found || 0} regions with active users</li>
              <li><strong>App stores:</strong> {propertyAnalysis.user_analysis?.app_stores?.length || 0} different stores</li>
            </ul>
          </div>
          
          {/* Key Metrics */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
            <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
              <div className="text-3xl font-bold text-blue-600 dark:text-blue-400">
                {propertyAnalysis.products_analyzed}
              </div>
              <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Products Found</div>
              <div className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                Products with enough data for reliable analysis
              </div>
            </div>
            
            <div className="text-center p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
              <div className="text-3xl font-bold text-purple-600 dark:text-purple-400">
                {Object.values(propertyAnalysis.price_buckets || {}).reduce((total, buckets) => total + buckets.length, 0)}
              </div>
              <div className="text-sm text-purple-800 dark:text-purple-300 font-medium">Price Groups</div>
              <div className="text-xs text-purple-600 dark:text-purple-400 mt-1">
                Intelligent price ranges created for analysis
              </div>
            </div>
            
            <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
              <div className="text-3xl font-bold text-green-600 dark:text-green-400">
                {propertyAnalysis.user_analysis?.total_countries_found || 0}
              </div>
              <div className="text-sm text-green-800 dark:text-green-300 font-medium">Countries</div>
              <div className="text-xs text-green-600 dark:text-green-400 mt-1">
                Countries with active users
              </div>
            </div>
            
            <div className="text-center p-4 bg-orange-50 dark:bg-orange-900/20 rounded-lg">
              <div className="text-3xl font-bold text-orange-600 dark:text-orange-400">
                {propertyAnalysis.total_combinations.toLocaleString()}
              </div>
              <div className="text-sm text-orange-800 dark:text-orange-300 font-medium">User Segments</div>
              <div className="text-xs text-orange-600 dark:text-orange-400 mt-1">
                Viable segments found in your data
              </div>
            </div>
          </div>

          {/* Geographic Market Overview */}
          {propertyAnalysis.user_analysis?.geographic_structure && (
            <div className="mb-6">
              <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Globe className="h-5 w-5 mr-2" />
                Global Market Overview
              </h4>
              
              {(() => {
                const geoData = propertyAnalysis.user_analysis.geographic_structure;
                const countries = Object.entries(geoData).map(([country, data]) => ({
                  name: country,
                  users: data.total_users,
                  regions: Object.keys(data.regions).length,
                  viableRegions: Object.values(data.regions).filter(r => r.user_count >= 50).length
                })).sort((a, b) => b.users - a.users);
                
                const topCountries = countries.slice(0, 10);
                const totalUsers = countries.reduce((sum, c) => sum + c.users, 0);
                const totalRegions = countries.reduce((sum, c) => sum + c.regions, 0);
                const viableRegions = countries.reduce((sum, c) => sum + c.viableRegions, 0);
                
                return (
                  <div className="space-y-4">
                    {/* Summary Stats */}
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 p-4 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                      <div className="text-center">
                        <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{countries.length}</div>
                        <div className="text-sm text-blue-800 dark:text-blue-300">Total Countries</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{totalRegions}</div>
                        <div className="text-sm text-indigo-800 dark:text-indigo-300">Total Regions</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-green-600 dark:text-green-400">{viableRegions}</div>
                        <div className="text-sm text-green-800 dark:text-green-300">Viable Regions</div>
                        <div className="text-xs text-green-600 dark:text-green-400">≥50 users each</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">{Math.round((viableRegions / totalRegions) * 100)}%</div>
                        <div className="text-sm text-purple-800 dark:text-purple-300">Data Coverage</div>
                        <div className="text-xs text-purple-600 dark:text-purple-400">Regions with sufficient data</div>
                      </div>
                    </div>
                    
                    {/* Top Countries Table */}
                    <div className="bg-white dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
                      <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-600">
                        <h5 className="text-sm font-semibold text-gray-900 dark:text-white">Top Markets by User Count</h5>
                      </div>
                      <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                          <thead className="bg-gray-50 dark:bg-gray-800">
                            <tr>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Country</th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Users</th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Market Share</th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Regions</th>
                              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Data Quality</th>
                            </tr>
                          </thead>
                          <tbody className="bg-white dark:bg-gray-700 divide-y divide-gray-200 dark:divide-gray-600">
                            {topCountries.map((country, idx) => (
                              <tr key={country.name} className={idx % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                                <td className="px-4 py-3 whitespace-nowrap">
                                  <div className="flex items-center">
                                    <span className="text-sm font-medium text-gray-900 dark:text-white">{country.name}</span>
                                    {idx < 3 && (
                                      <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                                        Top {idx + 1}
                                      </span>
                                    )}
                                  </div>
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-white font-medium">
                                  {country.users.toLocaleString()}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap">
                                  <div className="flex items-center">
                                    <div className="w-16 bg-gray-200 dark:bg-gray-600 rounded-full h-2 mr-2">
                                      <div 
                                        className="bg-blue-600 h-2 rounded-full" 
                                        style={{ width: `${(country.users / totalUsers) * 100}%` }}
                                      ></div>
                                    </div>
                                    <span className="text-sm text-gray-600 dark:text-gray-400">
                                      {Math.round((country.users / totalUsers) * 100)}%
                                    </span>
                                  </div>
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                                  {country.regions} total
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap">
                                  <div className="flex items-center">
                                    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                                      country.viableRegions / country.regions >= 0.5
                                        ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                                        : country.viableRegions / country.regions >= 0.25
                                        ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
                                        : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                                    }`}>
                                      {country.viableRegions}/{country.regions} viable
                                    </span>
                                  </div>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {countries.length > 10 && (
                        <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-600 text-sm text-gray-500 dark:text-gray-400">
                          Showing top 10 of {countries.length} countries. {countries.length - 10} additional countries with smaller user bases.
                        </div>
                      )}
                    </div>
                  </div>
                );
              })()}
            </div>
          )}

          {/* Product Segment Analysis */}
          {propertyAnalysis.user_segments && (
            <div className="mb-6">
              <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Package className="h-5 w-5 mr-2" />
                Product Segment Analysis - Hierarchical View
              </h4>
              
              {(() => {
                const productSegments = Object.entries(propertyAnalysis.user_segments);
                const totalSegments = productSegments.reduce((sum, [_, segments]) => sum + segments.length, 0);
                const viableSegments = productSegments.reduce((sum, [_, segments]) => 
                  sum + segments.filter(s => s.has_sufficient_data).length, 0);
                
                return (
                  <div className="space-y-6">
                    {/* Rollup Efficiency Summary */}
                    {propertyAnalysis.rollup_stats && (
                      <div className="p-4 bg-gradient-to-r from-green-50 to-emerald-50 dark:from-green-900/20 dark:to-emerald-900/20 rounded-lg border border-green-200 dark:border-green-800">
                        <h5 className="text-lg font-semibold text-green-900 dark:text-green-200 mb-3 flex items-center">
                          <TrendingUp className="h-5 w-5 mr-2" />
                          Intelligent Rollup Efficiency
                        </h5>
                        <div className="mb-3 text-sm text-green-800 dark:text-green-300">
                          <strong>How it works:</strong> When user segments have too few people for reliable statistics, 
                          we intelligently combine them with similar segments (same price range, similar economic regions) 
                          to reach the minimum group size of {config.min_cohort_size} users. This ensures all analysis 
                          results are statistically meaningful.
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                          <div className="text-center">
                            <div className="text-2xl font-bold text-gray-600 dark:text-gray-400">{propertyAnalysis.rollup_stats.total_segments}</div>
                            <div className="text-sm text-gray-700 dark:text-gray-300">Original Segments</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-green-600 dark:text-green-400">{propertyAnalysis.rollup_stats.viable_segments}</div>
                            <div className="text-sm text-green-700 dark:text-green-300">Viable Segments</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{propertyAnalysis.rollup_stats.rolled_up_segments}</div>
                            <div className="text-sm text-blue-700 dark:text-blue-300">Rolled Up</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                              {propertyAnalysis.rollup_stats.rolled_up_segments > 0 ? 
                                `${((propertyAnalysis.rollup_stats.rolled_up_segments / propertyAnalysis.rollup_stats.total_segments) * 100).toFixed(1)}%` :
                                '0%'
                              }
                            </div>
                            <div className="text-sm text-purple-700 dark:text-purple-300">Segments Optimized</div>
                            <div className="text-xs text-purple-600 dark:text-purple-400 mt-1">
                              {propertyAnalysis.rollup_stats.rolled_up_segments > 0 ? 
                                `${propertyAnalysis.rollup_stats.rolled_up_segments} segments combined for efficiency` :
                                'All segments had sufficient data'
                              }
                            </div>
                          </div>
                        </div>
                        
                        {/* Accuracy Distribution */}
                        {propertyAnalysis.rollup_stats.accuracy_distribution && (
                          <div className="mt-4 pt-4 border-t border-green-200 dark:border-green-700">
                            <h6 className="text-sm font-medium text-green-800 dark:text-green-200 mb-2">Accuracy Score Distribution:</h6>
                            <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
                              {/* Order accuracy scores from Very High to Very Low */}
                              {['very_high', 'high', 'medium', 'low', 'very_low'].map(score => {
                                const count = propertyAnalysis.rollup_stats.accuracy_distribution[score] || 0;
                                return (
                                  <div key={score} className="text-center p-2 bg-white dark:bg-gray-800 rounded border">
                                    <div className="font-semibold text-gray-900 dark:text-white">{count}</div>
                                    <div className="text-gray-600 dark:text-gray-400 capitalize">{score.replace('_', ' ')}</div>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                    
                    {/* Summary */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4 bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-900/20 dark:to-pink-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                      <div className="text-center">
                        <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">{productSegments.length}</div>
                        <div className="text-sm text-purple-800 dark:text-purple-300">Products Analyzed</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-pink-600 dark:text-pink-400">{totalSegments}</div>
                        <div className="text-sm text-pink-800 dark:text-pink-300">Total Segments</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-green-600 dark:text-green-400">{viableSegments}</div>
                        <div className="text-sm text-green-800 dark:text-green-300">Ready for Analysis</div>
                        <div className="text-xs text-green-600 dark:text-green-400">≥{config.min_cohort_size} users each</div>
                      </div>
                    </div>
                    
                    {/* Hierarchical Tree View for each Product */}
                    <div className="space-y-8">
                      {/* Legend */}
                      <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg border border-gray-200 dark:border-gray-600">
                        <h5 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">Understanding the Hierarchy View</h5>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                          <div>
                            <h6 className="font-medium text-gray-800 dark:text-gray-200 mb-2">Status Indicators:</h6>
                            <div className="space-y-1">
                              <div className="flex items-center">
                                <span className="px-1 py-0.5 rounded text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 mr-2">✅ PASSES</span>
                                <span className="text-gray-600 dark:text-gray-400">Has enough users (≥{config.min_cohort_size}) for reliable analysis</span>
                              </div>
                              <div className="flex items-center">
                                <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 mr-2">⚠️ HAS ROLLUPS</span>
                                <span className="text-gray-600 dark:text-gray-400">Contains segments that roll up to broader categories (hover for details)</span>
                              </div>
                              <div className="flex items-center">
                                <span className="px-1 py-0.5 rounded text-xs bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300 mr-2">❌ COMBINES</span>
                                <span className="text-gray-600 dark:text-gray-400">Entire level needs to combine with others (hover for details)</span>
                              </div>
                              <div className="flex items-center">
                                <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 mr-2">→ Target</span>
                                <span className="text-gray-600 dark:text-gray-400">Rollup target destination (hover for details)</span>
                              </div>
                            </div>
                          </div>
                          <div>
                            <h6 className="font-medium text-gray-800 dark:text-gray-200 mb-2">4-Level Hierarchy & Rollup Strategy:</h6>
                            <div className="space-y-1 text-gray-600 dark:text-gray-400">
                              <div>1. <strong>🏢📱💰 Product+Store+Price</strong> (Level 1)</div>
                              <div>2. <strong>🌍 Economic Tier</strong> (+ Economic grouping)</div>
                              <div>3. <strong>🏁 Country</strong> (+ Specific country)</div>
                              <div>4. <strong>📍 Region</strong> (+ Regional detail)</div>
                              <div className="mt-2 pt-2 border-t border-gray-300 dark:border-gray-600">
                                <strong>Rollup ceiling:</strong> Cannot roll up beyond Product+Store+Price level. Even if Level 1 fails, everything rolls up to that specific Product+Store+Price combination.
                              </div>
                              <div className="mt-2 text-xs italic">💡 Hover over any status indicator for detailed rollup information</div>
                            </div>
                          </div>
                        </div>
                      </div>
                      
                      {productSegments.map(([productId, segments]) => {
                        // Build hierarchical structure
                        const hierarchy = buildHierarchicalStructure(segments, config.min_cohort_size);
                        const priceBuckets = propertyAnalysis.price_buckets?.[productId] || [];
                        
                        return (
                          <div key={productId} className="bg-white dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
                            {/* Product Header */}
                            <div className="px-6 py-4 bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-900/20 dark:to-blue-900/20 border-b border-gray-200 dark:border-gray-600">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center">
                                  <Package className="h-6 w-6 text-indigo-600 dark:text-indigo-400 mr-3" />
                                  <div>
                                    <h5 className="text-lg font-semibold text-gray-900 dark:text-white">{productId}</h5>
                                    <p className="text-sm text-gray-600 dark:text-gray-400">
                                      Complete rollup hierarchy showing what passes and what combines
                                    </p>
                                  </div>
                                </div>
                                <div className="text-right">
                                  <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">
                                    {segments.reduce((sum, s) => sum + s.user_count, 0).toLocaleString()}
                                  </div>
                                  <div className="text-sm text-gray-600 dark:text-gray-400">Total Users</div>
                                </div>
                              </div>
                            </div>
                            
                            {/* Hierarchical Tree */}
                            <div className="p-6">
                              {Object.entries(hierarchy).map(([level1Key, level1Data]) => (
                                <div key={level1Key} className="mb-8 last:mb-0">
                                  {/* Product + Store + Price Level (Level 1) */}
                                  <div className="flex items-center mb-4">
                                    <div className="flex items-center">
                                      <div className="w-4 h-4 bg-purple-500 rounded mr-3"></div>
                                      <div className="flex items-center space-x-2">
                                        <span className="text-lg font-semibold text-gray-900 dark:text-white">
                                          🏢 {level1Data.productId} | 📱 {formatAppStore(level1Data.appStore)} | 💰 {level1Data.priceRange}
                                        </span>
                                        {/* Add segment ID for Product+Store+Price level */}
                                        {level1Data.segmentId && (
                                          <div className="flex items-center space-x-1">
                                            {getSegmentIdBadge(level1Data.segmentId, 'ID')}
                                            {level1Data.rollupTargetSegmentId && getRollupTargetIdBadge(level1Data.rollupTargetSegmentId)}
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                    <div className="ml-auto flex items-center space-x-4">
                                      <span className="text-xs text-gray-500 dark:text-gray-400">
                                        {level1Data.totalUsers.toLocaleString()} users
                                      </span>
                                      {getStatusIndicator(level1Data.passesThreshold, level1Data.rollupInfo, 'product_store_price', level1Data.rollupInfo.rollupTarget)}
                                    </div>
                                  </div>
                                  
                                  {/* Economic Tiers (Level 2) */}
                                  <div className="ml-6 space-y-4">
                                    {Object.entries(level1Data.economicTiers).map(([tierName, tierData]) => (
                                      <div key={tierName}>
                                        {/* Economic Tier Level */}
                                        <div className="flex items-center mb-3">
                                          <div className="flex items-center">
                                            <div className="w-3 h-3 bg-indigo-500 rounded mr-3"></div>
                                            <div className="flex items-center space-x-2">
                                              <span className="text-md font-medium text-gray-700 dark:text-gray-300">
                                                🌍 {tierName}
                                              </span>
                                              {/* Add segment ID for Economic Tier level */}
                                              {tierData.segmentId && (
                                                <div className="flex items-center space-x-1">
                                                  {getSegmentIdBadge(tierData.segmentId, 'ID')}
                                                  {tierData.rollupTargetSegmentId && getRollupTargetIdBadge(tierData.rollupTargetSegmentId)}
                                                </div>
                                              )}
                                            </div>
                                          </div>
                                          <div className="ml-auto flex items-center space-x-3">
                                            <span className="text-xs text-gray-500 dark:text-gray-400">
                                              {tierData.totalUsers.toLocaleString()} users
                                            </span>
                                            {getStatusIndicator(tierData.passesThreshold, tierData.rollupInfo, 'economic_tier', tierData.rollupInfo.rollupTarget)}
                                          </div>
                                        </div>
                                        
                                        {/* Countries (Level 3) */}
                                        <div className="ml-6 space-y-3">
                                          {Object.entries(tierData.countries).map(([country, countryData]) => (
                                            <div key={country}>
                                              {/* Country Level */}
                                              <div className="flex items-center mb-2">
                                                <div className="flex items-center">
                                                  <div className="w-2.5 h-2.5 bg-blue-500 rounded mr-3"></div>
                                                  <div className="flex items-center space-x-2">
                                                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                                                      🏁 {country}
                                                    </span>
                                                    {/* Add segment ID for Country level */}
                                                    {countryData.segmentId && (
                                                      <div className="flex items-center space-x-1">
                                                        {getSegmentIdBadge(countryData.segmentId, 'ID')}
                                                        {countryData.rollupTargetSegmentId && getRollupTargetIdBadge(countryData.rollupTargetSegmentId)}
                                                      </div>
                                                    )}
                                                  </div>
                                                </div>
                                                <div className="ml-auto flex items-center space-x-3">
                                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                                    {countryData.totalUsers.toLocaleString()} users
                                                  </span>
                                                  {getStatusIndicator(countryData.passesThreshold, countryData.rollupInfo, 'country', countryData.rollupInfo.rollupTarget)}
                                                </div>
                                              </div>
                                              
                                              {/* Regions (Level 4) */}
                                              {countryData.regions.length > 0 && (
                                                <div className="ml-6 space-y-1">
                                                  {countryData.regions.map((region, idx) => (
                                                    <div key={idx} className="flex items-center justify-between py-1">
                                                      <div className="flex items-center">
                                                        <div className="w-2 h-2 bg-green-500 rounded mr-2"></div>
                                                        <div className="flex items-center space-x-2">
                                                          <span className="text-xs text-gray-600 dark:text-gray-400">
                                                            📍 {region.name}
                                                          </span>
                                                          {/* Add segment ID for region */}
                                                          {region.segmentId && (
                                                            <div className="flex items-center space-x-1">
                                                              {getSegmentIdBadge(region.segmentId, 'ID')}
                                                              {region.rollupTargetSegmentId && getRollupTargetIdBadge(region.rollupTargetSegmentId)}
                                                            </div>
                                                          )}
                                                        </div>
                                                      </div>
                                                      <div className="flex items-center space-x-2">
                                                        <span className="text-xs text-gray-500 dark:text-gray-500">
                                                          {region.users.toLocaleString()} users
                                                        </span>
                                                        {region.passes ? (
                                                          <span className="px-1 py-0.5 rounded text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300">
                                                            ✅ PASSES
                                                          </span>
                                                        ) : (
                                                          <div className="relative group">
                                                            <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 cursor-help">
                                                              → {region.rollupTarget}
                                                            </span>
                                                            
                                                            {/* Hover tooltip for region rollup details */}
                                                            <div className="absolute bottom-full right-0 mb-2 px-3 py-2 bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900 text-xs rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-10 whitespace-nowrap max-w-xs">
                                                              <div className="text-xs">
                                                                <div className="font-medium mb-1">Rollup Details:</div>
                                                                <div>• Target: {region.rollupTarget}</div>
                                                                {region.rollupReason && (
                                                                  <div>• Reason: {region.rollupReason}</div>
                                                                )}
                                                                {region.propertiesDropped && region.propertiesDropped.length > 0 && (
                                                                  <div>• Properties dropped: {region.propertiesDropped.join(', ')}</div>
                                                                )}
                                                                {region.finalUserCount && region.finalUserCount !== region.originalUserCount && (
                                                                  <div>• Final user count: {region.finalUserCount.toLocaleString()}</div>
                                                                )}
                                                                {region.rollupTargetSegmentId && (
                                                                  <div>• Target Segment ID: {formatSegmentId(region.rollupTargetSegmentId)}</div>
                                                                )}
                                                              </div>
                                                              <div className="absolute top-full right-4 transform border-4 border-transparent border-t-gray-900 dark:border-t-gray-100"></div>
                                                            </div>
                                                          </div>
                                                        )}
                                                      </div>
                                                    </div>
                                                  ))}
                                                </div>
                                              )}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}
            </div>
          )}

          {/* Rollup Statistics */}
          {propertyAnalysis.rollup_stats && (
            <div className="mb-6">
              <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Package className="h-5 w-5 mr-2" />
                Segment Optimization Summary
              </h4>
              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div className="text-center">
                    <div className="text-2xl font-bold text-gray-600 dark:text-gray-400">{propertyAnalysis.rollup_stats.total_segments}</div>
                    <div className="text-sm text-gray-700 dark:text-gray-300">Original Segments</div>
                  </div>
                  <div className="text-center">
                    <div className="text-2xl font-bold text-green-600 dark:text-green-400">{propertyAnalysis.rollup_stats.viable_segments}</div>
                    <div className="text-sm text-green-700 dark:text-green-300">Viable Segments</div>
                  </div>
                  <div className="text-center">
                    <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{propertyAnalysis.rollup_stats.rolled_up_segments}</div>
                    <div className="text-sm text-blue-700 dark:text-blue-300">Rolled Up</div>
                  </div>
                  <div className="text-center">
                    <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                      {propertyAnalysis.rollup_stats.rolled_up_segments > 0 ? 
                        `${((propertyAnalysis.rollup_stats.rolled_up_segments / propertyAnalysis.rollup_stats.total_segments) * 100).toFixed(1)}%` :
                        '0%'
                      }
                    </div>
                    <div className="text-sm text-purple-700 dark:text-purple-300">Segments Optimized</div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Methodology Summary */}
          <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
            <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
              <Info className="h-4 w-4 mr-2" />
              Analysis Methodology
            </h4>
            <div className="text-sm text-blue-800 dark:text-blue-300 space-y-2">
              <div>
                <strong>User-Centric Analysis:</strong> Instead of generating theoretical combinations, we analyze your actual users 
                and group them based on their real properties (product purchases, geographic location, app store). This ensures 
                every segment represents real user behavior.
              </div>
              <div>
                <strong>Intelligent Grouping:</strong> When segments have insufficient users, we use a smart rollup strategy 
                that combines related segments while preserving meaningful distinctions.
              </div>
              {propertyAnalysis.bucketing_methodology && (
                <div>
                  <strong>Price Bucketing:</strong> {propertyAnalysis.bucketing_methodology.description}
                </div>
              )}
            </div>
          </div>

          {/* Hierarchical Rollup Analysis */}
          <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
            <h4 className="font-medium text-gray-900 dark:text-white mb-3">Hierarchical Rollup Analysis</h4>
            <div className="mb-4 flex items-center justify-between">
              <div className="text-sm text-gray-600 dark:text-gray-400">
                Shows how segments are grouped and rolled up when they don't meet the minimum threshold of {config.min_cohort_size} users
              </div>
              <button
                onClick={() => setShowFullDetails(!showFullDetails)}
                className="px-3 py-1 text-xs font-medium text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 rounded-full hover:bg-blue-100 dark:hover:bg-blue-900/30 transition-colors"
              >
                {showFullDetails ? 'Hide Details' : 'Show Full Details'}
              </button>
            </div>

            <div className="space-y-4">
              {Object.entries(hierarchicalData).map(([level1Key, level1Data]) => (
                <div key={level1Key} className="border border-gray-200 dark:border-gray-700 rounded-lg">
                  {/* Level 1: Product + Store + Price */}
                  <div className="bg-gray-50 dark:bg-gray-700/50 p-3 rounded-t-lg">
                    <button
                      onClick={() => toggleExpanded(level1Key)}
                      className="flex items-center justify-between w-full text-left"
                    >
                      <div className="flex items-center space-x-3">
                        <svg
                          className={`w-4 h-4 transform transition-transform ${expandedItems[level1Key] ? 'rotate-90' : ''}`}
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                        </svg>
                        <div>
                          <div className="font-medium text-gray-900 dark:text-white">
                            {level1Data.productId} • {level1Data.appStore} • {level1Data.priceRange}
                          </div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">
                            {level1Data.totalUsers.toLocaleString()} total users across {level1Data.rollupInfo.totalSegments} segments
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center space-x-2">
                        {getStatusIndicator(level1Data.passesThreshold, level1Data.rollupInfo, 'product', level1Data.rollupInfo.rollupTarget)}
                        <span className="text-xs text-gray-500 dark:text-gray-400">
                          {level1Data.rollupInfo.rolledUpSegments > 0 && `${level1Data.rollupInfo.rolledUpSegments} rolled up`}
                        </span>
                      </div>
                    </button>
                  </div>

                  {expandedItems[level1Key] && (
                    <div className="p-3 space-y-3">
                      {Object.entries(level1Data.economicTiers).map(([tierName, tierData]) => (
                        <div key={`${level1Key}-${tierName}`} className="border-l-2 border-gray-300 dark:border-gray-600 pl-4">
                          {/* Level 2: Economic Tier */}
                          <button
                            onClick={() => toggleExpanded(`${level1Key}-${tierName}`)}
                            className="flex items-center justify-between w-full text-left mb-2"
                          >
                            <div className="flex items-center space-x-3">
                              <svg
                                className={`w-3 h-3 transform transition-transform ${expandedItems[`${level1Key}-${tierName}`] ? 'rotate-90' : ''}`}
                                fill="none"
                                stroke="currentColor"
                                viewBox="0 0 24 24"
                              >
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                              </svg>
                              <div>
                                <div className="font-medium text-gray-800 dark:text-gray-200">
                                  {tierName}
                                </div>
                                <div className="text-xs text-gray-500 dark:text-gray-400">
                                  {tierData.totalUsers.toLocaleString()} users across {Object.keys(tierData.countries).length} countries
                                </div>
                              </div>
                            </div>
                            <div className="flex items-center space-x-2">
                              {getStatusIndicator(tierData.passesThreshold, tierData.rollupInfo, 'tier', tierData.rollupInfo.rollupTarget)}
                            </div>
                          </button>

                          {expandedItems[`${level1Key}-${tierName}`] && (
                            <div className="space-y-2 ml-4">
                              {Object.entries(tierData.countries).map(([country, countryData]) => (
                                <div key={`${level1Key}-${tierName}-${country}`} className="border-l border-gray-200 dark:border-gray-600 pl-3">
                                  {/* Level 3: Country */}
                                  <div className="flex items-center justify-between py-1">
                                    <div className="flex items-center space-x-2">
                                      <span className="font-medium text-gray-700 dark:text-gray-300 text-sm">
                                        {country}
                                      </span>
                                      <span className="text-xs text-gray-500 dark:text-gray-400">
                                        ({countryData.totalUsers.toLocaleString()} users)
                                      </span>
                                    </div>
                                    <div className="flex items-center space-x-2">
                                      {getStatusIndicator(countryData.passesThreshold, countryData.rollupInfo, 'country', countryData.rollupInfo.rollupTarget)}
                                    </div>
                                  </div>

                                  {/* Level 4: Regions (if any) */}
                                  {showFullDetails && countryData.regions.length > 0 && (
                                    <div className="ml-4 mt-1 space-y-1">
                                      {countryData.regions.map((region, idx) => (
                                        <div key={idx} className="flex items-center justify-between text-xs py-1">
                                          <div className="flex items-center space-x-2">
                                            <span className="text-gray-600 dark:text-gray-400">
                                              📍 {region.name}
                                            </span>
                                            <span className="text-gray-500 dark:text-gray-500">
                                              ({region.users.toLocaleString()} users)
                                            </span>
                                          </div>
                                          <div className="flex items-center space-x-1">
                                            {getStatusIndicator(region.passes, {
                                              rollupTarget: region.rollupTarget,
                                              rollupReasons: region.rollupReason ? [region.rollupReason] : [],
                                              finalUserCount: region.finalUserCount,
                                              propertiesDropped: region.propertiesDropped
                                            }, 'region', region.rollupTarget)}
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Summary Statistics */}
            <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
              <h4 className="font-medium text-gray-900 dark:text-white mb-3">Rollup Summary</h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Total Product Combinations</div>
                  <div className="font-medium text-gray-900 dark:text-white">
                    {Object.keys(hierarchicalData).length}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Segments Meeting Threshold</div>
                  <div className="font-medium text-green-600 dark:text-green-400">
                    {Object.values(hierarchicalData).filter(d => d.passesThreshold).length}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Segments Requiring Rollup</div>
                  <div className="font-medium text-orange-600 dark:text-orange-400">
                    {Object.values(hierarchicalData).filter(d => !d.passesThreshold).length}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500 dark:text-gray-400">Total Users Analyzed</div>
                  <div className="font-medium text-gray-900 dark:text-white">
                    {Object.values(hierarchicalData).reduce((sum, d) => sum + d.totalUsers, 0).toLocaleString()}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PropertyAnalysisResults; 