import React from 'react';
import { X, Target, Users, TrendingUp, Package, Store, DollarSign, Globe, MapPin, ArrowRight, BarChart3 } from 'lucide-react';
import { formatPercentage, getSegmentIdBadge, formatAppStore } from './utils/conversionUtils';

const RollupModal = ({ isOpen, onClose, rollupData }) => {
  if (!isOpen || !rollupData) return null;

  // Function to get economic tier for a country
  const getEconomicTier = (country) => {
    const economicTiers = {
      'tier1_high_income': ['US', 'CA', 'GB', 'AU', 'DE', 'FR', 'NL', 'SE', 'NO', 'DK', 'CH', 'AT'],
      'tier2_upper_middle': ['JP', 'KR', 'IT', 'ES', 'PT', 'GR', 'CZ', 'PL', 'HU', 'SK', 'SI', 'EE', 'LV', 'LT'],
      'tier3_emerging': ['CN', 'BR', 'MX', 'AR', 'CL', 'CO', 'PE', 'RU', 'TR', 'ZA', 'TH', 'MY', 'SG', 'HK'],
      'tier4_developing': ['IN', 'ID', 'PH', 'VN', 'BD', 'PK', 'NG', 'KE', 'EG', 'MA', 'UA', 'RO', 'BG']
    };
    
    for (const [tier, countries] of Object.entries(economicTiers)) {
      if (countries.includes(country)) {
        return tier.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase());
      }
    }
    return 'Tier 4 Developing';
  };

  const targetSegment = rollupData.targetSegment;
  const rollingUpSegments = rollupData.rollingUpSegments || [];

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 backdrop-blur-sm flex items-center justify-center p-4 z-50">
      <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-2xl max-w-7xl w-full max-h-[90vh] overflow-hidden">
        {/* Enhanced Header */}
        <div className="p-8 border-b border-gray-200 dark:border-gray-700 bg-gradient-to-r from-blue-600 to-purple-600 text-white">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-white bg-opacity-20 rounded-xl">
                <Target className="h-8 w-8" />
              </div>
              <div>
                <h2 className="text-2xl font-bold">Rollup Target Analysis</h2>
                <p className="text-blue-100 mt-1">
                  Segments consolidated into this viable target
                </p>
              </div>
            </div>
            <button
              onClick={onClose}
              className="p-2 hover:bg-white hover:bg-opacity-20 rounded-xl transition-colors"
            >
              <X className="h-6 w-6" />
            </button>
          </div>
        </div>

        {/* Target Segment Info */}
        <div className="p-8 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/10 dark:to-purple-900/10 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center">
            <BarChart3 className="h-6 w-6 mr-2 text-blue-600" />
            Target Segment Details
          </h3>
          
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <Package className="h-4 w-4 text-blue-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Product</span>
              </div>
              <div className="text-sm font-bold text-gray-900 dark:text-white truncate">
                {targetSegment.productId}
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <Store className="h-4 w-4 text-green-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Store</span>
              </div>
              <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                {formatAppStore(targetSegment.appStore)}
              </span>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <DollarSign className="h-4 w-4 text-yellow-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Price</span>
              </div>
              <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                {targetSegment.priceBucket}
              </span>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <Globe className="h-4 w-4 text-purple-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Tier</span>
              </div>
              <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                targetSegment.tier?.includes('Tier 1') ? 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200' :
                targetSegment.tier?.includes('Tier 2') ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200' :
                targetSegment.tier?.includes('Tier 3') ? 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200' :
                'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200'
              }`}>
                {targetSegment.tier}
              </span>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <Globe className="h-4 w-4 text-indigo-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Country</span>
              </div>
              <div className="text-sm font-bold text-gray-900 dark:text-white">
                {targetSegment.country || 'Any'}
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <MapPin className="h-4 w-4 text-red-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Region</span>
              </div>
              <div className="text-sm font-bold text-gray-900 dark:text-white">
                {targetSegment.region || 'Any'}
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center mb-2">
                <Users className="h-4 w-4 text-teal-600 mr-2" />
                <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Users</span>
              </div>
              <div className="text-lg font-bold text-teal-600 dark:text-teal-400">
                {targetSegment.cohortSize?.toLocaleString()}
              </div>
            </div>
          </div>

          {/* Segment ID */}
          <div className="mt-4 flex items-center space-x-2">
            <span className="text-sm text-gray-600 dark:text-gray-400">Target Segment ID:</span>
            {getSegmentIdBadge(targetSegment.segmentId)}
          </div>
        </div>

        {/* Summary Stats */}
        <div className="p-6 bg-gray-50 dark:bg-gray-900/50 border-b border-gray-200 dark:border-gray-700">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="text-center p-4 bg-white dark:bg-gray-800 rounded-xl shadow-sm">
              <div className="text-3xl font-bold text-blue-600 dark:text-blue-400">{rollingUpSegments.length}</div>
              <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">Segments Rolled Up</div>
            </div>
            <div className="text-center p-4 bg-white dark:bg-gray-800 rounded-xl shadow-sm">
              <div className="text-3xl font-bold text-green-600 dark:text-green-400">1</div>
              <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">Target Calculated</div>
            </div>
            <div className="text-center p-4 bg-white dark:bg-gray-800 rounded-xl shadow-sm">
              <div className="text-3xl font-bold text-purple-600 dark:text-purple-400">
                {rollingUpSegments.reduce((sum, seg) => sum + (seg.original_user_count || 0), 0).toLocaleString()}
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">Original Users</div>
            </div>
            <div className="text-center p-4 bg-white dark:bg-gray-800 rounded-xl shadow-sm">
              <div className="text-3xl font-bold text-orange-600 dark:text-orange-400">
                {Math.round((rollingUpSegments.length / (rollingUpSegments.length + 1)) * 100)}%
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">Consolidation Rate</div>
            </div>
          </div>
        </div>

        {/* Segments Table */}
        <div className="flex-1 overflow-auto max-h-96">
          {rollingUpSegments.length > 0 ? (
            <div className="p-6">
              <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4 flex items-center">
                <ArrowRight className="h-5 w-5 mr-2 text-orange-600" />
                Non-Viable Segments Rolling Up to Target ({rollingUpSegments.length})
              </h3>
              
              <div className="overflow-x-auto">
                <table className="w-full divide-y divide-gray-200 dark:divide-gray-700">
                  <thead className="bg-gradient-to-r from-gray-50 to-gray-100 dark:from-gray-700 dark:to-gray-600">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Segment ID
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Product ID
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Store
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Price
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Economic Tier
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Country
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Region
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Original Users
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 dark:text-gray-200 uppercase tracking-wider">
                        Rollup Reason
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                    {rollingUpSegments.map((segment, index) => {
                      const economicTier = getEconomicTier(segment.country);
                      
                      return (
                        <tr key={index} className="hover:bg-blue-50 dark:hover:bg-gray-700 transition-colors">
                          <td className="px-4 py-3 whitespace-nowrap">
                            {getSegmentIdBadge(segment.segment_id)}
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <div className="text-sm font-medium text-gray-900 dark:text-white">
                              {segment.product_id}
                            </div>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
                              {formatAppStore(segment.app_store)}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-bold bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                              {segment.price_bucket}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                              economicTier.includes('Tier 1') ? 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200' :
                              economicTier.includes('Tier 2') ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200' :
                              economicTier.includes('Tier 3') ? 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200' :
                              'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200'
                            }`}>
                              {economicTier}
                            </span>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <div className="text-sm font-medium text-gray-900 dark:text-white">
                              {segment.country || 'Any'}
                            </div>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <div className="text-sm text-gray-900 dark:text-white">
                              {segment.region || 'Any Region'}
                            </div>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-right">
                            <div className="text-sm font-bold text-gray-900 dark:text-white">
                              {(segment.original_user_count || 0).toLocaleString()}
                            </div>
                            <div className="text-xs text-red-600 dark:text-red-400">
                              Too few users
                            </div>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            <div className="text-sm text-gray-600 dark:text-gray-400">
                              {segment.rollup_description || 'Insufficient data'}
                            </div>
                            {segment.properties_dropped && segment.properties_dropped.length > 0 && (
                              <div className="text-xs text-orange-600 dark:text-orange-400 mt-1">
                                Dropped: {segment.properties_dropped.join(', ')}
                              </div>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div className="p-12 text-center">
              <div className="text-gray-400 dark:text-gray-500 mb-4">
                <Target className="h-16 w-16 mx-auto" />
              </div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
                No Rollup Segments
              </h3>
              <p className="text-gray-600 dark:text-gray-400">
                This segment had sufficient data and didn't require any rollups.
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-6 bg-gray-50 dark:bg-gray-900/50 border-t border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-600 dark:text-gray-400">
              {rollingUpSegments.length > 0 ? (
                <span>
                  {rollingUpSegments.length} non-viable segments consolidated into 1 target segment
                </span>
              ) : (
                <span>
                  This segment required no consolidation - sufficient data available
                </span>
              )}
            </div>
            <button
              onClick={onClose}
              className="inline-flex items-center px-6 py-2 border border-transparent rounded-lg text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default RollupModal; 