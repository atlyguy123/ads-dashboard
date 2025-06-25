import React, { useState, Fragment, useRef, useEffect } from 'react';
import { ChevronDown, ChevronRight, Layers, Table2, Search, AlignJustify, ChevronUp, ArrowUpDown } from 'lucide-react';
// 📋 ADDING NEW COLUMNS? Read: src/config/Column README.md for complete instructions
import { AVAILABLE_COLUMNS } from '../config/columns';
import ROASSparkline from './dashboard/ROASSparkline';



// Estimated ROAS Tooltip Component
const EstimatedRoasTooltip = ({ roas, estimatedRevenue, diffPercent, spend, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

  const handleMouseEnter = (e) => {
    setShowTooltip(true);
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  const adjustedRevenue = diffPercent > 0 ? estimatedRevenue / diffPercent : estimatedRevenue;

  return (
    <div className="relative">
      <span 
        className={`${colorClass} ${pipelineUpdatedClass} cursor-pointer hover:underline`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {formatNumber(roas, 2)}
      </span>
      
      {showTooltip && (
        <div 
          className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg p-3 min-w-64"
          style={{
            left: tooltipPosition.x - 128, // Center the tooltip
            top: tooltipPosition.y - 10,
            transform: 'translateY(-100%)'
          }}
        >
          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Estimated ROAS Calculation
          </div>
          <div className="space-y-1 text-xs">
            <div className="text-gray-700 dark:text-gray-300">
              Original Est. Revenue: {formatCurrency(estimatedRevenue)}
            </div>
            {diffPercent > 0 && (
              <>
                <div className="text-gray-700 dark:text-gray-300">
                  Trial Accuracy Ratio: {formatPercentage(diffPercent)} (Mixpanel/Meta)
                </div>
                <div className="text-gray-700 dark:text-gray-300">
                  Adjusted Revenue: {formatCurrency(adjustedRevenue)}
                </div>
                <div className="text-gray-500 dark:text-gray-400 text-xs italic">
                  (Revenue ÷ {formatPercentage(diffPercent)} to account for Meta accuracy)
                </div>
              </>
            )}
            <div className="text-gray-700 dark:text-gray-300">
              Spend: {formatCurrency(spend)}
            </div>
            <div className="border-t border-gray-200 dark:border-gray-600 mt-2 pt-2 font-medium">
              Est. ROAS: {formatCurrency(adjustedRevenue)} ÷ {formatCurrency(spend)} = {formatNumber(roas, 2)}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Accuracy Tooltip Component
const AccuracyTooltip = ({ average, breakdown, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const ref = useRef(null);

  const handleMouseEnter = (e) => {
    setShowTooltip(true);
    // Position tooltip relative to the cell
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  // Get color for accuracy level
  const getAccuracyColor = (level) => {
    switch (level.toLowerCase()) {
      case 'very_high': return 'text-green-700';
      case 'high': return 'text-green-600';
      case 'medium': return 'text-yellow-600';
      case 'low': return 'text-orange-600';
      case 'very_low': return 'text-red-600';
      default: return 'text-gray-600';
    }
  };

  const formatLevelName = (level) => {
    return level.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1)
    ).join(' ');
  };

  return (
    <div className="relative">
      <span 
        ref={ref}
        className={`${colorClass} ${pipelineUpdatedClass} cursor-pointer hover:underline`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {average}
      </span>
      
      {showTooltip && (
        <div 
          className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg p-3 min-w-48"
          style={{
            left: tooltipPosition.x - 96, // Center the tooltip (half of min-width)
            top: tooltipPosition.y - 10,
            transform: 'translateY(-100%)'
          }}
        >
          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Accuracy Breakdown
          </div>
          <div className="space-y-1">
            {Object.entries(breakdown)
              .sort(([,a], [,b]) => b.percentage - a.percentage) // Sort by percentage desc
              .map(([level, data]) => (
                <div key={level} className="flex justify-between items-center text-xs">
                  <span className={`${getAccuracyColor(level)} font-medium`}>
                    {formatLevelName(level)}:
                  </span>
                  <span className="text-gray-700 dark:text-gray-300">
                    {data.count} ({data.percentage}%)
                  </span>
                </div>
              ))}
          </div>
          <div className="border-t border-gray-200 dark:border-gray-600 mt-2 pt-2 text-xs text-gray-600 dark:text-gray-400">
            Total Users: {Object.values(breakdown).reduce((sum, data) => sum + data.count, 0)}
          </div>
        </div>
      )}
    </div>
  );
};

// Refund Rate Tooltip Component for showing minimum default explanations
const RefundRateTooltip = ({ value, type, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  
  const isMinimum = type === 'trial' ? Math.abs(value - 5.0) < 0.01 : Math.abs(value - 15.0) < 0.01;
  const minimumRate = type === 'trial' ? '5%' : '15%';
  
  if (!isMinimum) return null; // Only show tooltip for minimum values

  const handleMouseEnter = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
    setShowTooltip(true);
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  return (
    <>
      <span
        className={`${colorClass} ${pipelineUpdatedClass} cursor-help`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        *
      </span>
      {showTooltip && (
        <div
          className="fixed z-50 bg-gray-900 text-white text-xs rounded px-2 py-1 max-w-xs"
          style={{
            left: tooltipPosition.x,
            top: tooltipPosition.y,
            transform: 'translateX(-50%) translateY(-100%)'
          }}
        >
          <div className="text-center">
            <div className="font-medium">Default Minimum Applied</div>
            <div className="mt-1">
              This rate defaulted to {minimumRate} because either there weren't enough users 
              or the calculated rate was lower. We use this minimum to ensure reliable estimates.
            </div>
          </div>
        </div>
      )}
    </>
  );
};

// Conversion Rate Tooltip Component for showing individual user details
const ConversionRateTooltip = ({ row, columnKey, value, colorClass, dashboardParams }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const [userDetails, setUserDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showModal, setShowModal] = useState(false);
  const [allUsers, setAllUsers] = useState([]);
  const [copiedUserId, setCopiedUserId] = useState(null);
  
  // Filter states for modal
  const [filters, setFilters] = useState({
    country: '',
    region: '',
    device_category: '',
    economic_tier: '',
    product_id: ''
  });

  const handleMouseEnter = async (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;
    
    // Calculate initial position (above the element)
    let x = rect.left + rect.width / 2;
    let y = rect.top - 10;
    
    // Adjust horizontal position to stay within window bounds
    if (x < 100) {
      x = 100;
    } else if (x > windowWidth - 100) {
      x = windowWidth - 100;
    }
    
    // If too close to top, position below instead
    if (y < 300) {
      y = rect.bottom + 10;
    }
    
    setTooltipPosition({ x, y });
    setShowTooltip(true);
    setLoading(true);
    setError(null);
    
    try {
      // Extract entity information from row
      // For breakdown rows, use the parent entity type instead of row.type
      let entityType = row.type; // 'campaign', 'adset', or 'ad'
      const entityId = row.id; // e.g., 'campaign_123' or 'US_123' for breakdowns
      
      // Handle breakdown rows - they might not have proper entity_type
      if (entityId && entityId.includes('_') && !entityId.startsWith('campaign_') && !entityId.startsWith('adset_') && !entityId.startsWith('ad_')) {
        // This is a breakdown row like "US_123" - use the parent entity type
        entityType = row.entity_type || 'campaign'; // fallback to campaign if not specified
      }
      
      // Build API parameters
      let apiEntityId = entityId;
      let breakdownValue = null;
      
      // Extract breakdown_value and actual entity_id for breakdown rows
      if (entityId && entityId.includes('_') && !entityId.startsWith('campaign_') && !entityId.startsWith('adset_') && !entityId.startsWith('ad_')) {
        const parts = entityId.split('_');
        breakdownValue = parts[0]; // e.g., "US" from "US_120215772671800178"
        apiEntityId = parts.slice(1).join('_'); // e.g., "120215772671800178" from "US_120215772671800178"
      }
      
      const params = new URLSearchParams({
        entity_type: entityType,
        entity_id: apiEntityId,
        start_date: dashboardParams?.start_date || '2025-01-01',
        end_date: dashboardParams?.end_date || '2025-12-31',
        breakdown: dashboardParams?.breakdown || 'all'
      });
      
      // Add breakdown_value if this is a breakdown row
      if (breakdownValue) {
        params.append('breakdown_value', breakdownValue);
      }
      
      const response = await fetch(`/api/dashboard/analytics/user-details?${params.toString()}`);
      const result = await response.json();
      
      if (result.success) {
        // API returns data directly, not nested under 'data' property
        setUserDetails(result);
        // Store all users for modal (API returns up to 100)
        setAllUsers(result.users || []);
      } else {
        setError(result.error || 'Failed to load user details');
      }
    } catch (err) {
      setError('Network error loading user details');
      console.error('Error fetching user details:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
    setUserDetails(null);
    setError(null);
  };

  const handleClick = () => {
    if (userDetails && allUsers.length > 0) {
      setShowModal(true);
      setShowTooltip(false);
      // Reset filters when opening modal
      resetFilters();
    }
  };

  // Helper function to get all three rates for a user
  const getAllRates = (user) => {
    return {
      trial_conversion_rate: user.trial_conversion_rate || 0,
      trial_refund_rate: user.trial_refund_rate || 0,
      purchase_refund_rate: user.purchase_refund_rate || 0
    };
  };

  // Helper function to get unique filter options
  const getFilterOptions = (field) => {
    const values = allUsers.map(user => user[field]).filter(value => value && value !== 'N/A');
    return [...new Set(values)].sort();
  };

  // Helper function to filter users based on current filters
  const getFilteredUsers = () => {
    return allUsers.filter(user => {
      return Object.entries(filters).every(([key, value]) => {
        if (!value) return true; // No filter applied
        return user[key] === value;
      });
    });
  };

  // Helper function to reset filters
  const resetFilters = () => {
    setFilters({
      country: '',
      region: '',
      device_category: '',
      economic_tier: '',
      product_id: ''
    });
  };

  // Helper function to get the rate value for the specific column
  const getRateForColumn = (user) => {
    switch (columnKey) {
      case 'trial_conversion_rate':
        return user.trial_conversion_rate;
      case 'avg_trial_refund_rate':
        return user.trial_refund_rate;
      case 'purchase_refund_rate':
        return user.purchase_refund_rate;
      default:
        return 0;
    }
  };

  // Helper function to get rate label
  const getRateLabel = () => {
    switch (columnKey) {
      case 'trial_conversion_rate':
        return 'Trial Conversion Rate';
      case 'avg_trial_refund_rate':
        return 'Trial Refund Rate';
      case 'purchase_refund_rate':
        return 'Purchase Refund Rate';
      default:
        return 'Rate';
    }
  };

  // Copy user ID to clipboard
  const copyUserId = async (userId, index) => {
    try {
      await navigator.clipboard.writeText(userId);
      setCopiedUserId(index);
      setTimeout(() => setCopiedUserId(null), 2000);
    } catch (err) {
      console.error('Failed to copy user ID:', err);
    }
  };

  return (
    <>
      <span
        className={`${colorClass} cursor-pointer hover:underline`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
        title="Click to see all users"
      >
        {value !== undefined && value !== null ? `${formatNumber(value, 2)}%` : 'N/A'}
      </span>
      {showTooltip && (
        <div
          className="fixed z-50 bg-gray-900 text-white text-xs rounded-lg shadow-lg border border-gray-700 max-w-lg"
          style={{
            left: tooltipPosition.x,
            top: tooltipPosition.y,
            transform: tooltipPosition.y > 300 ? 'translateX(-50%) translateY(-100%)' : 'translateX(-50%)'
          }}
        >
          <div className="p-3">
            <div className="font-semibold text-blue-300 mb-2">{getRateLabel()}</div>
            
            {loading && (
              <div className="text-center py-2">
                <div className="text-gray-300">Loading user details...</div>
              </div>
            )}
            
            {error && (
              <div className="text-center py-2">
                <div className="text-red-300">Error: {error}</div>
              </div>
            )}
            
            {userDetails && !loading && (
              <div className="space-y-4">
                {/* Enhanced Summary Section */}
                <div className="bg-gradient-to-r from-blue-900/50 to-purple-900/50 p-3 rounded-lg border border-blue-700/50">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className="w-2 h-2 bg-blue-400 rounded-full"></div>
                      <span className="font-semibold text-blue-300">{getRateLabel()}</span>
                    </div>
                    <div className="text-xs text-gray-400">
                      {userDetails.users.length} users
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="text-center">
                      <div className="text-2xl font-bold text-white">
                        {userDetails.summary.total_users}
                      </div>
                      <div className="text-xs text-gray-300">Total Users</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-400">
                        {formatNumber(
                          columnKey === 'trial_conversion_rate' ? userDetails.summary.avg_trial_conversion_rate :
                          columnKey === 'avg_trial_refund_rate' ? userDetails.summary.avg_trial_refund_rate :
                          userDetails.summary.avg_purchase_refund_rate, 1
                        )}%
                      </div>
                      <div className="text-xs text-gray-300">Average Rate</div>
                    </div>
                  </div>
                </div>
                
                {userDetails.users.length > 0 && (
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-2">
                        <div className="w-2 h-2 bg-yellow-400 rounded-full"></div>
                        <span className="font-medium text-yellow-300">Top Performers</span>
                      </div>
                      <div className="text-xs text-gray-400">
                        Click to see all
                      </div>
                    </div>
                    <div className="max-h-96 overflow-y-auto space-y-2 pr-2">
                      {userDetails.users.slice(0, 10).map((user, index) => (
                        <div key={index} className="bg-gray-800/50 p-3 rounded-lg border border-gray-700/50 hover:border-gray-600/50 transition-all">
                          <div className="flex items-start justify-between">
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center space-x-2 mb-2">
                                <div className="w-6 h-6 bg-blue-600 text-white text-xs font-bold rounded-full flex items-center justify-center">
                                  {index + 1}
                                </div>
                                <div className="text-sm font-medium text-white truncate">
                                  U{index + 1} • {user.product_id}
                                </div>
                              </div>
                              <div className="grid grid-cols-2 gap-2 text-xs text-gray-300">
                                <div>📍 {user.country}, {user.region}</div>
                                <div>💰 {user.price_bucket}</div>
                                <div>📱 {user.device_category}</div>
                                <div>🎯 {user.economic_tier}</div>
                              </div>
                            </div>
                            <div className="text-right ml-3 flex-shrink-0">
                              <div className="text-lg font-bold text-green-400">
                                {formatNumber(getRateForColumn(user), 1)}%
                              </div>
                              <div className="text-xs text-gray-400 bg-gray-700/50 px-2 py-1 rounded mt-1">
                                {user.accuracy_score}
                              </div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                    
                    {userDetails.users.length > 10 && (
                      <div className="mt-3 text-center">
                        <div className="text-xs text-blue-300 bg-blue-900/30 px-3 py-2 rounded-lg border border-blue-700/50">
                          Showing top 10 of {userDetails.users.length} users • Click anywhere to see all
                        </div>
                      </div>
                    )}
                  </div>
                )}
                
                {userDetails.users.length === 0 && (
                  <div className="text-center py-8 text-gray-400">
                    <div className="text-4xl mb-2">📊</div>
                    <div className="font-medium">No users found</div>
                    <div className="text-xs mt-1">Try adjusting your filters</div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
      
      {/* Modal for all users */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={() => setShowModal(false)}>
          <div className="bg-gray-900 text-white rounded-lg shadow-lg max-w-4xl w-full mx-4 max-h-[80vh] overflow-hidden" onClick={(e) => e.stopPropagation()}>
            <div className="p-6 border-b border-gray-700 bg-gradient-to-r from-gray-800 to-gray-700">
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <h2 className="text-2xl font-bold text-white mb-3">{getRateLabel()} - All Users</h2>
                  <div className="flex items-center space-x-8 text-sm mb-4">
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                      <span className="text-gray-300">Total Users: <span className="font-bold text-white text-lg">{userDetails?.summary?.total_users || allUsers.length}</span></span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-green-500 rounded-full"></div>
                      <span className="text-gray-300">Average Rate: <span className="font-bold text-green-400 text-lg">{(() => {
                        const rate = columnKey === 'trial_conversion_rate' ? userDetails?.summary?.avg_trial_conversion_rate :
                                    columnKey === 'avg_trial_refund_rate' ? userDetails?.summary?.avg_trial_refund_rate :
                                    userDetails?.summary?.avg_purchase_refund_rate;
                        return formatNumber(rate, 2);
                      })()}%</span></span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-purple-500 rounded-full"></div>
                      <span className="text-gray-300">Filtered: <span className="font-bold text-purple-400 text-lg">{getFilteredUsers().length}</span></span>
                    </div>
                  </div>
                  
                  {/* Filter Controls */}
                  <div className="grid grid-cols-5 gap-3 mb-2">
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Country</label>
                      <select 
                        value={filters.country} 
                        onChange={(e) => setFilters({...filters, country: e.target.value})}
                        className="w-full bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600 focus:border-blue-500"
                      >
                        <option value="">All Countries</option>
                        {getFilterOptions('country').map(option => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Region</label>
                      <select 
                        value={filters.region} 
                        onChange={(e) => setFilters({...filters, region: e.target.value})}
                        className="w-full bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600 focus:border-blue-500"
                      >
                        <option value="">All Regions</option>
                        {getFilterOptions('region').map(option => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Device</label>
                      <select 
                        value={filters.device_category} 
                        onChange={(e) => setFilters({...filters, device_category: e.target.value})}
                        className="w-full bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600 focus:border-blue-500"
                      >
                        <option value="">All Devices</option>
                        {getFilterOptions('device_category').map(option => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Tier</label>
                      <select 
                        value={filters.economic_tier} 
                        onChange={(e) => setFilters({...filters, economic_tier: e.target.value})}
                        className="w-full bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600 focus:border-blue-500"
                      >
                        <option value="">All Tiers</option>
                        {getFilterOptions('economic_tier').map(option => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-gray-400 mb-1">Product</label>
                      <select 
                        value={filters.product_id} 
                        onChange={(e) => setFilters({...filters, product_id: e.target.value})}
                        className="w-full bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600 focus:border-blue-500"
                      >
                        <option value="">All Products</option>
                        {getFilterOptions('product_id').map(option => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  
                  <div className="flex justify-end">
                    <button 
                      onClick={resetFilters}
                      className="text-xs bg-gray-600 hover:bg-gray-500 text-white px-3 py-1 rounded transition-colors"
                    >
                      Reset Filters
                    </button>
                  </div>
                </div>
                <button 
                  onClick={() => setShowModal(false)}
                  className="text-gray-400 hover:text-white p-2 hover:bg-gray-600 rounded-full transition-colors ml-4"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>
            <div className="p-6 overflow-y-auto max-h-[65vh]">
              {getFilteredUsers().length > 0 ? (
                <div className="space-y-3">
                  {/* Sort filtered users by conversion rate (highest to lowest) */}
                  {getFilteredUsers()
                    .sort((a, b) => getRateForColumn(b) - getRateForColumn(a))
                    .map((user, index) => {
                      const allRates = getAllRates(user);
                      const activeRate = getRateForColumn(user);
                      const rateColor = activeRate >= 50 ? 'text-green-400' : activeRate >= 25 ? 'text-yellow-400' : 'text-orange-400';
                      
                      return (
                        <div key={index} className="flex justify-between items-center p-4 bg-gray-800 rounded-lg hover:bg-gray-700 transition-colors border border-gray-700 hover:border-gray-600">
                          <div className="flex-1">
                            <div className="flex items-center space-x-3 mb-3">
                              <div className="text-white font-bold text-lg bg-blue-600 rounded-full w-8 h-8 flex items-center justify-center">
                                {index + 1}
                              </div>
                              <div className="flex-1">
                                <div className="text-white font-semibold">U{index + 1}</div>
                                <div className="flex items-center space-x-2 mt-1">
                                  <div className="text-xs text-gray-400 font-mono bg-gray-700 px-2 py-1 rounded">
                                    {user.distinct_id}
                                  </div>
                                  <button
                                    onClick={() => copyUserId(user.distinct_id, index)}
                                    className="text-xs bg-blue-600 hover:bg-blue-500 text-white px-2 py-1 rounded transition-colors"
                                    title="Copy User ID"
                                  >
                                    {copiedUserId === index ? '✓ Copied' : 'Copy ID'}
                                  </button>
                                </div>
                              </div>
                            </div>
                            <div className="space-y-2 text-sm">
                              <div className="text-gray-300">
                                <span className="font-medium">Location:</span> {user.country} • {user.region}
                              </div>
                              <div className="text-gray-300">
                                <span className="font-medium">Store:</span> {user.device_category}
                              </div>
                              <div className="text-gray-300">
                                <span className="font-medium">Price:</span> {user.price_bucket}
                              </div>
                              <div className="text-gray-300">
                                <span className="font-medium">Tier:</span> {user.economic_tier}
                              </div>
                              <div className="text-gray-300">
                                <span className="font-medium">Product:</span> {user.product_id}
                              </div>
                            </div>
                            <div className="flex items-center space-x-4 mt-3 text-xs text-gray-500">
                              <span>Status: <span className="text-gray-400">{user.status}</span></span>
                              <span>Credited: <span className="text-gray-400">{user.credited_date}</span></span>
                              <span>Value: <span className="text-gray-400">${user.estimated_value}</span></span>
                            </div>
                          </div>
                          <div className="text-right ml-6">
                            {/* All three rates with highlighting for active one */}
                            <div className="space-y-2 mb-3">
                              <div className={`text-sm ${columnKey === 'trial_conversion_rate' ? 'font-bold text-blue-400 bg-blue-900/30 px-2 py-1 rounded' : 'text-gray-300'}`}>
                                Trial Conv: {formatNumber(allRates.trial_conversion_rate, 1)}%
                              </div>
                              <div className={`text-sm ${columnKey === 'avg_trial_refund_rate' ? 'font-bold text-blue-400 bg-blue-900/30 px-2 py-1 rounded' : 'text-gray-300'}`}>
                                Trial Refund: {formatNumber(allRates.trial_refund_rate, 1)}%
                              </div>
                              <div className={`text-sm ${columnKey === 'purchase_refund_rate' ? 'font-bold text-blue-400 bg-blue-900/30 px-2 py-1 rounded' : 'text-gray-300'}`}>
                                Purchase Refund: {formatNumber(allRates.purchase_refund_rate, 1)}%
                              </div>
                            </div>
                            {/* Accuracy score instead of High/Medium/Low */}
                            <div className="text-xs text-gray-400 bg-gray-700 px-2 py-1 rounded">
                              Accuracy: {user.accuracy_score}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                </div>
              ) : (
                <div className="text-center py-12 text-gray-400">
                  <div className="text-4xl mb-4">📊</div>
                  <div className="text-lg font-medium">
                    {allUsers.length > 0 ? 'No users match your filters' : 'No users found for this metric'}
                  </div>
                  <div className="text-sm mt-2">
                    {allUsers.length > 0 ? 'Try adjusting your filters above' : 'Try adjusting your date range'}
                  </div>
                  {allUsers.length > 0 && (
                    <button 
                      onClick={resetFilters}
                      className="mt-4 bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded transition-colors"
                    >
                      Reset Filters
                    </button>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
};

// Helper to format numbers, e.g., with commas
const formatNumber = (num, digits = 0) => {
  if (num === undefined || num === null) return 'N/A';
  return num.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
};

const formatCurrency = (num) => {
  if (num === undefined || num === null) return 'N/A';
  return num.toLocaleString(undefined, { style: 'currency', currency: 'USD' });
};

const formatPercentage = (num, digits = 2) => {
  if (num === undefined || num === null) return 'N/A';
  return `${(num * 100).toFixed(digits)}%`;
};

// Column categorization for event priority styling based on actual dashboard columns:
// TRIAL columns: "Trials (Meta)", "Trials (Mixpanel)", etc.
const TRIAL_RELATED_COLUMNS = [
  'mixpanel_trials_started',    // "Trials (Mixpanel)" 
  'meta_trials_started',        // "Trials (Meta)"
  'mixpanel_trials_ended',      // "Trials Ended (Mixpanel)"
  'mixpanel_trials_in_progress', // "Trials In Progress (Mixpanel)"  
  'click_to_trial_rate',        // "Click to Trial Rate"
  'mixpanel_cost_per_trial',    // "Cost per Trial (Mixpanel)"
  'meta_cost_per_trial',        // "Cost per Trial (Meta)"
  'trial_conversion_rate',      // "Trial Conversion Rate" (rate of trials converting to purchases)
  'avg_trial_refund_rate',      // "Trial Refund Rate" (rate of trial conversions that get refunded)
  'trial_accuracy_ratio'        // "Trial Accuracy Ratio"
];

// PURCHASE columns: "Purchases (Meta)", "Purchases (Mixpanel)", etc.
// Note: estimated_revenue_usd, profit, and estimated_roas are excluded from graying 
// because they should always remain visible as key metrics
const PURCHASE_RELATED_COLUMNS = [
  'mixpanel_purchases',         // "Purchases (Mixpanel)"
  'meta_purchases',             // "Purchases (Meta)"  
  'mixpanel_cost_per_purchase', // "Cost per Purchase (Mixpanel)"
  'meta_cost_per_purchase',     // "Cost per Purchase (Meta)"
  'purchase_refund_rate',       // "Purchase Refund Rate"
  'purchase_accuracy_ratio',    // "Purchase Accuracy Ratio"
  'mixpanel_conversions_net_refunds', // "Net Conversions (Mixpanel)"
  'mixpanel_revenue_usd',       // "Revenue (Mixpanel)"
  'mixpanel_refunds_usd'        // "Refunds (Mixpanel)"
  // estimated_revenue_usd, profit, and estimated_roas intentionally excluded 
  // so they never get grayed out - they're always-visible key metrics
];

// Helper to determine event priority based on Mixpanel counts
// Compares "Trial Started by Mixpanel" vs "Purchases in Mixpanel" 
const getEventPriority = (row) => {
  // Compares "Trial Started by Mixpanel" vs "Purchases in Mixpanel"
  const trialsCount = row.mixpanel_trials_started || 0;    // "Trial Started by Mixpanel"
  const purchasesCount = row.mixpanel_purchases || 0;      // "Purchases in Mixpanel"
  
  // When both are zero, default to graying out purchase columns (make trials the priority)
  if (trialsCount === 0 && purchasesCount === 0) return 'trials';
  
  // When trials > purchases, gray out purchase columns (trials are priority)
  if (trialsCount > purchasesCount) return 'trials';
  
  // When purchases > trials, gray out trial columns (purchases are priority)  
  if (purchasesCount > trialsCount) return 'purchases';
  
  // If they're equal and both > 0, no graying out
  return 'equal';
};

// Helper to check if a column should be grayed out based on event priority
const shouldGrayOutColumn = (columnKey, eventPriority) => {
  if (eventPriority === 'equal') return false;
  
  // When trials are priority, gray out purchase-related columns
  if (eventPriority === 'trials' && PURCHASE_RELATED_COLUMNS.includes(columnKey)) {
    return true;
  }
  
  // When purchases are priority, gray out trial-related columns
  if (eventPriority === 'purchases' && TRIAL_RELATED_COLUMNS.includes(columnKey)) {
    return true;
  }
  
  return false;
};

// Helper to get column type for visual differentiation
const getColumnType = (columnKey) => {
  if (TRIAL_RELATED_COLUMNS.includes(columnKey)) return 'trial';
  if (PURCHASE_RELATED_COLUMNS.includes(columnKey)) return 'purchase';
  return 'neutral';
};

// Helper to get column background class based on type
const getColumnBackgroundClass = (columnKey, isHovered = false) => {
  const columnType = getColumnType(columnKey);
  
  if (isHovered) {
    // Slightly stronger background on hover
    switch (columnType) {
      case 'trial': return 'bg-blue-50 dark:bg-blue-900/20';
      case 'purchase': return 'bg-green-50 dark:bg-green-900/20';
      default: return ''; // No special hover background for neutral columns
    }
  } else {
    // Subtle background for visual differentiation
    switch (columnType) {
      case 'trial': return 'bg-blue-25 dark:bg-blue-900/10';
      case 'purchase': return 'bg-green-25 dark:bg-green-900/10';
      default: return ''; // No background for neutral columns - normal grey
    }
  }
};

// Helper to calculate derived values for missing fields
const calculateDerivedValues = (row) => {
  // Frontend should NOT calculate values - all calculations should come from backend
  // Simply return the row as-is since backend provides all calculated fields
  return { ...row };
};

// Field color - use normal text colors
const getFieldColor = (fieldName, value) => {
  // Special color coding for accuracy column
  if (fieldName === 'average_accuracy' && value) {
    switch (value.toLowerCase()) {
      case 'very high': return 'text-green-700 dark:text-green-400';
      case 'high': return 'text-green-600 dark:text-green-400';
      case 'medium': return 'text-yellow-600 dark:text-yellow-400';
      case 'low': return 'text-orange-600 dark:text-orange-400';
      case 'very low': return 'text-red-600 dark:text-red-400';
      default: return 'text-gray-600 dark:text-gray-400';
    }
  }
  // Use standard text colors for all other fields
  return 'text-gray-900 dark:text-gray-100';
};

// Header color - use normal text colors
const getHeaderColor = (fieldName) => {
  // Use standard header colors for all fields
  return 'text-gray-700 dark:text-gray-300';
};

// ROAS color thresholds: <1 = Red, 1 = Yellow, >1.5 = Green
const roas_green_threshold = 1.5;
const roas_yellow_threshold = 1.0;

const getRoasColor = (roas) => {
  if (roas >= 3.0) return 'text-green-600 dark:text-green-400';
  if (roas >= 2.0) return 'text-yellow-600 dark:text-yellow-400';
  if (roas >= 1.0) return 'text-orange-600 dark:text-orange-400';
  return 'text-red-600 dark:text-red-400';
};



// Sort indicator component
const SortIndicator = ({ column, sortConfig }) => {
  if (sortConfig.column !== column) {
    return (
      <ArrowUpDown 
        size={12} 
        className="ml-1 text-gray-400 dark:text-gray-500 opacity-0 group-hover:opacity-100 transition-opacity" 
      />
    );
  }
  
  return sortConfig.direction === 'asc' ? (
    <ChevronUp size={12} className="ml-1 text-gray-600 dark:text-gray-300" />
  ) : (
    <ChevronDown size={12} className="ml-1 text-gray-600 dark:text-gray-300" />
  );
};

// ROAS Calculation Tooltip Component
const ROASCalculationTooltip = ({ row, roas, children }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

  const handleMouseEnter = (e) => {
    setShowTooltip(true);
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  // Calculate values for the tooltip
  const calculatedRow = calculateDerivedValues(row);
  const spend = calculatedRow.spend || 0;
  const estimatedRevenue = calculatedRow.estimated_revenue_usd || 0;
  const estimatedRevenueAdjusted = calculatedRow.estimated_revenue_adjusted || 0;
  
  // Event counts for accuracy calculation
  const mixpanelTrials = calculatedRow.mixpanel_trials_started || 0;
  const metaTrials = calculatedRow.meta_trials_started || 0;
  const mixpanelPurchases = calculatedRow.mixpanel_purchases || 0;
  const metaPurchases = calculatedRow.meta_purchases || 0;
  
  // Determine event priority and active accuracy ratio
  const eventPriority = mixpanelTrials === 0 && mixpanelPurchases === 0 ? 'trials' :
                       mixpanelTrials > mixpanelPurchases ? 'trials' :
                       mixpanelPurchases > mixpanelTrials ? 'purchases' : 'equal';
  
  const trialAccuracyRatio = metaTrials > 0 ? (mixpanelTrials / metaTrials) * 100 : 0;
  const purchaseAccuracyRatio = metaPurchases > 0 ? (mixpanelPurchases / metaPurchases) * 100 : 0;
  const activeAccuracyRatio = eventPriority === 'purchases' ? purchaseAccuracyRatio : trialAccuracyRatio;
  const activeAccuracyType = eventPriority === 'purchases' ? 'Purchase' : 'Trial';
  
  // Adjustment calculation
  const adjustmentFactor = activeAccuracyRatio > 0 && activeAccuracyRatio !== 100 ? (activeAccuracyRatio / 100) : 1;

  return (
    <div className="relative">
      <div 
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {children}
      </div>
      
      {showTooltip && (
        <div 
          className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg shadow-xl p-4 min-w-80"
          style={{
            left: tooltipPosition.x - 160, // Center the tooltip
            top: tooltipPosition.y - 10,
            transform: 'translateY(-100%)'
          }}
        >
          <div className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-3 border-b border-gray-200 dark:border-gray-600 pb-2">
            🎯 ROAS Calculation
          </div>
          
          <div className="space-y-2 text-sm text-gray-700 dark:text-gray-300">
            <div>1. Base Estimated Revenue: <span className="font-mono">{formatCurrency(estimatedRevenue)}</span></div>
            <div>2. Active Accuracy Ratio ({activeAccuracyType}): <span className="font-mono">{formatNumber(activeAccuracyRatio, 2)}%</span></div>
            <div>3. Adjustment Factor: <span className="font-mono">{formatNumber(adjustmentFactor, 3)}</span></div>
            <div>4. Adjusted Revenue: <span className="font-mono">{formatCurrency(estimatedRevenueAdjusted)}</span></div>
            <div>5. Spend: <span className="font-mono">{formatCurrency(spend)}</span></div>
            
            <div className="border-t border-gray-300 dark:border-gray-500 pt-2 mt-3 font-medium text-blue-600 dark:text-blue-400">
              <div className="text-base">
                🎯 ROAS: {formatCurrency(estimatedRevenueAdjusted)} ÷ {formatCurrency(spend)} = {formatNumber(roas, 2)}x
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export const DashboardGrid = ({ 
  data = [], 
  rowOrder = [],
  onRowOrderChange = null,
  columnVisibility = {}, 
  columnOrder = [],
  onColumnOrderChange = null,
  dashboardParams = null,
  sortConfig = { column: null, direction: 'asc' },
  onSort = () => {}
}) => {
  const [expandedRows, setExpandedRows] = useState({});
  const [expandedBreakdowns, setExpandedBreakdowns] = useState({});
  
  // Drag state
  const [draggedColumn, setDraggedColumn] = useState(null);
  const [dragOverColumn, setDragOverColumn] = useState(null);
  const [draggedRow, setDraggedRow] = useState(null);
  const [isDragging, setIsDragging] = useState(false);

  // Handle column header click for sorting (only if not dragging)
  const handleColumnHeaderClick = (columnKey) => {
    if (!isDragging && onSort) {
      onSort(columnKey);
    }
  };

  // Row drag state
  const [draggedRowId, setDraggedRowId] = useState(null);
  const [dragOverRowId, setDragOverRowId] = useState(null);

  // Get visible columns based on columnVisibility settings and column order
  const getOrderedVisibleColumns = () => {
    // Use column order if available, otherwise use default order
    const orderToUse = columnOrder.length > 0 ? columnOrder : AVAILABLE_COLUMNS.map(col => col.key);
    
    // Map the ordered keys to column objects, filtering out non-existent columns
    const orderedColumns = orderToUse
      .map(key => AVAILABLE_COLUMNS.find(col => col.key === key))
      .filter(col => col); // Remove any undefined columns
    
    // Filter by visibility
    if (Object.keys(columnVisibility).length === 0) {
      // If no visibility settings loaded yet, use default visibility
      return orderedColumns.filter(col => col.defaultVisible);
    } else {
      // Use explicit visibility settings - show column if explicitly true
      return orderedColumns.filter(col => columnVisibility[col.key] === true);
    }
  };

  const visibleColumns = getOrderedVisibleColumns();
  
  // Debug column visibility (cleanup after fix)
  console.log('Column Visibility Status:', {
    'visible_columns': visibleColumns.length,
    'estimated_revenue_adjusted_visible': visibleColumns.some(col => col.key === 'estimated_revenue_adjusted'),
    'mixpanel_revenue_net_visible': visibleColumns.some(col => col.key === 'mixpanel_revenue_net')
  });

  // Helper function to check if a column should be visible
  const isColumnVisible = (columnKey) => {
    if (Object.keys(columnVisibility).length === 0) {
      // If no visibility settings loaded yet, check default
      const column = AVAILABLE_COLUMNS.find(col => col.key === columnKey);
      return column ? column.defaultVisible : false;
    }
    return columnVisibility[columnKey] !== false;
  };



  // Helper function to get country flag emoji
const getCountryFlag = (countryCode) => {
  const countryFlags = {
    'AD': '🇦🇩', 'AE': '🇦🇪', 'AF': '🇦🇫', 'AG': '🇦🇬', 'AI': '🇦🇮', 'AL': '🇦🇱', 'AM': '🇦🇲', 'AO': '🇦🇴', 'AQ': '🇦🇶', 'AR': '🇦🇷', 'AS': '🇦🇸', 'AT': '🇦🇹', 'AU': '🇦🇺', 'AW': '🇦🇼', 'AX': '🇦🇽', 'AZ': '🇦🇿',
    'BA': '🇧🇦', 'BB': '🇧🇧', 'BD': '🇧🇩', 'BE': '🇧🇪', 'BF': '🇧🇫', 'BG': '🇧🇬', 'BH': '🇧🇭', 'BI': '🇧🇮', 'BJ': '🇧🇯', 'BL': '🇧🇱', 'BM': '🇧🇲', 'BN': '🇧🇳', 'BO': '🇧🇴', 'BQ': '🇧🇶', 'BR': '🇧🇷', 'BS': '🇧🇸', 'BT': '🇧🇹', 'BV': '🇧🇻', 'BW': '🇧🇼', 'BY': '🇧🇾', 'BZ': '🇧🇿',
    'CA': '🇨🇦', 'CC': '🇨🇨', 'CD': '🇨🇩', 'CF': '🇨🇫', 'CG': '🇨🇬', 'CH': '🇨🇭', 'CI': '🇨🇮', 'CK': '🇨🇰', 'CL': '🇨🇱', 'CM': '🇨🇲', 'CN': '🇨🇳', 'CO': '🇨🇴', 'CR': '🇨🇷', 'CU': '🇨🇺', 'CV': '🇨🇻', 'CW': '🇨🇼', 'CX': '🇨🇽', 'CY': '🇨🇾', 'CZ': '🇨🇿',
    'DE': '🇩🇪', 'DJ': '🇩🇯', 'DK': '🇩🇰', 'DM': '🇩🇲', 'DO': '🇩🇴', 'DZ': '🇩🇿',
    'EC': '🇪🇨', 'EE': '🇪🇪', 'EG': '🇪🇬', 'EH': '🇪🇭', 'ER': '🇪🇷', 'ES': '🇪🇸', 'ET': '🇪🇹',
    'FI': '🇫🇮', 'FJ': '🇫🇯', 'FK': '🇫🇰', 'FM': '🇫🇲', 'FO': '🇫🇴', 'FR': '🇫🇷',
    'GA': '🇬🇦', 'GB': '🇬🇧', 'GD': '🇬🇩', 'GE': '🇬🇪', 'GF': '🇬🇫', 'GG': '🇬🇬', 'GH': '🇬🇭', 'GI': '🇬🇮', 'GL': '🇬🇱', 'GM': '🇬🇲', 'GN': '🇬🇳', 'GP': '🇬🇵', 'GQ': '🇬🇶', 'GR': '🇬🇷', 'GS': '🇬🇸', 'GT': '🇬🇹', 'GU': '🇬🇺', 'GW': '🇬🇼', 'GY': '🇬🇾',
    'HK': '🇭🇰', 'HM': '🇭🇲', 'HN': '🇭🇳', 'HR': '🇭🇷', 'HT': '🇭🇹', 'HU': '🇭🇺',
    'ID': '🇮🇩', 'IE': '🇮🇪', 'IL': '🇮🇱', 'IM': '🇮🇲', 'IN': '🇮🇳', 'IO': '🇮🇴', 'IQ': '🇮🇶', 'IR': '🇮🇷', 'IS': '🇮🇸', 'IT': '🇮🇹',
    'JE': '🇯🇪', 'JM': '🇯🇲', 'JO': '🇯🇴', 'JP': '🇯🇵',
    'KE': '🇰🇪', 'KG': '🇰🇬', 'KH': '🇰🇭', 'KI': '🇰🇮', 'KM': '🇰🇲', 'KN': '🇰🇳', 'KP': '🇰🇵', 'KR': '🇰🇷', 'KW': '🇰🇼', 'KY': '🇰🇾', 'KZ': '🇰🇿',
    'LA': '🇱🇦', 'LB': '🇱🇧', 'LC': '🇱🇨', 'LI': '🇱🇮', 'LK': '🇱🇰', 'LR': '🇱🇷', 'LS': '🇱🇸', 'LT': '🇱🇹', 'LU': '🇱🇺', 'LV': '🇱🇻', 'LY': '🇱🇾',
    'MA': '🇲🇦', 'MC': '🇲🇨', 'MD': '🇲🇩', 'ME': '🇲🇪', 'MF': '🇲🇫', 'MG': '🇲🇬', 'MH': '🇲🇭', 'MK': '🇲🇰', 'ML': '🇲🇱', 'MM': '🇲🇲', 'MN': '🇲🇳', 'MO': '🇲🇴', 'MP': '🇲🇵', 'MQ': '🇲🇶', 'MR': '🇲🇷', 'MS': '🇲🇸', 'MT': '🇲🇹', 'MU': '🇲🇺', 'MV': '🇲🇻', 'MW': '🇲🇼', 'MX': '🇲🇽', 'MY': '🇲🇾', 'MZ': '🇲🇿',
    'NA': '🇳🇦', 'NC': '🇳🇨', 'NE': '🇳🇪', 'NF': '🇳🇫', 'NG': '🇳🇬', 'NI': '🇳🇮', 'NL': '🇳🇱', 'NO': '🇳🇴', 'NP': '🇳🇵', 'NR': '🇳🇷', 'NU': '🇳🇺', 'NZ': '🇳🇿',
    'OM': '🇴🇲',
    'PA': '🇵🇦', 'PE': '🇵🇪', 'PF': '🇵🇫', 'PG': '🇵🇬', 'PH': '🇵🇭', 'PK': '🇵🇰', 'PL': '🇵🇱', 'PM': '🇵🇲', 'PN': '🇵🇳', 'PR': '🇵🇷', 'PS': '🇵🇸', 'PT': '🇵🇹', 'PW': '🇵🇼', 'PY': '🇵🇾',
    'QA': '🇶🇦',
    'RE': '🇷🇪', 'RO': '🇷🇴', 'RS': '🇷🇸', 'RU': '🇷🇺', 'RW': '🇷🇼',
    'SA': '🇸🇦', 'SB': '🇸🇧', 'SC': '🇸🇨', 'SD': '🇸🇩', 'SE': '🇸🇪', 'SG': '🇸🇬', 'SH': '🇸🇭', 'SI': '🇸🇮', 'SJ': '🇸🇯', 'SK': '🇸🇰', 'SL': '🇸🇱', 'SM': '🇸🇲', 'SN': '🇸🇳', 'SO': '🇸🇴', 'SR': '🇸🇷', 'SS': '🇸🇸', 'ST': '🇸🇹', 'SV': '🇸🇻', 'SX': '🇸🇽', 'SY': '🇸🇾', 'SZ': '🇸🇿',
    'TC': '🇹🇨', 'TD': '🇹🇩', 'TF': '🇹🇫', 'TG': '🇹🇬', 'TH': '🇹🇭', 'TJ': '🇹🇯', 'TK': '🇹🇰', 'TL': '🇹🇱', 'TM': '🇹🇲', 'TN': '🇹🇳', 'TO': '🇹🇴', 'TR': '🇹🇷', 'TT': '🇹🇹', 'TV': '🇹🇻', 'TW': '🇹🇼', 'TZ': '🇹🇿',
    'UA': '🇺🇦', 'UG': '🇺🇬', 'UM': '🇺🇲', 'US': '🇺🇸', 'UY': '🇺🇾', 'UZ': '🇺🇿',
    'VA': '🇻🇦', 'VC': '🇻🇨', 'VE': '🇻🇪', 'VG': '🇻🇬', 'VI': '🇻🇮', 'VN': '🇻🇳', 'VU': '🇻🇺',
    'WF': '🇼🇫', 'WS': '🇼🇸',
    'XK': '🇽🇰',
    'YE': '🇾🇪', 'YT': '🇾🇹',
    'ZA': '🇿🇦', 'ZM': '🇿🇲', 'ZW': '🇿🇼'
  };
  return countryFlags[countryCode?.toUpperCase()] || '';
};

// Helper function to get country full name
const getCountryName = (countryCode) => {
  const countryNames = {
    'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina', 'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AX': 'Åland Islands', 'AZ': 'Azerbaijan',
    'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin', 'BL': 'Saint Barthélemy', 'BM': 'Bermuda', 'BN': 'Brunei', 'BO': 'Bolivia', 'BQ': 'Caribbean Netherlands', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus', 'BZ': 'Belize',
    'CA': 'Canada', 'CC': 'Cocos Islands', 'CD': 'Democratic Republic of the Congo', 'CF': 'Central African Republic', 'CG': 'Republic of the Congo', 'CH': 'Switzerland', 'CI': 'Côte d\'Ivoire', 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CU': 'Cuba', 'CV': 'Cape Verde', 'CW': 'Curaçao', 'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czech Republic',
    'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria',
    'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia',
    'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands', 'FM': 'Micronesia', 'FO': 'Faroe Islands', 'FR': 'France',
    'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia', 'GF': 'French Guiana', 'GG': 'Guernsey', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'South Georgia and the South Sandwich Islands', 'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana',
    'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary',
    'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IM': 'Isle of Man', 'IN': 'India', 'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy',
    'JE': 'Jersey', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan',
    'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'North Korea', 'KR': 'South Korea', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan',
    'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya',
    'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'ME': 'Montenegro', 'MF': 'Saint Martin', 'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'North Macedonia', 'ML': 'Mali', 'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia', 'MZ': 'Mozambique',
    'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru', 'NU': 'Niue', 'NZ': 'New Zealand',
    'OM': 'Oman',
    'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn', 'PR': 'Puerto Rico', 'PS': 'Palestine', 'PT': 'Portugal', 'PW': 'Palau', 'PY': 'Paraguay',
    'QA': 'Qatar',
    'RE': 'Réunion', 'RO': 'Romania', 'RS': 'Serbia', 'RU': 'Russia', 'RW': 'Rwanda',
    'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SE': 'Sweden', 'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen', 'SK': 'Slovakia', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'SS': 'South Sudan', 'ST': 'São Tomé and Príncipe', 'SV': 'El Salvador', 'SX': 'Sint Maarten', 'SY': 'Syria', 'SZ': 'Swaziland',
    'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau', 'TL': 'East Timor', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan', 'TZ': 'Tanzania',
    'UA': 'Ukraine', 'UG': 'Uganda', 'UM': 'United States Minor Outlying Islands', 'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan',
    'VA': 'Vatican City', 'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'British Virgin Islands', 'VI': 'U.S. Virgin Islands', 'VN': 'Vietnam', 'VU': 'Vanuatu',
    'WF': 'Wallis and Futuna', 'WS': 'Samoa',
    'XK': 'Kosovo',
    'YE': 'Yemen', 'YT': 'Mayotte',
    'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'
  };
  return countryNames[countryCode?.toUpperCase()] || countryCode;
};

// Helper function to render a cell value with proper formatting and coloring
// 📋 ADDING NEW COLUMN FORMATTING? Read: src/config/Column README.md for instructions
const renderCellValue = (row, columnKey, isPipelineUpdated = false, eventPriority = null, dashboardParams = null) => {
    const calculatedRow = calculateDerivedValues(row);
    let value = calculatedRow[columnKey];
    let formattedValue = 'N/A';
    let isEstimated = false;

    // Format values based on column type
    switch (columnKey) {
      case 'spend':
      case 'mixpanel_revenue_usd':
      case 'estimated_revenue_usd':
      case 'estimated_revenue_adjusted':
      case 'mixpanel_revenue_net':
      case 'mixpanel_refunds_usd':
      case 'profit':
      case 'mixpanel_cost_per_trial':
      case 'mixpanel_cost_per_purchase':
      case 'meta_cost_per_trial':
      case 'meta_cost_per_purchase':
        formattedValue = formatCurrency(value);
        break;
      case 'impressions':
      case 'clicks':
      case 'mixpanel_trials_started':
      case 'meta_trials_started':
      case 'mixpanel_trials_ended':
      case 'mixpanel_trials_in_progress':
      case 'mixpanel_purchases':
      case 'meta_purchases':
      case 'mixpanel_converted_amount':
      case 'mixpanel_conversions_net_refunds':
      case 'total_attributed_users':
        formattedValue = formatNumber(value);
        break;
      case 'click_to_trial_rate':
      case 'trial_accuracy_ratio':
      case 'purchase_accuracy_ratio':
        formattedValue = value !== undefined && value !== null ? `${formatNumber(value * 100, 2)}%` : 'N/A';
        break;
      case 'trial_conversion_rate':
        if (value !== undefined && value !== null) {
          // Use the new ConversionRateTooltip for trial conversion rate
          // Backend already returns percentage (0-100), no need to multiply by 100
          formattedValue = (
            <ConversionRateTooltip 
              row={row}
              columnKey={columnKey}
              value={value} // Already in percentage
              colorClass=""
              dashboardParams={dashboardParams}
            />
          );
        } else {
          formattedValue = 'N/A';
        }
        break;
      case 'avg_trial_refund_rate':
        if (value !== undefined && value !== null) {
          const hasMinimumFlag = Math.abs(value - 5.0) < 0.01;
          formattedValue = (
            <span>
              <ConversionRateTooltip 
                row={row}
                columnKey={columnKey}
                value={value} // Already in percentage
                colorClass=""
                dashboardParams={dashboardParams}
              />
              {hasMinimumFlag && (
                <RefundRateTooltip 
                  value={value} 
                  type="trial" 
                  colorClass="text-blue-500" 
                  pipelineUpdatedClass="" 
                />
              )}
            </span>
          );
        } else {
          formattedValue = 'N/A';
        }
        break;
      case 'purchase_refund_rate':
        if (value !== undefined && value !== null) {
          const hasMinimumFlag = Math.abs(value - 15.0) < 0.01;
          formattedValue = (
            <span>
              <ConversionRateTooltip 
                row={row}
                columnKey={columnKey}
                value={value} // Already in percentage
                colorClass=""
                dashboardParams={dashboardParams}
              />
              {hasMinimumFlag && (
                <RefundRateTooltip 
                  value={value} 
                  type="purchase" 
                  colorClass="text-orange-500" 
                  pipelineUpdatedClass="" 
                />
              )}
            </span>
          );
        } else {
          formattedValue = 'N/A';
        }
        break;
      case 'estimated_roas':
        formattedValue = formatNumber(value, 2);
        break;
      case 'performance_impact_score':
        if (value !== undefined && value !== null && value > 0) {
          formattedValue = formatNumber(value, 0);
        } else {
          formattedValue = '0';
        }
        break;
      case 'segment_accuracy_average':
        formattedValue = value || 'N/A';
        break;
      default:
        formattedValue = value || 'N/A';
    }

    // Apply special styling for ROAS and performance columns
    let colorClass = getFieldColor(columnKey, value);
    if (columnKey === 'estimated_roas') {
      colorClass = getRoasColor(value);
    } else if (columnKey === 'performance_impact_score') {
      // Static performance impact score color system (thresholds are always the same)
      // Note: The score values themselves are time-scaled in the backend
      if (value >= 7500) {
        colorClass = 'text-purple-600 dark:text-purple-400 font-bold'; // Exceptional
      } else if (value >= 2500) {
        colorClass = 'text-blue-600 dark:text-blue-400 font-semibold'; // Strong
      } else if (value >= 1000) {
        colorClass = 'text-green-600 dark:text-green-400 font-semibold'; // Good
      } else if (value >= 500) {
        colorClass = 'text-yellow-600 dark:text-yellow-400'; // Moderate
      } else if (value >= 200) {
        colorClass = 'text-orange-600 dark:text-orange-400'; // Low
      } else if (value >= 50) {
        colorClass = 'text-red-600 dark:text-red-400'; // Poor
      } else {
        colorClass = 'text-gray-600 dark:text-gray-400'; // Minimal
      }
    }

    // Special accuracy ratio color coding logic
    // Only apply if:
    // 1. Column is trial_accuracy_ratio or purchase_accuracy_ratio
    // 2. Column would NOT be grayed out (is the active column)
    // 3. Corresponding Meta count > 5
    if ((columnKey === 'trial_accuracy_ratio' || columnKey === 'purchase_accuracy_ratio') && 
        value !== undefined && value !== null) {
      
      // Check if this column would be grayed out - if so, don't apply special coloring
      const wouldBeGrayedOut = eventPriority && shouldGrayOutColumn(columnKey, eventPriority);
      
      if (!wouldBeGrayedOut) {
        // Get the corresponding Meta count
        const metaCount = columnKey === 'trial_accuracy_ratio' 
          ? (calculatedRow.meta_trials_started || 0)
          : (calculatedRow.meta_purchases || 0);
        
        // Only apply color coding if Meta count > 5
        if (metaCount > 5) {
          // Apply color thresholds (value is decimal, so convert to percentage)
          const percentageValue = value * 100;
          if (percentageValue < 10) {
            colorClass = 'text-red-600 dark:text-red-400 font-semibold'; // < 10%: Red
          } else if (percentageValue < 20) {
            colorClass = 'text-orange-600 dark:text-orange-400 font-semibold'; // < 20%: Orange
          } else if (percentageValue <= 30) {
            colorClass = 'text-yellow-600 dark:text-yellow-400 font-semibold'; // ≤ 30%: Yellow
          }
          // > 30%: Keep normal color (no special coloring)
        }
      }
    }

    // Check if this column should be grayed out based on event priority
    if (eventPriority && shouldGrayOutColumn(columnKey, eventPriority)) {
      colorClass = 'text-gray-500 dark:text-gray-500';
    }



          // Special rendering for accuracy column with tooltip
      if (columnKey === 'segment_accuracy_average' && row.accuracy_breakdown) {
        return (
          <AccuracyTooltip 
            average={formattedValue}
            breakdown={row.accuracy_breakdown}
            colorClass={colorClass}
            pipelineUpdatedClass=""
          />
        );
      }

    // Special rendering for estimated ROAS column with sparkline
    if (columnKey === 'estimated_roas') {
      // Extract the actual ID from the row.id field (format: "campaign_123", "adset_456", "ad_789")
      const entityId = row.id ? row.id.split('_')[1] : null;
      
      // Check if this is a breakdown row (has breakdown-specific ID format like "US_120217904661980178")
      const isBreakdownRow = row.id && row.id.includes('_') && !row.id.startsWith('campaign_') && !row.id.startsWith('adset_') && !row.id.startsWith('ad_');
      
      if (isBreakdownRow) {
        // DEBUG: Log breakdown row detection
        console.log('🔥 SPARKLINE CELL DEBUG (BREAKDOWN):', {
          rowId: row.id,
          entityType: row.entity_type,
          currentROAS: value,
          isBreakdownDetected: true,
          breakdown: dashboardParams?.breakdown
        });
        
        // For breakdown rows, show sparkline with breakdown entity info
        const [breakdownValue, parentEntityId] = row.id.split('_', 2);
        return (
          <ROASSparkline 
            entityType={row.entity_type || 'campaign'}  // Use entity_type from breakdown data
            entityId={row.id}  // Use full breakdown ID (e.g., "US_120217904661980178")
            currentROAS={value}
            conversionCount={calculatedRow.mixpanel_purchases || 0}
            breakdown={dashboardParams?.breakdown || 'all'}
            startDate={dashboardParams?.start_date || '2025-04-01'}
            endDate={dashboardParams?.end_date || '2025-04-10'}
            isBreakdownEntity={true}  // Flag to indicate this is a breakdown entity
            calculationTooltip={<ROASCalculationTooltip row={row} roas={value} />}
          />
        );
      }
      
      // DEBUG: Log the row data to understand the structure
      console.log('🔥 SPARKLINE CELL DEBUG (REGULAR):', {
        rowId: row.id,
        rowType: row.type,
        entityId: entityId,
        hasSpend: !!row.spend,
        hasRevenue: !!calculatedRow.estimated_revenue_usd,
        isBreakdownDetected: false
      });
      
      return (
        <ROASSparkline 
          entityType={row.type}
          entityId={entityId}
          currentROAS={value}
          conversionCount={calculatedRow.mixpanel_purchases || 0}
          breakdown={dashboardParams?.breakdown || 'all'}
          startDate={dashboardParams?.start_date || '2025-04-01'}
          endDate={dashboardParams?.end_date || '2025-04-10'}
          calculationTooltip={<ROASCalculationTooltip row={row} roas={value} />}
        />
      );
    }

          return (
        <span className={colorClass}>
          {formattedValue}
          {isEstimated && <span className="ml-1 text-xs">*</span>}
        </span>
      );
  };

  // Simple column drag handlers
  const handleColumnDragStart = (e, columnKey) => {
    setDraggedColumn(columnKey);
    setIsDragging(true);
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', columnKey);
  };

  const handleColumnDragOver = (e) => {
    e.preventDefault();
  };

  const handleColumnDragEnter = (e, columnKey) => {
    if (draggedColumn && draggedColumn !== columnKey) {
      setDragOverColumn(columnKey);
    }
  };

  const handleColumnDragLeave = (e) => {
    // Only clear if we're leaving the th element itself
    if (!e.currentTarget.contains(e.relatedTarget)) {
      setDragOverColumn(null);
    }
  };

  const handleColumnDrop = (e, targetColumnKey) => {
    e.preventDefault();
    
    if (draggedColumn && draggedColumn !== targetColumnKey && onColumnOrderChange) {
      const currentOrder = [...columnOrder];
      const draggedIndex = currentOrder.indexOf(draggedColumn);
      const targetIndex = currentOrder.indexOf(targetColumnKey);
      
      if (draggedIndex !== -1 && targetIndex !== -1) {
        // Remove dragged column
        const [draggedCol] = currentOrder.splice(draggedIndex, 1);
        
        // Insert at target position
        const newTargetIndex = draggedIndex < targetIndex ? targetIndex : targetIndex + 1;
        currentOrder.splice(newTargetIndex, 0, draggedCol);
        
        console.log(`🔄 Column reordered: '${draggedColumn}' moved from position ${draggedIndex} to ${newTargetIndex}`);
        onColumnOrderChange(currentOrder);
      }
    }
    
    setDraggedColumn(null);
    setDragOverColumn(null);
    // Add delay to prevent accidental sorting after drag
    setTimeout(() => setIsDragging(false), 100);
  };

  const handleColumnDragEnd = (e) => {
    setDraggedColumn(null);
    setDragOverColumn(null);
    // Add delay to prevent accidental sorting after drag
    setTimeout(() => setIsDragging(false), 100);
  };

  // Row drag handlers
  const handleRowDragStart = (e, id) => {
    setDraggedRowId(id);
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setDragImage(new Image(), 0, 0); // hide ghost for cleaner UX
  };

  const handleRowDragEnter = (e, id) => {
    e.preventDefault();
    if (id !== dragOverRowId) setDragOverRowId(id);
  };

  const handleRowDrop = (e, targetId) => {
    e.preventDefault();
    if (!draggedRowId || draggedRowId === targetId) return;

    const newOrder = [...rowOrder];
    const from = newOrder.indexOf(draggedRowId);
    const to = newOrder.indexOf(targetId);
    newOrder.splice(to, 0, newOrder.splice(from, 1)[0]);

    onRowOrderChange?.(newOrder);
    cleanupRowDrag();
  };

  const handleRowDragEnd = cleanupRowDrag;

  function cleanupRowDrag() {
    setDraggedRowId(null);
    setDragOverRowId(null);
  }

  const toggleExpand = (id) => {
    setExpandedRows(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleBreakdown = (id) => {
    setExpandedBreakdowns(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const expandAllRows = () => {
    const newExpandedRows = {};
    const processRows = (rows) => {
      rows.forEach(row => {
        newExpandedRows[row.id] = true;
        if (row.children) {
          processRows(row.children);
        }
      });
    };
    processRows(data);
    setExpandedRows(newExpandedRows);
  };

  const collapseAllRows = () => {
    setExpandedRows({});
  };

  const expandAllBreakdowns = () => {
    const newExpandedBreakdowns = {};
    const processRows = (rows) => {
      rows.forEach(row => {
        if (row.breakdowns && row.breakdowns.length > 0) {
          newExpandedBreakdowns[row.id] = true;
        }
        if (row.children) {
          processRows(row.children);
        }
      });
    };
    processRows(data);
    setExpandedBreakdowns(newExpandedBreakdowns);
  };

  const collapseAllBreakdowns = () => {
    setExpandedBreakdowns({});
  };

  const renderAllBreakdownRows = (row, level) => {
    const breakdownNodes = [];
    
    if (!row.breakdowns) return breakdownNodes;
    
    row.breakdowns.forEach(breakdown => {
      // REMOVED: Breakdown header row as requested by user

      breakdown.values.forEach((value, index) => {
        const calculatedValue = calculateDerivedValues(value);
        
        // Determine if this is a mapped breakdown value
        const isMapped = value.meta_value && value.mixpanel_value && value.meta_value !== value.mixpanel_value;
        const mappingInfo = isMapped ? `${value.meta_value} → ${value.mixpanel_value}` : value.name;
        
        breakdownNodes.push(
          <tr key={`${row.id}-${breakdown.type}-${index}`} className="border-b border-gray-200 dark:border-gray-700 bg-gray-50/30 dark:bg-gray-900/30 text-xs hover:bg-blue-50/20 dark:hover:bg-blue-900/20">
            <td className="sticky left-0 px-3 py-1 whitespace-nowrap bg-gray-50/30 dark:bg-gray-900/30 z-10">
              <div className="flex items-center">
                <span className="opacity-0 w-8"></span> {/* Space for chart/info icons */}
                <div style={{ paddingLeft: `${(level + 1) * 20 + 12}px` }} className="flex items-center space-x-2">
                  <span className="text-gray-700 dark:text-gray-300 flex items-center space-x-2">
                    <span className="text-lg">{getCountryFlag(value.name)}</span>
                    <span className="font-medium">{value.name} • {getCountryName(value.name)}</span>
                    {isMapped && (
                      <span className="text-xs text-gray-500 dark:text-gray-400">
                        ({mappingInfo})
                      </span>
                    )}
                  </span>
                  {value.total_users && (
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                      ({value.total_users.toLocaleString()} users)
                    </span>
                  )}
                </div>
              </div>
            </td>
            {visibleColumns.slice(1).map((column) => {
              if (column.key === 'campaign_name' || column.key === 'adset_name') {
                return <td key={column.key} className="px-3 py-1"></td>;
              }
              
              // Enhanced breakdown value rendering with mapping awareness
              return (
                <td key={column.key} className="px-3 py-1 whitespace-nowrap text-right">
                  <span className={`${getFieldColor(column.key, calculatedValue[column.key])}`}>
                    {renderCellValue(calculatedValue, column.key, false, getEventPriority(calculatedValue), dashboardParams)}
                  </span>
                </td>
              );
            })}
          </tr>
        );
      });
      
      // REMOVED: Summary row for breakdown as requested by user
    });
    
    return breakdownNodes;
  };

  const renderRow = (row, level) => {
    const isExpanded = !!expandedRows[row.id];
    const isBreakdownExpanded = !!expandedBreakdowns[row.id];
    const rowNodes = [];



    const calculatedRow = calculateDerivedValues(row);

    // Determine event priority
    const eventPriority = getEventPriority(row);

    rowNodes.push(
      <tr 
        key={row.id} 
        draggable={level === 0}
        onDragStart={e => handleRowDragStart(e, row.id)}
        onDragEnter={e => handleRowDragEnter(e, row.id)}
        onDragOver={e => e.preventDefault()}
        onDrop={e => handleRowDrop(e, row.id)}
        onDragEnd={handleRowDragEnd}
        className={`border-b border-gray-200 dark:border-gray-700 ${
        level === 0 
          ? 'bg-gray-50 dark:bg-gray-800 font-semibold' 
          : level === 1 
            ? 'bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-200' 
            : 'bg-gray-100 dark:bg-gray-600 text-gray-800 dark:text-gray-200'
      } ${draggedRowId === row.id ? 'opacity-50' : ''} ${dragOverRowId === row.id && draggedRowId !== row.id ? 'ring-2 ring-blue-400' : ''}`}>
        
        {/* Name column - always visible */}
        <td className={`sticky left-0 px-3 py-2 whitespace-nowrap z-10 ${
          level === 0 
            ? 'bg-gray-50 dark:bg-gray-800'
            : level === 1 
              ? 'bg-white dark:bg-gray-700'
              : 'bg-gray-100 dark:bg-gray-600'
        }`}>
          <div className="flex items-center">
            <span style={{ paddingLeft: `${level * 20}px` }}>
              {row.children && row.children.length > 0 ? (
                <button onClick={() => toggleExpand(row.id)} className="mr-1 p-1">
                  {isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                </button>
              ) : (
                <span className="inline-block w-8"></span>
              )}
            </span>
            
            {row.breakdowns && row.breakdowns.length > 0 && (
              <button 
                onClick={() => toggleBreakdown(row.id)} 
                className={`mr-2 p-1 rounded ${isBreakdownExpanded ? 'text-blue-500 bg-blue-100 dark:bg-blue-900/30' : 'text-gray-400 hover:text-blue-400'}`}
                title="Toggle breakdowns"
              >
                <Search size={14} />
              </button>
            )}
            
            <span className={`${level === 0 ? 'text-gray-900 dark:text-gray-100' : level === 1 ? 'text-gray-800 dark:text-gray-200' : 'text-gray-700 dark:text-gray-100'}`}>
              {level === 0 
                ? (row.name || row.campaign_name)
                : level === 1 
                  ? (row.name || row.adset_name)
                  : (row.name || row.ad_name)
              }
            </span>
          </div>
        </td>

        {/* Dynamic columns based on visibility */}
        {visibleColumns.slice(1).map((column) => {
          // Special handling for campaign/adset name columns based on level
          if (column.key === 'campaign_name') {
            if (level > 0) {
              return <td key={column.key} className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">{row.campaign_name}</td>;
            } else {
              return <td key={column.key} className="px-3 py-2"></td>;
            }
          }
          
          if (column.key === 'adset_name') {
            if (level > 1) {
              return <td key={column.key} className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">{row.adset_name}</td>;
            } else {
              return <td key={column.key} className="px-3 py-2"></td>;
            }
          }

          // Regular data columns
          const fieldColor = getFieldColor(column.key, calculatedRow[column.key]);
          const isRoasColumn = column.key === 'roas' || column.key === 'estimated_roas';
          const roasColor = isRoasColumn ? getRoasColor(calculatedRow[column.key]) : '';
          const finalColor = isRoasColumn ? roasColor : fieldColor;
          


          // Check if the column should be grayed out based on event priority
          const shouldGrayOut = shouldGrayOutColumn(column.key, eventPriority);
          const grayedOutColor = shouldGrayOut ? 'text-gray-500 dark:text-gray-500' : finalColor;

          // Get column background class for visual differentiation
          const columnBackgroundClass = getColumnBackgroundClass(column.key);

                      return (
              <td key={column.key} className={`px-3 py-2 whitespace-nowrap text-right ${grayedOutColor} ${isRoasColumn ? 'font-medium' : ''} ${columnBackgroundClass}`}>
                {renderCellValue(calculatedRow, column.key, false, eventPriority, dashboardParams)}
              </td>
            );
        })}
      </tr>
    );

    if (isBreakdownExpanded && row.breakdowns) {
      rowNodes.push(...renderAllBreakdownRows(row, level));
    }

    if (isExpanded && row.children) {
      row.children.forEach(childRow => {
        rowNodes.push(...renderRow(childRow, level + 1));
      });
    }
    return rowNodes;
  };

  if (!data || data.length === 0) {
    return <div className="p-4 text-center text-gray-500 dark:text-gray-400">No data available.</div>;
  }

  // Prioritize column sorting over manual row ordering
  // If a column sort is active, use the sorted data directly
  // Otherwise, use manual row ordering if available
  const orderedData = (sortConfig.column && sortConfig.column !== null) 
    ? data // Use data as-is (already sorted by Dashboard component)
    : (rowOrder.length
        ? rowOrder.map(id => data.find(r => r.id === id)).filter(Boolean)
        : data);

  const allRows = orderedData.reduce(
    (acc, campaign) => acc.concat(renderRow(campaign, 0)),
    []
  );

  return (
    <div className="shadow-soft rounded-2xl">
      <div className="bg-white dark:bg-gray-800 px-4 py-2 flex space-x-4 border-b border-gray-200 dark:border-gray-700">
        <div className="inline-flex rounded-md shadow-sm">
          <button 
            onClick={expandAllRows} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-l-lg border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Layers size={14} className="mr-1" /> Expand All
          </button>
          <button 
            onClick={collapseAllRows} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-r-lg border border-l-0 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Table2 size={14} className="mr-1" /> Collapse All
          </button>
        </div>
        
        <div className="inline-flex rounded-md shadow-sm">
          <button 
            onClick={expandAllBreakdowns} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-l-lg border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Search size={14} className="mr-1" /> Show Breakdowns
          </button>
          <button 
            onClick={collapseAllBreakdowns} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-r-lg border border-l-0 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <AlignJustify size={14} className="mr-1" /> Hide Breakdowns
          </button>
        </div>


      </div>
      
      {/* Table container with proper horizontal scrolling */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
          <thead className="bg-gray-100 dark:bg-gray-800">
            <tr>
              {/* Name column - always visible and not draggable */}
              <th 
                scope="col" 
                className={`sticky left-0 px-3 py-3 text-left text-xs font-medium uppercase tracking-wider bg-gray-100 dark:bg-gray-800 z-20 cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors group
                  ${sortConfig.column === 'name' ? 'border-b-2 border-blue-500 dark:border-blue-400 bg-blue-50 dark:bg-blue-900/20' : ''}`}
                onClick={() => handleColumnHeaderClick('name')}
              >
                <div className="flex items-center">
                  <span>Name</span>
                  <SortIndicator column="name" sortConfig={sortConfig} />
                </div>
              </th>
              
              {/* Dynamic columns based on visibility - draggable and sortable */}
              {visibleColumns.slice(1).map((column) => {
                const columnType = getColumnType(column.key);
                const backgroundClass = getColumnBackgroundClass(column.key, dragOverColumn === column.key);
                
                return (
                  <th 
                    key={column.key} 
                    scope="col"
                    draggable={!column.alwaysVisible}
                    onDragStart={(e) => handleColumnDragStart(e, column.key)}
                    onDragOver={handleColumnDragOver}
                    onDragEnter={(e) => handleColumnDragEnter(e, column.key)}
                    onDragLeave={handleColumnDragLeave}
                    onDrop={(e) => handleColumnDrop(e, column.key)}
                    onDragEnd={handleColumnDragEnd}
                    onClick={() => handleColumnHeaderClick(column.key)}
                    className={`px-3 py-3 text-${column.key === 'campaign_name' || column.key === 'adset_name' ? 'left' : 'right'} text-xs font-medium uppercase tracking-wider ${getHeaderColor(column.key)} ${backgroundClass}
                      cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors duration-150 group
                      ${sortConfig.column === column.key ? 'bg-blue-50 dark:bg-blue-900/20 border-b-2 border-blue-500 dark:border-blue-400' : ''}
                      ${dragOverColumn === column.key && draggedColumn !== column.key ? 'bg-blue-100 dark:bg-blue-900 border-2 border-blue-300 dark:border-blue-600' : ''}
                      ${columnType === 'trial' ? 'border-l-2 border-blue-200 dark:border-blue-800' : ''}
                      ${columnType === 'purchase' ? 'border-l-2 border-green-200 dark:border-green-800' : ''}`}
                    title={`Click to sort by "${column.label}"${!column.alwaysVisible ? '. Drag to reorder column.' : ''}`}
                  >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center">
                      <span>{column.label}</span>
                      <SortIndicator column={column.key} sortConfig={sortConfig} />
                    </div>
                    {!column.alwaysVisible && (
                      <span className="ml-1 text-gray-400 dark:text-gray-500 text-lg leading-none hover:text-gray-600 dark:hover:text-gray-300 transition-colors duration-150" style={{ transform: 'rotate(90deg)' }}>⋮⋮</span>
                    )}
                  </div>
                </th>
              );
              })}
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
            {allRows.map(row => <Fragment key={row.key}>{row}</Fragment>)}
          </tbody>
        </table>
      </div>
    </div>
  );
}; 