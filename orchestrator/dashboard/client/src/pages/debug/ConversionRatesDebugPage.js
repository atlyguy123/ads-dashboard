import React, { useState, useEffect, useMemo } from 'react';
import { conversionRatesDebugApi } from '../../services/debugApi';

// Statistics Card Component
const StatCard = ({ title, value, icon, color = "blue", subtitle = null }) => {
  const colorClasses = {
    blue: "bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800 text-blue-800 dark:text-blue-200",
    green: "bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800 text-green-800 dark:text-green-200",
    yellow: "bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800 text-yellow-800 dark:text-yellow-200",
    red: "bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800 text-red-800 dark:text-red-200",
    purple: "bg-purple-50 dark:bg-purple-900/20 border-purple-200 dark:border-purple-800 text-purple-800 dark:text-purple-200",
    orange: "bg-orange-50 dark:bg-orange-900/20 border-orange-200 dark:border-orange-800 text-orange-800 dark:text-orange-200"
  };

  return (
    <div className={`border rounded-lg p-4 ${colorClasses[color]}`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium opacity-75">{title}</p>
          <p className="text-2xl font-bold">{value?.toLocaleString() || 0}</p>
          {subtitle && <p className="text-xs opacity-60 mt-1">{subtitle}</p>}
        </div>
        <div className="text-3xl opacity-75">{icon}</div>
      </div>
    </div>
  );
};

// Filter Controls Component
const FilterControls = ({ filters, onFilterChange, onApplyFilters, onReset, loading }) => {
  return (
    <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-4 space-y-4">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Filter Controls</h3>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Product ID
          </label>
          <input
            type="text"
            value={filters.product_id}
            onChange={(e) => onFilterChange('product_id', e.target.value)}
            placeholder="Filter by product ID..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white"
          />
        </div>
        
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Min Users
          </label>
          <input
            type="number"
            value={filters.min_users}
            onChange={(e) => onFilterChange('min_users', e.target.value)}
            placeholder="Min user count..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white"
          />
        </div>
        
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Max Users
          </label>
          <input
            type="number"
            value={filters.max_users}
            onChange={(e) => onFilterChange('max_users', e.target.value)}
            placeholder="Max user count..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white"
          />
        </div>
        
        <div className="flex items-center">
          <input
            type="checkbox"
            id="show_errors_only"
            checked={filters.show_errors_only}
            onChange={(e) => onFilterChange('show_errors_only', e.target.checked)}
            className="rounded border-gray-300 text-blue-600 shadow-sm focus:border-blue-300 focus:ring focus:ring-blue-200 focus:ring-opacity-50"
          />
          <label htmlFor="show_errors_only" className="ml-2 text-sm text-gray-700 dark:text-gray-300">
            Show validation errors only
          </label>
        </div>
        
        <div className="flex items-center">
          <input
            type="checkbox"
            id="show_viable_only"
            checked={filters.show_viable_only}
            onChange={(e) => onFilterChange('show_viable_only', e.target.checked)}
            className="rounded border-gray-300 text-blue-600 shadow-sm focus:border-blue-300 focus:ring focus:ring-blue-200 focus:ring-opacity-50"
          />
          <label htmlFor="show_viable_only" className="ml-2 text-sm text-gray-700 dark:text-gray-300">
            Show viable segments only (≥12 users)
          </label>
        </div>
      </div>
      
      <div className="flex gap-2">
        <button
          onClick={onApplyFilters}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white px-4 py-2 rounded-md font-medium"
        >
          {loading ? 'Applying...' : 'Apply Filters'}
        </button>
        <button
          onClick={onReset}
          disabled={loading}
          className="bg-gray-600 hover:bg-gray-700 disabled:bg-gray-400 text-white px-4 py-2 rounded-md font-medium"
        >
          Reset
        </button>
      </div>
    </div>
  );
};

// Hierarchy Tree Component  
const HierarchyTree = ({ hierarchyTree = [], expandedNodes, onToggleNode }) => {
  const formatRate = (rate) => {
    if (rate === null || rate === undefined) return 'N/A';
    return (rate * 100).toFixed(2) + '%';
  };

  const getTreeIndent = (level) => {
    const chars = ['', '├── ', '│   ├── ', '│   │   ├── ', '│   │   │   ├── ', '│   │   │   │   ├── '];
    return chars[level] || '│   '.repeat(level) + '├── ';
  };

  const renderNode = (node, index) => {
    const nodeKey = `${node.level}_${node.value}_${index}`;
    const isExpanded = expandedNodes.has(nodeKey);
    
    // Determine status and color
    let statusColor = 'text-gray-600 dark:text-gray-400';
    let statusText = 'default';
    
    if (node.rates && node.is_leaf) {
      if (!node.rates_consistent) {
        statusColor = 'text-red-600 dark:text-red-400 font-bold';
        statusText = 'ERROR';
      } else if ((node.cohort_user_count || 0) < 12) {
        statusColor = 'text-yellow-600 dark:text-yellow-400';
        statusText = 'small cohort';
      } else if (node.uses_default) {
        statusColor = 'text-orange-600 dark:text-orange-400';
        statusText = 'default';
      } else if (node.is_viable) {
        statusColor = 'text-green-600 dark:text-green-400';
        statusText = formatRate(node.rates.trial_conversion_rate);
      } else {
        statusColor = 'text-yellow-600 dark:text-yellow-400';
        statusText = 'small cohort';
      }
    }

    return (
      <div key={nodeKey} className="font-mono text-sm">
        <div 
          className={`flex items-center py-1 px-2 hover:bg-gray-50 dark:hover:bg-gray-700/30 cursor-pointer ${
            !node.rates_consistent ? 'bg-red-50 dark:bg-red-900/20' : ''
          }`}
          onClick={() => onToggleNode(nodeKey)}
        >
          {/* Tree Structure with Indentation */}
          <div className="flex items-center min-w-0" style={{ minWidth: '400px' }}>
            <span className="text-gray-400 dark:text-gray-500">
              {getTreeIndent(node.level)}
            </span>
            <span className="text-gray-900 dark:text-white font-medium truncate">
              {node.value}
            </span>
            {node.has_children && (
              <span className="ml-2 text-gray-400">
                {isExpanded ? '▼' : '▶'}
              </span>
            )}
          </div>

          {/* Level Info */}
          <div className="w-20 text-center text-gray-500 dark:text-gray-400 text-xs">
            level {node.level}
          </div>

          {/* Status */}
          <div className={`w-24 text-center ${statusColor} font-medium text-xs`}>
            {statusText}
          </div>

          {/* Total User Count */}
          <div className="w-24 text-center text-gray-700 dark:text-gray-300 font-medium">
            {node.user_count.toLocaleString()}
          </div>

          {/* Cohort User Count (used for rate calculation) */}
          <div className={`w-24 text-center font-medium ${
            node.cohort_user_count >= 12 
              ? 'text-green-600 dark:text-green-400' 
              : 'text-red-600 dark:text-red-400'
          }`}>
            {node.cohort_user_count?.toLocaleString() || 0}
          </div>

          {/* Trial Conversion Rate */}
          <div className="w-20 text-center text-blue-600 dark:text-blue-400 text-xs">
            {node.rates && node.is_leaf ? formatRate(node.rates.trial_conversion_rate) : '-'}
          </div>

          {/* Trial Refund Rate */}
          <div className="w-20 text-center text-green-600 dark:text-green-400 text-xs">
            {node.rates && node.is_leaf ? formatRate(node.rates.trial_converted_to_refund_rate) : '-'}
          </div>

          {/* Purchase Refund Rate */}
          <div className="w-20 text-center text-purple-600 dark:text-purple-400 text-xs">
            {node.rates && node.is_leaf ? formatRate(node.rates.initial_purchase_to_refund_rate) : '-'}
          </div>

          {/* Viable Status - Only show for leaf nodes */}
          <div className="w-16 text-center">
            {node.is_leaf && (
              <span className={`text-xs font-bold ${
                (node.cohort_user_count || 0) >= 12 
                  ? (node.is_viable ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400')
                  : 'text-gray-400 dark:text-gray-500'
              }`}>
                {(node.cohort_user_count || 0) >= 12 ? (node.is_viable ? '✓' : '✗') : '-'}
              </span>
            )}
          </div>

          {/* Error Indicator */}
          <div className="w-12 text-center">
            {!node.rates_consistent && (
              <span className="text-red-600 dark:text-red-400 text-xs font-bold">⚠️</span>
            )}
          </div>
        </div>
        
        {/* Show individual users when expanded and it's a leaf node */}
        {isExpanded && node.is_leaf && node.users && node.users.length > 0 && (
          <div className="ml-8 mt-1 mb-2 bg-gray-50 dark:bg-gray-800 rounded p-2">
            <div className="text-xs text-gray-600 dark:text-gray-400 mb-1">
              Individual Users ({node.users.length}):
              {node.rate_errors && node.rate_errors.length > 0 && (
                <span className="ml-2 text-red-600 dark:text-red-400 font-bold">
                  ⚠️ {node.rate_errors.length} rate inconsistencies detected
                </span>
              )}
            </div>
            <div className="max-h-32 overflow-y-auto space-y-1">
              {node.users.slice(0, 20).map((user, idx) => {
                // Determine if this user is in cohort window (same logic as backend)
                const today = new Date();
                const cohortStart = new Date(today.getTime() - 53 * 24 * 60 * 60 * 1000);
                const cohortEnd = new Date(today.getTime() - 8 * 24 * 60 * 60 * 1000);
                const userCreditedDate = new Date(user.credited_date + 'T00:00:00');
                const isInCohortWindow = userCreditedDate >= cohortStart && userCreditedDate <= cohortEnd;
                
                const hasRateError = node.rate_errors && node.rate_errors.some(err => err.user_id === user.distinct_id);
                
                return (
                  <div key={idx} className={`text-xs flex justify-between ${
                    hasRateError
                      ? 'text-red-700 dark:text-red-300 bg-red-100 dark:bg-red-900/30 px-1 rounded'
                      : isInCohortWindow
                      ? 'text-green-700 dark:text-green-300 bg-green-50 dark:bg-green-900/20 px-1 rounded'
                      : 'text-gray-700 dark:text-gray-300'
                  }`}>
                    <span className="font-mono">{user.distinct_id}</span>
                    <span>
                      {formatRate(user.trial_conversion_rate)} | 
                      {formatRate(user.trial_converted_to_refund_rate)} | 
                      {formatRate(user.initial_purchase_to_refund_rate)} | 
                      {user.accuracy_score}
                      {hasRateError && <span className="ml-1 text-red-600 dark:text-red-400">⚠️</span>}
                      {isInCohortWindow && <span className="ml-1 text-green-600 dark:text-green-400">📅</span>}
                    </span>
                  </div>
                );
              })}
              {node.users.length > 20 && (
                <div className="text-xs text-gray-500 italic">
                  ... and {node.users.length - 20} more users
                </div>
              )}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400 mt-2">
              <span className="text-green-600 dark:text-green-400">📅 = In cohort window (used for rates)</span>
              {node.rate_errors && node.rate_errors.length > 0 && (
                <span className="ml-3 text-red-600 dark:text-red-400">⚠️ = Rate inconsistency</span>
              )}
            </div>
          </div>
        )}
      </div>
    );
  };

  if (!hierarchyTree || hierarchyTree.length === 0) {
    return (
      <div className="text-center py-12">
        <div className="text-gray-400 text-4xl mb-4">🌳</div>
        <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">No Hierarchy Data Found</h3>
        <p className="text-gray-500 dark:text-gray-400">Load the overview data to see the conversion rates hierarchy.</p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-x-auto">
      {/* Header */}
      <div className="bg-gray-50 dark:bg-gray-700 px-2 py-3 border-b border-gray-200 dark:border-gray-600">
        <div className="flex items-center font-semibold text-xs text-gray-700 dark:text-gray-300">
          <div style={{ minWidth: '400px' }}>Property Hierarchy</div>
          <div className="w-20 text-center">Level</div>
          <div className="w-24 text-center">Status</div>
          <div className="w-24 text-center">Total Users</div>
          <div className="w-24 text-center">Cohort Users</div>
          <div className="w-20 text-center">Trial Conv</div>
          <div className="w-20 text-center">Trial Ref</div>
          <div className="w-20 text-center">Purch Ref</div>
          <div className="w-16 text-center">Viable</div>
          <div className="w-12 text-center">Error</div>
        </div>
      </div>

      {/* Tree Content */}
      <div className="divide-y divide-gray-100 dark:divide-gray-700">
        {hierarchyTree.map((node, index) => renderNode(node, index))}
      </div>
    </div>
  );
};

// Main Conversion Rates Debug Page Component
const ConversionRatesDebugPage = () => {
  const [loading, setLoading] = useState(false);
  const [filterLoading, setFilterLoading] = useState(false);
  const [statistics, setStatistics] = useState(null);
  const [hierarchyTree, setHierarchyTree] = useState([]);
  const [validationErrors, setValidationErrors] = useState([]);
  const [error, setError] = useState(null);
  const [expandedNodes, setExpandedNodes] = useState(new Set());
  
  // Filter state
  const [filters, setFilters] = useState({
    product_id: '',
    min_users: '',
    max_users: '',
    show_errors_only: false,
    show_viable_only: false
  });

  // Load overview data on component mount
  useEffect(() => {
    loadOverviewData();
  }, []);

  const loadOverviewData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await conversionRatesDebugApi.loadOverview();
      
      if (result.success) {
        setStatistics(result.data.statistics);
        setHierarchyTree(result.data.hierarchy_tree);
        setValidationErrors(result.data.validation_errors);
        // Auto-expand first few levels for better initial view
        const autoExpanded = new Set();
        result.data.hierarchy_tree.slice(0, 10).forEach((node, idx) => {
          if (node.level < 2) {
            autoExpanded.add(`${node.level}_${node.value}_${idx}`);
          }
        });
        setExpandedNodes(autoExpanded);
      } else {
        setError(result.error || 'Failed to load conversion rates data');
      }
    } catch (err) {
      setError('Failed to load conversion rates data: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleFilterChange = (key, value) => {
    setFilters(prev => ({
      ...prev,
      [key]: value
    }));
  };

  const applyFilters = async () => {
    setFilterLoading(true);
    setError(null);
    
    try {
      // Only send non-empty filters
      const activeFilters = {};
      Object.keys(filters).forEach(key => {
        if (filters[key] !== '' && filters[key] !== false) {
          activeFilters[key] = filters[key];
        }
      });
      
      const result = await conversionRatesDebugApi.loadCohortTree({ filters: activeFilters });
      
      if (result.success) {
        setHierarchyTree(result.data.hierarchy_tree);
      } else {
        setError(result.error || 'Failed to filter conversion rates data');
      }
    } catch (err) {
      setError('Failed to filter conversion rates data: ' + err.message);
    } finally {
      setFilterLoading(false);
    }
  };

  const resetFilters = () => {
    setFilters({
      product_id: '',
      min_users: '',
      max_users: '',
      show_errors_only: false,
      show_viable_only: false
    });
    loadOverviewData(); // Reload full data
  };

  const toggleNode = (nodeKey) => {
    const newExpanded = new Set(expandedNodes);
    if (newExpanded.has(nodeKey)) {
      newExpanded.delete(nodeKey);
    } else {
      newExpanded.add(nodeKey);
    }
    setExpandedNodes(newExpanded);
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
            🔍 Conversion Rates Debug
          </h1>
          <p className="text-gray-600 dark:text-gray-300">
            Debug and analyze the hierarchical cohort matching system. Each line shows the actual property values 
            with their conversion rates, user counts, and validation status in a clean tree format.
          </p>
        </div>

        {/* Algorithm Info */}
        <div className="mb-6 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h2 className="text-lg font-semibold text-blue-900 dark:text-blue-100 mb-3 flex items-center">
            <span className="mr-2">ℹ️</span>
            Tree Structure: product_id → price → store → economic_tier → country → region
          </h2>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-sm">
            <div>
              <h3 className="font-medium text-blue-800 dark:text-blue-200 mb-2">🎯 How to Read the Tree</h3>
              <ul className="space-y-1 text-blue-700 dark:text-blue-300">
                <li><strong>Actual Values:</strong> Shows real data (US, CA, $9.99, app_store, etc.)</li>
                <li><strong>Level:</strong> Depth in hierarchy (0=product, 5=region)</li>
                <li><strong>Status:</strong> conversion_rate, "default", "small cohort", or "ERROR"</li>
                <li><strong>Users:</strong> Total user count for this path</li>
                <li><strong>Viable:</strong> ✓ if ≥12 users, ✗ if not viable</li>
              </ul>
            </div>
            
            <div>
              <h3 className="font-medium text-blue-800 dark:text-blue-200 mb-2">⚠️ Error Detection</h3>
              <ul className="space-y-1 text-blue-700 dark:text-blue-300">
                <li><strong>Red Background:</strong> Inconsistent rates within same segment</li>
                <li><strong>⚠️ Icon:</strong> Rate validation error detected</li>
                <li><strong>ERROR Status:</strong> Users have different conversion rates</li>
                <li><strong>Click to Expand:</strong> View individual user details</li>
              </ul>
            </div>
          </div>
        </div>

        {/* Error Display */}
        {error && (
          <div className="mb-6 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
            <h3 className="text-red-800 dark:text-red-200 font-medium">Error</h3>
            <p className="text-red-600 dark:text-red-300">{error}</p>
          </div>
        )}

        {/* Validation Errors Alert */}
        {validationErrors && validationErrors.length > 0 && (
          <div className="mb-6 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
            <h3 className="text-red-800 dark:text-red-200 font-medium mb-2">
              ⚠️ Validation Errors Found ({validationErrors.length})
            </h3>
            <div className="max-h-32 overflow-y-auto space-y-2">
              {validationErrors.slice(0, 5).map((err, idx) => (
                <div key={idx} className="text-sm text-red-600 dark:text-red-300">
                  <strong>{err.path}:</strong> {err.message} (Users: {err.user_count})
                </div>
              ))}
              {validationErrors.length > 5 && (
                <div className="text-sm text-red-500 italic">
                  ... and {validationErrors.length - 5} more validation errors
                </div>
              )}
            </div>
          </div>
        )}

        {/* Statistics Cards */}
        {statistics && (
          <div className="mb-6">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">📊 Hierarchy Statistics</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
              <StatCard title="Total Users" value={statistics.total_users} icon="👥" color="blue" />
              <StatCard 
                title="Cohort Window Users" 
                value={statistics.cohort_window_info?.total_cohort_users || 0} 
                icon="📅" 
                color="purple" 
                subtitle={`${statistics.cohort_window_info?.start_date} to ${statistics.cohort_window_info?.end_date}`}
              />
              <StatCard title="Total Segments" value={statistics.total_segments} icon="🏷️" color="orange" />
              <StatCard title="Valid Segments" value={statistics.valid_segments} icon="✅" color="green" subtitle="Consistent rates" />
            </div>
            
            {/* Additional Info */}
            <div className="mb-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
              <h3 className="text-yellow-800 dark:text-yellow-200 font-medium mb-2">
                ⚠️ Important: Cohort Window Filtering
              </h3>
              <p className="text-sm text-yellow-700 dark:text-yellow-300">
                Rate calculations only use users whose <strong>credited_date</strong> falls within the 45-day cohort window 
                ({statistics.cohort_window_info?.start_date} to {statistics.cohort_window_info?.end_date}). 
                If a segment has 13 total users but only 8 cohort window users, it will use default rates since 8 &lt; 12.
              </p>
            </div>

            {/* Segment Math Validation */}
            <div className="mb-4">
              {(() => {
                const segmentSum = statistics.valid_segments + statistics.invalid_segments;
                const isSegmentMathValid = segmentSum === statistics.total_segments;
                return (
                  <div className={`text-sm px-3 py-2 rounded-md ${
                    isSegmentMathValid 
                      ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-800' 
                      : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
                  }`}>
                    <span className="font-medium">
                      {isSegmentMathValid ? '✅ Segment Math Check: ' : '❌ Segment Math Error: '}
                    </span>
                    Valid ({statistics.valid_segments.toLocaleString()}) + Invalid ({statistics.invalid_segments.toLocaleString()}) = {segmentSum.toLocaleString()} / Total Segments: {statistics.total_segments.toLocaleString()}
                    {!isSegmentMathValid && ' (These should match!)'}
                  </div>
                );
              })()}
            </div>
            
            {/* User Accuracy Distribution */}
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">👥 User Accuracy Distribution</h3>
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
                Distribution of users by accuracy level (should add up to total users):
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                <StatCard 
                  title="Very High Accuracy" 
                  value={statistics.accuracy_breakdown.very_high} 
                  icon="🎯" 
                  color="green" 
                  subtitle="Using all 6 properties including store" 
                />
                <StatCard 
                  title="High Accuracy" 
                  value={statistics.accuracy_breakdown.high} 
                  icon="🔵" 
                  color="blue" 
                  subtitle="Using 5 properties including store" 
                />
                <StatCard 
                  title="Medium Accuracy" 
                  value={statistics.accuracy_breakdown.medium} 
                  icon="🟡" 
                  color="yellow" 
                  subtitle="Using 4 properties including store" 
                />
                <StatCard 
                  title="Low Accuracy" 
                  value={statistics.accuracy_breakdown.low} 
                  icon="🟠" 
                  color="orange" 
                  subtitle="Using core 3 properties including store" 
                />
                <StatCard 
                  title="Default Fallback" 
                  value={statistics.accuracy_breakdown.default} 
                  icon="⚙️" 
                  color="red" 
                  subtitle="Fallback rates when cohort too small" 
                />
              </div>
              
              {/* User Math Validation */}
              <div className="mt-3 text-center">
                {(() => {
                  const accuracySum = Object.values(statistics.accuracy_breakdown).reduce((sum, count) => sum + count, 0);
                  const isValid = accuracySum === statistics.total_users;
                  return (
                    <div className={`text-sm px-3 py-2 rounded-md ${
                      isValid 
                        ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-800' 
                        : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
                    }`}>
                      <span className="font-medium">
                        {isValid ? '✅ User Math Check: ' : '❌ User Math Error: '}
                      </span>
                      User accuracy sum: {accuracySum.toLocaleString()} / Total users: {statistics.total_users.toLocaleString()}
                      {!isValid && ' (These should match!)'}
                    </div>
                  );
                })()}
              </div>
            </div>

            {/* Segment Accuracy Distribution */}
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">🏷️ Segment Accuracy Distribution</h3>
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
                Distribution of segments by accuracy level (should add up to total segments):
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                <StatCard 
                  title="Very High Accuracy" 
                  value={statistics.segment_accuracy_breakdown?.very_high || 0} 
                  icon="🎯" 
                  color="green" 
                  subtitle="Segments using all 6 properties" 
                />
                <StatCard 
                  title="High Accuracy" 
                  value={statistics.segment_accuracy_breakdown?.high || 0} 
                  icon="🔵" 
                  color="blue" 
                  subtitle="Segments using 5 properties" 
                />
                <StatCard 
                  title="Medium Accuracy" 
                  value={statistics.segment_accuracy_breakdown?.medium || 0} 
                  icon="🟡" 
                  color="yellow" 
                  subtitle="Segments using 4 properties" 
                />
                <StatCard 
                  title="Low Accuracy" 
                  value={statistics.segment_accuracy_breakdown?.low || 0} 
                  icon="🟠" 
                  color="orange" 
                  subtitle="Segments using 3 properties" 
                />
                <StatCard 
                  title="Default Fallback" 
                  value={statistics.segment_accuracy_breakdown?.default || 0} 
                  icon="⚙️" 
                  color="red" 
                  subtitle="Segments with fallback rates" 
                />
              </div>
              
              {/* Segment Math Validation */}
              <div className="mt-3 text-center">
                {(() => {
                  const segmentAccuracySum = statistics.segment_accuracy_breakdown 
                    ? Object.values(statistics.segment_accuracy_breakdown).reduce((sum, count) => sum + count, 0)
                    : 0;
                  const isValid = segmentAccuracySum === statistics.total_segments;
                  return (
                    <div className={`text-sm px-3 py-2 rounded-md ${
                      isValid 
                        ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-800' 
                        : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
                    }`}>
                      <span className="font-medium">
                        {isValid ? '✅ Segment Math Check: ' : '❌ Segment Math Error: '}
                      </span>
                      Segment accuracy sum: {segmentAccuracySum.toLocaleString()} / Total segments: {statistics.total_segments.toLocaleString()}
                      {!isValid && ' (These should match!)'}
                    </div>
                  );
                })()}
              </div>
            </div>

          </div>
        )}

        {/* Filter Controls */}
        <div className="mb-6">
          <FilterControls
            filters={filters}
            onFilterChange={handleFilterChange}
            onApplyFilters={applyFilters}
            onReset={resetFilters}
            loading={filterLoading}
          />
        </div>

        {/* Hierarchy Tree */}
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              🌳 Conversion Rates Hierarchy Tree
            </h2>
            <div className="flex items-center space-x-4">
              <span className="text-sm text-gray-500 dark:text-gray-400">
                {hierarchyTree.length.toLocaleString()} nodes shown
              </span>
              {!loading && (
                <button
                  onClick={loadOverviewData}
                  className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md font-medium text-sm"
                >
                  Refresh Data
                </button>
              )}
            </div>
          </div>

          {loading ? (
            <div className="text-center py-12">
              <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              <p className="mt-2 text-gray-500 dark:text-gray-400">Loading conversion rates hierarchy...</p>
            </div>
          ) : (
            <HierarchyTree
              hierarchyTree={hierarchyTree}
              expandedNodes={expandedNodes}
              onToggleNode={toggleNode}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default ConversionRatesDebugPage;