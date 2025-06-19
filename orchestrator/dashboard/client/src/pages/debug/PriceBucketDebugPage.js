import React, { useState, useEffect, useMemo } from 'react';
import { priceBucketDebugApi } from '../../services/debugApi';

// Statistics Card Component
const StatCard = ({ title, value, icon, color = "blue" }) => {
  const colorClasses = {
    blue: "bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800 text-blue-800 dark:text-blue-200",
    green: "bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800 text-green-800 dark:text-green-200",
    yellow: "bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800 text-yellow-800 dark:text-yellow-200",
    red: "bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800 text-red-800 dark:text-red-200",
    purple: "bg-purple-50 dark:bg-purple-900/20 border-purple-200 dark:border-purple-800 text-purple-800 dark:text-purple-200"
  };

  return (
    <div className={`border rounded-lg p-4 ${colorClasses[color]}`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium opacity-75">{title}</p>
          <p className="text-2xl font-bold">{value?.toLocaleString() || 0}</p>
        </div>
        <div className="text-3xl opacity-75">{icon}</div>
      </div>
    </div>
  );
};

// Filter Component
const FilterControls = ({ filters, onFilterChange, onSearch, onReset, loading }) => {
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
            Country
          </label>
          <input
            type="text"
            value={filters.country}
            onChange={(e) => onFilterChange('country', e.target.value)}
            placeholder="Filter by country..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white"
          />
        </div>
        
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Min Price Bucket
          </label>
          <input
            type="number"
            step="0.01"
            value={filters.min_bucket}
            onChange={(e) => onFilterChange('min_bucket', e.target.value)}
            placeholder="Min bucket value..."
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-white"
          />
        </div>
        
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Max Price Bucket
          </label>
          <input
            type="number"
            step="0.01"
            value={filters.max_bucket}
            onChange={(e) => onFilterChange('max_bucket', e.target.value)}
            placeholder="Max bucket value..."
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
      </div>
      
      <div className="flex gap-2">
        <button
          onClick={onSearch}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white px-4 py-2 rounded-md font-medium"
        >
          {loading ? 'Searching...' : 'Search'}
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

// Data Table Component
const DataTable = ({ data, onSort, sortConfig }) => {
  const getSortIcon = (column) => {
    if (sortConfig.key !== column) {
      return <span className="text-gray-400">↕️</span>;
    }
    return sortConfig.direction === 'asc' ? <span className="text-blue-600">↑</span> : <span className="text-blue-600">↓</span>;
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-700">
            <tr>
                             <th
                 className="px-3 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/6"
                 onClick={() => onSort('product_id')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Product ID</span>
                   {getSortIcon('product_id')}
                 </div>
               </th>
                                            <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/12"
                 onClick={() => onSort('country')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Country</span>
                   {getSortIcon('country')}
                 </div>
               </th>
               <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/8"
                 onClick={() => onSort('event_type')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Event Type</span>
                   {getSortIcon('event_type')}
                 </div>
               </th>
               <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/8"
                 onClick={() => onSort('price_bucket')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Price Bucket</span>
                   {getSortIcon('price_bucket')}
                 </div>
               </th>
               <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/8"
                 onClick={() => onSort('user_count')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Total Users</span>
                   {getSortIcon('user_count')}
                 </div>
               </th>
               <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/8"
                 onClick={() => onSort('properly_sorted_count')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Properly Sorted</span>
                   {getSortIcon('properly_sorted_count')}
                 </div>
               </th>
               <th
                 className="px-2 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 w-1/8"
                 onClick={() => onSort('inherited_count')}
               >
                 <div className="flex items-center space-x-1">
                   <span>Inherited</span>
                   {getSortIcon('inherited_count')}
                 </div>
               </th>
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                         {data.map((row, index) => (
               <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                 <td className="px-3 py-3 text-sm font-medium text-gray-900 dark:text-white break-words">
                   {row.product_id}
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   {row.country}
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   {row.event_type === 'RC Initial purchase' ? (
                     <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
                       RC Initial purchase
                     </span>
                   ) : row.event_type === 'RC Trial converted' ? (
                     <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                       RC Trial converted
                     </span>
                   ) : (
                     <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">
                       No Conversion
                     </span>
                   )}
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   {row.price_bucket === 0 ? (
                     <span className="font-medium text-gray-600 dark:text-gray-400">
                       $0.00
                     </span>
                   ) : (
                     <span className="font-medium text-green-600 dark:text-green-400">
                       {formatCurrency(row.price_bucket)}
                     </span>
                   )}
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   <span className="font-medium">{row.user_count.toLocaleString()}</span>
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   <span className="font-medium text-green-600 dark:text-green-400">{row.properly_sorted_count.toLocaleString()}</span>
                 </td>
                 <td className="px-2 py-3 whitespace-nowrap text-sm text-gray-500 dark:text-gray-300">
                   <span className="font-medium text-yellow-600 dark:text-yellow-400">{row.inherited_count.toLocaleString()}</span>
                 </td>
               </tr>
             ))}
          </tbody>
        </table>
        {data.length === 0 && (
          <div className="text-center py-12">
            <div className="text-gray-400 text-4xl mb-4">📊</div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">No Data Found</h3>
            <p className="text-gray-500 dark:text-gray-400">Try adjusting your filters or load the overview data.</p>
          </div>
        )}
      </div>
    </div>
  );
};

// Main Price Bucket Debug Page Component
const PriceBucketDebugPage = () => {
  const [loading, setLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [statistics, setStatistics] = useState(null);
  const [tableData, setTableData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [error, setError] = useState(null);
  const [isFiltered, setIsFiltered] = useState(false);
  
  // Filter state
  const [filters, setFilters] = useState({
    product_id: '',
    country: '',
    min_bucket: '',
    max_bucket: '',
    min_users: '',
    max_users: ''
  });
  
  // Sort state
  const [sortConfig, setSortConfig] = useState({ key: 'product_id', direction: 'asc' });

  // Load overview data on component mount
  useEffect(() => {
    loadOverviewData();
  }, []);

  const loadOverviewData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await priceBucketDebugApi.loadOverview();
      
      if (result.success) {
        setStatistics(result.data.statistics);
        setTableData(result.data.table_data);
        setFilteredData(result.data.table_data);
        setIsFiltered(false);
      } else {
        setError(result.error || 'Failed to load price bucket data');
      }
    } catch (err) {
      setError('Failed to load price bucket data: ' + err.message);
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

  const handleSearch = async () => {
    setSearchLoading(true);
    setError(null);
    
    try {
      // Only send non-empty filters
      const activeFilters = {};
      Object.keys(filters).forEach(key => {
        if (filters[key] !== '') {
          activeFilters[key] = filters[key];
        }
      });
      
      const result = await priceBucketDebugApi.searchData(activeFilters);
      
      if (result.success) {
        setFilteredData(result.data.filtered_data);
        setIsFiltered(true);
      } else {
        setError(result.error || 'Failed to search price bucket data');
      }
    } catch (err) {
      setError('Failed to search price bucket data: ' + err.message);
    } finally {
      setSearchLoading(false);
    }
  };

  const handleReset = () => {
    setFilters({
      product_id: '',
      country: '',
      min_bucket: '',
      max_bucket: '',
      min_users: '',
      max_users: ''
    });
    setFilteredData(tableData);
    setIsFiltered(false);
  };

  const handleSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  // Sort the displayed data
  const sortedData = useMemo(() => {
    if (!filteredData) return [];
    
    return [...filteredData].sort((a, b) => {
      let aVal = a[sortConfig.key];
      let bVal = b[sortConfig.key];
      
      // Handle numeric sorts
      if (sortConfig.key === 'price_bucket' || sortConfig.key === 'user_count' || sortConfig.key === 'properly_sorted_count' || sortConfig.key === 'inherited_count') {
        aVal = Number(aVal);
        bVal = Number(bVal);
      }
      
      // Handle string sorts (ensure they're strings)
      if (sortConfig.key === 'product_id' || sortConfig.key === 'country' || sortConfig.key === 'event_type') {
        aVal = String(aVal);
        bVal = String(bVal);
      }
      
      if (aVal < bVal) {
        return sortConfig.direction === 'asc' ? -1 : 1;
      }
      if (aVal > bVal) {
        return sortConfig.direction === 'asc' ? 1 : -1;
      }
      return 0;
    });
  }, [filteredData, sortConfig]);

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
            💰 Price Bucket Debug
          </h1>
          <p className="text-gray-600 dark:text-gray-300">
            Debug and analyze price bucket assignments from the pipeline. View statistics and explore detailed bucket assignments.
          </p>
        </div>

        {/* Price Bucket Assignment Algorithm Info */}
        <div className="mb-6 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h2 className="text-lg font-semibold text-blue-900 dark:text-blue-100 mb-3 flex items-center">
            <span className="mr-2">ℹ️</span>
            Price Bucket Assignment Algorithm (v3 - Iterative Bucketing)
          </h2>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-sm">
            <div>
              <h3 className="font-medium text-blue-800 dark:text-blue-200 mb-2">🎯 Assignment Process (Two-Pass System)</h3>
              <ul className="space-y-1 text-blue-700 dark:text-blue-300">
                <li><strong>Pass 1:</strong> Direct conversions + strict backward inheritance</li>
                <li><strong>Pass 2:</strong> Closest-time conversion inheritance for remaining users</li>
                <li>Buckets created using iterative merge algorithm with thresholds</li>
                <li>Groups similar prices: 17.5% difference OR $5.00 absolute difference</li>
              </ul>
            </div>
            
                         <div>
               <h3 className="font-medium text-blue-800 dark:text-blue-200 mb-2">📊 Assignment Types Explained</h3>
               <ul className="space-y-1 text-blue-700 dark:text-blue-300">
                 <li><strong>Properly Sorted:</strong> Users with direct conversions (RC Initial purchase, RC Trial converted)</li>
                 <li><strong>Inherited:</strong> Trial users who inherited bucket from prior/closest conversions</li>
                 <li><strong>Zero Bucket ($0.00):</strong> No conversion events OR events found but price didn't fit any bucket</li>
                 <li><strong>Properly Sorted vs Inherited:</strong> Shows breakdown of direct conversions vs inherited buckets</li>
               </ul>
             </div>
          </div>
          
          <div className="mt-4 pt-3 border-t border-blue-200 dark:border-blue-700">
            <h3 className="font-medium text-blue-800 dark:text-blue-200 mb-2">⚠️ Important Notes</h3>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-sm text-blue-700 dark:text-blue-300">
              <div>
                <ul className="space-y-1">
                  <li>• <strong>Country-Specific:</strong> Buckets are created per country-product-event combination</li>
                  <li>• <strong>Event-Specific:</strong> Different buckets for "RC Initial purchase" vs "RC Trial converted"</li>
                  <li>• <strong>Inheritance Priority:</strong> Prior trial conversions → closest conversion (any type)</li>
                </ul>
              </div>
              <div>
                                 <ul className="space-y-1">
                   <li>• <strong>Zero Values ($0.00):</strong> No conversion/trial events found OR conversion price outside all bucket ranges</li>
                   <li>• <strong>Valid Lifecycle:</strong> Only users with valid_lifecycle=1 and valid_user=1 are included</li>
                   <li>• <strong>Bucket Calculation:</strong> Uses average price of all conversions in the bucket</li>
                 </ul>
              </div>
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

        {/* Statistics Cards */}
        {statistics && (
          <div className="mb-6">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">📊 Bucket Assignment Statistics</h2>
                                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-4">
                <StatCard title="Total User-Product Pairs" value={statistics.total_user_product_pairs} icon="📊" color="blue" />
                <StatCard title="Valid Lifecycle Users" value={statistics.total_users} icon="✅" color="green" />
                <StatCard title="Properly Sorted" value={statistics.properly_sorted} icon="🎯" color="green" />
             </div>
             <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <StatCard title="Inherited Buckets" value={statistics.inherited} icon="⬆️" color="yellow" />
                <StatCard title="Zero Buckets ($0.00)" value={statistics.zero_bucket} icon="0️⃣" color="red" />
                <StatCard title="Unique Buckets" value={statistics.unique_buckets} icon="🔢" color="purple" />
             </div>
          </div>
        )}

        {/* Filter Controls */}
        <div className="mb-6">
          <FilterControls
            filters={filters}
            onFilterChange={handleFilterChange}
            onSearch={handleSearch}
            onReset={handleReset}
            loading={searchLoading}
          />
        </div>

        {/* Data Table */}
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              📋 Price Bucket Data {isFiltered && '(Filtered)'}
            </h2>
            <div className="flex items-center space-x-4">
              <span className="text-sm text-gray-500 dark:text-gray-400">
                Showing {sortedData.length.toLocaleString()} rows
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
              <p className="mt-2 text-gray-500 dark:text-gray-400">Loading price bucket data...</p>
            </div>
          ) : (
            <DataTable
              data={sortedData}
              onSort={handleSort}
              sortConfig={sortConfig}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default PriceBucketDebugPage; 