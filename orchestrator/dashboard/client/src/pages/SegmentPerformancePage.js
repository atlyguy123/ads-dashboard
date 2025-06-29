import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { 
  RefreshCw, 
  Search, 
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Filter,
  Users,
  TrendingUp,
  Target,
  AlertTriangle,
  Info
} from 'lucide-react';
import { dashboardApi } from '../services/dashboardApi';
import MultiSelectDropdown from '../components/MultiSelectDropdown';

export const SegmentPerformancePage = () => {
  // State management with localStorage
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [segments, setSegments] = useState(() => {
    const saved = localStorage.getItem('segment_analysis_data');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved segment data:', e);
      }
    }
    return [];
  });
  const [lastUpdated, setLastUpdated] = useState(() => {
    return localStorage.getItem('segment_analysis_last_updated') || null;
  });

  // Filter state with localStorage
  const [filters, setFilters] = useState(() => {
    const saved = localStorage.getItem('segment_analysis_filters');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved segment filters:', e);
      }
    }
    return {
      product_id: '',
      store: '',
      country: '',
      region: '',
      price_buckets: [], // Changed to array for multi-select
      accuracy_scores: [], // Changed to array for multi-select
      min_user_count: 0
    };
  });

  // Sorting state with localStorage
  const [sortConfig, setSortConfig] = useState(() => {
    const saved = localStorage.getItem('segment_analysis_sort_config');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved segment sort config:', e);
      }
    }
    return {
      column: 'trial_conversion_rate',
      direction: 'desc'
    };
  });

  // Search state with localStorage
  const [searchText, setSearchText] = useState(() => {
    return localStorage.getItem('segment_analysis_search_text') || '';
  });

  // Available filter options (will be populated from data)
  const [filterOptions, setFilterOptions] = useState(() => {
    const saved = localStorage.getItem('segment_analysis_filter_options');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved filter options:', e);
      }
    }
    return {
      product_ids: [],
      stores: [],
      countries: [],
      regions: [],
      price_buckets: [],
      accuracy_scores: []
    };
  });

  // Save states to localStorage
  useEffect(() => {
    localStorage.setItem('segment_analysis_data', JSON.stringify(segments));
  }, [segments]);

  useEffect(() => {
    if (lastUpdated) {
      localStorage.setItem('segment_analysis_last_updated', lastUpdated);
    }
  }, [lastUpdated]);

  useEffect(() => {
    localStorage.setItem('segment_analysis_filters', JSON.stringify(filters));
  }, [filters]);

  useEffect(() => {
    localStorage.setItem('segment_analysis_sort_config', JSON.stringify(sortConfig));
  }, [sortConfig]);

  useEffect(() => {
    localStorage.setItem('segment_analysis_search_text', searchText);
  }, [searchText]);

  useEffect(() => {
    localStorage.setItem('segment_analysis_filter_options', JSON.stringify(filterOptions));
  }, [filterOptions]);

  // Auto-select all accuracy scores and price buckets when filter options are loaded
  useEffect(() => {
    if (filterOptions.accuracy_scores && filterOptions.accuracy_scores.length > 0 && (!filters.accuracy_scores || filters.accuracy_scores.length === 0)) {
      setFilters(prev => ({
        ...prev,
        accuracy_scores: [...filterOptions.accuracy_scores]
      }));
    }
  }, [filterOptions.accuracy_scores, filters.accuracy_scores]);

  useEffect(() => {
    if (filterOptions.price_buckets && filterOptions.price_buckets.length > 0 && (!filters.price_buckets || filters.price_buckets.length === 0)) {
      setFilters(prev => ({
        ...prev,
        price_buckets: [...filterOptions.price_buckets]
      }));
    }
  }, [filterOptions.price_buckets, filters.price_buckets]);

  // Fetch segments data
  const fetchSegments = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
      console.log('🔍 Fetching segment performance data...');
      
      // Convert arrays to comma-separated strings for API
      const apiFilters = {
        ...filters,
        accuracy_score: (filters.accuracy_scores || []).join(','),
        price_bucket: (filters.price_buckets || []).join(',')
      };
      delete apiFilters.accuracy_scores;
      delete apiFilters.price_buckets;
      
      const response = await dashboardApi.getSegmentPerformance({
        ...apiFilters,
        sort_column: sortConfig.column,
        sort_direction: sortConfig.direction
      });
      
      if (response.success) {
        setSegments(response.data.segments || []);
        setFilterOptions(response.data.filter_options || {});
        setLastUpdated(new Date().toISOString());
        console.log('✅ Segment data loaded successfully:', response.data.segments?.length, 'segments');
      } else {
        setError(response.error || 'Failed to fetch segment data');
      }
    } catch (error) {
      console.error('Segment fetch error:', error);
      setError(error.message || 'Failed to fetch segment data');
    } finally {
      setLoading(false);
    }
  }, [filters, sortConfig]);

  // Initial load
  useEffect(() => {
    fetchSegments();
  }, []);

  // Handle sorting
  const handleSort = (column) => {
    setSortConfig(prevConfig => {
      if (prevConfig.column === column) {
        return {
          ...prevConfig,
          direction: prevConfig.direction === 'asc' ? 'desc' : 'asc'
        };
      } else {
        return {
          column,
          direction: 'desc' // Default to descending for most metrics
        };
      }
    });
  };

  // Handle filter changes
  const handleFilterChange = (key, value) => {
    setFilters(prev => ({
      ...prev,
      [key]: value
    }));
  };

  // Handle search
  const filteredSegments = useMemo(() => {
    if (!searchText) return segments;
    
    const searchLower = searchText.toLowerCase();
    return segments.filter(segment => 
      segment.product_id?.toLowerCase().includes(searchLower) ||
      segment.store?.toLowerCase().includes(searchLower) ||
      segment.country?.toLowerCase().includes(searchLower) ||
      segment.region?.toLowerCase().includes(searchLower) ||
      segment.price_bucket?.toString().includes(searchLower) ||
      segment.accuracy_score?.toLowerCase().includes(searchLower)
    );
  }, [segments, searchText]);

  // Get sort icon
  const getSortIcon = (column) => {
    if (sortConfig.column !== column) {
      return <ArrowUpDown className="h-4 w-4 text-gray-400" />;
    }
    return sortConfig.direction === 'asc' ? 
      <ArrowUp className="h-4 w-4 text-blue-500" /> : 
      <ArrowDown className="h-4 w-4 text-blue-500" />;
  };

  // Format percentage
  const formatPercentage = (value) => {
    if (value == null) return 'N/A';
    return `${(value * 100).toFixed(1)}%`;
  };

  // Get accuracy badge color
  const getAccuracyBadgeColor = (accuracy) => {
    switch (accuracy) {
      case 'very_high': return 'bg-green-100 text-green-800';
      case 'high': return 'bg-blue-100 text-blue-800';
      case 'medium': return 'bg-yellow-100 text-yellow-800';
      case 'low': return 'bg-orange-100 text-orange-800';
      case 'default': return 'bg-gray-100 text-gray-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  // Calculate summary stats
  const summaryStats = useMemo(() => {
    if (!filteredSegments.length) return null;
    
    const totalUsers = filteredSegments.reduce((sum, segment) => sum + (segment.user_count || 0), 0);
    const avgTrialConversion = filteredSegments.reduce((sum, segment) => sum + (segment.trial_conversion_rate || 0), 0) / filteredSegments.length;
    const avgTrialRefundRate = filteredSegments.reduce((sum, segment) => sum + (segment.trial_converted_to_refund_rate || 0), 0) / filteredSegments.length;
    const avgPurchaseRefundRate = filteredSegments.reduce((sum, segment) => sum + (segment.initial_purchase_to_refund_rate || 0), 0) / filteredSegments.length;
    
    return {
      totalSegments: filteredSegments.length,
      totalUsers,
      avgTrialConversion,
      avgTrialRefundRate,
      avgPurchaseRefundRate
    };
  }, [filteredSegments]);

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center">
            <AlertTriangle className="mx-auto h-12 w-12 text-red-500" />
            <h1 className="mt-4 text-3xl font-bold text-gray-900 dark:text-gray-100">
              Error Loading Segments
            </h1>
            <p className="mt-2 text-gray-600 dark:text-gray-400">{error}</p>
            <button 
              onClick={() => {
                setError(null);
                fetchSegments();
              }}
              className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <RefreshCw className="mr-2 h-4 w-4" />
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                Segment Performance Analysis
              </h1>
              <p className="mt-2 text-gray-600 dark:text-gray-400">
                Analyze conversion rates by user segment characteristics
              </p>
            </div>
            
            <button 
              onClick={fetchSegments}
              disabled={loading}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                <>
                  <RefreshCw className="mr-2 h-4 w-4" />
                  Refresh Data
                </>
              )}
            </button>
          </div>
        </div>

        {/* Summary Stats */}
        {summaryStats && (
          <div className="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <Target className="h-6 w-6 text-purple-500" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Segments</p>
                  <p className="text-xl font-bold text-gray-900 dark:text-gray-100">
                    {summaryStats.totalSegments.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <Users className="h-6 w-6 text-blue-500" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Total Users</p>
                  <p className="text-xl font-bold text-gray-900 dark:text-gray-100">
                    {summaryStats.totalUsers.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <TrendingUp className="h-6 w-6 text-green-500" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Avg Trial Conv.</p>
                  <p className="text-xl font-bold text-gray-900 dark:text-gray-100">
                    {formatPercentage(summaryStats.avgTrialConversion)}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <TrendingUp className="h-6 w-6 text-yellow-500" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Avg Trial Refund</p>
                  <p className="text-xl font-bold text-gray-900 dark:text-gray-100">
                    {formatPercentage(summaryStats.avgTrialRefundRate)}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <TrendingUp className="h-6 w-6 text-red-500" />
                <div className="ml-3">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Avg Purchase Refund</p>
                  <p className="text-xl font-bold text-gray-900 dark:text-gray-100">
                    {formatPercentage(summaryStats.avgPurchaseRefundRate)}
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Filters */}
        <div className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-4">
          <div className="flex items-center mb-4">
            <Filter className="h-5 w-5 text-gray-500 mr-2" />
            <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">Filters</h3>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Product ID Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Product ID
              </label>
              <select
                value={filters.product_id}
                onChange={(e) => handleFilterChange('product_id', e.target.value)}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Products</option>
                {filterOptions.product_ids?.map(id => (
                  <option key={id} value={id}>{id}</option>
                ))}
              </select>
            </div>

            {/* Store Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Store
              </label>
              <select
                value={filters.store}
                onChange={(e) => handleFilterChange('store', e.target.value)}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Stores</option>
                {filterOptions.stores?.map(store => (
                  <option key={store} value={store}>{store}</option>
                ))}
              </select>
            </div>

            {/* Country Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Country
              </label>
              <select
                value={filters.country}
                onChange={(e) => handleFilterChange('country', e.target.value)}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Countries</option>
                {filterOptions.countries?.map(country => (
                  <option key={country} value={country}>{country}</option>
                ))}
              </select>
            </div>

            {/* Region Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Region
              </label>
              <select
                value={filters.region}
                onChange={(e) => handleFilterChange('region', e.target.value)}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Regions</option>
                {filterOptions.regions?.map(region => (
                  <option key={region} value={region}>{region}</option>
                ))}
              </select>
            </div>

            {/* Price Bucket Multi-Select */}
            <MultiSelectDropdown
              label="Price Buckets"
              options={filterOptions.price_buckets?.map(bucket => ({
                value: bucket,
                label: `$${parseFloat(bucket).toFixed(2)}`
              })) || []}
              selectedValues={filters.price_buckets || []}
              onChange={(newValues) => handleFilterChange('price_buckets', newValues)}
              placeholder="Select price buckets..."
            />

            {/* Accuracy Levels Multi-Select */}
            <MultiSelectDropdown
              label="Accuracy Levels"
              options={filterOptions.accuracy_scores?.map(score => ({
                value: score,
                label: score.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())
              })) || []}
              selectedValues={filters.accuracy_scores || []}
              onChange={(newValues) => handleFilterChange('accuracy_scores', newValues)}
              placeholder="Select accuracy levels..."
            />

            {/* Min User Count Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Min User Count
              </label>
              <input
                type="number"
                value={filters.min_user_count}
                onChange={(e) => handleFilterChange('min_user_count', parseInt(e.target.value) || 0)}
                min="0"
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="0"
              />
            </div>

            {/* Apply Filters Button */}
            <div className="flex items-end">
              <button
                onClick={fetchSegments}
                disabled={loading}
                className="w-full bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium py-2 px-4 rounded-md transition-colors"
              >
                Apply Filters
              </button>
            </div>
          </div>
        </div>

        {/* Search */}
        <div className="mb-6">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search segments..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              className="pl-10 pr-4 py-3 w-full border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>

        {/* Last Updated */}
        {lastUpdated && (
          <div className="mb-4 flex items-center text-sm text-gray-500 dark:text-gray-400">
            <Info className="mr-2 h-4 w-4" />
            Last updated: {new Date(lastUpdated).toLocaleString()}
          </div>
        )}

        {/* Data Table */}
        {loading ? (
          <div className="flex items-center justify-center h-64 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="text-center">
              <RefreshCw className="mx-auto h-8 w-8 text-blue-500 animate-spin" />
              <p className="mt-2 text-lg font-medium text-gray-900 dark:text-gray-100">
                Loading segment data...
              </p>
            </div>
          </div>
        ) : filteredSegments.length > 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-700">
                  <tr>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('product_id')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Product ID</span>
                        {getSortIcon('product_id')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                      Store
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                      Country
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                      Region
                    </th>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('price_bucket')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Price Bucket</span>
                        {getSortIcon('price_bucket')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('user_count')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Users</span>
                        {getSortIcon('user_count')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('trial_conversion_rate')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Trial Conv. Rate</span>
                        {getSortIcon('trial_conversion_rate')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('trial_converted_to_refund_rate')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Trial Refund Rate</span>
                        {getSortIcon('trial_converted_to_refund_rate')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left">
                      <button
                        onClick={() => handleSort('initial_purchase_to_refund_rate')}
                        className="flex items-center space-x-1 text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-100"
                      >
                        <span>Purchase Refund Rate</span>
                        {getSortIcon('initial_purchase_to_refund_rate')}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                      Accuracy
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                  {filteredSegments.map((segment, index) => (
                    <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 dark:text-gray-100">
                        {segment.product_id || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                        {segment.store || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                        {segment.country || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                        {segment.region || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                        {segment.price_bucket ? `$${parseFloat(segment.price_bucket).toFixed(2)}` : 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                        <div className="flex items-center">
                          <Users className="h-4 w-4 text-gray-400 mr-1" />
                          {segment.user_count?.toLocaleString() || 0}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                        <span className={`font-medium ${
                          (segment.trial_conversion_rate || 0) > 0.3 ? 'text-green-600' :
                          (segment.trial_conversion_rate || 0) > 0.2 ? 'text-yellow-600' : 'text-red-600'
                        }`}>
                          {formatPercentage(segment.trial_conversion_rate)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                        <span className={`font-medium ${
                          (segment.trial_converted_to_refund_rate || 0) < 0.2 ? 'text-green-600' :
                          (segment.trial_converted_to_refund_rate || 0) < 0.4 ? 'text-yellow-600' : 'text-red-600'
                        }`}>
                          {formatPercentage(segment.trial_converted_to_refund_rate)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                        <span className={`font-medium ${
                          (segment.initial_purchase_to_refund_rate || 0) < 0.3 ? 'text-green-600' :
                          (segment.initial_purchase_to_refund_rate || 0) < 0.5 ? 'text-yellow-600' : 'text-red-600'
                        }`}>
                          {formatPercentage(segment.initial_purchase_to_refund_rate)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${getAccuracyBadgeColor(segment.accuracy_score)}`}>
                          {segment.accuracy_score || 'N/A'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-8 text-center">
            <Target className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-4 text-lg font-semibold text-gray-900 dark:text-gray-100">
              No Segments Found
            </h3>
            <p className="mt-2 text-gray-600 dark:text-gray-400">
              {searchText ? 
                `No segments match your search "${searchText}". Try adjusting your search terms.` :
                'No segments match your current filters. Try adjusting the filter criteria.'
              }
            </p>
          </div>
        )}
      </div>
    </div>
  );
}; 