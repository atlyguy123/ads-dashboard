import React from 'react';
import { Search, RefreshCw } from 'lucide-react';

const SearchInterface = ({
  searchFilters,
  onSearchFiltersChange,
  onSearch,
  isSearching
}) => {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
      <div className="p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
          Step 4: Explore Conversion Results
        </h3>
        <p className="text-gray-600 dark:text-gray-400 mb-4">
          Search and filter the conversion analysis results to find insights about specific user segments, products, or regions.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Product ID
            </label>
            <input
              type="text"
              placeholder="e.g., product_123"
              value={searchFilters.product_id}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, product_id: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Filter by specific product identifier
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Region
            </label>
            <input
              type="text"
              placeholder="e.g., North America"
              value={searchFilters.region}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, region: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Filter by geographic region
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Country
            </label>
            <input
              type="text"
              placeholder="e.g., US"
              value={searchFilters.country}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, country: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Filter by specific country code
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              App Store
            </label>
            <input
              type="text"
              placeholder="e.g., APP_STORE"
              value={searchFilters.app_store}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, app_store: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Filter by app store platform
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Minimum Users in Segment
            </label>
            <input
              type="number"
              placeholder="50"
              value={searchFilters.min_cohort_size}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, min_cohort_size: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Only show segments with at least this many users
            </p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Maximum Results
            </label>
            <input
              type="number"
              placeholder="100"
              value={searchFilters.limit}
              onChange={(e) => onSearchFiltersChange({ ...searchFilters, limit: parseInt(e.target.value) || 100 })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Limit number of results returned
            </p>
          </div>
        </div>

        <div className="flex items-center gap-4 mb-4">
          <button
            onClick={onSearch}
            disabled={isSearching}
            className="inline-flex items-center px-6 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isSearching ? (
              <RefreshCw className="animate-spin h-4 w-4 mr-2" />
            ) : (
              <Search className="h-4 w-4 mr-2" />
            )}
            {isSearching ? 'Searching...' : 'Search Results'}
          </button>
          
          <div className="text-sm text-gray-500 dark:text-gray-400">
            Leave filters empty to see all results
          </div>
        </div>
      </div>
    </div>
  );
};

export default SearchInterface; 