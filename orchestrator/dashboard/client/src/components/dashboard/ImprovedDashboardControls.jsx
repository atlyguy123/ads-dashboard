import React, { useState, useEffect } from 'react';
import { Calendar, RefreshCw, Clock, ChevronDown } from 'lucide-react';

// Date range presets based on yesterday (not today)
const getDatePresets = () => {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  
  const formatDate = (date) => date.toISOString().split('T')[0];
  
  return [
    {
      id: 'yesterday',
      label: 'Yesterday',
      start_date: formatDate(yesterday),
      end_date: formatDate(yesterday)
    },
    {
      id: '7days',
      label: '7 Days',
      start_date: formatDate(new Date(yesterday.getTime() - 6 * 24 * 60 * 60 * 1000)),
      end_date: formatDate(yesterday)
    },
    {
      id: '14days',
      label: '14 Days',
      start_date: formatDate(new Date(yesterday.getTime() - 13 * 24 * 60 * 60 * 1000)),
      end_date: formatDate(yesterday)
    },
    {
      id: '30days',
      label: '30 Days',
      start_date: formatDate(new Date(yesterday.getTime() - 29 * 24 * 60 * 60 * 1000)),
      end_date: formatDate(yesterday)
    }
  ];
};

const ImprovedDashboardControls = ({
  dateRange,
  breakdown,
  hierarchy,
  breakdownFilters,
  onDateRangeChange,
  onBreakdownChange,
  onHierarchyChange,
  onBreakdownFiltersChange,
  onRefresh,
  loading,
  backgroundLoading
}) => {
  const [selectedPreset, setSelectedPreset] = useState('7days'); // Default to 7 days
  const datePresets = getDatePresets();
  
  // Get yesterday's date for the info note
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayFormatted = yesterday.toLocaleDateString('en-US', { 
    month: 'short', 
    day: 'numeric', 
    year: 'numeric' 
  });

  // Initialize with 7 days preset on mount if no date range is set
  useEffect(() => {
    const sevenDaysPreset = datePresets.find(p => p.id === '7days');
    if (sevenDaysPreset && (!dateRange?.start_date || !dateRange?.end_date)) {
      onDateRangeChange(sevenDaysPreset);
    }
  }, []);

  // Update selected preset when date range changes externally
  useEffect(() => {
    if (dateRange?.start_date && dateRange?.end_date) {
      const matchingPreset = datePresets.find(preset => 
        preset.start_date === dateRange.start_date && 
        preset.end_date === dateRange.end_date
      );
      setSelectedPreset(matchingPreset ? matchingPreset.id : 'custom');
    }
  }, [dateRange]);

  const handlePresetChange = (presetId) => {
    setSelectedPreset(presetId);
    if (presetId !== 'custom') {
      const preset = datePresets.find(p => p.id === presetId);
      if (preset) {
        onDateRangeChange({
          start_date: preset.start_date,
          end_date: preset.end_date
        });
      }
    }
  };

  const handleCustomDateChange = (field, value) => {
    setSelectedPreset('custom');
    onDateRangeChange({
      ...dateRange,
      [field]: value
    });
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
      <div className="p-6">
        {/* Header Section */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-3">
            <Calendar className="h-5 w-5 text-blue-600" />
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
              Analytics Controls
            </h2>
          </div>
          <div className="flex items-center text-xs text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-700 px-3 py-1.5 rounded-full">
            <Clock className="mr-1.5 h-3 w-3" />
            Data through {yesterdayFormatted}
          </div>
        </div>

        {/* Controls Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-end">
          
          {/* Date Range Section */}
          <div className="lg:col-span-5">
            <label className="block text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
              Date Range
            </label>
            
            {/* Date Presets */}
            <div className="flex flex-wrap gap-2 mb-4">
              {datePresets.map((preset) => (
                <button
                  key={preset.id}
                  onClick={() => handlePresetChange(preset.id)}
                  className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-200 ${
                    selectedPreset === preset.id
                      ? 'bg-blue-600 text-white shadow-md transform scale-[1.02]'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
                  }`}
                >
                  {preset.label}
                </button>
              ))}
              <button
                onClick={() => handlePresetChange('custom')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-200 ${
                  selectedPreset === 'custom'
                    ? 'bg-blue-600 text-white shadow-md transform scale-[1.02]'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
                }`}
              >
                Custom
              </button>
            </div>
            
            {/* Custom Date Inputs */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">From</label>
                <input
                  type="date"
                  value={dateRange?.start_date || ''}
                  onChange={(e) => handleCustomDateChange('start_date', e.target.value)}
                  className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm py-2.5"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">To</label>
                <input
                  type="date"
                  value={dateRange?.end_date || ''}
                  onChange={(e) => handleCustomDateChange('end_date', e.target.value)}
                  className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm py-2.5"
                />
              </div>
            </div>
          </div>

          {/* Breakdown */}
          <div className="lg:col-span-2">
            <label className="block text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
              Breakdown
            </label>
            <select
              value={breakdown || 'all'}
              onChange={(e) => onBreakdownChange(e.target.value)}
              className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 py-2.5"
            >
              <option value="all">All</option>
              <option value="country">Country</option>
              <option value="region">Region</option>
              <option value="device">Device</option>
            </select>
          </div>

          {/* Hierarchy */}
          <div className="lg:col-span-3">
            <label className="block text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
              Hierarchy
            </label>
            <select
              value={hierarchy || 'campaign'}
              onChange={(e) => onHierarchyChange(e.target.value)}
              className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 py-2.5"
            >
              <option value="campaign">Campaign → Ad Set → Ad</option>
              <option value="adset">Ad Set → Ad</option>
              <option value="ad">Ad Only</option>
            </select>
          </div>

          {/* Refresh Button */}
          <div className="lg:col-span-2">
            <button
              onClick={onRefresh}
              disabled={loading || backgroundLoading}
              className="w-full inline-flex items-center justify-center px-6 py-3 border border-transparent text-sm font-semibold rounded-lg text-white bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 disabled:opacity-50 disabled:cursor-not-allowed shadow-md hover:shadow-lg transition-all duration-200 transform hover:scale-[1.02] disabled:transform-none"
            >
              {(loading || backgroundLoading) ? (
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

        {/* Breakdown Filtering Section */}
        {breakdown !== 'all' && (
          <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
            <div className="flex items-center justify-between mb-4">
              <label className="block text-sm font-semibold text-gray-700 dark:text-gray-300">
                Breakdown Filtering
              </label>
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="breakdown-filter-enabled"
                  checked={breakdownFilters?.enabled || false}
                  onChange={(e) => onBreakdownFiltersChange({
                    ...breakdownFilters,
                    enabled: e.target.checked
                  })}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="breakdown-filter-enabled" className="ml-2 text-sm text-gray-600 dark:text-gray-400">
                  Enable filtering
                </label>
              </div>
            </div>

            {breakdownFilters?.enabled && (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {/* Filter Type */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">
                    Filter Type
                  </label>
                  <div className="space-y-2">
                    <label className="flex items-center">
                      <input
                        type="radio"
                        name="breakdown-filter-type"
                        value="limit"
                        checked={breakdownFilters?.type === 'limit'}
                        onChange={(e) => onBreakdownFiltersChange({
                          ...breakdownFilters,
                          type: e.target.value
                        })}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                      />
                      <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                        Limit to top results
                      </span>
                    </label>
                    <label className="flex items-center">
                      <input
                        type="radio"
                        name="breakdown-filter-type"
                        value="spend"
                        checked={breakdownFilters?.type === 'spend'}
                        onChange={(e) => onBreakdownFiltersChange({
                          ...breakdownFilters,
                          type: e.target.value
                        })}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                      />
                      <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                        Minimum spend filter
                      </span>
                    </label>
                  </div>
                </div>

                {/* Limit Count Input */}
                {breakdownFilters?.type === 'limit' && (
                  <div>
                    <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                      Limit to top
                    </label>
                    <input
                      type="number"
                      min="1"
                      max="50"
                      value={breakdownFilters?.limitCount || 10}
                      onChange={(e) => onBreakdownFiltersChange({
                        ...breakdownFilters,
                        limitCount: parseInt(e.target.value) || 10
                      })}
                      className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm py-2"
                      placeholder="10"
                    />
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                      Show top {breakdownFilters?.limitCount || 10} results based on current sort
                    </p>
                  </div>
                )}

                {/* Minimum Spend Input */}
                {breakdownFilters?.type === 'spend' && (
                  <div>
                    <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                      Minimum spend ($)
                    </label>
                    <input
                      type="number"
                      min="0"
                      step="10"
                      value={breakdownFilters?.minSpend || 100}
                      onChange={(e) => onBreakdownFiltersChange({
                        ...breakdownFilters,
                        minSpend: parseFloat(e.target.value) || 100
                      })}
                      className="block w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm py-2"
                      placeholder="100"
                    />
                    <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                      Hide breakdown values with spend below ${breakdownFilters?.minSpend || 100}
                    </p>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ImprovedDashboardControls; 