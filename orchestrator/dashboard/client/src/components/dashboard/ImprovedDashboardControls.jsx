import React, { useState, useEffect, useRef } from 'react';
import { 
  Calendar, 
  RefreshCw, 
  Settings, 
  Database, 
  Clock, 
  Eye, 
  ChevronDown, 
  ChevronUp, 
  Filter,
  BarChart3,
  Layers,
  Target,
  Zap,
  Info,
  Check,
  X
} from 'lucide-react';
import StatusIndicator from './StatusIndicator';
import { dashboardApi } from '../../services/dashboardApi';

// Date range presets based on yesterday
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
  loading = false,
  backgroundLoading = false
}) => {
  const [selectedPreset, setSelectedPreset] = useState('7days');
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [lastRefreshSettings, setLastRefreshSettings] = useState({
    dateRange,
    breakdown,
    hierarchy,
    breakdownFilters
  });
  const [availableDateRange, setAvailableDateRange] = useState({
    earliest_date: '2025-01-01', // Fallback
    latest_date: new Date().toISOString().split('T')[0]
  });
  const datePresets = getDatePresets();

  // Fetch available date range from API when component mounts
  useEffect(() => {
    const fetchAvailableDateRange = async () => {
      try {
        const response = await dashboardApi.getAvailableDateRange();
        if (response.success && response.data) {
          setAvailableDateRange(response.data);
        }
      } catch (error) {
        console.error('Failed to fetch available date range:', error);
        // Keep the fallback values set in initial state
      }
    };

    fetchAvailableDateRange();
  }, []);

  // Update selected preset when date range changes externally
  useEffect(() => {
    const matchingPreset = datePresets.find(preset => 
      preset.start_date === dateRange.start_date && 
      preset.end_date === dateRange.end_date
    );
    setSelectedPreset(matchingPreset ? matchingPreset.id : 'custom');
  }, [dateRange]);

  // Track changes to detect stale data
  useEffect(() => {
    const currentSettings = { dateRange, breakdown, hierarchy, breakdownFilters };
    const hasChanges = JSON.stringify(currentSettings) !== JSON.stringify(lastRefreshSettings);
    setHasUnsavedChanges(hasChanges);
  }, [dateRange, breakdown, hierarchy, breakdownFilters, lastRefreshSettings]);

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

  const handleBreakdownFilterToggle = () => {
    onBreakdownFiltersChange({
      ...breakdownFilters,
      enabled: !breakdownFilters.enabled
    });
  };

  const handleFilterTypeChange = (type) => {
    onBreakdownFiltersChange({
      ...breakdownFilters,
      type: type
    });
  };

  const handleLimitChange = (value) => {
    onBreakdownFiltersChange({
      ...breakdownFilters,
      limitCount: parseInt(value)
    });
  };

  const handleMinSpendChange = (value) => {
    onBreakdownFiltersChange({
      ...breakdownFilters,
      minSpend: parseFloat(value)
    });
  };

  const handleRefresh = () => {
    // Update last refresh settings to current settings
    setLastRefreshSettings({
      dateRange,
      breakdown,
      hierarchy,
      breakdownFilters
    });
    setHasUnsavedChanges(false);
    onRefresh();
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
              <BarChart3 className="h-5 w-5 text-blue-600 dark:text-blue-400" />
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                Analytics Controls
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Configure your dashboard view and data filters
              </p>
            </div>
          </div>
          
                      {/* Quick Refresh */}
            <div className="flex items-center space-x-3">
              <StatusIndicator 
                status={backgroundLoading ? 'loading' : (hasUnsavedChanges ? 'info' : 'success')} 
                message={backgroundLoading ? 'Updating...' : (hasUnsavedChanges ? 'Settings changed - refresh needed' : 'Data up to date')}
                size="md"
              />
            
            <button
              onClick={handleRefresh}
              disabled={loading || backgroundLoading}
              className={`inline-flex items-center px-4 py-2 ${
                hasUnsavedChanges 
                  ? 'bg-orange-600 hover:bg-orange-700 ring-2 ring-orange-300 dark:ring-orange-700' 
                  : 'bg-blue-600 hover:bg-blue-700'
              } disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-all focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2`}
            >
              {loading ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Loading...
                </>
              ) : (
                <>
                  <RefreshCw className="mr-2 h-4 w-4" />
                  {hasUnsavedChanges ? 'Apply Changes' : 'Refresh'}
                </>
              )}
            </button>
          </div>
        </div>
      </div>

      <div className="p-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-6">
          
          {/* Section 1: Time Range */}
          <div className="xl:col-span-2">
            <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-4">
              <div className="flex items-center space-x-2 mb-4">
                <Calendar className="h-4 w-4 text-gray-600 dark:text-gray-400" />
                <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">Time Range</h4>
              </div>
              
              {/* Date Presets */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
                {datePresets.map((preset) => (
                  <button
                    key={preset.id}
                    onClick={() => handlePresetChange(preset.id)}
                    className={`px-3 py-2 text-xs font-medium rounded-md transition-all ${
                      selectedPreset === preset.id
                        ? 'bg-blue-600 text-white shadow-sm ring-1 ring-blue-600'
                        : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                    }`}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
              
                             {/* Custom Date Range */}
               <div className="grid grid-cols-2 gap-3">
                 <div className="relative">
                   <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">From</label>
                   <div className="relative">
                     <input
                       type="date"
                       value={dateRange.start_date}
                       min={availableDateRange.earliest_date}
                       max={availableDateRange.latest_date}
                       onChange={(e) => handleCustomDateChange('start_date', e.target.value)}
                       className="w-full pl-10 pr-3 py-2.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:focus:border-blue-400 transition-all duration-200 cursor-pointer hover:border-gray-400 dark:hover:border-gray-500"
                       style={{ colorScheme: 'light' }}
                     />
                     <Calendar className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
                   </div>
                 </div>
                 <div className="relative">
                   <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">To</label>
                   <div className="relative">
                     <input
                       type="date"
                       value={dateRange.end_date}
                       min={availableDateRange.earliest_date}
                       max={availableDateRange.latest_date}
                       onChange={(e) => handleCustomDateChange('end_date', e.target.value)}
                       className="w-full pl-10 pr-3 py-2.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:focus:border-blue-400 transition-all duration-200 cursor-pointer hover:border-gray-400 dark:hover:border-gray-500"
                       style={{ colorScheme: 'light' }}
                     />
                     <Calendar className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
                   </div>
                 </div>
               </div>
            </div>
          </div>

          {/* Section 2: Data Structure */}
          <div>
            <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-4">
              <div className="flex items-center space-x-2 mb-4">
                <Layers className="h-4 w-4 text-gray-600 dark:text-gray-400" />
                <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">Structure</h4>
              </div>
              
              <div className="space-y-3">
                <div>
                  <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">Hierarchy</label>
                  <select
                    value={hierarchy}
                    onChange={(e) => onHierarchyChange(e.target.value)}
                    className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  >
                    <option value="campaign">Campaign → Ad Set → Ad</option>
                    <option value="adset">Ad Set → Ad</option>
                    <option value="ad">Ad Only</option>
                  </select>
                </div>

                <div>
                  <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">Breakdown</label>
                                     <select
                     value={breakdown}
                     onChange={(e) => onBreakdownChange(e.target.value)}
                     className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                   >
                     <option value="all">No Breakdown</option>
                     <option value="country">By Country</option>
                   </select>
                </div>
              </div>
            </div>
          </div>

          {/* Section 3: Advanced Filters */}
          <div>
            <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-4">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center space-x-2">
                  <Filter className="h-4 w-4 text-gray-600 dark:text-gray-400" />
                  <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">Filters</h4>
                </div>
                
                {breakdown !== 'all' && (
                  <button
                    onClick={handleBreakdownFilterToggle}
                    className={`inline-flex items-center px-2 py-1 text-xs font-medium rounded-md transition-colors ${
                      breakdownFilters.enabled
                        ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                        : 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400'
                    }`}
                  >
                    {breakdownFilters.enabled ? (
                      <>
                        <Check className="mr-1 h-3 w-3" />
                        Active
                      </>
                    ) : (
                      <>
                        <X className="mr-1 h-3 w-3" />
                        Inactive
                      </>
                    )}
                  </button>
                )}
              </div>

                             {breakdown === 'all' ? (
                 <div className="text-center py-6">
                   <div className="text-gray-400 dark:text-gray-500 text-sm">
                     Enable breakdown to use filters
                   </div>
                 </div>
               ) : !breakdownFilters.enabled ? (
                 <div className="text-center py-6">
                   <div className="text-gray-400 dark:text-gray-500 text-sm mb-2">
                     Filters are disabled
                   </div>
                   <div className="text-xs text-gray-500 dark:text-gray-400">
                     Click "Active" above to enable filtering options
                   </div>
                 </div>
               ) : (
                 <div className="space-y-3">
                   {/* Filter Type Toggle */}
                   <div className="grid grid-cols-2 gap-1 p-1 bg-gray-200 dark:bg-gray-700 rounded-md">
                     <button
                       onClick={() => handleFilterTypeChange('limit')}
                       className={`px-2 py-1.5 text-xs font-medium rounded transition-colors ${
                         breakdownFilters.type === 'limit'
                           ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm'
                           : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100'
                       }`}
                     >
                       <Target className="inline mr-1 h-3 w-3" />
                       Top N
                     </button>
                     <button
                       onClick={() => handleFilterTypeChange('spend')}
                       className={`px-2 py-1.5 text-xs font-medium rounded transition-colors ${
                         breakdownFilters.type === 'spend'
                           ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm'
                           : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100'
                       }`}
                     >
                       <Zap className="inline mr-1 h-3 w-3" />
                       Min Spend
                     </button>
                   </div>

                   {/* Filter Controls */}
                   <div className="pt-2 border-t border-gray-200 dark:border-gray-600">
                     {breakdownFilters.type === 'limit' ? (
                       <div>
                         <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
                           Show top items
                         </label>
                         <div className="flex items-center space-x-3">
                           <input
                             type="range"
                             min="5"
                             max="50"
                             step="5"
                             value={breakdownFilters.limitCount}
                             onChange={(e) => handleLimitChange(e.target.value)}
                             className="flex-1 h-2 bg-gray-200 dark:bg-gray-600 rounded-lg appearance-none cursor-pointer"
                           />
                           <span className="text-sm font-medium text-gray-900 dark:text-gray-100 min-w-[2rem] text-center">
                             {breakdownFilters.limitCount}
                           </span>
                         </div>
                       </div>
                     ) : (
                       <div>
                         <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
                           Minimum spend ($)
                         </label>
                         <input
                           type="number"
                           min="0"
                           step="10"
                           value={breakdownFilters.minSpend}
                           onChange={(e) => handleMinSpendChange(e.target.value)}
                           className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                         />
                       </div>
                     )}
                   </div>
                 </div>
               )}
            </div>
          </div>
        </div>

                 {/* Status Bar */}
         <div className="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
           <div className="flex items-center justify-between text-sm text-gray-500 dark:text-gray-400">
             <div className="flex items-center space-x-4">
               {breakdown !== 'all' && (
                 <div className="flex items-center space-x-2">
                   <Info className="h-4 w-4" />
                   <span>
                     Breakdown: {breakdown.charAt(0).toUpperCase() + breakdown.slice(1)}
                     {breakdownFilters.enabled && (
                       <span className="ml-1 text-blue-600 dark:text-blue-400">
                         (filtered)
                       </span>
                     )}
                   </span>
                 </div>
               )}
             </div>
             
             <div className="text-xs">
               {dateRange.start_date} to {dateRange.end_date}
             </div>
           </div>
         </div>
      </div>
    </div>
  );
};

export default ImprovedDashboardControls; 