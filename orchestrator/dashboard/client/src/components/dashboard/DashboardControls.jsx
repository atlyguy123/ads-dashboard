// Dashboard Controls Component
// 
// Provides the main controls for the dashboard including date range selection,
// configuration dropdown, and data refresh functionality.

import React, { useState, useEffect, useRef } from 'react';
import { Calendar, RefreshCw, Settings, Database, Clock, Eye, ChevronDown, ChevronUp, ArrowUp, ArrowDown } from 'lucide-react';

// Define all available columns with their display names and default visibility
const AVAILABLE_COLUMNS = [
  { key: 'name', label: 'Name', defaultVisible: true, alwaysVisible: true }, // Always visible
  { key: 'campaign_name', label: 'Campaign', defaultVisible: true },
  { key: 'adset_name', label: 'Ad Set', defaultVisible: true },
  { key: 'impressions', label: 'Impressions', defaultVisible: true },
  { key: 'clicks', label: 'Clicks', defaultVisible: true },
  { key: 'spend', label: 'Spend', defaultVisible: true },
  { key: 'meta_trials_started', label: 'Trials (Meta)', defaultVisible: true },
  { key: 'mixpanel_trials_started', label: 'Trials (Mixpanel)', defaultVisible: true },
  { key: 'meta_purchases', label: 'Purchases (Meta)', defaultVisible: true },
  { key: 'mixpanel_purchases', label: 'Purchases (Mixpanel)', defaultVisible: true },
  { key: 'trial_accuracy_ratio', label: 'Trial Accuracy Ratio', defaultVisible: true },
  { key: 'mixpanel_trials_ended', label: 'Trials Ended (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_trials_in_progress', label: 'Trials In Progress (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_refunds_usd', label: 'Refunds (Mixpanel)', defaultVisible: true },
  { key: 'mixpanel_revenue_usd', label: 'Revenue (Mixpanel)', defaultVisible: true },
  { key: 'mixpanel_conversions_net_refunds', label: 'Net Conversions (Mixpanel)', defaultVisible: false },
  { key: 'mixpanel_cost_per_trial', label: 'Cost per Trial (Mixpanel)', defaultVisible: true },
  { key: 'mixpanel_cost_per_purchase', label: 'Cost per Purchase (Mixpanel)', defaultVisible: true },
  { key: 'meta_cost_per_trial', label: 'Cost per Trial (Meta)', defaultVisible: false },
  { key: 'meta_cost_per_purchase', label: 'Cost per Purchase (Meta)', defaultVisible: false },
  { key: 'click_to_trial_rate', label: 'Click to Trial Rate', defaultVisible: true },
  { key: 'trial_conversion_rate', label: 'Trial Conversion Rate', defaultVisible: true },
  { key: 'avg_trial_refund_rate', label: 'Trial Refund Rate', defaultVisible: true },
  { key: 'purchase_accuracy_ratio', label: 'Purchase Accuracy Ratio', defaultVisible: false },
  { key: 'purchase_refund_rate', label: 'Purchase Refund Rate', defaultVisible: true },
  { key: 'estimated_revenue_usd', label: 'Estimated Revenue', defaultVisible: true },
  { key: 'profit', label: 'Profit', defaultVisible: true },
  { key: 'estimated_roas', label: 'ROAS', defaultVisible: true },
  { key: 'segment_accuracy_average', label: 'Avg. Accuracy', defaultVisible: true }
];

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

const DashboardControls = ({ 
  onRefresh, 
  isLoading = false, 
  configurations = {}, 
  selectedConfig = null,  // Changed from 'basic_ad_data' to null
  onConfigChange,
  lastUpdated = null,
  onGetCurrentParams = null,  // New prop to get current params
  onColumnVisibilityChange = null,  // New prop to pass column visibility changes
  onColumnOrderChange = null,  // New prop to pass column order changes
  dateRange,
  breakdown,
  hierarchy,
  onDateRangeChange,
  onBreakdownChange,
  onHierarchyChange
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

  // Initialize with 7 days preset on mount
  useEffect(() => {
    const sevenDaysPreset = datePresets.find(p => p.id === '7days');
    if (sevenDaysPreset && (!dateRange.start_date || !dateRange.end_date)) {
      onDateRangeChange(sevenDaysPreset);
    }
  }, []);

  // Update selected preset when date range changes externally
  useEffect(() => {
    const matchingPreset = datePresets.find(preset => 
      preset.start_date === dateRange.start_date && 
      preset.end_date === dateRange.end_date
    );
    setSelectedPreset(matchingPreset ? matchingPreset.id : 'custom');
  }, [dateRange]);

  // Column visibility state
  const [columnVisibility, setColumnVisibility] = useState({});
  
  // Column order state - array of column keys in display order
  const [columnOrder, setColumnOrder] = useState(AVAILABLE_COLUMNS.map(col => col.key));
  
  const [isColumnSelectorOpen, setIsColumnSelectorOpen] = useState(false);
  
  // Ref for the dropdown container
  const dropdownRef = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsColumnSelectorOpen(false);
      }
    };

    if (isColumnSelectorOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [isColumnSelectorOpen]);

  // Load saved date range from localStorage on component mount
  useEffect(() => {
    const savedDateRange = localStorage.getItem('dashboard_date_range');
    if (savedDateRange) {
      try {
        const parsed = JSON.parse(savedDateRange);
        onDateRangeChange(parsed);
      } catch (e) {
        console.warn('Failed to parse saved date range:', e);
      }
    }
  }, []);

  // Load saved breakdown from localStorage on component mount
  useEffect(() => {
    const savedBreakdown = localStorage.getItem('dashboard_breakdown');
    if (savedBreakdown) {
      onBreakdownChange(savedBreakdown);
    }
  }, []);

  // Load saved column visibility from localStorage on component mount
  useEffect(() => {
    const savedVisibility = localStorage.getItem('dashboard_column_visibility');
    if (savedVisibility) {
      try {
        const parsed = JSON.parse(savedVisibility);
        setColumnVisibility(parsed);
      } catch (e) {
        console.warn('Failed to parse saved column visibility:', e);
        // Set default visibility if parsing fails
        const defaultVisibility = {};
        AVAILABLE_COLUMNS.forEach(col => {
          defaultVisibility[col.key] = col.defaultVisible;
        });
        setColumnVisibility(defaultVisibility);
      }
    } else {
      // Set default visibility if no saved state
      const defaultVisibility = {};
      AVAILABLE_COLUMNS.forEach(col => {
        defaultVisibility[col.key] = col.defaultVisible;
      });
      setColumnVisibility(defaultVisibility);
    }
  }, []);

  // Load saved column order from localStorage on component mount
  useEffect(() => {
    const savedOrder = localStorage.getItem('dashboard_column_order');
    if (savedOrder) {
      try {
        const parsed = JSON.parse(savedOrder);
        // Validate that all current columns are in the saved order
        const currentKeys = AVAILABLE_COLUMNS.map(col => col.key);
        const validOrder = parsed.filter(key => currentKeys.includes(key));
        
        // Add any new columns that weren't in the saved order
        const missingKeys = currentKeys.filter(key => !validOrder.includes(key));
        const finalOrder = [...validOrder, ...missingKeys];
        
        setColumnOrder(finalOrder);
      } catch (e) {
        console.warn('Failed to parse saved column order:', e);
      }
    }
  }, []);

  // Save date range to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('dashboard_date_range', JSON.stringify(dateRange));
  }, [dateRange]);

  // Save breakdown to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('dashboard_breakdown', breakdown);
  }, [breakdown]);

  // Save column visibility to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('dashboard_column_visibility', JSON.stringify(columnVisibility));
    // Notify parent component about column visibility changes
    if (onColumnVisibilityChange) {
      onColumnVisibilityChange(columnVisibility);
    }
  }, [columnVisibility]); // Remove onColumnVisibilityChange from dependencies

  // Save column order to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('dashboard_column_order', JSON.stringify(columnOrder));
    // Notify parent component about column order changes
    if (onColumnOrderChange) {
      onColumnOrderChange(columnOrder);
    }
  }, [columnOrder]); // Remove onColumnOrderChange from dependencies

  // Provide current params to parent component whenever they change
  useEffect(() => {
    if (onGetCurrentParams) {
      const currentParams = {
        ...dateRange,
        config_key: selectedConfig,
        breakdown: breakdown
      };
      onGetCurrentParams(currentParams);
    }
  }, [dateRange, selectedConfig, breakdown]); // Remove onGetCurrentParams from dependencies

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

  const handleColumnToggle = (columnKey) => {
    setColumnVisibility(prev => ({
      ...prev,
      [columnKey]: !prev[columnKey]
    }));
  };

  const handleSelectAll = () => {
    const newVisibility = {};
    AVAILABLE_COLUMNS.forEach(col => {
      newVisibility[col.key] = true;
    });
    setColumnVisibility(newVisibility);
  };

  const handleSelectNone = () => {
    const newVisibility = {};
    AVAILABLE_COLUMNS.forEach(col => {
      // Always keep name column visible
      newVisibility[col.key] = col.alwaysVisible || false;
    });
    setColumnVisibility(newVisibility);
  };

  const handleResetToDefaults = () => {
    const defaultVisibility = {};
    AVAILABLE_COLUMNS.forEach(col => {
      defaultVisibility[col.key] = col.defaultVisible;
    });
    setColumnVisibility(defaultVisibility);
    
    // Reset column order too
    setColumnOrder(AVAILABLE_COLUMNS.map(col => col.key));
  };

  const moveColumn = (columnKey, direction) => {
    const currentIndex = columnOrder.indexOf(columnKey);
    if (currentIndex === -1) return;
    
    let newIndex;
    if (direction === 'up') {
      newIndex = currentIndex - 1;
    } else {
      newIndex = currentIndex + 1;
    }
    
    // Prevent moving beyond bounds or into the name column position (index 0)
    if (newIndex < 1 || newIndex >= columnOrder.length) return;
    
    const newOrder = [...columnOrder];
    const [movedColumn] = newOrder.splice(currentIndex, 1);
    newOrder.splice(newIndex, 0, movedColumn);
    
    setColumnOrder(newOrder);
  };

  const formatLastUpdated = (timestamp) => {
    if (!timestamp) return 'Never';
    try {
      return new Date(timestamp).toLocaleString();
    } catch (e) {
      return 'Invalid date';
    }
  };

  const visibleColumnsCount = Object.values(columnVisibility).filter(Boolean).length;

  // Get ordered columns for display in dropdown
  const orderedColumns = columnOrder.map(key => 
    AVAILABLE_COLUMNS.find(col => col.key === key)
  ).filter(Boolean);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-soft p-6 mb-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 flex items-center">
          <Settings size={20} className="mr-2" />
          Dashboard Controls
        </h2>
        
        {lastUpdated && (
          <div className="flex items-center text-sm text-gray-500 dark:text-gray-400">
            <Clock size={14} className="mr-1" />
            Last updated: {formatLastUpdated(lastUpdated)}
          </div>
        )}
      </div>

      {/* Controls Grid */}
      <div className="grid grid-cols-1 xl:grid-cols-12 gap-6 items-end">
        {/* Date Range Section - Takes up more space */}
        <div className="xl:col-span-5">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
            Date Range
          </label>
          
          {/* Date Presets */}
          <div className="flex flex-wrap gap-2 mb-3">
            {datePresets.map((preset) => (
              <button
                key={preset.id}
                onClick={() => handlePresetChange(preset.id)}
                className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                  selectedPreset === preset.id
                    ? 'bg-blue-100 text-blue-700 border-2 border-blue-300 dark:bg-blue-900 dark:text-blue-300 dark:border-blue-600'
                    : 'bg-gray-100 text-gray-700 border-2 border-transparent hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
                }`}
              >
                {preset.label}
              </button>
            ))}
            <button
              onClick={() => handlePresetChange('custom')}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                selectedPreset === 'custom'
                  ? 'bg-blue-100 text-blue-700 border-2 border-blue-300 dark:bg-blue-900 dark:text-blue-300 dark:border-blue-600'
                  : 'bg-gray-100 text-gray-700 border-2 border-transparent hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
              }`}
            >
              Custom
            </button>
          </div>
          
          {/* Custom Date Inputs */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">From</label>
              <input
                type="date"
                value={dateRange.start_date}
                onChange={(e) => handleCustomDateChange('start_date', e.target.value)}
                className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">To</label>
              <input
                type="date"
                value={dateRange.end_date}
                onChange={(e) => handleCustomDateChange('end_date', e.target.value)}
                className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm"
              />
            </div>
          </div>
        </div>

        {/* Configuration Dropdown */}
        <div className="xl:col-span-2">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            <Database size={16} className="inline mr-2" />
            Data Configuration
          </label>
          <select
            value={selectedConfig || ''}
            onChange={(e) => onConfigChange && onConfigChange(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md 
                     bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100
                     focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
          >
            {!selectedConfig && (
              <option value="" disabled>
                Loading configurations...
              </option>
            )}
            {Object.entries(configurations).map(([key, config]) => (
              <option key={key} value={key}>
                {config.name}
                {config.day_count ? ` (${config.day_count} days)` : ''}
                {config.is_default ? ' - Default' : ''}
              </option>
            ))}
          </select>
          {configurations[selectedConfig] && (
            <div className="text-xs text-gray-500 dark:text-gray-400 mt-1 space-y-1">
              <p>{configurations[selectedConfig].description}</p>
              {configurations[selectedConfig].date_range && (
                <p className="flex items-center">
                  <Calendar size={12} className="mr-1" />
                  Available: {configurations[selectedConfig].date_range}
                </p>
              )}
              {configurations[selectedConfig].day_count && (
                <p className="flex items-center">
                  <Database size={12} className="mr-1" />
                  {configurations[selectedConfig].day_count} days of data
                </p>
              )}
            </div>
          )}
        </div>

        {/* Breakdown */}
        <div className="xl:col-span-2">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            <Settings size={16} className="inline mr-2" />
            Data Breakdown
          </label>
          <select
            value={breakdown}
            onChange={(e) => onBreakdownChange(e.target.value)}
            className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
          >
            <option value="all">All</option>
            <option value="country">Country</option>
            <option value="region">Region</option>
            <option value="device">Device</option>
          </select>
        </div>

        {/* Hierarchy */}
        <div className="xl:col-span-3">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Hierarchy
          </label>
          <select
            value={hierarchy}
            onChange={(e) => onHierarchyChange(e.target.value)}
            className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
          >
            <option value="campaign">Campaign → Ad Set → Ad</option>
            <option value="adset">Ad Set → Ad</option>
            <option value="ad">Ad Only</option>
          </select>
        </div>

        {/* Column Selection */}
        <div className="space-y-2 relative xl:col-span-2">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
            <Eye size={16} className="inline mr-2" />
            Column Visibility
          </label>
          <button
            onClick={() => setIsColumnSelectorOpen(!isColumnSelectorOpen)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md 
                     bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100
                     focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm
                     flex items-center justify-between"
          >
            <span>{visibleColumnsCount} columns visible</span>
            {isColumnSelectorOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </button>
          
          {isColumnSelectorOpen && (
            <div ref={dropdownRef} className="absolute top-full left-0 right-0 z-50 mt-1 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg max-h-[480px] overflow-y-auto">
              {/* Quick Actions */}
              <div className="p-3 border-b border-gray-200 dark:border-gray-600 sticky top-0 bg-white dark:bg-gray-700 z-10">
                <div className="flex space-x-2 mb-2">
                  <button
                    onClick={handleSelectAll}
                    className="px-2 py-1 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 dark:hover:bg-blue-800"
                  >
                    Select All
                  </button>
                  <button
                    onClick={handleSelectNone}
                    className="px-2 py-1 text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-500"
                  >
                    Select None
                  </button>
                  <button
                    onClick={handleResetToDefaults}
                    className="px-2 py-1 text-xs bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-300 rounded hover:bg-green-200 dark:hover:bg-green-800"
                  >
                    Reset to Defaults
                  </button>
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  💡 Tip: Drag column headers in the table or use arrows below to reorder
                </div>
              </div>
              
              {/* Column Checkboxes with Reorder Controls */}
              <div className="p-3 space-y-2">
                {orderedColumns.map((column, index) => {
                  const canMoveUp = index > 1; // Skip name column (index 0) and first moveable column
                  const canMoveDown = index < orderedColumns.length - 1;
                  
                  return (
                    <div key={column.key} className="flex items-center group">
                      <input
                        type="checkbox"
                        checked={columnVisibility[column.key] || false}
                        onChange={() => handleColumnToggle(column.key)}
                        disabled={column.alwaysVisible}
                        className="mr-3 h-4 w-4 text-blue-600 rounded border-gray-300 dark:border-gray-500 focus:ring-blue-500"
                      />
                      <span className={`flex-1 text-sm ${
                        column.alwaysVisible 
                          ? 'text-gray-500 dark:text-gray-400' 
                          : 'text-gray-700 dark:text-gray-300 group-hover:text-gray-900 dark:group-hover:text-gray-100'
                      }`}>
                        {column.label}
                        {column.alwaysVisible && <span className="ml-1 text-xs">(always visible)</span>}
                      </span>
                      
                      {/* Reorder Controls - only for non-alwaysVisible columns */}
                      {!column.alwaysVisible && (
                        <div className="flex items-center space-x-1 ml-2">
                          <button
                            onClick={() => moveColumn(column.key, 'up')}
                            disabled={!canMoveUp}
                            className={`p-1 rounded ${canMoveUp 
                              ? 'text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-200 dark:hover:bg-gray-500' 
                              : 'text-gray-300 dark:text-gray-600 cursor-not-allowed'}`}
                            title="Move up"
                          >
                            <ArrowUp size={12} />
                          </button>
                          <button
                            onClick={() => moveColumn(column.key, 'down')}
                            disabled={!canMoveDown}
                            className={`p-1 rounded ${canMoveDown 
                              ? 'text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-200 dark:hover:bg-gray-500' 
                              : 'text-gray-300 dark:text-gray-600 cursor-not-allowed'}`}
                            title="Move down"
                          >
                            <ArrowDown size={12} />
                          </button>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Refresh Button */}
        <div className="xl:col-span-2">
          <button
            onClick={onRefresh}
            disabled={isLoading || !selectedConfig}
            className={`w-full inline-flex items-center justify-center px-4 py-2.5 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm transition-colors`}
          >
            {(isLoading || !selectedConfig) ? (
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
    </div>
  );
};

export { DashboardControls, AVAILABLE_COLUMNS }; 