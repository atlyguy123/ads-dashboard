import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { 
  RefreshCw, 
  Search, 
  BarChart3, 
  Clock,
  Database,
  TrendingUp,
  Users,
  DollarSign,
  Eye,
  ChevronDown,
  ChevronUp,
  AlertTriangle,
  UserCheck
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { DashboardGrid } from '../components/DashboardGrid';
import OverviewROASSparkline from '../components/dashboard/OverviewROASSparkline';
import OverviewSpendSparkline from '../components/dashboard/OverviewSpendSparkline';
import OverviewRevenueSparkline from '../components/dashboard/OverviewRevenueSparkline';
import OverviewProfitSparkline from '../components/dashboard/OverviewProfitSparkline';

import ImprovedDashboardControls from '../components/dashboard/ImprovedDashboardControls';
import { dashboardApi } from '../services/dashboardApi';
import { getCurrentETDate, formatDateForDisplay } from '../config/api';


// Import centralized column configuration
// 📋 NEED TO MODIFY COLUMNS? Read: src/config/Column README.md for step-by-step instructions
import { AVAILABLE_COLUMNS, migrateColumnOrder, migrateColumnVisibility } from '../config/columns';
import { useColumnValidation } from '../hooks/useColumnValidation';

export const Dashboard = () => {
  const navigate = useNavigate();
  
  // Main dashboard state
  const [loading, setLoading] = useState(false);
  const [backgroundLoading, setBackgroundLoading] = useState(false);
  const [error, setError] = useState(null);
  const [dashboardData, setDashboardData] = useState(() => {
    // Load saved dashboard data immediately
    const saved = localStorage.getItem('dashboard_data');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved dashboard data:', e);
      }
    }
    return [];
  });
  const [lastUpdated, setLastUpdated] = useState(() => {
    return localStorage.getItem('dashboard_last_updated') || null;
  });
  
  // Pipeline status state
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  
  // Dashboard controls state
  const [dateRange, setDateRange] = useState(() => {
    const saved = localStorage.getItem('dashboard_date_range');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved date range:', e);
      }
    }
    return {
      start_date: '2024-01-01',
      end_date: getCurrentETDate()
    };
  });

  const [breakdown, setBreakdown] = useState(() => {
    return localStorage.getItem('dashboard_breakdown') || 'all';
  });

  const [hierarchy, setHierarchy] = useState(() => {
    return localStorage.getItem('dashboard_hierarchy') || 'campaign';
  });

  const [textFilter, setTextFilter] = useState(() => {
    return localStorage.getItem('dashboard_text_filter') || '';
  });

  // Breakdown filtering state
  const [breakdownFilters, setBreakdownFilters] = useState(() => {
    const saved = localStorage.getItem('dashboard_breakdown_filters');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved breakdown filters:', e);
      }
    }
    return {
      enabled: false,
      type: 'limit', // 'limit' or 'spend'
      limitCount: 10,
      minSpend: 100
    };
  });

  

  
  // Column visibility state - using centralized migration
  const [columnVisibility, setColumnVisibility] = useState(() => {
    const saved = localStorage.getItem('dashboard_column_visibility');
    let savedVisibility = {};
    
    if (saved) {
      try {
        savedVisibility = JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved column visibility:', e);
        savedVisibility = {};
      }
    }
    
    // Use centralized migration function
    return migrateColumnVisibility(savedVisibility);
  });

  // Column order state - using centralized migration
  const [columnOrder, setColumnOrder] = useState(() => {
    const saved = localStorage.getItem('dashboard_column_order');
    let savedOrder = [];
    
    if (saved) {
      try {
        savedOrder = JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved column order:', e);
        savedOrder = [];
      }
    }
    
    // Use centralized migration function
    return migrateColumnOrder(savedOrder);
  });
  
  // Row order state  
  const [rowOrder, setRowOrder] = useState(() => {
    const saved = localStorage.getItem('dashboard_row_order');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved row order:', e);
      }
    }
    return [];
  });

  // Sorting state - default to spend descending (highest first)
  const [sortConfig, setSortConfig] = useState(() => {
    const saved = localStorage.getItem('dashboard_sort_config');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved sort config:', e);
      }
    }
    return {
      column: 'spend',
      direction: 'desc'
    };
  });

  // Column visibility dropdown state
  const [showColumnSelector, setShowColumnSelector] = useState(false);

  // Validate column consistency (prevents future issues)
  const validationResult = useColumnValidation(columnOrder, columnVisibility);
  
  // Column visibility monitoring
  useEffect(() => {
    // Column visibility state updated
  }, [columnVisibility, validationResult]);



  // Track if initial load has been performed
  const hasInitialLoadRef = useRef(false);
  
  // Refs to hold current values without triggering re-renders
  const currentSettingsRef = useRef({ dateRange, breakdown, hierarchy });
  
  // Update refs when settings change
  useEffect(() => {
    currentSettingsRef.current = { dateRange, breakdown, hierarchy };
  }, [dateRange, breakdown, hierarchy]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (showColumnSelector && !event.target.closest('.column-selector-container')) {
        setShowColumnSelector(false);
      }
    };

    if (showColumnSelector) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [showColumnSelector]);

  // Save states to localStorage
  useEffect(() => {
    localStorage.setItem('dashboard_date_range', JSON.stringify(dateRange));
  }, [dateRange]);

  useEffect(() => {
    localStorage.setItem('dashboard_breakdown', breakdown);
  }, [breakdown]);

  useEffect(() => {
    localStorage.setItem('dashboard_hierarchy', hierarchy);
  }, [hierarchy]);

  useEffect(() => {
    localStorage.setItem('dashboard_column_visibility', JSON.stringify(columnVisibility));
    console.log('💾 Column visibility saved to localStorage:', Object.keys(columnVisibility).filter(key => columnVisibility[key]).length, 'visible columns');
  }, [columnVisibility]);

  // Save additional states to localStorage
  useEffect(() => {
    localStorage.setItem('dashboard_text_filter', textFilter);
  }, [textFilter]);

  useEffect(() => {
    localStorage.setItem('dashboard_breakdown_filters', JSON.stringify(breakdownFilters));
  }, [breakdownFilters]);

  useEffect(() => {
    localStorage.setItem('dashboard_column_order', JSON.stringify(columnOrder));
    console.log('💾 Column order saved to localStorage:', columnOrder.length, 'columns in order');
  }, [columnOrder]);

  useEffect(() => {
    localStorage.setItem('dashboard_row_order', JSON.stringify(rowOrder));
  }, [rowOrder]);

  useEffect(() => {
    localStorage.setItem('dashboard_sort_config', JSON.stringify(sortConfig));
  }, [sortConfig]);

  useEffect(() => {
    localStorage.setItem('dashboard_data', JSON.stringify(dashboardData));
  }, [dashboardData]);

  useEffect(() => {
    if (lastUpdated) {
      localStorage.setItem('dashboard_last_updated', lastUpdated);
    }
  }, [lastUpdated]);

  // Column visibility functions  
  // 📋 MODIFY COLUMN FUNCTIONS? Read: src/config/Column README.md for best practices
  const handleColumnToggle = (columnKey) => {
    setColumnVisibility(prev => {
      const newVisibility = {
        ...prev,
        [columnKey]: !prev[columnKey]
      };
      console.log(`🔄 Column '${columnKey}' toggled to:`, newVisibility[columnKey]);
      return newVisibility;
    });
  };

  const handleSelectAllColumns = () => {
    const newVisibility = {};
    AVAILABLE_COLUMNS.forEach(col => {
      newVisibility[col.key] = true;
    });
    setColumnVisibility(newVisibility);
  };

  const handleSelectNoColumns = () => {
    const newVisibility = { ...columnVisibility };
    AVAILABLE_COLUMNS.forEach(col => {
      if (!col.alwaysVisible) {
        newVisibility[col.key] = false;
      }
    });
    setColumnVisibility(newVisibility);
  };

  // REMOVED: No longer needed - column migration now works correctly
  


  // Handle sorting functionality
  const handleSort = (column) => {
    setSortConfig(prevConfig => {
      if (prevConfig.column === column) {
        // Toggle direction if same column
        return {
          ...prevConfig,
          direction: prevConfig.direction === 'asc' ? 'desc' : 'asc'
        };
      } else {
        // New column - default to descending for numeric columns, ascending for text
        const numericColumns = [
          'impressions', 'clicks', 'spend', 'trials_combined', 'purchases_combined',
          'meta_trials_started', 'mixpanel_trials_started', 'meta_purchases', 'mixpanel_purchases', 'trial_accuracy_ratio', 'mixpanel_trials_ended',
          'mixpanel_trials_in_progress', 'mixpanel_refunds_usd', 'mixpanel_revenue_usd',
          'mixpanel_revenue_net', 'mixpanel_conversions_net_refunds', 'mixpanel_cost_per_trial', 'mixpanel_cost_per_purchase',
          'meta_cost_per_trial', 'meta_cost_per_purchase', 'click_to_trial_rate',
          'trial_conversion_rate', 'avg_trial_refund_rate', 'purchase_accuracy_ratio',
          'purchase_refund_rate', 'estimated_revenue_usd', 'estimated_revenue_adjusted', 'profit', 'estimated_roas', 'performance_impact_score'
        ];
        
        return {
          column,
          direction: numericColumns.includes(column) ? 'desc' : 'asc'
        };
      }
    });
    
    // Clear manual row ordering when column sorting is applied
    setRowOrder([]);
  };

  // Sort data based on current sort configuration
  const sortData = (data, sortConfig) => {
    if (!sortConfig.column) return data;
    
    // Hierarchical sorting: Sort not just the top level, but all children recursively
    // This ensures that when sorting by Performance Impact Score (or any metric):
    // - Campaigns are sorted by the metric
    // - Adsets within each campaign are also sorted by the metric  
    // - Ads within each adset are also sorted by the metric
    // This provides consistent sorting throughout the entire hierarchy
    const sortRecursively = (items) => {
      const sorted = [...items].sort((a, b) => {
        let aValue = a[sortConfig.column];
        let bValue = b[sortConfig.column];
        
        // Handle undefined/null values
        if (aValue == null && bValue == null) return 0;
        if (aValue == null) return sortConfig.direction === 'asc' ? -1 : 1;
        if (bValue == null) return sortConfig.direction === 'asc' ? 1 : -1;
        
        // Handle numeric comparisons
        if (typeof aValue === 'number' && typeof bValue === 'number') {
          return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue;
        }
        
        // Handle string comparisons
        const aStr = String(aValue).toLowerCase();
        const bStr = String(bValue).toLowerCase();
        
        if (sortConfig.direction === 'asc') {
          return aStr.localeCompare(bStr);
        } else {
          return bStr.localeCompare(aStr);
        }
      });
      
      // Recursively sort children arrays
      return sorted.map(item => {
        const updatedItem = { ...item };
        
        // Sort children arrays (hierarchy)
        if (item.children && item.children.length > 0) {
          updatedItem.children = sortRecursively(item.children);
        }
        
        // Sort breakdown values within each entity
        if (item.breakdowns && item.breakdowns.length > 0) {
          updatedItem.breakdowns = item.breakdowns.map(breakdown => ({
            ...breakdown,
            values: [...breakdown.values].sort((a, b) => {
              let aValue = a[sortConfig.column];
              let bValue = b[sortConfig.column];
              
              // Handle undefined/null values
              if (aValue == null && bValue == null) return 0;
              if (aValue == null) return sortConfig.direction === 'asc' ? -1 : 1;
              if (bValue == null) return sortConfig.direction === 'asc' ? 1 : -1;
              
              // Handle numeric comparisons
              if (typeof aValue === 'number' && typeof bValue === 'number') {
                return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue;
              }
              
              // Handle string comparisons
              const aStr = String(aValue).toLowerCase();
              const bStr = String(bValue).toLowerCase();
              
              if (sortConfig.direction === 'asc') {
                return aStr.localeCompare(bStr);
              } else {
                return bStr.localeCompare(aStr);
              }
            })
          }));
        }
        
        return updatedItem;
      });
    };
    
    return sortRecursively(data);
  };

  // Filter data based on text input
  const filterRecursive = useCallback((items) => {
    if (!textFilter.trim()) return items;
    
    const filterText = textFilter.toLowerCase();
    
    return items.reduce((acc, item) => {
      // Check if this item matches the filter
      const matches = (
        (item.name && item.name.toLowerCase().includes(filterText)) ||
        (item.campaign_name && item.campaign_name.toLowerCase().includes(filterText)) ||
        (item.adset_name && item.adset_name.toLowerCase().includes(filterText)) ||
        (item.ad_name && item.ad_name.toLowerCase().includes(filterText))
      );
      
      // Filter children recursively
      const filteredChildren = item.children ? filterRecursive(item.children) : [];
      
      // Include this item if it matches OR if it has matching children
      if (matches || filteredChildren.length > 0) {
        acc.push({
          ...item,
          children: filteredChildren
        });
      }
      
      return acc;
    }, []);
  }, [textFilter]);

  // Apply breakdown filtering to data
  const applyBreakdownFiltering = useCallback((data) => {
    if (!breakdownFilters.enabled || breakdown === 'all') {
      return data;
    }

    return data.map(row => {
      if (!row.breakdowns || row.breakdowns.length === 0) {
        return row;
      }

      const filteredBreakdowns = row.breakdowns.map(breakdownSection => {
        let filteredValues = breakdownSection.values;

        if (breakdownFilters.type === 'spend') {
          // Filter by minimum spend
          filteredValues = filteredValues.filter(value => 
            (value.spend || 0) >= breakdownFilters.minSpend
          );
        } else if (breakdownFilters.type === 'limit') {
          // Sort by current sort config and limit to top N
          const sortColumn = sortConfig.column || 'spend';
          const sortDirection = sortConfig.direction || 'desc';
          
          filteredValues = [...filteredValues].sort((a, b) => {
            const aVal = a[sortColumn] || 0;
            const bVal = b[sortColumn] || 0;
            
            if (sortDirection === 'desc') {
              return bVal - aVal;
            } else {
              return aVal - bVal;
            }
          }).slice(0, breakdownFilters.limitCount);
        }

        return {
          ...breakdownSection,
          values: filteredValues
        };
      });

      return {
        ...row,
        breakdowns: filteredBreakdowns
      };
    });
  }, [breakdownFilters, breakdown, sortConfig]);

  // Filter and sort data
  const processedData = useMemo(() => {
    if (!dashboardData || dashboardData.length === 0) return [];
    
    // Apply text filter first
    const filteredData = textFilter ? 
      dashboardData.filter(row => 
        filterRecursive([row]).length > 0
      ) : dashboardData;
    
    // Apply breakdown filtering
    const breakdownFilteredData = applyBreakdownFiltering(filteredData);
    
    // Apply sorting
    const sortedData = sortData(breakdownFilteredData, sortConfig);
    
    return sortedData;
  }, [dashboardData, textFilter, sortConfig, filterRecursive, applyBreakdownFiltering]);

  // Check pipeline status
  const checkPipelineStatus = useCallback(async () => {
    try {
      const result = await dashboardApi.checkPipelineStatus();
      setPipelineRunning(result.isRunning);
      setPipelineStatus(result.status);
      
      if (result.isRunning) {
        console.log('🔄 Master pipeline is currently running - data refresh disabled');
      }
    } catch (error) {
      console.error('Error checking pipeline status:', error);
      // Set safe defaults on error
      setPipelineRunning(false);
      setPipelineStatus(null);
    }
  }, []);

  // Handle background data refresh (doesn't show main loading state)
  const handleBackgroundRefresh = useCallback(async () => {
    // Check if pipeline is running first
    if (pipelineRunning) {
      console.log('⚠️ Background refresh skipped - master pipeline is running');
      return;
    }
    
    setBackgroundLoading(true);
    setError(null);
    
    try {
      const { dateRange, breakdown, hierarchy } = currentSettingsRef.current;
      console.log('🔄 Background refresh - fetching fresh data:', {
        dateRange,
        breakdown,
        hierarchy
      });
      
      const response = await dashboardApi.getAnalyticsData({
        start_date: dateRange.start_date,
        end_date: dateRange.end_date,
        breakdown: breakdown,
        group_by: hierarchy,
        enable_breakdown_mapping: true
      });
      
      if (response.success) {
        setDashboardData(response.data || []);
        setLastUpdated(new Date().toISOString());
        
        // Initialize row order with data IDs if not already set AND no column sorting is active
        if (response.data && response.data.length > 0 && rowOrder.length === 0 && (!sortConfig.column)) {
          setRowOrder(response.data.map(r => r.id));
        }
        
        console.log('✅ Background refresh completed successfully');
      } else {
        setError(response.error || 'Failed to fetch analytics data');
      }
    } catch (error) {
      console.error('Background refresh error:', error);
      setError(error.message || 'Failed to refresh dashboard data');
    } finally {
      setBackgroundLoading(false);
    }
  }, [rowOrder.length, pipelineRunning]);

  // Handle dashboard data refresh (fast, pre-computed data)
  const handleRefresh = useCallback(async () => {
    // Check if pipeline is running first
    if (pipelineRunning) {
      console.log('⚠️ Dashboard refresh skipped - master pipeline is running');
      setError('Cannot refresh data while master pipeline is running. Please wait for the pipeline to complete.');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      const { dateRange, breakdown, hierarchy } = currentSettingsRef.current;
      console.log('🔄 Fetching pre-computed analytics data:', {
        dateRange,
        breakdown,
        hierarchy
      });
      
      const response = await dashboardApi.getAnalyticsData({
        start_date: dateRange.start_date,
        end_date: dateRange.end_date,
        breakdown: breakdown,
        group_by: hierarchy,
        enable_breakdown_mapping: true
      });
      
      if (response.success) {
        setDashboardData(response.data || []);
        setLastUpdated(new Date().toISOString());
        
        // Initialize row order with data IDs only if no column sorting is active
        if (response.data && response.data.length > 0 && (!sortConfig.column)) {
          setRowOrder(response.data.map(r => r.id));
        }
        
        console.log('✅ Dashboard data loaded successfully');
        console.log('📊 Data summary:', {
          totalRows: response.data?.length || 0,
          breakdown: breakdown,
          hierarchy: hierarchy
        });
        
      } else {
        setError(response.error || 'Failed to fetch analytics data');
      }
    } catch (error) {
      console.error('Dashboard refresh error:', error);
      setError(error.message || 'Failed to refresh dashboard data');
    } finally {
      setLoading(false);
    }
  }, [pipelineRunning]);

  // Initialize component state on mount (no automatic data refresh)
  useEffect(() => {
    if (hasInitialLoadRef.current) return; // Prevent multiple initial loads
    
    const hasValidData = dashboardData && dashboardData.length > 0;
    
    if (hasValidData) {
      // Show cached data without refreshing - user must manually refresh
      console.log('📋 Loading cached data (no auto-refresh)');
      
      // Initialize row order with cached data IDs only if no column sorting is active
      if (dashboardData.length > 0 && rowOrder.length === 0 && (!sortConfig.column)) {
        setRowOrder(dashboardData.map(r => r.id));
      }
    } else {
      // No cached data - show empty state, user must click refresh
      console.log('📋 No cached data available - waiting for user action');
    }
    
    hasInitialLoadRef.current = true;
  }, []); // Empty dependency array - only run once on mount

  // Pipeline status polling
  useEffect(() => {
    // Check pipeline status on mount
    checkPipelineStatus();
    
    // Set up polling interval
    const interval = setInterval(checkPipelineStatus, 10000); // Check every 10 seconds
    
    return () => clearInterval(interval);
  }, [checkPipelineStatus]);

  // REMOVED: Auto-refresh on parameter changes - now only refresh when user clicks "Refresh Data"
  // This gives users full control over when data is fetched



  // Get dashboard stats
  const getDashboardStats = () => {
    if (!dashboardData || dashboardData.length === 0) return {};
    
    const calculateStats = (items) => {
      return items.reduce((acc, item) => {
        acc.totalSpend += item.spend || 0;
        acc.totalImpressions += item.impressions || 0;
        acc.totalClicks += item.clicks || 0;
        // UPDATED: Use accuracy-adjusted estimated revenue for dashboard summary
        acc.totalRevenue += item.estimated_revenue_adjusted || item.estimated_revenue_usd || 0;
        acc.totalProfit += item.profit || 0;
        return acc;
      }, {
        totalSpend: 0,
        totalImpressions: 0,
        totalClicks: 0,
        totalRevenue: 0,
        totalProfit: 0
      });
    };

    return calculateStats(processedData);
  };

  // Get human-readable sort column name
  const getSortColumnLabel = () => {
    if (!sortConfig.column) return 'None';
    
    const column = AVAILABLE_COLUMNS.find(col => col.key === sortConfig.column);
    return column ? column.label : sortConfig.column;
  };

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center">
            <AlertTriangle className="mx-auto h-12 w-12 text-red-500" />
            <h1 className="mt-4 text-3xl font-bold text-gray-900 dark:text-gray-100">
              Dashboard Error
            </h1>
            <p className="mt-2 text-gray-600 dark:text-gray-400">{error}</p>
            <button 
              onClick={() => {
                setError(null);
                handleRefresh();
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
                Campaign Dashboard
              </h1>
              <p className="mt-2 text-gray-600 dark:text-gray-400">
                Advanced analytics for Meta advertising performance
              </p>
            </div>
            
            <div className="flex items-center space-x-6">
              {/* Segment Analysis Button */}
              <button
                onClick={() => navigate('/segment-analysis')}
                className="flex items-center space-x-2 px-3 py-2 text-gray-600 dark:text-gray-300 hover:text-blue-500 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700"
                title="Segment Performance Analysis"
              >
                <UserCheck className="h-4 w-4" />
                <span className="text-sm">Segments</span>
              </button>

              {/* Column Visibility Control */}
              <div className="relative column-selector-container">
                <button
                  onClick={() => setShowColumnSelector(!showColumnSelector)}
                  className="flex items-center space-x-2 px-3 py-2 text-gray-600 dark:text-gray-300 hover:text-blue-500 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700"
                >
                  <Eye className="h-4 w-4" />
                  <span className="text-sm">Columns</span>
                  {showColumnSelector ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                </button>
                
                {showColumnSelector && (
                  <div className="absolute right-0 mt-2 w-64 bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 z-50 max-h-80 overflow-y-auto">
                    <div className="p-3 border-b border-gray-200 dark:border-gray-700">
                      <div className="flex space-x-2 mb-2">
                        <button
                          onClick={handleSelectAllColumns}
                          className="px-2 py-1 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 dark:hover:bg-blue-800"
                        >
                          Show All
                        </button>
                        <button
                          onClick={handleSelectNoColumns}
                          className="px-2 py-1 text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-500"
                        >
                          Hide All
                        </button>

                      </div>
                    </div>
                    
                    <div className="p-3 space-y-2">
                      {AVAILABLE_COLUMNS.map((column) => (
                        <div key={column.key} className="flex items-center">
                          <input
                            type="checkbox"
                            checked={columnVisibility[column.key] || false}
                            onChange={() => handleColumnToggle(column.key)}
                            disabled={column.alwaysVisible}
                            className="mr-3 h-4 w-4 text-blue-600 rounded border-gray-300 dark:border-gray-500 focus:ring-blue-500"
                          />
                          <span className={`text-sm ${
                            column.alwaysVisible 
                              ? 'text-gray-500 dark:text-gray-400' 
                              : 'text-gray-700 dark:text-gray-300'
                          }`}>
                            {column.label}
                            {column.alwaysVisible && <span className="ml-1 text-xs">(always visible)</span>}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Dashboard Controls */}
        <div className="mb-6">
          <ImprovedDashboardControls
            dateRange={dateRange}
            breakdown={breakdown}
            hierarchy={hierarchy}
            breakdownFilters={breakdownFilters}
            onDateRangeChange={setDateRange}
            onBreakdownChange={setBreakdown}
            onHierarchyChange={setHierarchy}
            onBreakdownFiltersChange={setBreakdownFilters}
            onRefresh={() => {
              // Use background refresh if we have existing data, otherwise regular refresh
              if (dashboardData && dashboardData.length > 0) {
                handleBackgroundRefresh();
              } else {
                handleRefresh();
              }
            }}
            loading={loading}
            backgroundLoading={backgroundLoading}
            pipelineRunning={pipelineRunning}
            pipelineStatus={pipelineStatus}
          />
        </div>

        {/* Search and Filter */}
        <div className="mb-6">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search campaigns, ad sets, or ads..."
              value={textFilter}
              onChange={(e) => setTextFilter(e.target.value)}
              className="pl-10 pr-4 py-3 w-full border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>

        {/* Summary Stats */}
        {(() => {
          const stats = getDashboardStats();
          if (!stats || Object.keys(stats).length === 0) return null;
          
          const roas = stats.totalSpend > 0 ? stats.totalRevenue / stats.totalSpend : 0;
          
          return (
            <div className="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <DollarSign className="h-8 w-8 text-blue-500" />
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Total Spend</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                        ${Math.round(stats.totalSpend).toLocaleString()}
                      </p>
                    </div>
                  </div>
                  <div className="ml-4">
                    <OverviewSpendSparkline 
                      dateRange={dateRange}
                      breakdown={breakdown}
                      width={180}
                      height={60}
                    />
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <TrendingUp className="h-8 w-8 text-green-500" />
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">EST. Revenue</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                        ${Math.round(stats.totalRevenue).toLocaleString()}
                      </p>
                    </div>
                  </div>
                  <div className="ml-4">
                    <OverviewRevenueSparkline 
                      dateRange={dateRange}
                      breakdown={breakdown}
                      width={180}
                      height={60}
                    />
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <BarChart3 className={`h-8 w-8 ${stats.totalProfit >= 0 ? 'text-green-500' : 'text-red-500'}`} />
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">EST. Profit</p>
                      <p className={`text-2xl font-bold ${stats.totalProfit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        ${Math.round(stats.totalProfit).toLocaleString()}
                      </p>
                    </div>
                  </div>
                  <div className="ml-4">
                    <OverviewProfitSparkline 
                      dateRange={dateRange}
                      breakdown={breakdown}
                      width={180}
                      height={60}
                    />
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <Users className="h-8 w-8 text-purple-500" />
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">ROAS</p>
                      <p className={`text-2xl font-bold ${
                        roas < 0.5 ? 'text-red-400' :
                        roas >= 0.5 && roas < 0.75 ? 'text-orange-400' :
                        roas >= 0.75 && roas < 1.0 ? 'text-yellow-400' :
                        roas >= 1.0 && roas < 1.25 ? 'text-green-400' :
                        roas >= 1.25 && roas < 1.5 ? 'text-blue-400' :
                        'text-purple-400'
                      }`}>
                        {roas.toFixed(2)}x
                      </p>
                    </div>
                  </div>
                  <div className="ml-4">
                    <OverviewROASSparkline 
                      dateRange={dateRange}
                      breakdown={breakdown}
                      width={180}
                      height={60}
                    />
                  </div>
                </div>
              </div>
            </div>
          );
        })()}

        {/* Last Updated with Background Loading Indicator */}
        {(lastUpdated || backgroundLoading) && (
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center text-sm text-gray-500 dark:text-gray-400">
              <Clock className="mr-2 h-4 w-4" />
              Last updated: {lastUpdated ? formatDateForDisplay(lastUpdated) : 'Loading...'}
            </div>
            
            {backgroundLoading && (
              <div className="flex items-center space-x-2 px-3 py-1 bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 rounded-full text-xs border border-blue-200 dark:border-blue-800">
                <RefreshCw className="h-3 w-3 animate-spin" />
                <span>Refreshing data...</span>
              </div>
            )}
          </div>
        )}

        {/* Dashboard Content */}
        {loading ? (
          <div className="flex items-center justify-center h-64 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="text-center">
              <RefreshCw className="mx-auto h-8 w-8 text-blue-500 animate-spin" />
              <p className="mt-2 text-lg font-medium text-gray-900 dark:text-gray-100">
                Loading dashboard data...
              </p>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Fetching pre-computed analytics data
              </p>
            </div>
          </div>
        ) : processedData.length > 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <DashboardGrid 
              data={processedData}
              rowOrder={rowOrder}
              onRowOrderChange={setRowOrder}
              columnVisibility={columnVisibility}
              columnOrder={columnOrder}
              onColumnOrderChange={setColumnOrder}
              dashboardParams={{
                start_date: dateRange.start_date,
                end_date: dateRange.end_date,
                breakdown: breakdown,
                hierarchy: hierarchy
              }}
              sortConfig={sortConfig}
              onSort={handleSort}
            />
          </div>
        ) : (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-8 text-center">
            <Database className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-4 text-lg font-semibold text-gray-900 dark:text-gray-100">
              No Data Available
            </h3>
            <p className="mt-2 text-gray-600 dark:text-gray-400 max-w-md mx-auto">
              {textFilter ? 
                `No results found for "${textFilter}". Try adjusting your search terms.` :
                'Click "Refresh Data" to load campaign information for the selected date range and settings.'
              }
            </p>
            {!textFilter && (
              <button 
                onClick={handleRefresh}
                disabled={loading || backgroundLoading}
                className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
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
            )}
          </div>
        )}
      </div>
      

    </div>
  );
}; 