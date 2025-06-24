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
  AlertTriangle
} from 'lucide-react';
import { DashboardGrid } from '../components/DashboardGrid';
import { DebugModal } from '../components/DebugModal';
import { GraphModal } from '../components/GraphModal';
import TimelineModal from '../components/TimelineModal';
import ImprovedDashboardControls from '../components/dashboard/ImprovedDashboardControls';
import { dashboardApi } from '../services/dashboardApi';


// Import centralized column configuration
// 📋 NEED TO MODIFY COLUMNS? Read: src/config/Column README.md for step-by-step instructions
import { AVAILABLE_COLUMNS, migrateColumnOrder, migrateColumnVisibility } from '../config/columns';
import { useColumnValidation } from '../hooks/useColumnValidation';

export const Dashboard = () => {
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
      end_date: new Date().toISOString().split('T')[0]
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
  
  // Modal states
  const [isDebugModalOpen, setIsDebugModalOpen] = useState(false);
  const [isGraphModalOpen, setIsGraphModalOpen] = useState(false);
  const [isTimelineModalOpen, setIsTimelineModalOpen] = useState(false);
  const [selectedRowData, setSelectedRowData] = useState(null);
  

  
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
  
  // Debug column visibility (temporary)
  useEffect(() => {
    console.log('Column Status:', {
      'estimated_revenue_adjusted': columnVisibility.estimated_revenue_adjusted,
      'visible_columns_count': Object.keys(columnVisibility).filter(key => columnVisibility[key]).length,
      'validation_passed': validationResult.isValid
    });
  }, [columnVisibility, validationResult]);

  // Pipeline state variables (for tracking running/queued pipelines)
  const [runningPipelines, setRunningPipelines] = useState(new Set());
  const [pipelineQueue, setPipelineQueue] = useState([]);
  const [activePipelineCount, setActivePipelineCount] = useState(0);
  const [maxConcurrentPipelines, setMaxConcurrentPipelines] = useState(8);

  // Track if initial load has been performed
  const hasInitialLoadRef = useRef(false);

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
          'impressions', 'clicks', 'spend', 'meta_trials_started', 'mixpanel_trials_started',
          'meta_purchases', 'mixpanel_purchases', 'trial_accuracy_ratio', 'mixpanel_trials_ended',
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
    
    return [...data].sort((a, b) => {
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

  // Filter and sort data
  const processedData = useMemo(() => {
    if (!dashboardData || dashboardData.length === 0) return [];
    
    // Apply text filter first
    const filteredData = textFilter ? 
      dashboardData.filter(row => 
        filterRecursive([row]).length > 0
      ) : dashboardData;
    
    // Apply sorting
    const sortedData = sortData(filteredData, sortConfig);
    
    return sortedData;
  }, [dashboardData, textFilter, sortConfig, filterRecursive]);

  // Handle background data refresh (doesn't show main loading state)
  const handleBackgroundRefresh = useCallback(async () => {
    setBackgroundLoading(true);
    setError(null);
    
    try {
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
  }, [dateRange, breakdown, hierarchy, rowOrder.length]);

  // Handle dashboard data refresh (fast, pre-computed data)
  const handleRefresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
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
  }, [dateRange, breakdown, hierarchy]);

  // Auto-refresh on component mount (after functions are defined)
  useEffect(() => {
    if (hasInitialLoadRef.current) return; // Prevent multiple initial loads
    
    const hasValidData = dashboardData && dashboardData.length > 0;
    
    if (hasValidData) {
      // If we have cached data, show it immediately and refresh in background
      console.log('🔄 Loading cached data and refreshing in background');
      handleBackgroundRefresh();
    } else {
      // If no cached data, do a regular refresh
      console.log('🔄 No cached data, performing initial load');
      handleRefresh();
    }
    
    hasInitialLoadRef.current = true;
  }, [handleBackgroundRefresh, handleRefresh]); // eslint-disable-line react-hooks/exhaustive-deps

  // 🔥 CRITICAL FIX: Auto-refresh when breakdown, dateRange, or hierarchy changes
  useEffect(() => {
    if (!hasInitialLoadRef.current) return; // Don't trigger before initial load
    
    console.log('🔄 Dashboard parameters changed, refreshing data:', {
      dateRange,
      breakdown,
      hierarchy
    });
    
    // Use background refresh if we have existing data, otherwise regular refresh
    if (dashboardData && dashboardData.length > 0) {
      handleBackgroundRefresh();
    } else {
      handleRefresh();
    }
  }, [dateRange, breakdown, hierarchy, handleBackgroundRefresh, handleRefresh]);

  // Handle row actions
  const handleRowAction = (action, rowData) => {
    setSelectedRowData(rowData);
    if (action === 'graph') {
      setIsGraphModalOpen(true);
    } else if (action === 'debug') {
      setIsDebugModalOpen(true);
    } else if (action === 'timeline') {
      setIsTimelineModalOpen(true);
    }
  };

  // Modal close handlers
  const closeDebugModal = () => {
    setIsDebugModalOpen(false);
    setSelectedRowData(null);
  };

  const closeGraphModal = () => {
    setIsGraphModalOpen(false);
    setSelectedRowData(null);
  };

  const closeTimelineModal = () => {
    setIsTimelineModalOpen(false);
    setSelectedRowData(null);
  };

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
              {/* Sort Status Indicator */}
              <div className="flex items-center space-x-2 text-xs text-gray-600 dark:text-gray-400">
                <TrendingUp size={14} />
                <span>Sorted by:</span>
                <span className="font-medium text-gray-900 dark:text-gray-100">
                  {getSortColumnLabel()}
                </span>
                <span className="text-gray-500">
                  ({sortConfig.direction === 'asc' ? 'Low to High' : 'High to Low'})
                </span>
              </div>

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
            onDateRangeChange={setDateRange}
            onBreakdownChange={setBreakdown}
            onHierarchyChange={setHierarchy}
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
                <div className="flex items-center">
                  <DollarSign className="h-8 w-8 text-blue-500" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Total Spend</p>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      ${stats.totalSpend.toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center">
                  <TrendingUp className="h-8 w-8 text-green-500" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Estimated Revenue</p>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      ${stats.totalRevenue.toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center">
                  <BarChart3 className={`h-8 w-8 ${stats.totalProfit >= 0 ? 'text-green-500' : 'text-red-500'}`} />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Profit</p>
                    <p className={`text-2xl font-bold ${stats.totalProfit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      ${stats.totalProfit.toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="flex items-center">
                  <Users className="h-8 w-8 text-purple-500" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600 dark:text-gray-400">ROAS</p>
                    <p className={`text-2xl font-bold ${roas >= 2 ? 'text-green-600' : roas >= 1 ? 'text-yellow-600' : 'text-red-600'}`}>
                      {roas.toFixed(2)}x
                    </p>
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
              Last updated: {lastUpdated ? new Date(lastUpdated).toLocaleString() : 'Loading...'}
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
              onRowAction={handleRowAction}
              columnVisibility={columnVisibility}
              columnOrder={columnOrder}
              onColumnOrderChange={setColumnOrder}
              runningPipelines={runningPipelines}
              pipelineQueue={pipelineQueue}
              activePipelineCount={activePipelineCount}
              maxConcurrentPipelines={maxConcurrentPipelines}
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
      
      {/* Modals */}
      {selectedRowData && (
        <>
          <DebugModal 
            isOpen={isDebugModalOpen} 
            onClose={closeDebugModal} 
            data={selectedRowData} 
          />
          <GraphModal 
            isOpen={isGraphModalOpen} 
            onClose={closeGraphModal} 
            data={selectedRowData}
            dashboardParams={{
              start_date: dateRange.start_date,
              end_date: dateRange.end_date,
              breakdown: breakdown,
              hierarchy: hierarchy
            }}
          />
          <TimelineModal 
            isOpen={isTimelineModalOpen} 
            onClose={closeTimelineModal} 
            data={selectedRowData}
            rowData={selectedRowData}
          />
        </>
      )}
    </div>
  );
}; 