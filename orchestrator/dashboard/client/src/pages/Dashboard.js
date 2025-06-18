import React, { useState, useEffect } from 'react';
import { 
  RefreshCw, 
  Search, 
  Filter, 
  BarChart3, 
  AlertTriangle, 
  CheckCircle, 
  Clock,
  Database,
  TrendingUp,
  Users,
  DollarSign,
  Eye,
  EyeOff,
  Grid,
  List,
  Settings,
  XCircle
} from 'lucide-react';
import { DashboardGrid } from '../components/DashboardGrid';
import { DebugModal } from '../components/DebugModal';
import { GraphModal } from '../components/GraphModal';
import TimelineModal from '../components/TimelineModal';
import AnalyticsPipelineControls from '../components/dashboard/AnalyticsPipelineControls';
import { DashboardControls } from '../components/dashboard/DashboardControls';
import { dashboardApi } from '../services/dashboardApi';

export const Dashboard = () => {
  // Main dashboard state
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [dashboardData, setDashboardData] = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  
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

  const [textFilter, setTextFilter] = useState('');
  
  // Modal states
  const [isDebugModalOpen, setIsDebugModalOpen] = useState(false);
  const [isGraphModalOpen, setIsGraphModalOpen] = useState(false);
  const [isTimelineModalOpen, setIsTimelineModalOpen] = useState(false);
  const [selectedRowData, setSelectedRowData] = useState(null);
  
  // UI state
  const [viewMode, setViewMode] = useState('grid');
  const [showSettings, setShowSettings] = useState(false);
  
  // Column visibility state
  const [columnVisibility, setColumnVisibility] = useState(() => {
    const saved = localStorage.getItem('dashboard_column_visibility');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn('Failed to parse saved column visibility:', e);
      }
    }
    // Default column visibility
    return {
      name: true,
      campaign_name: true,
      adset_name: true,
      impressions: true,
      clicks: true,
      spend: true,
      meta_trials_started: true,
      mixpanel_trials_started: true,
      meta_purchases: true,
      mixpanel_purchases: true,
      mixpanel_revenue_usd: true,
      estimated_roas: true,
      profit: true,
      trial_accuracy_ratio: true
    };
  });

  // Column order state
  const [columnOrder, setColumnOrder] = useState([]);
  
  // Row order state  
  const [rowOrder, setRowOrder] = useState([]);

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
  }, [columnVisibility]);

  // Filter data based on text filter
  const filteredData = React.useMemo(() => {
    if (!textFilter.trim()) return dashboardData;
    
    const filterText = textFilter.toLowerCase();
    
    const filterRecursive = (items) => {
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
    };
    
    return filterRecursive(dashboardData);
  }, [dashboardData, textFilter]);

  // Handle dashboard data refresh (fast, pre-computed data)
  const handleRefresh = async () => {
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
        group_by: hierarchy  // Fix parameter name to match backend API
      });
      
      if (response.success) {
        setDashboardData(response.data || []);
        setLastUpdated(new Date().toISOString());
        
        // Initialize row order with data IDs
        if (response.data && response.data.length > 0) {
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
  };

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

  // Get stats for summary cards
  const getDashboardStats = () => {
    if (!dashboardData.length) return null;
    
    const calculateStats = (items) => {
      let totalSpend = 0;
      let totalRevenue = 0;
      let totalImpressions = 0;
      let totalClicks = 0;
      let totalTrials = 0;
      let totalPurchases = 0;
      let count = 0;
      
      items.forEach(item => {
        // Only sum the top-level items (campaigns) - their totals already include adsets/ads
        totalSpend += parseFloat(item.spend || 0);
        totalRevenue += parseFloat(item.mixpanel_revenue_usd || item.estimated_revenue_usd || 0);
        totalImpressions += parseInt(item.impressions || 0);
        totalClicks += parseInt(item.clicks || 0);
        totalTrials += parseInt(item.mixpanel_trials_started || 0);
        totalPurchases += parseInt(item.mixpanel_purchases || 0);
        count++;
        
        // DO NOT add children stats - they're already included in the campaign totals
        // This prevents double-counting since campaign metrics are aggregated from their adsets/ads
      });
      
      return {
        totalSpend,
        totalRevenue,
        totalImpressions,
        totalClicks,
        totalTrials,
        totalPurchases,
        count
      };
    };
    
    const stats = calculateStats(dashboardData);
    const profit = stats.totalRevenue - stats.totalSpend;
    const roas = stats.totalSpend > 0 ? stats.totalRevenue / stats.totalSpend : 0;
    
    return { ...stats, profit, roas };
  };

  const stats = getDashboardStats();

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
            <div className="flex items-center space-x-4">
              <button
                onClick={() => setViewMode(viewMode === 'grid' ? 'list' : 'grid')}
                className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              >
                {viewMode === 'grid' ? <List className="h-5 w-5" /> : <Grid className="h-5 w-5" />}
              </button>
              <button
                onClick={() => setShowSettings(!showSettings)}
                className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              >
                <Settings className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>

        {/* Analytics Pipeline Status */}
        <div className="mb-6">
          <AnalyticsPipelineControls />
        </div>

        {/* Dashboard Controls */}
        <div className="mb-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="p-6">
              <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                {/* Date Range */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Date Range
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <input
                      type="date"
                      value={dateRange.start_date}
                      onChange={(e) => setDateRange(prev => ({ ...prev, start_date: e.target.value }))}
                      className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    />
                    <input
                      type="date"
                      value={dateRange.end_date}
                      onChange={(e) => setDateRange(prev => ({ ...prev, end_date: e.target.value }))}
                      className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    />
                  </div>
                </div>

                {/* Breakdown */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Breakdown
                  </label>
                  <select
                    value={breakdown}
                    onChange={(e) => setBreakdown(e.target.value)}
                    className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  >
                    <option value="all">All</option>
                    <option value="country">Country</option>
                    <option value="region">Region</option>
                    <option value="device">Device</option>
                  </select>
                </div>

                {/* Hierarchy */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Hierarchy
                  </label>
                  <select
                    value={hierarchy}
                    onChange={(e) => setHierarchy(e.target.value)}
                    className="block w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  >
                    <option value="campaign">Campaign → Ad Set → Ad</option>
                    <option value="adset">Ad Set → Ad</option>
                    <option value="ad">Ad Only</option>
                  </select>
                </div>

                {/* Actions */}
                <div className="flex flex-col justify-end">
                  <button
                    onClick={handleRefresh}
                    disabled={loading}
                    className="inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
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
            </div>
          </div>
        </div>

        {/* Column Management Settings Panel */}
        {showSettings && (
          <div className="mb-6">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">
                    Column Settings
                  </h3>
                  <button
                    onClick={() => setShowSettings(false)}
                    className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                  >
                    <XCircle className="h-5 w-5" />
                  </button>
                </div>
                <DashboardControls
                  onColumnVisibilityChange={setColumnVisibility}
                  onColumnOrderChange={setColumnOrder}
                />
              </div>
            </div>
          </div>
        )}

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
        {stats && (
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
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Revenue</p>
                  <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                    ${stats.totalRevenue.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <BarChart3 className={`h-8 w-8 ${stats.profit >= 0 ? 'text-green-500' : 'text-red-500'}`} />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Profit</p>
                  <p className={`text-2xl font-bold ${stats.profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    ${stats.profit.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
            
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <div className="flex items-center">
                <Users className="h-8 w-8 text-purple-500" />
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600 dark:text-gray-400">ROAS</p>
                  <p className={`text-2xl font-bold ${stats.roas >= 1.5 ? 'text-green-600' : stats.roas >= 1.0 ? 'text-yellow-600' : 'text-red-600'}`}>
                    {stats.roas.toFixed(2)}x
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Last Updated */}
        {lastUpdated && (
          <div className="mb-4 flex items-center text-sm text-gray-500 dark:text-gray-400">
            <Clock className="mr-2 h-4 w-4" />
            Last updated: {new Date(lastUpdated).toLocaleString()}
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
        ) : filteredData.length > 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
            <DashboardGrid 
              data={filteredData}
              rowOrder={rowOrder}
              onRowOrderChange={setRowOrder}
              onRowAction={handleRowAction}
              columnVisibility={columnVisibility}
              columnOrder={columnOrder}
              onColumnOrderChange={setColumnOrder}
              dashboardParams={{
                start_date: dateRange.start_date,
                end_date: dateRange.end_date,
                breakdown: breakdown,
                hierarchy: hierarchy
              }}
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
                className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Refresh Data
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