import { useState, useEffect, useCallback } from 'react';
import { dashboardApi } from '../services/dashboardApi';

/**
 * Shared hook for overview chart data that all sparklines can use
 * This prevents multiple API calls for the same data
 */
const useOverviewChartData = (dateRange, breakdown, refreshTrigger = 0) => {
  // Calculate consistent cache key using the actual API date range (28 days ending at end_date)
  const getCacheKey = useCallback(() => {
    if (!dateRange?.end_date) return null;
    const endDate = new Date(dateRange.end_date);
    const startDate = new Date(endDate);
    startDate.setDate(endDate.getDate() - 27);
    return `overview_chart_data_${startDate.toISOString().split('T')[0]}_${dateRange.end_date}_${breakdown}`;
  }, [dateRange?.end_date, breakdown]);

  const [chartData, setChartData] = useState(() => {
    // Load cached data immediately using consistent cache key
    const cacheKey = getCacheKey();
    if (cacheKey) {
      const cached = localStorage.getItem(cacheKey);
      if (cached) {
        try {
          console.log('📊 Loading cached overview chart data');
          return JSON.parse(cached);
        } catch (e) {
          console.warn('Failed to parse cached overview chart data:', e);
        }
      }
    }
    return [];
  });
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const loadChartData = useCallback(async () => {
    if (!dateRange?.end_date) {
      return;
    }
    
    const cacheKey = getCacheKey();
    
    // FORCE REFRESH: Always update when refresh button is clicked
    if (refreshTrigger > 0) {
      console.log('📊 FORCED REFRESH: Clearing cache and fetching fresh data');
      if (cacheKey) {
        localStorage.removeItem(cacheKey);
      }
      // Clear the current state data to force fresh rendering
      setChartData([]);
    } else {
      // Only use cache on initial load (refreshTrigger === 0)
      const hasValidCachedData = chartData.length > 0;
      if (hasValidCachedData) {
        console.log('📊 Using cached overview chart data for initial load');
        return; // Use cached data only for initial load
      }
      console.log('📊 No cached data found, making initial API call');
    }
    
    console.log('📊 Fetching overview chart data:', { 
      refreshTrigger, 
      isForceRefresh: refreshTrigger > 0,
      dateRange: dateRange.end_date,
      breakdown 
    });
    
    setLoading(true);
    setError(null);
    
    try {
      // Calculate 28 days ending at the end_date
      const endDate = new Date(dateRange.end_date);
      const startDate = new Date(endDate);
      startDate.setDate(endDate.getDate() - 27); // 27 days back = 28 days total
      
      const response = await dashboardApi.getOverviewROASChartData({
        start_date: startDate.toISOString().split('T')[0],
        end_date: dateRange.end_date,
        breakdown: breakdown
      });
      
      console.log('📊 Overview chart API response:', response);
      
      if (response && response.success && response.chart_data) {
        console.log('📊 Setting chart data with', response.chart_data.length, 'days');
        setChartData(response.chart_data);
        
        // Cache the data using consistent cache key
        if (cacheKey) {
          localStorage.setItem(cacheKey, JSON.stringify(response.chart_data));
          console.log('📊 Cached overview chart data with key:', cacheKey);
        }
      } else {
        console.error('📊 Invalid overview chart API response:', response);
        setError('Invalid API response');
      }
    } catch (error) {
      console.error('📊 Overview chart API error:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  }, [dateRange, breakdown, refreshTrigger, getCacheKey, chartData.length]);

  useEffect(() => {
    loadChartData();
  }, [loadChartData]);

  return {
    chartData,
    loading,
    error
  };
};

export default useOverviewChartData;