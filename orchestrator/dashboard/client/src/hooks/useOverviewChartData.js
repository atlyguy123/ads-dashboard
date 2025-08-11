import { useState, useEffect, useCallback } from 'react';
import { dashboardApi } from '../services/dashboardApi';

/**
 * Shared hook for overview chart data that all sparklines can use
 * This prevents multiple API calls for the same data
 */
const useOverviewChartData = (dateRange, breakdown, hierarchy = 'campaign', refreshTrigger = 0) => {


  const [chartData, setChartData] = useState([]);
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const loadChartData = useCallback(async () => {
    if (!dateRange?.end_date) {
      return;
    }
    
    console.log('📊 NO CACHE: Always fetching fresh sparkline data');
    
    console.log('📊 Fetching overview chart data:', { 
      refreshTrigger, 
      isForceRefresh: refreshTrigger > 0,
      dateRange: dateRange.end_date,
      breakdown 
    });
    
    setLoading(true);
    setError(null);
    
    try {
      // Calculate 28 days ending at the end_date (SPARKLINES ALWAYS 28 DAYS)
      const endDate = new Date(dateRange.end_date);
      const startDate = new Date(endDate);
      startDate.setDate(endDate.getDate() - 27); // 27 days back = 28 days total
      
      console.log('📊 Sparklines using 28 days:', {
        start_date: startDate.toISOString().split('T')[0],
        end_date: dateRange.end_date
      });
      
      const response = await dashboardApi.getOverviewROASChartData({
        start_date: startDate.toISOString().split('T')[0],
        end_date: dateRange.end_date,
        breakdown: breakdown,
        group_by: hierarchy
      });
      
      console.log('📊 Overview chart API response:', response);
      
      if (response && response.success && response.chart_data) {
        console.log('📊 Setting chart data with', response.chart_data.length, 'days');
        setChartData(response.chart_data);
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
  }, [dateRange, breakdown, hierarchy, refreshTrigger]);

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