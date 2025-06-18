import React, { useEffect, useState } from 'react';
import { X } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, BarChart, Bar } from 'recharts';
import { dashboardApi } from '../services/dashboardApi';

export const GraphModal = ({ isOpen, onClose, data, dashboardParams }) => {
  const [modalData, setModalData] = useState({
    trendData: [],
    summaryStats: {},
    entityInfo: {}
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen && data && dashboardParams) {
      const fetchModalData = async () => {
        setIsLoading(true);
        setError(null);
        try {
          // Determine entity type and ID based on the data structure
          let entityType, entityId, entityName;
          
          if (data.id && data.id.startsWith('campaign_')) {
            entityType = 'campaign';
            entityId = data.id.replace('campaign_', '');
            entityName = data.campaign_name || data.name;
          } else if (data.id && data.id.startsWith('adset_')) {
            entityType = 'adset';
            entityId = data.id.replace('adset_', '');
            entityName = data.adset_name || data.name;
          } else if (data.id && data.id.startsWith('ad_')) {
            entityType = 'ad';
            entityId = data.id.replace('ad_', '');
            entityName = data.ad_name || data.name;
          } else {
            throw new Error('Unable to determine entity type from data');
          }

          const chartParams = {
            entity_type: entityType,
            entity_id: entityId,
            breakdown: dashboardParams.breakdown || 'all',
            start_date: dashboardParams.start_date,
            end_date: dashboardParams.end_date
          };

          const response = await dashboardApi.getAnalyticsChartData(chartParams);
          
          if (response.success) {
            // Analytics chart data returns chart_data array with daily metrics
            const chartData = response.chart_data || [];
            
            // Calculate summary stats from chart data
            const summaryStats = chartData.length > 0 ? {
              total_spend: chartData.reduce((sum, day) => sum + (parseFloat(day.spend) || 0), 0),
              total_revenue: chartData.reduce((sum, day) => sum + (parseFloat(day.estimated_revenue_usd) || 0), 0),
              avg_roas: chartData.reduce((sum, day) => sum + (parseFloat(day.estimated_roas) || 0), 0) / chartData.length,
              avg_ctr: chartData.reduce((sum, day) => sum + (parseFloat(day.ctr) || 0), 0) / chartData.length
            } : {};
            
            setModalData({
              trendData: chartData.map(day => ({
                date: day.date,
                spend: parseFloat(day.spend) || 0,
                revenue_usd: parseFloat(day.estimated_revenue_usd) || 0,
                total_trials_started: parseInt(day.mixpanel_trials) || 0,
                roas: parseFloat(day.estimated_roas) || 0,
                ctr: (parseFloat(day.clicks) || 0) / (parseFloat(day.impressions) || 1) * 100,
                impressions: parseInt(day.impressions) || 0,
                clicks: parseInt(day.clicks) || 0
              })),
              summaryStats,
              entityInfo: {
                name: entityName,
                type: entityType,
                id: entityId,
                dateRange: response.date_range
              }
            });
          } else {
            throw new Error(response.error || 'Failed to load chart data');
          }
        } catch (err) {
          console.error('Error fetching graph modal data:', err);
          setError(err.message || 'Failed to load graph data. Please try again later.');
        } finally {
          setIsLoading(false);
        }
      };
      fetchModalData();
    }
  }, [isOpen, data, dashboardParams]);

  if (!isOpen || !data) return null;

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value || 0);
  };

  const formatNumber = (value, decimals = 0) => {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }).format(value || 0);
  };

  const formatPercentage = (value, decimals = 2) => {
    return `${formatNumber((value || 0) * 100, decimals)}%`;
  };

  return (
    <div className={`fixed inset-0 z-50 overflow-y-auto ${isOpen ? 'block' : 'hidden'}`}>
      <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
        <div className="fixed inset-0 transition-opacity" aria-hidden="true">
          <div className="absolute inset-0 bg-black/50"></div>
        </div>
        <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
        <div className="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-6xl sm:w-full">
          <div className="bg-white dark:bg-gray-800 px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="sm:flex sm:items-start">
              <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left w-full">
                <h3 className="text-xl leading-6 font-semibold text-gray-900 dark:text-gray-100" id="modal-title">
                  Performance Trends: {modalData.entityInfo.name || data.name}
                </h3>
                <button onClick={onClose} className="absolute top-4 right-4 p-1 text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300">
                  <X size={20} />
                </button>
                <div className="mt-4">
                  {isLoading ? (
                    <div className="flex justify-center items-center h-64">
                      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
                    </div>
                  ) : error ? (
                    <div className="text-center text-red-500 p-4">{error}</div>
                  ) : (
                    <div className="space-y-8">
                      {/* Summary Stats */}
                      {modalData.summaryStats && Object.keys(modalData.summaryStats).length > 0 && (
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
                          <div className="text-center">
                            <div className="text-2xl font-bold text-blue-600">{formatCurrency(modalData.summaryStats.total_spend)}</div>
                            <div className="text-sm text-gray-600 dark:text-gray-400">Total Spend</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-green-600">{formatCurrency(modalData.summaryStats.total_revenue)}</div>
                            <div className="text-sm text-gray-600 dark:text-gray-400">Total Revenue</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600">{formatNumber(modalData.summaryStats.avg_roas, 2)}</div>
                            <div className="text-sm text-gray-600 dark:text-gray-400">Avg ROAS</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-orange-600">{formatPercentage(modalData.summaryStats.avg_ctr)}</div>
                            <div className="text-sm text-gray-600 dark:text-gray-400">Avg CTR</div>
                          </div>
                        </div>
                      )}

                      {/* Trend Chart */}
                      <div>
                        <h3 className="text-lg font-medium mb-2 dark:text-gray-100">Daily Performance Trends</h3>
                        <ResponsiveContainer width="100%" height={400}>
                          <LineChart data={modalData.trendData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#4A5568" />
                            <XAxis 
                              dataKey="date" 
                              stroke="#A0AEC0"
                              tick={{ fontSize: 12 }}
                              tickFormatter={(value) => {
                                const date = new Date(value);
                                return `${date.getMonth() + 1}/${date.getDate()}`;
                              }}
                            />
                            <YAxis yAxisId="left" label={{ value: 'USD / Count', angle: -90, position: 'insideLeft', fill: '#A0AEC0' }} stroke="#A0AEC0" />
                            <YAxis yAxisId="right" orientation="right" label={{ value: 'ROAS', angle: 90, position: 'insideRight', fill: '#A0AEC0' }} stroke="#A0AEC0" />
                            <Tooltip 
                              contentStyle={{ backgroundColor: '#2D3748', borderColor: '#4A5568' }} 
                              itemStyle={{ color: '#E2E8F0' }}
                              labelFormatter={(value) => `Date: ${value}`}
                              formatter={(value, name) => {
                                if (name === 'Spend (USD)' || name === 'Revenue (USD)') {
                                  return [formatCurrency(value), name];
                                } else if (name === 'ROAS') {
                                  return [formatNumber(value, 2), name];
                                } else if (name === 'CTR') {
                                  return [formatPercentage(value), name];
                                }
                                return [formatNumber(value), name];
                              }}
                            />
                            <Legend wrapperStyle={{ color: '#A0AEC0' }} />
                            <Line yAxisId="left" type="monotone" dataKey="spend" stroke="#8884d8" name="Spend (USD)" strokeWidth={2} />
                            <Line yAxisId="left" type="monotone" dataKey="revenue_usd" stroke="#82ca9d" name="Revenue (USD)" strokeWidth={2} />
                            <Line yAxisId="left" type="monotone" dataKey="total_trials_started" stroke="#ffc658" name="Trials Started" strokeWidth={2} />
                            <Line yAxisId="right" type="monotone" dataKey="roas" stroke="#ff7300" name="ROAS" strokeWidth={2} />
                            <Line yAxisId="right" type="monotone" dataKey="ctr" stroke="#8dd1e1" name="CTR" strokeWidth={2} />
                          </LineChart>
                        </ResponsiveContainer>
                      </div>

                      {/* Additional Metrics Charts */}
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                        <div>
                          <h3 className="text-lg font-medium mb-2 dark:text-gray-100">Impressions vs Clicks</h3>
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={modalData.trendData}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#4A5568" />
                              <XAxis 
                                dataKey="date" 
                                stroke="#A0AEC0"
                                tick={{ fontSize: 10 }}
                                tickFormatter={(value) => {
                                  const date = new Date(value);
                                  return `${date.getMonth() + 1}/${date.getDate()}`;
                                }}
                              />
                              <YAxis stroke="#A0AEC0" />
                              <Tooltip 
                                contentStyle={{ backgroundColor: '#2D3748', borderColor: '#4A5568' }} 
                                itemStyle={{ color: '#E2E8F0' }}
                                formatter={(value, name) => [formatNumber(value), name]}
                              />
                              <Bar dataKey="impressions" fill="#82ca9d" name="Impressions" />
                              <Bar dataKey="clicks" fill="#8884d8" name="Clicks" />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                        <div>
                          <h3 className="text-lg font-medium mb-2 dark:text-gray-100">Cost Metrics</h3>
                          <ResponsiveContainer width="100%" height={250}>
                            <LineChart data={modalData.trendData}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#4A5568" />
                              <XAxis 
                                dataKey="date" 
                                stroke="#A0AEC0"
                                tick={{ fontSize: 10 }}
                                tickFormatter={(value) => {
                                  const date = new Date(value);
                                  return `${date.getMonth() + 1}/${date.getDate()}`;
                                }}
                              />
                              <YAxis stroke="#A0AEC0" />
                              <Tooltip 
                                contentStyle={{ backgroundColor: '#2D3748', borderColor: '#4A5568' }} 
                                itemStyle={{ color: '#E2E8F0' }}
                                formatter={(value, name) => {
                                  if (name === 'CPC' || name === 'CPM') {
                                    return [formatCurrency(value), name];
                                  }
                                  return [formatNumber(value, 2), name];
                                }}
                              />
                              <Line type="monotone" dataKey="cpc" stroke="#ff7300" name="CPC" strokeWidth={2} />
                              <Line type="monotone" dataKey="cpm" stroke="#8dd1e1" name="CPM" strokeWidth={2} />
                            </LineChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
            <button
              type="button"
              onClick={onClose}
              className="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-800 text-base font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}; 