import React, { useState, useEffect } from 'react';
import { valueEstimationDebugApi } from '../../services/debugApi';

const ValueEstimationDebugPage = () => {
  const [overviewData, setOverviewData] = useState(null);
  const [examplesData, setExamplesData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshingStatus, setRefreshingStatus] = useState(null);

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const [overviewResponse, examplesResponse] = await Promise.all([
        valueEstimationDebugApi.loadOverview(),
        valueEstimationDebugApi.loadExamples()
      ]);
      
      if (overviewResponse.success) {
        setOverviewData(overviewResponse);
      }
      
      if (examplesResponse.success) {
        setExamplesData(examplesResponse);
      }
      
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const refreshStatusExamples = async (statusType, statusValue) => {
    const refreshKey = `${statusType}-${statusValue}`;
    setRefreshingStatus(refreshKey);
    
    try {
      console.log('Refreshing status:', statusType, statusValue);
      const response = await valueEstimationDebugApi.refreshStatus(statusType, statusValue);
      console.log('Refresh response:', response);
      
      if (response.success) {
        console.log('Updating examples data with:', response.examples);
        // Update the specific status examples
        setExamplesData(prev => {
          const newData = {
            ...prev,
            [`${statusType}_examples`]: {
              ...prev[`${statusType}_examples`],
              [statusValue]: response.examples
            }
          };
          console.log('New examples data:', newData);
          return newData;
        });
      } else {
        console.error('Refresh failed:', response);
      }
    } catch (err) {
      console.error('Error refreshing status examples:', err);
    } finally {
      setRefreshingStatus(null);
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  const StatusListItem = ({ status, count, percentage, isLast }) => (
    <div className={`flex justify-between items-center py-3 px-4 ${!isLast ? 'border-b border-gray-700' : ''} hover:bg-gray-700 transition-colors`}>
      <div className="flex flex-col">
        <span className="font-medium text-blue-300">{status.replace(/_/g, ' ')}</span>
      </div>
      <div className="text-right">
        <div className="text-lg font-bold text-white">{formatNumber(count)}</div>
        <div className="text-sm text-gray-400">{percentage}% of total</div>
      </div>
    </div>
  );

  const ValueDistributionItem = ({ range, count, percentage, isLast }) => (
    <div className={`flex justify-between items-center py-3 px-4 ${!isLast ? 'border-b border-gray-700' : ''} hover:bg-gray-700 transition-colors`}>
      <div className="flex flex-col">
        <span className="font-medium text-green-300">{range}</span>
      </div>
      <div className="text-right">
        <div className="text-lg font-bold text-white">{formatNumber(count)}</div>
        <div className="text-sm text-gray-400">{percentage}% of total</div>
      </div>
    </div>
  );

  const ExampleCard = ({ user, index }) => (
    <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
      <div className="grid grid-cols-2 gap-2 text-sm">
        <div>
          <span className="text-gray-400">User ID:</span>
          <span className="text-white ml-2 font-mono">{user.distinct_id.substring(0, 8)}...</span>
        </div>
        <div>
          <span className="text-gray-400">Value:</span>
          <span className="text-green-300 ml-2 font-bold">{formatCurrency(user.current_value)}</span>
        </div>
        <div>
          <span className="text-gray-400">Product:</span>
          <span className="text-white ml-2">{user.product_id}</span>
        </div>
        <div>
          <span className="text-gray-400">Country:</span>
          <span className="text-white ml-2">{user.country || 'N/A'}</span>
        </div>
        <div>
          <span className="text-gray-400">Region:</span>
          <span className="text-white ml-2">{user.region || 'N/A'}</span>
        </div>
        <div>
          <span className="text-gray-400">Start:</span>
          <span className="text-white ml-2">{user.credited_date || 'N/A'}</span>
        </div>
        <div>
          <span className="text-gray-400">Days:</span>
          <span className="text-white ml-2">{user.days_since_start || 'N/A'}</span>
        </div>
        <div>
          <span className="text-gray-400">Accuracy:</span>
          <span className="text-yellow-300 ml-2 capitalize">{user.accuracy_score || 'unknown'}</span>
        </div>
        <div>
          <span className="text-gray-400">Price Bucket:</span>
          <span className="text-green-300 ml-2 font-semibold">{formatCurrency(user.price_bucket)}</span>
        </div>
        <div>
          <span className="text-gray-400">Trial Conv:</span>
          <span className="text-red-300 ml-2 font-semibold">{(user.trial_conversion_rate * 100).toFixed(1)}%</span>
        </div>
        <div>
          <span className="text-gray-400">Trial Refund:</span>
          <span className="text-red-300 ml-2 font-semibold">{(user.trial_refund_rate * 100).toFixed(1)}%</span>
        </div>
        <div>
          <span className="text-gray-400">Purchase Refund:</span>
          <span className="text-red-300 ml-2 font-semibold">{(user.purchase_refund_rate * 100).toFixed(1)}%</span>
        </div>
      </div>
    </div>
  );

  if (loading) {
    return (
      <div className="p-6 bg-gray-900 min-h-screen">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-700 rounded w-1/4 mb-6"></div>
          <div className="space-y-4">
            <div className="h-32 bg-gray-800 rounded"></div>
            <div className="h-64 bg-gray-800 rounded"></div>
            <div className="h-64 bg-gray-800 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 bg-gray-900 min-h-screen">
        <div className="bg-red-900 border border-red-700 rounded-lg p-4">
          <h2 className="text-red-300 font-bold mb-2">Error Loading Data</h2>
          <p className="text-red-200">{error}</p>
          <button 
            onClick={loadData}
            className="mt-4 bg-red-700 hover:bg-red-600 text-white px-4 py-2 rounded transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-white">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold text-white">💰 Value Estimation Debug</h1>
        <button 
          onClick={loadData}
          className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-lg transition-colors"
        >
          Refresh Data
        </button>
      </div>

      {/* Overview Statistics */}
      {overviewData && (
        <div className="mb-8">
          <h2 className="text-xl font-bold mb-4 text-blue-300">📊 Overview Statistics</h2>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-8">
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-white">{formatNumber(overviewData.statistics.total_user_product_pairs)}</div>
              <div className="text-sm text-gray-400">Total Pairs</div>
            </div>
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-green-400">{formatNumber(overviewData.statistics.processed_pairs)}</div>
              <div className="text-sm text-gray-400">Processed</div>
            </div>
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-blue-400">{formatNumber(overviewData.statistics.pairs_with_values)}</div>
              <div className="text-sm text-gray-400">With Values</div>
            </div>
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-yellow-400">{formatCurrency(overviewData.statistics.avg_value)}</div>
              <div className="text-sm text-gray-400">Avg Value</div>
            </div>
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-purple-400">{formatCurrency(overviewData.statistics.total_estimated_value)}</div>
              <div className="text-sm text-gray-400">Total Value</div>
            </div>
            <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
              <div className="text-2xl font-bold text-orange-400">{overviewData.statistics.processing_rate}%</div>
              <div className="text-sm text-gray-400">Processing Rate</div>
            </div>
          </div>

          {/* Current Status Breakdown */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
            <div className="bg-gray-800 rounded-lg border border-gray-700">
              <div className="p-4 border-b border-gray-700">
                <h3 className="text-lg font-bold text-blue-300">🔄 Current Status Breakdown</h3>
                <p className="text-sm text-gray-400">Chronological user lifecycle progression</p>
              </div>
              <div className="max-h-96 overflow-y-auto">
                {overviewData.status_breakdown.map((item, index) => (
                  <StatusListItem
                    key={item.status}
                    status={item.status}
                    count={item.count}
                    percentage={item.percentage}
                    isLast={index === overviewData.status_breakdown.length - 1}
                  />
                ))}
              </div>
            </div>

            {/* Value Status Breakdown */}
            <div className="bg-gray-800 rounded-lg border border-gray-700">
              <div className="p-4 border-b border-gray-700">
                <h3 className="text-lg font-bold text-green-300">💎 Value Status Breakdown</h3>
                <p className="text-sm text-gray-400">Value calculation stages</p>
              </div>
              <div className="max-h-96 overflow-y-auto">
                {overviewData.value_status_breakdown.map((item, index) => (
                  <StatusListItem
                    key={item.status}
                    status={item.status}
                    count={item.count}
                    percentage={item.percentage}
                    isLast={index === overviewData.value_status_breakdown.length - 1}
                  />
                ))}
              </div>
            </div>

            {/* Current Value Distribution */}
            <div className="bg-gray-800 rounded-lg border border-gray-700">
              <div className="p-4 border-b border-gray-700">
                <h3 className="text-lg font-bold text-purple-300">💰 Current Value Distribution</h3>
                <p className="text-sm text-gray-400">Value ranges in $20 increments up to $120+</p>
              </div>
              <div className="max-h-96 overflow-y-auto">
                {overviewData.value_distribution.map((item, index) => (
                  <ValueDistributionItem
                    key={item.range}
                    range={item.range}
                    count={item.count}
                    percentage={item.percentage}
                    isLast={index === overviewData.value_distribution.length - 1}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Examples Sections */}
      {examplesData && (
        <div className="space-y-8">
          {/* Current Status Examples */}
          <div>
            <h2 className="text-xl font-bold mb-4 text-blue-300">📋 Current Status Examples</h2>
            <p className="text-gray-400 mb-4">Sample users for each current status</p>
            <div className="space-y-6">
              {Object.entries(examplesData.current_status_examples || {}).map(([status, examples]) => (
                <div key={status} className="bg-gray-800 rounded-lg border border-gray-700">
                  <div className="p-4 border-b border-gray-700 flex justify-between items-center">
                    <h3 className="text-lg font-medium text-blue-300">
                      {status.replace(/_/g, ' ')} ({examples.length} examples)
                    </h3>
                    <button
                      onClick={() => refreshStatusExamples('current_status', status)}
                      disabled={refreshingStatus === `current_status-${status}`}
                      className="bg-blue-600 hover:bg-blue-500 disabled:bg-blue-800 text-white px-3 py-1 rounded text-sm transition-colors"
                    >
                      {refreshingStatus === `current_status-${status}` ? '⟳' : '↻'} Refresh
                    </button>
                  </div>
                  {examples.length > 0 ? (
                    <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-4">
                      {examples.map((user, index) => (
                        <ExampleCard key={`${user.distinct_id}-${index}`} user={user} index={index} />
                      ))}
                    </div>
                  ) : (
                    <div className="p-4">
                      <div className="bg-gray-700 border border-gray-600 rounded-lg p-4 text-center">
                        <div className="text-gray-400 text-lg mb-2">⚠️ No Examples Available</div>
                        <div className="text-gray-500 text-sm">
                          This status exists in the system but currently has no user examples.
                        </div>
                        <div className="text-gray-600 text-xs mt-2">
                          Try refreshing to see if new examples become available.
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Value Status Examples */}
          <div>
            <h2 className="text-xl font-bold mb-4 text-green-300">🎯 Value Status Examples</h2>
            <p className="text-gray-400 mb-4">Sample users for each value status</p>
            <div className="space-y-6">
              {Object.entries(examplesData.value_status_examples || {}).map(([status, examples]) => (
                <div key={status} className="bg-gray-800 rounded-lg border border-gray-700">
                  <div className="p-4 border-b border-gray-700 flex justify-between items-center">
                    <h3 className="text-lg font-medium text-green-300">
                      {status.replace(/_/g, ' ')} ({examples.length} examples)
                    </h3>
                    <button
                      onClick={() => refreshStatusExamples('value_status', status)}
                      disabled={refreshingStatus === `value_status-${status}`}
                      className="bg-green-600 hover:bg-green-500 disabled:bg-green-800 text-white px-3 py-1 rounded text-sm transition-colors"
                    >
                      {refreshingStatus === `value_status-${status}` ? '⟳' : '↻'} Refresh
                    </button>
                  </div>
                  {examples.length > 0 ? (
                    <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-4">
                      {examples.map((user, index) => (
                        <ExampleCard key={`${user.distinct_id}-${index}`} user={user} index={index} />
                      ))}
                    </div>
                  ) : (
                    <div className="p-4">
                      <div className="bg-gray-700 border border-gray-600 rounded-lg p-4 text-center">
                        <div className="text-gray-400 text-lg mb-2">⚠️ No Examples Available</div>
                        <div className="text-gray-500 text-sm">
                          This status exists in the system but currently has no user examples.
                        </div>
                        <div className="text-gray-600 text-xs mt-2">
                          Try refreshing to see if new examples become available.
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ValueEstimationDebugPage; 