import React, { useState, useEffect } from 'react';
import { api } from '../../services/api';

const HistoricalDataViewer = () => {
  const [configurations, setConfigurations] = useState([]);
  const [configData, setConfigData] = useState({});
  const [loading, setLoading] = useState(true);
  const [expandedConfigs, setExpandedConfigs] = useState(new Set());
  const [deletingConfig, setDeletingConfig] = useState(null);

  // Load all configurations and their data on component mount
  useEffect(() => {
    loadAllData();
  }, []);

  const loadAllData = async () => {
    setLoading(true);
    try {
      // Get all configurations
      const configs = await api.getHistoricalConfigurations();
      setConfigurations(configs);

      // Load data for each configuration
      const dataPromises = configs.map(async (config) => {
        if (config.day_count > 0) {
          try {
            const data = await api.exportHistoricalData({
              start_date: config.earliest_date,
              end_date: config.latest_date,
              fields: config.fields,
              breakdowns: config.breakdowns
            });
            return { configHash: config.config_hash, data };
          } catch (error) {
            console.error(`Error loading data for config ${config.config_hash}:`, error);
            return { configHash: config.config_hash, data: null };
          }
        }
        return { configHash: config.config_hash, data: null };
      });

      const results = await Promise.all(dataPromises);
      const dataMap = {};
      results.forEach(({ configHash, data }) => {
        dataMap[configHash] = data;
      });
      
      setConfigData(dataMap);
    } catch (error) {
      console.error('Error loading configurations:', error);
    } finally {
      setLoading(false);
    }
  };

  const deleteConfiguration = async (configHash) => {
    if (!window.confirm('Are you sure you want to delete this entire dataset? This action cannot be undone.')) {
      return;
    }

    setDeletingConfig(configHash);
    try {
      await api.deleteHistoricalConfiguration(configHash);
      // Reload data to reflect the deletion
      await loadAllData();
    } catch (error) {
      console.error('Error deleting configuration:', error);
      alert('Failed to delete configuration. Please try again.');
    } finally {
      setDeletingConfig(null);
    }
  };

  const toggleConfigExpansion = (configHash) => {
    const newExpanded = new Set(expandedConfigs);
    if (newExpanded.has(configHash)) {
      newExpanded.delete(configHash);
    } else {
      newExpanded.add(configHash);
    }
    setExpandedConfigs(newExpanded);
  };

  const processDataForTable = (exportData) => {
    if (!exportData || !exportData.data || exportData.data.length === 0) {
      return null;
    }

    const fields = exportData.config.fields.split(',').map(f => f.trim());
    const hasBreakdowns = exportData.config.breakdowns && exportData.config.breakdowns.trim();
    
    // Get all dates (should now be unique since backend duplicates are fixed)
    const dates = exportData.data.map(d => d.date).sort();
    
    // Process data by date
    const dateData = {};
    const businessMetrics = {};
    
    exportData.data.forEach(dayData => {
      const date = dayData.date;
      
      // Extract business metrics if available
      if (dayData.business_metrics) {
        businessMetrics[date] = dayData.business_metrics;
      }
      
      // Extract metaData from the API response structure
      let metaData = [];
      if (dayData.data) {
        // Historical export API structure: dayData.data.data.data contains the actual records array
        if (dayData.data.data && Array.isArray(dayData.data.data.data)) {
          metaData = dayData.data.data.data;
        } else if (Array.isArray(dayData.data.data)) {
          metaData = dayData.data.data;
        } else if (Array.isArray(dayData.data)) {
          metaData = dayData.data;
        } else {
          console.warn('Unexpected data structure for date:', date, dayData.data);
          metaData = [];
        }
      }
      
      dateData[date] = processMetricsForDate(metaData, fields, hasBreakdowns);
    });

    return {
      dates,
      dateData,
      fields,
      hasBreakdowns,
      config: exportData.config,
      businessMetrics
    };
  };

  const processMetricsForDate = (records, fields, hasBreakdowns) => {
    // Ensure records is always an array
    if (!records) {
      return {};
    }
    
    if (!Array.isArray(records)) {
      console.warn('processMetricsForDate: records is not an array:', typeof records, records);
      return {};
    }
    
    if (records.length === 0) {
      return {};
    }

    const result = {};
    
    fields.forEach(field => {
      if (isNumericField(field)) {
        // For numeric fields, sum all values
        const total = records.reduce((sum, record) => {
          const value = parseFloat(record[field]) || 0;
          return sum + value;
        }, 0);
        result[field] = total;
      } else if (isCountableField(field)) {
        // For text fields, count unique values
        const uniqueValues = new Set();
        records.forEach(record => {
          if (record[field]) {
            uniqueValues.add(record[field]);
          }
        });
        result[field] = uniqueValues.size;
      } else {
        // For other fields, just indicate presence
        result[field] = records.length > 0 ? 'Data' : 'No Data';
      }
    });

    // If there are breakdowns, also process breakdown-specific data
    if (hasBreakdowns) {
      result._breakdownData = processBreakdownData(records, fields);
    }

    return result;
  };

  const processBreakdownData = (records, fields) => {
    const breakdownGroups = {};
    
    // Ensure records is an array
    if (!Array.isArray(records)) {
      console.warn('processBreakdownData: records is not an array:', typeof records, records);
      return breakdownGroups;
    }
    
    records.forEach(record => {
      // Get breakdown values (all keys that aren't in fields)
      const breakdownKey = Object.keys(record)
        .filter(key => !fields.includes(key) && !['date_start', 'date_stop'].includes(key))
        .map(key => `${key}:${record[key]}`)
        .join(' | ');
      
      if (!breakdownGroups[breakdownKey]) {
        breakdownGroups[breakdownKey] = {};
      }
      
      fields.forEach(field => {
        if (isNumericField(field)) {
          const value = parseFloat(record[field]) || 0;
          breakdownGroups[breakdownKey][field] = (breakdownGroups[breakdownKey][field] || 0) + value;
        }
      });
    });
    
    return breakdownGroups;
  };

  const isNumericField = (field) => {
    const numericFields = [
      'impressions', 'clicks', 'spend', 'reach', 'frequency', 'cpc', 'cpm', 'cpp',
      'unique_clicks', 'unique_ctr', 'inline_link_clicks', 'outbound_clicks',
      'estimated_ad_recallers', 'estimated_ad_recall_rate'
    ];
    return numericFields.includes(field) || field.includes('action') || field.includes('conversion');
  };

  const isCountableField = (field) => {
    const countableFields = [
      'ad_name', 'campaign_name', 'adset_name', 'ad_id', 'campaign_id', 'adset_id'
    ];
    return countableFields.includes(field);
  };

  const formatValue = (field, value) => {
    if (typeof value === 'number') {
      if (field === 'spend' || field.includes('cost')) {
        return `$${value.toFixed(2)}`;
      } else if (field.includes('rate') || field.includes('ctr')) {
        return `${(value * 100).toFixed(2)}%`;
      } else if (Number.isInteger(value)) {
        return value.toLocaleString();
      } else {
        return value.toFixed(2);
      }
    } else if (typeof value === 'number' && isCountableField(field)) {
      return `${value} ${field.includes('campaign') ? 'campaigns' : field.includes('ad') ? 'ads' : 'items'}`;
    }
    return value;
  };

  const renderDataTable = (config, tableData) => {
    if (!tableData) {
      return (
        <div className="text-gray-500 text-center py-4">
          No data available for this configuration
        </div>
      );
    }

    const { dates, dateData, fields, hasBreakdowns, businessMetrics } = tableData;
    const configHash = config.config_hash;

    return (
      <div className="space-y-6">
        {/* Business Metrics Section */}
        {businessMetrics && Object.keys(businessMetrics).length > 0 && (
          <div>
            <h4 className="text-md font-medium mb-3 text-blue-800">💰 Business Metrics Summary</h4>
            <div className="overflow-x-auto">
              <table className="min-w-full border border-gray-200 bg-blue-50">
                <thead className="bg-blue-100">
                  <tr>
                    <th className="px-4 py-2 text-left text-sm font-medium text-blue-800 border-r">
                      Business Concept
                    </th>
                    {dates.map(date => (
                      <th key={`${configHash}-bm-header-${date}`} className="px-3 py-2 text-center text-sm font-medium text-blue-800 border-r">
                        {date}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white">
                  {/* Get concept names from the first date that has data */}
                  {(() => {
                    const firstDateWithData = dates.find(date => businessMetrics[date] && Object.keys(businessMetrics[date]).length > 0);
                    if (!firstDateWithData) return null;
                    
                    const conceptNames = Object.keys(businessMetrics[firstDateWithData]);
                    return conceptNames.map((conceptName, conceptIndex) => (
                      <tr key={`${configHash}-bm-${conceptName}`} className={conceptIndex % 2 === 0 ? 'bg-blue-25' : 'bg-white'}>
                        <td className="px-4 py-2 text-sm font-medium text-blue-900 border-r capitalize">
                          {conceptName === 'trial_started' ? 'Trial Started' : 
                           conceptName === 'initial_purchase' ? 'Initial Purchase' : 
                           conceptName.replace(/_/g, ' ')}
                        </td>
                        {dates.map(date => (
                          <td key={`${configHash}-bm-${conceptName}-${date}`} className="px-3 py-2 text-sm text-blue-800 text-center border-r font-medium">
                            {businessMetrics[date] && businessMetrics[date][conceptName]?.count !== undefined 
                              ? businessMetrics[date][conceptName].count
                              : '-'
                            }
                          </td>
                        ))}
                      </tr>
                    ));
                  })()}
                </tbody>
              </table>
            </div>
          </div>
        )}
        
        {/* Raw Data Table */}
        <div>
          <h4 className="text-md font-medium mb-3 text-gray-800">📊 Raw Data Metrics</h4>
          <div className="overflow-x-auto">
            <table className="min-w-full border border-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-700 border-r">
                    Metric
                  </th>
                  {dates.map(date => (
                    <th key={`${configHash}-rm-header-${date}`} className="px-3 py-2 text-center text-sm font-medium text-gray-700 border-r">
                      {date}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white">
                {fields.map((field, fieldIndex) => (
                  <tr key={`${configHash}-rm-${field}`} className={fieldIndex % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                    <td className="px-4 py-2 text-sm font-medium text-gray-900 border-r">
                      {field}
                    </td>
                    {dates.map(date => (
                      <td key={`${configHash}-rm-${field}-${date}`} className="px-3 py-2 text-sm text-gray-700 text-center border-r">
                        {dateData[date] && dateData[date][field] !== undefined 
                          ? formatValue(field, dateData[date][field])
                          : '-'
                        }
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Breakdown data if available */}
        {hasBreakdowns && Object.keys(dateData).some(date => dateData[date]._breakdownData) && (
          <div className="mt-6">
            <h5 className="text-sm font-medium text-gray-700 mb-3">Breakdown Data</h5>
            {dates.map(date => {
              const breakdownData = dateData[date]._breakdownData;
              if (!breakdownData || Object.keys(breakdownData).length === 0) return null;
              
              return (
                <div key={`${configHash}-bd-${date}`} className="mb-4">
                  <h6 className="text-xs font-medium text-gray-600 mb-2">{date}</h6>
                  <div className="overflow-x-auto">
                    <table className="min-w-full border border-gray-200 text-xs">
                      <thead className="bg-gray-100">
                        <tr>
                          <th className="px-2 py-1 text-left text-gray-700 border-r">Breakdown</th>
                          {fields.filter(isNumericField).map(field => (
                            <th key={`${configHash}-bd-${date}-header-${field}`} className="px-2 py-1 text-center text-gray-700 border-r">
                              {field}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(breakdownData).map(([breakdownKey, values]) => (
                          <tr key={`${configHash}-bd-${date}-${breakdownKey}`} className="bg-white">
                            <td className="px-2 py-1 text-gray-900 border-r">{breakdownKey}</td>
                            {fields.filter(isNumericField).map(field => (
                              <td key={`${configHash}-bd-${date}-${breakdownKey}-${field}`} className="px-2 py-1 text-center text-gray-700 border-r">
                                {formatValue(field, values[field] || 0)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mr-3"></div>
        <span>Loading historical data...</span>
      </div>
    );
  }

  if (configurations.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <p>No historical data configurations found.</p>
        <p className="text-sm mt-2">Start a historical collection job to see data here.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-medium">Historical Data Overview</h3>
        <button 
          onClick={loadAllData}
          className="text-sm text-blue-600 hover:text-blue-800"
        >
          Refresh Data
        </button>
      </div>

      {configurations.map(config => {
        const isExpanded = expandedConfigs.has(config.config_hash);
        const data = configData[config.config_hash];
        const tableData = data ? processDataForTable(data) : null;

        return (
          <div key={config.config_hash} className="border border-gray-200 rounded-lg">
            <div 
              className="p-4 bg-gray-50 cursor-pointer hover:bg-gray-100"
              onClick={() => toggleConfigExpansion(config.config_hash)}
            >
              <div className="flex justify-between items-center">
                <div>
                  <div className="font-medium text-gray-900">
                    Fields: {config.fields}
                  </div>
                  {config.breakdowns && (
                    <div className="text-sm text-gray-600">
                      Breakdowns: {config.breakdowns}
                    </div>
                  )}
                  <div className="text-sm text-gray-500 mt-1">
                    {config.day_count} days • {config.earliest_date} to {config.latest_date}
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      deleteConfiguration(config.config_hash);
                    }}
                    disabled={deletingConfig === config.config_hash}
                    className="px-2 py-1 text-xs bg-red-500 text-white rounded hover:bg-red-600 disabled:bg-gray-400"
                    title="Delete this entire dataset"
                  >
                    {deletingConfig === config.config_hash ? 'Deleting...' : 'Delete'}
                  </button>
                  <div className="text-gray-400">
                    {isExpanded ? '−' : '+'}
                  </div>
                </div>
              </div>
            </div>

            {isExpanded && (
              <div className="p-4 border-t">
                {renderDataTable(config, tableData)}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

export default HistoricalDataViewer; 