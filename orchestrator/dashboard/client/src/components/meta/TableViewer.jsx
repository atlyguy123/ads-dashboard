import React, { useState, useEffect } from 'react';
import { api } from '../../services/api';

const TableViewer = () => {
  const [tablesOverview, setTablesOverview] = useState(null);
  const [selectedTable, setSelectedTable] = useState('ad_performance_daily');
  const [aggregatedData, setAggregatedData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showAllTables, setShowAllTables] = useState(false);
  
  // Date range update states
  const [startDate, setStartDate] = useState('2025-04-01');
  const [endDate, setEndDate] = useState(() => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  });
  const [isUpdating, setIsUpdating] = useState(false);
  const [updateError, setUpdateError] = useState(null);
  const [updateSuccess, setUpdateSuccess] = useState(null);

  // Delete date states  
  const [deleteError, setDeleteError] = useState(null);
  const [deleteSuccess, setDeleteSuccess] = useState(null);

  // Bulk selection states
  const [selectedDates, setSelectedDates] = useState(new Set());
  const [isBulkDeleting, setIsBulkDeleting] = useState(false);

  // Load tables overview and auto-load default table on component mount
  useEffect(() => {
    loadTablesOverview();
    loadAggregatedData('ad_performance_daily');
  }, []);

  const loadTablesOverview = async () => {
    try {
      const overview = await api.getTablesOverview();
      setTablesOverview(overview);
    } catch (error) {
      console.error('Error loading tables overview:', error);
      setError(`Failed to load tables overview: ${error.message}`);
    }
  };

  const getBreakdownTypeForTable = (tableName) => {
    const mapping = {
      'ad_performance_daily': null,
      'ad_performance_daily_country': 'country',
      'ad_performance_daily_region': 'region',
      'ad_performance_daily_device': 'device'
    };
    return mapping[tableName] || null;
  };

  const loadAggregatedData = async (tableName) => {
    setLoading(true);
    setError(null);
    // Clear selections when switching tables
    setSelectedDates(new Set());
    try {
      const data = await api.getTableAggregatedData(tableName);
      setAggregatedData(data);
      setSelectedTable(tableName);
    } catch (error) {
      console.error(`Error loading aggregated data for ${tableName}:`, error);
      setError(`Failed to load data: ${error.message}`);
      setAggregatedData(null);
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateTable = async () => {
    if (!selectedTable || selectedTable === 'composite_validation') return;
    
    // Input validation
    if (!startDate || !endDate) {
      setUpdateError('Both start date and end date are required');
      return;
    }
    
    if (startDate > endDate) {
      setUpdateError('Start date must be before or equal to end date');
      return;
    }
    
    const today = new Date().toISOString().split('T')[0];
    if (endDate > today) {
      setUpdateError('End date cannot be in the future');
      return;
    }
    
    setIsUpdating(true);
    setUpdateError(null);
    setUpdateSuccess(null);
    
    try {
      const breakdownType = getBreakdownTypeForTable(selectedTable);
      await api.updateMetaTable({
        table_name: selectedTable,
        breakdown_type: breakdownType,
        start_date: startDate,
        end_date: endDate,
        skip_existing: true
      });
      
      setUpdateSuccess(`Successfully filled gaps in ${selectedTable} from ${startDate} to ${endDate}`);
      
      // Refresh table data after successful update
      setTimeout(() => {
        loadAggregatedData(selectedTable);
        setUpdateSuccess(null);
      }, 3000);
      
    } catch (error) {
      console.error('Error updating table:', error);
      setUpdateError(`Failed to update table: ${error.message}`);
         } finally {
       setIsUpdating(false);
     }
   };

  // Clear selections when table changes
  const clearSelection = () => {
    setSelectedDates(new Set());
  };

  // Handle date checkbox selection
  const handleDateSelection = (date, checked) => {
    const newSelectedDates = new Set(selectedDates);
    if (checked) {
      newSelectedDates.add(date);
    } else {
      newSelectedDates.delete(date);
    }
    setSelectedDates(newSelectedDates);
  };

  // Handle select all / deselect all
  const handleSelectAll = (checked) => {
    if (checked) {
      setSelectedDates(new Set(aggregatedData?.dates || []));
    } else {
      setSelectedDates(new Set());
    }
  };



  // Bulk delete functionality
  const handleBulkDelete = async () => {
    if (selectedTable === 'composite_validation' || selectedDates.size === 0) return;
    
    const dateList = Array.from(selectedDates).sort();
    const confirmation = window.confirm(
      `Are you sure you want to delete all data for ${selectedDates.size} selected dates from ${selectedTable}?\n\nDates: ${dateList.join(', ')}\n\nThis action cannot be undone.`
    );
    
    if (!confirmation) return;

    setIsBulkDeleting(true);
    setDeleteError(null);
    setDeleteSuccess(null);

    let successCount = 0;
    let errorCount = 0;
    let totalRowsDeleted = 0;
    const errors = [];

    try {
      // Delete dates one by one to avoid overwhelming the server
      for (const date of dateList) {
        try {
          const result = await api.deleteTableDate(selectedTable, date);
          successCount++;
          totalRowsDeleted += result.rows_deleted;
        } catch (error) {
          errorCount++;
          const errorMessage = error.response?.data?.error || error.message || 'Unknown error';
          errors.push(`${date}: ${errorMessage}`);
        }
      }

      // Show results
      if (errorCount === 0) {
        setDeleteSuccess(`Successfully deleted ${totalRowsDeleted} rows across ${successCount} dates from ${selectedTable}`);
      } else if (successCount === 0) {
        setDeleteError(`Failed to delete all ${selectedDates.size} dates:\n${errors.join('\n')}`);
      } else {
        setDeleteSuccess(`Partially successful: ${successCount} dates deleted (${totalRowsDeleted} rows), ${errorCount} failed`);
        if (errors.length > 0) {
          console.error('Bulk delete errors:', errors);
        }
      }

      // Clear selection and refresh
      setSelectedDates(new Set());
      setTimeout(() => {
        loadAggregatedData(selectedTable);
        setDeleteSuccess(null);
      }, 3000);
      
    } catch (error) {
      console.error('Error in bulk delete:', error);
      setDeleteError(`Bulk delete failed: ${error.message}`);
    } finally {
      setIsBulkDeleting(false);
    }
  };

  const formatMetricName = (metricName) => {
    // Handle breakdown count metrics
    if (metricName === 'unique_countries') return 'Countries';
    if (metricName === 'unique_devices') return 'Devices';
    if (metricName === 'unique_regions') return 'Regions';
    
    return metricName
      .replace(/_/g, ' ')
      .replace(/\b\w/g, l => l.toUpperCase())
      .replace('Unique ', '')
      .replace('Total ', '');
  };

  const formatValue = (value, metricName) => {
    if (value === null || value === undefined) return '—';
    
    if (metricName.includes('spend')) {
      return `$${value.toLocaleString()}`;
    }
    
    if (metricName.includes('unique') || metricName.includes('total')) {
      return value.toLocaleString();
    }
    
    return value.toLocaleString();
  };

  const formatDate = (dateStr) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric'
    });
  };

  const getAdPerformanceTables = () => {
    if (!tablesOverview) return [];
    return Object.entries(tablesOverview)
      .filter(([tableName, tableInfo]) => 
        tableName.startsWith('ad_performance_')
      )
      .sort(([a], [b]) => a.localeCompare(b));
  };

  const getOtherEmptyTables = () => {
    if (!tablesOverview) return [];
    return Object.entries(tablesOverview)
      .filter(([tableName, tableInfo]) => 
        !tableName.startsWith('ad_performance_') && tableInfo.row_count === 0
      )
      .sort(([a], [b]) => a.localeCompare(b));
  };

  const renderDateAnalysis = () => {
    if (!aggregatedData?.date_analysis) return null;
    
    const analysis = aggregatedData.date_analysis;
    
    return (
      <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <h4 className="font-semibold text-gray-900 mb-3">Data Overview</h4>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">{analysis.available_days}</div>
            <div className="text-sm text-gray-600">Available Days</div>
          </div>
          <div className="text-center">
            <div className="text-lg font-semibold text-gray-700">
              {analysis.date_range ? analysis.date_range.start : 'N/A'}
            </div>
            <div className="text-sm text-gray-600">First Date</div>
          </div>
          <div className="text-center">
            <div className="text-lg font-semibold text-gray-700">
              {analysis.date_range ? analysis.date_range.end : 'N/A'}
            </div>
            <div className="text-sm text-gray-600">Last Date</div>
          </div>
        </div>

        {analysis.warnings && analysis.warnings.length > 0 && (
          <div className="space-y-2">
            {analysis.warnings.map((warning, index) => (
              <div key={index} className="flex items-center space-x-2 text-amber-700 bg-amber-50 px-3 py-2 rounded">
                <span className="text-amber-500">⚠️</span>
                <span className="text-sm font-medium">{warning}</span>
              </div>
            ))}
            
            {analysis.missing_dates && analysis.missing_dates.length > 0 && (
              <details className="mt-2">
                <summary className="cursor-pointer text-sm text-gray-600 hover:text-gray-800">
                  View missing dates ({analysis.missing_dates.length})
                </summary>
                <div className="mt-2 text-xs text-gray-600 max-h-32 overflow-y-auto">
                  {analysis.missing_dates.join(', ')}
                </div>
              </details>
            )}
            
            {analysis.empty_days && analysis.empty_days.length > 0 && (
              <details className="mt-2">
                <summary className="cursor-pointer text-sm text-gray-600 hover:text-gray-800">
                  View empty days ({analysis.empty_days.length})
                </summary>
                <div className="mt-2 text-xs text-gray-600 max-h-32 overflow-y-auto">
                  {analysis.empty_days.join(', ')}
                </div>
              </details>
            )}
          </div>
        )}
      </div>
    );
  };

  if (error) {
    return (
      <div className="p-6 bg-red-50 border border-red-200 rounded-lg">
        <h3 className="text-red-800 font-medium text-lg">Error</h3>
        <p className="text-red-600 mt-2">{error}</p>
        <button 
          onClick={() => window.location.reload()}
          className="mt-4 px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
        >
          Reload Page
        </button>
      </div>
    );
  }

  const adPerformanceTables = getAdPerformanceTables();
  const otherEmptyTables = getOtherEmptyTables();

  return (
    <div className="space-y-8">
      {/* Aggregated Data View */}
      <div className="bg-white border border-gray-200 rounded-lg shadow-sm">
        <div className="px-8 py-6 border-b border-gray-200 bg-gray-50">
          <div className="flex justify-between items-center">
            <div>
              <h2 className="text-2xl font-semibold text-gray-900">
                {selectedTable.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
              </h2>
              <p className="text-gray-600 mt-1">
                Complete daily performance metrics{aggregatedData?.breakdown_dimension && ` (aggregated across ${aggregatedData.breakdown_dimension})`}
              </p>
            </div>
          </div>
        </div>

        <div className="p-8">
          {/* Date Range Update Controls - Only show for specific tables */}
          {selectedTable !== 'composite_validation' && (
            <div className="mb-6 p-4 bg-gray-50 border border-gray-200 rounded-lg">
              <h4 className="font-semibold text-gray-900 mb-3">Fill Missing Data</h4>
              <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-end">
                <div className="flex-1 min-w-0">
                  <label htmlFor="start-date" className="block text-sm font-medium text-gray-700 mb-1">
                    Start Date
                  </label>
                  <input
                    id="start-date"
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    disabled={isUpdating}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100 disabled:cursor-not-allowed"
                  />
                </div>
                <div className="flex-1 min-w-0">
                  <label htmlFor="end-date" className="block text-sm font-medium text-gray-700 mb-1">
                    End Date
                  </label>
                  <input
                    id="end-date"
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    disabled={isUpdating}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100 disabled:cursor-not-allowed"
                  />
                </div>
                <button
                  onClick={handleUpdateTable}
                  disabled={isUpdating || !startDate || !endDate || startDate > endDate}
                  className="px-6 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
                >
                  {isUpdating ? (
                    <div className="flex items-center space-x-2">
                      <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full"></div>
                      <span>Updating...</span>
                    </div>
                  ) : (
                    'Fill Gaps'
                  )}
                </button>
              </div>
              
              {/* Success/Error Messages */}
              {updateSuccess && (
                <div className="mt-3 p-3 bg-green-50 border border-green-200 rounded-md">
                  <div className="flex items-center space-x-2">
                    <span className="text-green-500">✅</span>
                    <span className="text-sm text-green-800">{updateSuccess}</span>
                  </div>
                </div>
              )}
              
                             {updateError && (
                 <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-md">
                   <div className="flex items-center space-x-2">
                     <span className="text-red-500">❌</span>
                     <span className="text-sm text-red-800">{updateError}</span>
                   </div>
                 </div>
               )}
             </div>
           )}

           {/* Delete Success/Error Messages */}
           {deleteSuccess && (
             <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-md">
               <div className="flex items-center space-x-2">
                 <span className="text-green-500">✅</span>
                 <span className="text-sm text-green-800">{deleteSuccess}</span>
               </div>
             </div>
           )}
           
           {deleteError && (
             <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md">
               <div className="flex items-center space-x-2">
                 <span className="text-red-500">❌</span>
                 <span className="text-sm text-red-800">{deleteError}</span>
               </div>
             </div>
           )}

           {/* Bulk Action Bar */}
           {selectedDates.size > 0 && selectedTable !== 'composite_validation' && (
             <div className="mb-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
               <div className="flex items-center justify-between">
                 <div className="flex items-center space-x-4">
                   <div className="text-sm font-medium text-blue-900">
                     {selectedDates.size} date{selectedDates.size !== 1 ? 's' : ''} selected
                   </div>
                   <button
                     onClick={() => setSelectedDates(new Set())}
                     className="text-sm text-blue-600 hover:text-blue-800 underline"
                   >
                     Clear selection
                   </button>
                 </div>
                 <button
                   onClick={handleBulkDelete}
                   disabled={isBulkDeleting}
                   className="px-4 py-2 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
                 >
                   {isBulkDeleting ? (
                     <div className="flex items-center space-x-2">
                       <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full"></div>
                       <span>Deleting...</span>
                     </div>
                   ) : (
                     `Delete ${selectedDates.size} date${selectedDates.size !== 1 ? 's' : ''}`
                   )}
                 </button>
               </div>
             </div>
           )}

          {aggregatedData && renderDateAnalysis()}

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mr-3"></div>
              <span className="text-gray-600">Loading {selectedTable === 'composite_validation' ? 'all' : 'table'} metrics...</span>
            </div>
          ) : aggregatedData && aggregatedData.dates && aggregatedData.dates.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="min-w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-sm font-semibold text-gray-900 border-r border-gray-200 sticky left-0 bg-gray-50 z-10">
                        <div className="flex items-center space-x-2">
                          <span>Metric</span>
                          {selectedTable !== 'composite_validation' && aggregatedData?.dates && (
                            <input
                              type="checkbox"
                              checked={selectedDates.size === aggregatedData.dates.length && aggregatedData.dates.length > 0}
                              onChange={(e) => handleSelectAll(e.target.checked)}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                              title="Select all dates"
                            />
                          )}
                        </div>
                      </th>
                      {aggregatedData.dates.map((date) => {
                        const analysis = aggregatedData.date_analysis;
                        const isMissing = analysis?.missing_dates?.includes(date);
                        const isEmpty = analysis?.empty_days?.includes(date);
                        const canSelect = selectedTable !== 'composite_validation';
                        const isSelected = selectedDates.has(date);
                        
                        return (
                          <th 
                            key={date}
                            className={`px-3 py-3 text-center text-sm font-semibold border-r border-gray-200 min-w-[100px] relative group ${
                              isSelected ? 'bg-blue-100 text-blue-900 ring-2 ring-blue-300' :
                              isMissing ? 'bg-red-100 text-red-800' : 
                              isEmpty ? 'bg-yellow-100 text-yellow-800' : 
                              'text-gray-900'
                            }`}
                          >
                            <div className="flex flex-col items-center">
                              <div>{formatDate(date)}</div>
                              <div className="text-xs font-normal mt-1 opacity-75">{date}</div>
                              {(isMissing || isEmpty) && (
                                <div className="text-xs mt-1">
                                  {isMissing ? '❌' : isEmpty ? '⚠️' : ''}
                                </div>
                              )}
                              
                              {/* Hover-revealed checkbox for date selection */}
                              {canSelect && (
                                <div className={`mt-1 transition-opacity duration-200 ${
                                  isSelected ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'
                                }`}>
                                  <input
                                    type="checkbox"
                                    checked={isSelected}
                                    onChange={(e) => handleDateSelection(date, e.target.checked)}
                                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded cursor-pointer"
                                    title={`Select ${date} for bulk deletion`}
                                  />
                                </div>
                              )}
                            </div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-100">
                    {aggregatedData.metric_names.map((metricName, index) => (
                      <tr key={metricName} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        <td className="px-6 py-2 text-sm font-medium text-gray-900 border-r border-gray-200 sticky left-0 bg-inherit z-10">
                          {formatMetricName(metricName)}
                        </td>
                        {aggregatedData.dates.map((date) => {
                          const value = aggregatedData.metrics_data[date]?.[metricName];
                          const analysis = aggregatedData.date_analysis;
                          const isEmpty = analysis?.empty_days?.includes(date);
                          
                          // Check for validation errors if this is composite validation
                          const isComposite = aggregatedData.table_name === 'composite_validation';
                          const validationError = isComposite ? aggregatedData.validation_errors?.[date]?.[metricName] : null;
                          
                          const isMissingBase = value === 'MISSING_BASE';
                          const isMissingTable = value === 'MISSING_TABLE';
                          const isMissingDate = value === 'MISSING_DATE';
                          const isZeroBreakdown = (metricName === 'unique_countries' || metricName === 'unique_devices' || metricName === 'unique_regions') && value === 0;
                          const isMissing = isMissingBase || isMissingTable || isMissingDate || isZeroBreakdown;
                          
                          // Each breakdown table is treated individually for each date
                          // Show X only if ALL breakdown tables are missing data
                          const hasMissingTables = validationError && validationError.type === 'missing';
                          // Show discrepancies for tables that have data, even if some other tables are missing
                          const hasDiscrepancy = validationError && (validationError.type === 'discrepancy' || validationError.type === 'mixed');
                          
                          // Determine color based on percentage difference
                          let discrepancyColor = 'text-red-600';
                          let bgColor = 'bg-red-50';
                          
                          if (hasDiscrepancy) {
                            const percentage = validationError.max_percentage;
                            if (percentage < 5) {
                              discrepancyColor = 'text-yellow-600';
                              bgColor = 'bg-yellow-50';
                            } else if (percentage < 10) {
                              discrepancyColor = 'text-orange-600';
                              bgColor = 'bg-orange-50';
                            } else {
                              discrepancyColor = 'text-red-600';
                              bgColor = 'bg-red-50';
                            }
                          }
                          
                          return (
                            <td 
                              key={`${metricName}-${date}`}
                              className={`px-3 py-2 text-center text-sm border-r border-gray-200 font-mono ${
                                // PRIORITY 1: Base data missing or ALL breakdown tables missing - show red X
                                isMissing || hasMissingTables ? 'bg-red-50' :
                                // PRIORITY 2: Discrepancies (some/all tables have data with differences) - show colored discrepancy
                                hasDiscrepancy ? bgColor :
                                // PRIORITY 3: Empty day (no activity across all tables) - show yellow warning
                                isEmpty ? 'bg-yellow-50 text-yellow-800' : 
                                // DEFAULT: Normal data display
                                'text-gray-700'
                              }`}
                            >
                              {/* PRIORITY 1: Base data missing */}
                              {isMissing ? (
                                <div className="text-red-600 font-bold text-lg" title="Base table missing data">✗</div>
                              ) : /* PRIORITY 2: ALL breakdown tables missing */ hasMissingTables ? (
                                <div className="text-red-600 font-bold text-lg" title={
                                  `All tables missing: ${validationError.missing_tables.join(', ')}`
                                }>✗</div>
                              ) : /* PRIORITY 3: Show discrepancies for tables that have data */ hasDiscrepancy ? (
                                <div>
                                  <div className={`${discrepancyColor} font-bold`}>{formatValue(value, metricName)}</div>
                                  <div className={`text-xs ${discrepancyColor} mt-1`}>
                                    {validationError.discrepancies.map((d, i) => (
                                      <div key={i}>{d.table}: {d.value}</div>
                                    ))}
                                    {validationError.type === 'mixed' && validationError.missing_tables && (
                                      <div className="text-red-600 mt-1">
                                        Missing: {validationError.missing_tables.join(', ')}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              ) : /* DEFAULT: Normal value display */ (
                                formatValue(value, metricName)
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : aggregatedData ? (
              <div className="flex items-center justify-center py-12 text-gray-500">
                <div className="text-center">
                  <div className="text-lg mb-2">📭</div>
                  <div className="font-medium text-gray-700 mb-2">Empty Table</div>
                  <div className="text-sm">
                    This table contains no data yet. Use the "Fill Gaps" feature above to populate it with Meta advertising data.
                  </div>
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center py-12 text-gray-500">
                <div className="text-center">
                  <div className="text-lg mb-2">📊</div>
                  <div>Select a table to view metrics</div>
                </div>
              </div>
            )}
          </div>
        </div>

      {/* Table Selector */}
      <div className="bg-white border border-gray-200 rounded-lg shadow-sm">
        <div className="px-8 py-6 border-b border-gray-200">
          <div className="flex justify-between items-center">
            <div>
              <h3 className="text-lg font-medium text-gray-900">Available Performance Tables</h3>
              <p className="text-gray-600 mt-1">Select a table to view complete aggregated daily metrics</p>
            </div>
            <button
              onClick={() => loadAggregatedData('composite_validation')}
              className={`px-6 py-2 rounded-lg font-medium transition-colors ${
                selectedTable === 'composite_validation'
                  ? 'bg-purple-600 text-white'
                  : 'bg-purple-100 text-purple-700 hover:bg-purple-200'
              }`}
            >
              All
            </button>
          </div>
        </div>
        
        <div className="p-8">
          {/* All Ad Performance Tables */}
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-6">
            {adPerformanceTables.map(([tableName, tableInfo]) => {
              const isEmpty = tableInfo.row_count === 0;
              return (
                <div 
                  key={tableName}
                  className={`p-6 border-2 rounded-lg cursor-pointer transition-all duration-200 ${
                    selectedTable === tableName 
                      ? 'border-blue-500 bg-blue-50 shadow-md' 
                      : isEmpty 
                        ? 'border-orange-200 hover:border-orange-300 hover:bg-orange-50'
                        : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                  }`}
                  onClick={() => loadAggregatedData(tableName)}
                >
                  <div className="flex justify-between items-start mb-3">
                    <h4 className="font-semibold text-gray-900 text-sm">
                      {tableName.replace('ad_performance_daily_', '').replace('ad_performance_daily', 'Base') || 'Base'}
                    </h4>
                    <span className={`px-2 py-1 text-xs font-medium rounded ${
                      isEmpty 
                        ? 'bg-orange-100 text-orange-800' 
                        : 'bg-green-100 text-green-800'
                    }`}>
                      {isEmpty ? 'Empty' : `${tableInfo.row_count.toLocaleString()} rows`}
                    </span>
                  </div>
                  <div className="text-xs text-gray-600 mb-2">
                    {tableInfo.columns.length} columns
                  </div>
                  <div className="text-xs text-gray-500 mb-2">
                    {tableName === 'ad_performance_daily' 
                      ? 'No breakdown dimension' 
                      : `Breakdown by ${tableName.replace('ad_performance_daily_', '')}`
                    }
                  </div>
                  {isEmpty ? (
                    <div className="text-xs text-orange-600 bg-orange-50 px-2 py-1 rounded">
                      <div className="font-medium">No data available</div>
                      <div className="opacity-75">Use "Fill Gaps" to populate</div>
                    </div>
                  ) : tableInfo.date_range ? (
                    <div className="text-xs text-blue-600 bg-blue-50 px-2 py-1 rounded">
                      <div className="font-medium">{tableInfo.available_days} days available</div>
                      <div className="opacity-75">
                        {(() => {
                          const startDate = new Date(tableInfo.date_range.start);
                          const endDate = new Date(tableInfo.date_range.end);
                          const sameYear = startDate.getFullYear() === endDate.getFullYear();
                          
                          if (sameYear) {
                            return `${startDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} to ${endDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}`;
                          } else {
                            return `${startDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} to ${endDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}`;
                          }
                        })()}
                      </div>
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>

          {/* Show/Hide Other Empty Tables */}
          {otherEmptyTables.length > 0 && (
            <div className="border-t border-gray-200 pt-6">
              <button 
                onClick={() => setShowAllTables(!showAllTables)}
                className="flex items-center space-x-2 text-sm text-gray-600 hover:text-gray-800 transition-colors"
              >
                <span>{showAllTables ? 'Hide' : 'Show'} other empty tables ({otherEmptyTables.length})</span>
                <span className="transform transition-transform duration-200" style={{transform: showAllTables ? 'rotate(180deg)' : 'rotate(0deg)'}}>
                  ▼
                </span>
              </button>
              
              {showAllTables && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mt-4">
                  {otherEmptyTables.map(([tableName, tableInfo]) => (
                    <div 
                      key={tableName}
                      className="p-4 border border-gray-200 rounded-lg bg-gray-50"
                    >
                      <div className="flex justify-between items-start mb-2">
                        <h4 className="font-medium text-gray-700 text-sm">{tableName}</h4>
                        <span className="px-2 py-1 text-xs rounded bg-gray-200 text-gray-600">
                          0 rows
                        </span>
                      </div>
                      <div className="text-xs text-gray-500">
                        {tableInfo.columns.length} columns
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default TableViewer; 