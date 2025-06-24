import React, { useState, Fragment, useRef, useEffect } from 'react';
import { ChevronDown, ChevronRight, BarChart2, Info, Layers, Table2, Search, AlignJustify, Play, Clock, Sparkles, ChevronUp, ArrowUpDown } from 'lucide-react';
// 📋 ADDING NEW COLUMNS? Read: src/config/Column README.md for complete instructions
import { AVAILABLE_COLUMNS } from '../config/columns';
import ROASSparkline from './dashboard/ROASSparkline';

// Pipeline Update Badge Component
const PipelineBadge = ({ isPipelineUpdated, className = "" }) => {
  if (!isPipelineUpdated) return null;
  
  return (
    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 ${className}`}>
      <Sparkles size={12} className="mr-1" />
      Pipeline
    </span>
  );
};

// Estimated ROAS Tooltip Component
const EstimatedRoasTooltip = ({ roas, estimatedRevenue, diffPercent, spend, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

  const handleMouseEnter = (e) => {
    setShowTooltip(true);
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  const adjustedRevenue = diffPercent > 0 ? estimatedRevenue / diffPercent : estimatedRevenue;

  return (
    <div className="relative">
      <span 
        className={`${colorClass} ${pipelineUpdatedClass} cursor-pointer hover:underline`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {formatNumber(roas, 2)}
      </span>
      
      {showTooltip && (
        <div 
          className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg p-3 min-w-64"
          style={{
            left: tooltipPosition.x - 128, // Center the tooltip
            top: tooltipPosition.y - 10,
            transform: 'translateY(-100%)'
          }}
        >
          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Estimated ROAS Calculation
          </div>
          <div className="space-y-1 text-xs">
            <div className="text-gray-700 dark:text-gray-300">
              Original Est. Revenue: {formatCurrency(estimatedRevenue)}
            </div>
            {diffPercent > 0 && (
              <>
                <div className="text-gray-700 dark:text-gray-300">
                  Trial Accuracy Ratio: {formatPercentage(diffPercent)} (Mixpanel/Meta)
                </div>
                <div className="text-gray-700 dark:text-gray-300">
                  Adjusted Revenue: {formatCurrency(adjustedRevenue)}
                </div>
                <div className="text-gray-500 dark:text-gray-400 text-xs italic">
                  (Revenue ÷ {formatPercentage(diffPercent)} to account for Meta accuracy)
                </div>
              </>
            )}
            <div className="text-gray-700 dark:text-gray-300">
              Spend: {formatCurrency(spend)}
            </div>
            <div className="border-t border-gray-200 dark:border-gray-600 mt-2 pt-2 font-medium">
              Est. ROAS: {formatCurrency(adjustedRevenue)} ÷ {formatCurrency(spend)} = {formatNumber(roas, 2)}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Accuracy Tooltip Component
const AccuracyTooltip = ({ average, breakdown, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const ref = useRef(null);

  const handleMouseEnter = (e) => {
    setShowTooltip(true);
    // Position tooltip relative to the cell
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  // Get color for accuracy level
  const getAccuracyColor = (level) => {
    switch (level.toLowerCase()) {
      case 'very_high': return 'text-green-700';
      case 'high': return 'text-green-600';
      case 'medium': return 'text-yellow-600';
      case 'low': return 'text-orange-600';
      case 'very_low': return 'text-red-600';
      default: return 'text-gray-600';
    }
  };

  const formatLevelName = (level) => {
    return level.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1)
    ).join(' ');
  };

  return (
    <div className="relative">
      <span 
        ref={ref}
        className={`${colorClass} ${pipelineUpdatedClass} cursor-pointer hover:underline`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {average}
      </span>
      
      {showTooltip && (
        <div 
          className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg p-3 min-w-48"
          style={{
            left: tooltipPosition.x - 96, // Center the tooltip (half of min-width)
            top: tooltipPosition.y - 10,
            transform: 'translateY(-100%)'
          }}
        >
          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Accuracy Breakdown
          </div>
          <div className="space-y-1">
            {Object.entries(breakdown)
              .sort(([,a], [,b]) => b.percentage - a.percentage) // Sort by percentage desc
              .map(([level, data]) => (
                <div key={level} className="flex justify-between items-center text-xs">
                  <span className={`${getAccuracyColor(level)} font-medium`}>
                    {formatLevelName(level)}:
                  </span>
                  <span className="text-gray-700 dark:text-gray-300">
                    {data.count} ({data.percentage}%)
                  </span>
                </div>
              ))}
          </div>
          <div className="border-t border-gray-200 dark:border-gray-600 mt-2 pt-2 text-xs text-gray-600 dark:text-gray-400">
            Total Users: {Object.values(breakdown).reduce((sum, data) => sum + data.count, 0)}
          </div>
        </div>
      )}
    </div>
  );
};

// Refund Rate Tooltip Component for showing minimum default explanations
const RefundRateTooltip = ({ value, type, colorClass, pipelineUpdatedClass }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  
  const isMinimum = type === 'trial' ? Math.abs(value - 5.0) < 0.01 : Math.abs(value - 15.0) < 0.01;
  const minimumRate = type === 'trial' ? '5%' : '15%';
  
  if (!isMinimum) return null; // Only show tooltip for minimum values

  const handleMouseEnter = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    setTooltipPosition({
      x: rect.left + rect.width / 2,
      y: rect.top - 10
    });
    setShowTooltip(true);
  };

  const handleMouseLeave = () => {
    setShowTooltip(false);
  };

  return (
    <>
      <span
        className={`${colorClass} ${pipelineUpdatedClass} cursor-help`}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        *
      </span>
      {showTooltip && (
        <div
          className="fixed z-50 bg-gray-900 text-white text-xs rounded px-2 py-1 max-w-xs"
          style={{
            left: tooltipPosition.x,
            top: tooltipPosition.y,
            transform: 'translateX(-50%) translateY(-100%)'
          }}
        >
          <div className="text-center">
            <div className="font-medium">Default Minimum Applied</div>
            <div className="mt-1">
              This rate defaulted to {minimumRate} because either there weren't enough users 
              or the calculated rate was lower. We use this minimum to ensure reliable estimates.
            </div>
          </div>
        </div>
      )}
    </>
  );
};

// Helper to format numbers, e.g., with commas
const formatNumber = (num, digits = 0) => {
  if (num === undefined || num === null) return 'N/A';
  return num.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
};

const formatCurrency = (num) => {
  if (num === undefined || num === null) return 'N/A';
  return num.toLocaleString(undefined, { style: 'currency', currency: 'USD' });
};

const formatPercentage = (num, digits = 2) => {
  if (num === undefined || num === null) return 'N/A';
  return `${(num * 100).toFixed(digits)}%`;
};

// Column categorization for event priority styling based on actual dashboard columns:
// TRIAL columns: "Trials (Meta)", "Trials (Mixpanel)", etc.
const TRIAL_RELATED_COLUMNS = [
  'mixpanel_trials_started',    // "Trials (Mixpanel)" 
  'meta_trials_started',        // "Trials (Meta)"
  'mixpanel_trials_ended',      // "Trials Ended (Mixpanel)"
  'mixpanel_trials_in_progress', // "Trials In Progress (Mixpanel)"  
  'click_to_trial_rate',        // "Click to Trial Rate"
  'mixpanel_cost_per_trial',    // "Cost per Trial (Mixpanel)"
  'meta_cost_per_trial',        // "Cost per Trial (Meta)"
  'trial_conversion_rate',      // "Trial Conversion Rate" (rate of trials converting to purchases)
  'avg_trial_refund_rate',      // "Trial Refund Rate" (rate of trial conversions that get refunded)
  'trial_accuracy_ratio'        // "Trial Accuracy Ratio"
];

// PURCHASE columns: "Purchases (Meta)", "Purchases (Mixpanel)", etc.
// Note: estimated_revenue_usd, profit, and estimated_roas are excluded from graying 
// because they should always remain visible as key metrics
const PURCHASE_RELATED_COLUMNS = [
  'mixpanel_purchases',         // "Purchases (Mixpanel)"
  'meta_purchases',             // "Purchases (Meta)"  
  'mixpanel_cost_per_purchase', // "Cost per Purchase (Mixpanel)"
  'meta_cost_per_purchase',     // "Cost per Purchase (Meta)"
  'purchase_refund_rate',       // "Purchase Refund Rate"
  'purchase_accuracy_ratio',    // "Purchase Accuracy Ratio"
  'mixpanel_conversions_net_refunds', // "Net Conversions (Mixpanel)"
  'mixpanel_revenue_usd',       // "Revenue (Mixpanel)"
  'mixpanel_refunds_usd'        // "Refunds (Mixpanel)"
  // estimated_revenue_usd, profit, and estimated_roas intentionally excluded 
  // so they never get grayed out - they're always-visible key metrics
];

// Helper to determine event priority based on Mixpanel counts
// Compares "Trial Started by Mixpanel" vs "Purchases in Mixpanel" 
const getEventPriority = (row) => {
  // Compares "Trial Started by Mixpanel" vs "Purchases in Mixpanel"
  const trialsCount = row.mixpanel_trials_started || 0;    // "Trial Started by Mixpanel"
  const purchasesCount = row.mixpanel_purchases || 0;      // "Purchases in Mixpanel"
  
  // When both are zero, default to graying out purchase columns (make trials the priority)
  if (trialsCount === 0 && purchasesCount === 0) return 'trials';
  
  // When trials > purchases, gray out purchase columns (trials are priority)
  if (trialsCount > purchasesCount) return 'trials';
  
  // When purchases > trials, gray out trial columns (purchases are priority)  
  if (purchasesCount > trialsCount) return 'purchases';
  
  // If they're equal and both > 0, no graying out
  return 'equal';
};

// Helper to check if a column should be grayed out based on event priority
const shouldGrayOutColumn = (columnKey, eventPriority) => {
  if (eventPriority === 'equal') return false;
  
  // When trials are priority, gray out purchase-related columns
  if (eventPriority === 'trials' && PURCHASE_RELATED_COLUMNS.includes(columnKey)) {
    return true;
  }
  
  // When purchases are priority, gray out trial-related columns
  if (eventPriority === 'purchases' && TRIAL_RELATED_COLUMNS.includes(columnKey)) {
    return true;
  }
  
  return false;
};

// Helper to get column type for visual differentiation
const getColumnType = (columnKey) => {
  if (TRIAL_RELATED_COLUMNS.includes(columnKey)) return 'trial';
  if (PURCHASE_RELATED_COLUMNS.includes(columnKey)) return 'purchase';
  return 'neutral';
};

// Helper to get column background class based on type
const getColumnBackgroundClass = (columnKey, isHovered = false) => {
  const columnType = getColumnType(columnKey);
  
  if (isHovered) {
    // Slightly stronger background on hover
    switch (columnType) {
      case 'trial': return 'bg-blue-50 dark:bg-blue-900/20';
      case 'purchase': return 'bg-green-50 dark:bg-green-900/20';
      default: return ''; // No special hover background for neutral columns
    }
  } else {
    // Subtle background for visual differentiation
    switch (columnType) {
      case 'trial': return 'bg-blue-25 dark:bg-blue-900/10';
      case 'purchase': return 'bg-green-25 dark:bg-green-900/10';
      default: return ''; // No background for neutral columns - normal grey
    }
  }
};

// Helper to calculate derived values for missing fields
const calculateDerivedValues = (row) => {
  // Frontend should NOT calculate values - all calculations should come from backend
  // Simply return the row as-is since backend provides all calculated fields
  return { ...row };
};

// Field color - use normal text colors
const getFieldColor = (fieldName, value) => {
  // Special color coding for accuracy column
  if (fieldName === 'average_accuracy' && value) {
    switch (value.toLowerCase()) {
      case 'very high': return 'text-green-700 dark:text-green-400';
      case 'high': return 'text-green-600 dark:text-green-400';
      case 'medium': return 'text-yellow-600 dark:text-yellow-400';
      case 'low': return 'text-orange-600 dark:text-orange-400';
      case 'very low': return 'text-red-600 dark:text-red-400';
      default: return 'text-gray-600 dark:text-gray-400';
    }
  }
  // Use standard text colors for all other fields
  return 'text-gray-900 dark:text-gray-100';
};

// Header color - use normal text colors
const getHeaderColor = (fieldName) => {
  // Use standard header colors for all fields
  return 'text-gray-700 dark:text-gray-300';
};

// ROAS color thresholds: <1 = Red, 1 = Yellow, >1.5 = Green
const roas_green_threshold = 1.5;
const roas_yellow_threshold = 1.0;

const getRoasColor = (roas) => {
  if (roas >= 3.0) return 'text-green-600 dark:text-green-400';
  if (roas >= 2.0) return 'text-yellow-600 dark:text-yellow-400';
  if (roas >= 1.0) return 'text-orange-600 dark:text-orange-400';
  return 'text-red-600 dark:text-red-400';
};

// Sort indicator component
const SortIndicator = ({ column, sortConfig }) => {
  if (sortConfig.column !== column) {
    return (
      <ArrowUpDown 
        size={12} 
        className="ml-1 text-gray-400 dark:text-gray-500 opacity-0 group-hover:opacity-100 transition-opacity" 
      />
    );
  }
  
  return sortConfig.direction === 'asc' ? (
    <ChevronUp size={12} className="ml-1 text-gray-600 dark:text-gray-300" />
  ) : (
    <ChevronDown size={12} className="ml-1 text-gray-600 dark:text-gray-300" />
  );
};

export const DashboardGrid = ({ 
  data = [], 
  rowOrder = [],
  onRowOrderChange = null,
  onRowAction = () => {}, 
  columnVisibility = {}, 
  columnOrder = [],
  onColumnOrderChange = null,
  runningPipelines = new Set(),
  pipelineQueue = [],
  activePipelineCount = 0,
  maxConcurrentPipelines = 8,
  dashboardParams = null,
  sortConfig = { column: null, direction: 'asc' },
  onSort = () => {}
}) => {
  const [expandedRows, setExpandedRows] = useState({});
  const [expandedBreakdowns, setExpandedBreakdowns] = useState({});
  
  // Drag state
  const [draggedColumn, setDraggedColumn] = useState(null);
  const [dragOverColumn, setDragOverColumn] = useState(null);
  const [draggedRow, setDraggedRow] = useState(null);
  const [isDragging, setIsDragging] = useState(false);

  // Handle column header click for sorting (only if not dragging)
  const handleColumnHeaderClick = (columnKey) => {
    if (!isDragging && onSort) {
      onSort(columnKey);
    }
  };

  // Row drag state
  const [draggedRowId, setDraggedRowId] = useState(null);
  const [dragOverRowId, setDragOverRowId] = useState(null);

  // Get visible columns based on columnVisibility settings and column order
  const getOrderedVisibleColumns = () => {
    // Use column order if available, otherwise use default order
    const orderToUse = columnOrder.length > 0 ? columnOrder : AVAILABLE_COLUMNS.map(col => col.key);
    
    // Map the ordered keys to column objects, filtering out non-existent columns
    const orderedColumns = orderToUse
      .map(key => AVAILABLE_COLUMNS.find(col => col.key === key))
      .filter(col => col); // Remove any undefined columns
    
    // Filter by visibility
    if (Object.keys(columnVisibility).length === 0) {
      // If no visibility settings loaded yet, use default visibility
      return orderedColumns.filter(col => col.defaultVisible);
    } else {
      // Use explicit visibility settings - show column if explicitly true
      return orderedColumns.filter(col => columnVisibility[col.key] === true);
    }
  };

  const visibleColumns = getOrderedVisibleColumns();
  
  // Debug column visibility (cleanup after fix)
  console.log('Column Visibility Status:', {
    'visible_columns': visibleColumns.length,
    'estimated_revenue_adjusted_visible': visibleColumns.some(col => col.key === 'estimated_revenue_adjusted'),
    'mixpanel_revenue_net_visible': visibleColumns.some(col => col.key === 'mixpanel_revenue_net')
  });

  // Helper function to check if a column should be visible
  const isColumnVisible = (columnKey) => {
    if (Object.keys(columnVisibility).length === 0) {
      // If no visibility settings loaded yet, check default
      const column = AVAILABLE_COLUMNS.find(col => col.key === columnKey);
      return column ? column.defaultVisible : false;
    }
    return columnVisibility[columnKey] !== false;
  };

  // Helper function to check if a pipeline is running for a specific row
  const isPipelineRunning = (rowId) => {
    return runningPipelines.has(rowId);
  };

  // Helper function to check if a pipeline is queued for a specific row
  const isPipelineQueued = (rowId) => {
    return pipelineQueue.some(item => item.id === rowId);
  };

  // Helper function to get pipeline status
  const getPipelineStatus = (rowId) => {
    if (isPipelineRunning(rowId)) return 'running';
    if (isPipelineQueued(rowId)) return 'queued';
    return 'idle';
  };

  // Helper function to render a cell value with proper formatting and coloring
  // 📋 ADDING NEW COLUMN FORMATTING? Read: src/config/Column README.md for instructions
  const renderCellValue = (row, columnKey, isPipelineUpdated = false, eventPriority = null) => {
    const calculatedRow = calculateDerivedValues(row);
    let value = calculatedRow[columnKey];
    let formattedValue = 'N/A';
    let isEstimated = false;

    // Format values based on column type
    switch (columnKey) {
      case 'spend':
      case 'mixpanel_revenue_usd':
      case 'estimated_revenue_usd':
      case 'estimated_revenue_adjusted':
      case 'mixpanel_revenue_net':
      case 'mixpanel_refunds_usd':
      case 'profit':
      case 'mixpanel_cost_per_trial':
      case 'mixpanel_cost_per_purchase':
      case 'meta_cost_per_trial':
      case 'meta_cost_per_purchase':
        formattedValue = formatCurrency(value);
        break;
      case 'impressions':
      case 'clicks':
      case 'mixpanel_trials_started':
      case 'meta_trials_started':
      case 'mixpanel_trials_ended':
      case 'mixpanel_trials_in_progress':
      case 'mixpanel_purchases':
      case 'meta_purchases':
      case 'mixpanel_converted_amount':
      case 'mixpanel_conversions_net_refunds':
      case 'total_attributed_users':
        formattedValue = formatNumber(value);
        break;
      case 'click_to_trial_rate':
      case 'trial_conversion_rate':
      case 'trial_accuracy_ratio':
      case 'purchase_accuracy_ratio':
        formattedValue = value !== undefined && value !== null ? `${formatNumber(value, 2)}%` : 'N/A';
        break;
      case 'avg_trial_refund_rate':
        if (value !== undefined && value !== null) {
          const roundedValue = formatNumber(value, 2);
          const hasMinimumFlag = Math.abs(value - 5.0) < 0.01;
          formattedValue = (
            <span>
              {roundedValue}%
              {hasMinimumFlag && (
                <RefundRateTooltip 
                  value={value} 
                  type="trial" 
                  colorClass="text-blue-500" 
                  pipelineUpdatedClass="" 
                />
              )}
            </span>
          );
        } else {
          formattedValue = 'N/A';
        }
        break;
      case 'purchase_refund_rate':
        if (value !== undefined && value !== null) {
          const roundedValue = formatNumber(value, 2);
          const hasMinimumFlag = Math.abs(value - 15.0) < 0.01;
          formattedValue = (
            <span>
              {roundedValue}%
              {hasMinimumFlag && (
                <RefundRateTooltip 
                  value={value} 
                  type="purchase" 
                  colorClass="text-orange-500" 
                  pipelineUpdatedClass="" 
                />
              )}
            </span>
          );
        } else {
          formattedValue = 'N/A';
        }
        break;
      case 'estimated_roas':
        formattedValue = formatNumber(value, 2);
        break;
      case 'segment_accuracy_average':
        formattedValue = value || 'N/A';
        break;
      default:
        formattedValue = value || 'N/A';
    }

    // Apply special styling for ROAS columns
    let colorClass = getFieldColor(columnKey, value);
    if (columnKey === 'estimated_roas') {
      colorClass = getRoasColor(value);
    }

    // Check if this column should be grayed out based on event priority
    if (eventPriority && shouldGrayOutColumn(columnKey, eventPriority)) {
      colorClass = 'text-gray-500 dark:text-gray-500';
    }

    // Add pipeline update styling for key metrics
    const pipelineUpdatedClass = isPipelineUpdated && 
      ['mixpanel_purchases', 'mixpanel_revenue_usd', 'mixpanel_revenue_net', 'estimated_revenue_usd', 'estimated_revenue_adjusted', 'estimated_roas', 'mixpanel_trials_started', 'mixpanel_refunds_usd', 'segment_accuracy_average'].includes(columnKey) 
        ? 'font-bold text-green-600 dark:text-green-400' : '';

    // Special rendering for accuracy column with tooltip
    if (columnKey === 'segment_accuracy_average' && row.accuracy_breakdown) {
      return (
        <AccuracyTooltip 
          average={formattedValue}
          breakdown={row.accuracy_breakdown}
          colorClass={colorClass}
          pipelineUpdatedClass={pipelineUpdatedClass}
        />
      );
    }

    // Special rendering for estimated ROAS column with sparkline
    if (columnKey === 'estimated_roas') {
      // Extract the actual ID from the row.id field (format: "campaign_123", "adset_456", "ad_789")
      const entityId = row.id ? row.id.split('_')[1] : null;
      
      // DEBUG: Log the row data to understand the structure
      console.log('🔥 SPARKLINE CELL DEBUG:', {
        rowId: row.id,
        rowType: row.type,
        entityId: entityId,
        hasSpend: !!row.spend,
        hasRevenue: !!calculatedRow.estimated_revenue_usd,
        fullRow: row
      });
      
      return (
        <ROASSparkline 
          entityType={row.type}
          entityId={entityId}
          currentROAS={value}
          conversionCount={calculatedRow.mixpanel_purchases || 0}
          breakdown={dashboardParams?.breakdown || 'all'}
          startDate={dashboardParams?.start_date || '2025-04-01'}
          endDate={dashboardParams?.end_date || '2025-04-10'}
        />
      );
    }

    return (
      <span className={`${colorClass} ${pipelineUpdatedClass}`}>
        {formattedValue}
        {isEstimated && <span className="ml-1 text-xs">*</span>}
      </span>
    );
  };

  // Simple column drag handlers
  const handleColumnDragStart = (e, columnKey) => {
    setDraggedColumn(columnKey);
    setIsDragging(true);
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', columnKey);
  };

  const handleColumnDragOver = (e) => {
    e.preventDefault();
  };

  const handleColumnDragEnter = (e, columnKey) => {
    if (draggedColumn && draggedColumn !== columnKey) {
      setDragOverColumn(columnKey);
    }
  };

  const handleColumnDragLeave = (e) => {
    // Only clear if we're leaving the th element itself
    if (!e.currentTarget.contains(e.relatedTarget)) {
      setDragOverColumn(null);
    }
  };

  const handleColumnDrop = (e, targetColumnKey) => {
    e.preventDefault();
    
    if (draggedColumn && draggedColumn !== targetColumnKey && onColumnOrderChange) {
      const currentOrder = [...columnOrder];
      const draggedIndex = currentOrder.indexOf(draggedColumn);
      const targetIndex = currentOrder.indexOf(targetColumnKey);
      
      if (draggedIndex !== -1 && targetIndex !== -1) {
        // Remove dragged column
        const [draggedCol] = currentOrder.splice(draggedIndex, 1);
        
        // Insert at target position
        const newTargetIndex = draggedIndex < targetIndex ? targetIndex : targetIndex + 1;
        currentOrder.splice(newTargetIndex, 0, draggedCol);
        
        console.log(`🔄 Column reordered: '${draggedColumn}' moved from position ${draggedIndex} to ${newTargetIndex}`);
        onColumnOrderChange(currentOrder);
      }
    }
    
    setDraggedColumn(null);
    setDragOverColumn(null);
    // Add delay to prevent accidental sorting after drag
    setTimeout(() => setIsDragging(false), 100);
  };

  const handleColumnDragEnd = (e) => {
    setDraggedColumn(null);
    setDragOverColumn(null);
    // Add delay to prevent accidental sorting after drag
    setTimeout(() => setIsDragging(false), 100);
  };

  // Row drag handlers
  const handleRowDragStart = (e, id) => {
    setDraggedRowId(id);
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setDragImage(new Image(), 0, 0); // hide ghost for cleaner UX
  };

  const handleRowDragEnter = (e, id) => {
    e.preventDefault();
    if (id !== dragOverRowId) setDragOverRowId(id);
  };

  const handleRowDrop = (e, targetId) => {
    e.preventDefault();
    if (!draggedRowId || draggedRowId === targetId) return;

    const newOrder = [...rowOrder];
    const from = newOrder.indexOf(draggedRowId);
    const to = newOrder.indexOf(targetId);
    newOrder.splice(to, 0, newOrder.splice(from, 1)[0]);

    onRowOrderChange?.(newOrder);
    cleanupRowDrag();
  };

  const handleRowDragEnd = cleanupRowDrag;

  function cleanupRowDrag() {
    setDraggedRowId(null);
    setDragOverRowId(null);
  }

  const toggleExpand = (id) => {
    setExpandedRows(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleBreakdown = (id) => {
    setExpandedBreakdowns(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const expandAllRows = () => {
    const newExpandedRows = {};
    const processRows = (rows) => {
      rows.forEach(row => {
        newExpandedRows[row.id] = true;
        if (row.children) {
          processRows(row.children);
        }
      });
    };
    processRows(data);
    setExpandedRows(newExpandedRows);
  };

  const collapseAllRows = () => {
    setExpandedRows({});
  };

  const expandAllBreakdowns = () => {
    const newExpandedBreakdowns = {};
    const processRows = (rows) => {
      rows.forEach(row => {
        if (row.breakdowns && row.breakdowns.length > 0) {
          newExpandedBreakdowns[row.id] = true;
        }
        if (row.children) {
          processRows(row.children);
        }
      });
    };
    processRows(data);
    setExpandedBreakdowns(newExpandedBreakdowns);
  };

  const collapseAllBreakdowns = () => {
    setExpandedBreakdowns({});
  };

  const renderAllBreakdownRows = (row, level) => {
    const breakdownNodes = [];
    
    if (!row.breakdowns) return breakdownNodes;
    
    row.breakdowns.forEach(breakdown => {
      // Enhanced breakdown header with mapping info
      breakdownNodes.push(
        <tr key={`${row.id}-${breakdown.type}-header`} className="border-b border-gray-200 dark:border-gray-700 bg-blue-50/50 dark:bg-blue-900/50">
          <td className="sticky left-0 px-3 py-1 whitespace-nowrap bg-blue-50/50 dark:bg-blue-900/50 z-10">
            <div className="flex items-center">
              <span className="opacity-0 w-8"></span> {/* Space for chart/info icons */}
              <span style={{ paddingLeft: `${(level + 1) * 20}px` }} className="text-xs font-medium text-blue-700 dark:text-blue-300">
                📊 {breakdown.type.charAt(0).toUpperCase() + breakdown.type.slice(1)} Breakdown
                {breakdown.values && breakdown.values.length > 0 && (
                  <span className="ml-2 text-xs text-gray-500 dark:text-gray-400">
                    ({breakdown.values.length} segments)
                  </span>
                )}
              </span>
            </div>
          </td>
          {visibleColumns.slice(1).map((column) => (
            <td key={column.key} className="px-3 py-1 text-center text-xs text-blue-600 dark:text-blue-400">
              {column.key === 'name' ? 'Segment' : ''}
            </td>
          ))}
        </tr>
      );

      breakdown.values.forEach((value, index) => {
        const calculatedValue = calculateDerivedValues(value);
        
        // Determine if this is a mapped breakdown value
        const isMapped = value.meta_value && value.mixpanel_value && value.meta_value !== value.mixpanel_value;
        const mappingInfo = isMapped ? `${value.meta_value} → ${value.mixpanel_value}` : value.name;
        
        breakdownNodes.push(
          <tr key={`${row.id}-${breakdown.type}-${index}`} className="border-b border-gray-200 dark:border-gray-700 bg-gray-50/30 dark:bg-gray-900/30 text-xs hover:bg-blue-50/20 dark:hover:bg-blue-900/20">
            <td className="sticky left-0 px-3 py-1 whitespace-nowrap bg-gray-50/30 dark:bg-gray-900/30 z-10">
              <div className="flex items-center">
                <span className="opacity-0 w-8"></span> {/* Space for chart/info icons */}
                <div style={{ paddingLeft: `${(level + 1) * 20 + 12}px` }} className="flex items-center space-x-2">
                  <span className="text-gray-700 dark:text-gray-300">
                    {mappingInfo}
                  </span>
                  {isMapped && (
                    <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-200" title="Meta-Mixpanel Mapping Applied">
                      🔗
                    </span>
                  )}
                  {value.total_users && (
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                      ({value.total_users.toLocaleString()} users)
                    </span>
                  )}
                </div>
              </div>
            </td>
            {visibleColumns.slice(1).map((column) => {
              if (column.key === 'campaign_name' || column.key === 'adset_name') {
                return <td key={column.key} className="px-3 py-1"></td>;
              }
              
              // Enhanced breakdown value rendering with mapping awareness
              return (
                <td key={column.key} className="px-3 py-1 whitespace-nowrap text-right">
                  <div className="flex flex-col">
                    <span className={`${getFieldColor(column.key, calculatedValue[column.key])}`}>
                      {renderCellValue(calculatedValue, column.key, false, getEventPriority(calculatedValue))}
                    </span>
                    {/* Show accuracy ratios for trial/purchase metrics */}
                    {(column.key === 'mixpanel_trials_started' && value.trial_accuracy_ratio !== undefined) && (
                      <span className="text-xs text-gray-500 dark:text-gray-400">
                        {(value.trial_accuracy_ratio * 100).toFixed(1)}% accuracy
                      </span>
                    )}
                    {(column.key === 'mixpanel_purchases' && value.purchase_accuracy_ratio !== undefined) && (
                      <span className="text-xs text-gray-500 dark:text-gray-400">
                        {(value.purchase_accuracy_ratio * 100).toFixed(1)}% accuracy
                      </span>
                    )}
                  </div>
                </td>
              );
            })}
          </tr>
        );
      });
      
      // Add summary row for breakdown if multiple values
      if (breakdown.values && breakdown.values.length > 1) {
        const summaryData = breakdown.values.reduce((acc, value) => {
          Object.keys(value).forEach(key => {
            if (typeof value[key] === 'number') {
              acc[key] = (acc[key] || 0) + value[key];
            }
          });
          return acc;
        }, {});
        
        const calculatedSummary = calculateDerivedValues(summaryData);
        
        breakdownNodes.push(
          <tr key={`${row.id}-${breakdown.type}-summary`} className="border-b border-gray-300 dark:border-gray-600 bg-blue-100/50 dark:bg-blue-800/50 text-xs font-medium">
            <td className="sticky left-0 px-3 py-1 whitespace-nowrap bg-blue-100/50 dark:bg-blue-800/50 z-10">
              <div className="flex items-center">
                <span className="opacity-0 w-8"></span>
                <span style={{ paddingLeft: `${(level + 1) * 20 + 12}px` }} className="text-blue-700 dark:text-blue-300 italic">
                  📋 {breakdown.type} Total ({breakdown.values.length} segments)
                </span>
              </div>
            </td>
            {visibleColumns.slice(1).map((column) => {
              if (column.key === 'campaign_name' || column.key === 'adset_name') {
                return <td key={column.key} className="px-3 py-1"></td>;
              }
              return (
                <td key={column.key} className="px-3 py-1 whitespace-nowrap text-right font-medium text-blue-700 dark:text-blue-300">
                  {renderCellValue(calculatedSummary, column.key, false, getEventPriority(calculatedSummary))}
                </td>
              );
            })}
          </tr>
        );
      }
    });
    
    return breakdownNodes;
  };

  const renderRow = (row, level) => {
    const isExpanded = !!expandedRows[row.id];
    const isBreakdownExpanded = !!expandedBreakdowns[row.id];
    const rowNodes = [];

    // Check if this row has been updated with pipeline data
    const isPipelineUpdated = row._pipelineUpdated;
    const pipelineTimestamp = row._pipelineTimestamp;

    const calculatedRow = calculateDerivedValues(row);

    // Determine event priority
    const eventPriority = getEventPriority(row);

    rowNodes.push(
      <tr 
        key={row.id} 
        draggable={level === 0}
        onDragStart={e => handleRowDragStart(e, row.id)}
        onDragEnter={e => handleRowDragEnter(e, row.id)}
        onDragOver={e => e.preventDefault()}
        onDrop={e => handleRowDrop(e, row.id)}
        onDragEnd={handleRowDragEnd}
        className={`border-b border-gray-200 dark:border-gray-700 ${
        level === 0 
          ? 'bg-gray-50 dark:bg-gray-800 font-semibold' 
          : level === 1 
            ? 'bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-200' 
            : 'bg-gray-100 dark:bg-gray-600 text-gray-800 dark:text-gray-200'
      } ${(isPipelineUpdated || row._pipelineUpdated) ? 'ring-2 ring-green-400 bg-green-50 dark:bg-green-900/20' : ''} ${draggedRowId === row.id ? 'opacity-50' : ''} ${dragOverRowId === row.id && draggedRowId !== row.id ? 'ring-2 ring-blue-400' : ''}`}>
        
        {/* Name column - always visible */}
        <td className={`sticky left-0 px-3 py-2 whitespace-nowrap z-10 ${
          level === 0 
            ? ((isPipelineUpdated || row._pipelineUpdated) ? 'bg-green-50 dark:bg-green-900/20' : 'bg-gray-50 dark:bg-gray-800')
            : level === 1 
              ? ((isPipelineUpdated || row._pipelineUpdated) ? 'bg-green-50 dark:bg-green-900/20' : 'bg-white dark:bg-gray-700')
              : ((isPipelineUpdated || row._pipelineUpdated) ? 'bg-green-50 dark:bg-green-900/20' : 'bg-gray-100 dark:bg-gray-600')
        }`}>
          <div className="flex items-center">
            <button onClick={() => onRowAction('graph', row)} className="mr-2 p-1 hover:text-blue-500" title="View Chart"><BarChart2 size={16} /></button>
            <button onClick={() => onRowAction('debug', row)} className="mr-2 p-1 hover:text-orange-500" title="Debug Info"><Info size={16} /></button>
            <button 
              onClick={() => onRowAction('pipeline', row)} 
              disabled={isPipelineRunning(row.id) || isPipelineQueued(row.id)}
              className={`mr-2 p-1 ${
                isPipelineRunning(row.id) ? 'text-green-500 cursor-not-allowed' : 
                isPipelineQueued(row.id) ? 'text-yellow-500 cursor-not-allowed' : 
                'hover:text-green-500'
              }`} 
              title={
                isPipelineRunning(row.id) ? 'Pipeline Running...' : 
                isPipelineQueued(row.id) ? 'Pipeline Queued...' : 
                'Run Pipeline'
              }
            >
              {isPipelineRunning(row.id) ? (
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-green-500"></div>
              ) : isPipelineQueued(row.id) ? (
                <div className="animate-pulse rounded-full h-4 w-4 border-2 border-yellow-500 bg-yellow-200"></div>
              ) : (
                <Play size={16} />
              )}
            </button>
            <button onClick={() => onRowAction('timeline', row)} className="mr-2 p-1 hover:text-purple-500" title="View Timeline"><Clock size={16} /></button>
            <span className="flex items-center">
              <span style={{ paddingLeft: `${level * 20}px` }}>
                {row.children && row.children.length > 0 ? (
                  <button onClick={() => toggleExpand(row.id)} className="mr-1 p-1">
                    {isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                  </button>
                ) : (
                  <span className="inline-block w-8"></span>
                )}
              </span>
              
              {row.breakdowns && row.breakdowns.length > 0 && (
                <button 
                  onClick={() => toggleBreakdown(row.id)} 
                  className={`mr-2 p-1 rounded ${isBreakdownExpanded ? 'text-blue-500 bg-blue-100 dark:bg-blue-900/30' : 'text-gray-400 hover:text-blue-400'}`}
                  title="Toggle breakdowns"
                >
                  <Search size={14} />
                </button>
              )}
              
              {/* Pipeline update indicator */}
              {(isPipelineUpdated || row._pipelineUpdated) && (
                <PipelineBadge 
                  isPipelineUpdated={true} 
                  className="mr-2" 
                />
              )}
              
              {/* Pipeline running indicator */}
              {isPipelineRunning(row.id) && (
                <span className="mr-2 px-2 py-1 bg-blue-100 dark:bg-blue-800 text-blue-800 dark:text-blue-200 text-xs rounded-full animate-pulse">
                  🔄 Running
                </span>
              )}
              
              {/* Pipeline queued indicator */}
              {isPipelineQueued(row.id) && (
                <span className="mr-2 px-2 py-1 bg-yellow-100 dark:bg-yellow-800 text-yellow-800 dark:text-yellow-200 text-xs rounded-full animate-pulse">
                  ⏳ Queued
                </span>
              )}
              
              <span className={`${level === 0 ? 'text-gray-900 dark:text-gray-100' : level === 1 ? 'text-gray-800 dark:text-gray-200' : 'text-gray-700 dark:text-gray-100'}`}>
                {level === 0 
                  ? (row.name || row.campaign_name)
                  : level === 1 
                    ? (row.name || row.adset_name)
                    : (row.name || row.ad_name)
                }
              </span>
            </span>
          </div>
        </td>

        {/* Dynamic columns based on visibility */}
        {visibleColumns.slice(1).map((column) => {
          // Special handling for campaign/adset name columns based on level
          if (column.key === 'campaign_name') {
            if (level > 0) {
              return <td key={column.key} className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">{row.campaign_name}</td>;
            } else {
              return <td key={column.key} className="px-3 py-2"></td>;
            }
          }
          
          if (column.key === 'adset_name') {
            if (level > 1) {
              return <td key={column.key} className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">{row.adset_name}</td>;
            } else {
              return <td key={column.key} className="px-3 py-2"></td>;
            }
          }

          // Regular data columns
          const fieldColor = getFieldColor(column.key, calculatedRow[column.key]);
          const isRoasColumn = column.key === 'roas' || column.key === 'estimated_roas';
          const roasColor = isRoasColumn ? getRoasColor(calculatedRow[column.key]) : '';
          const finalColor = isRoasColumn ? roasColor : fieldColor;
          
          const pipelineHighlight = (isPipelineUpdated || row._pipelineUpdated) && 
            ['total_conversions', 'revenue_usd', 'estimated_conversions', 'estimated_revenue_usd', 'roas', 'estimated_roas', 'total_trials_started', 'total_refunds_usd', 'total_converted_amount_mixpanel', 'mixpanel_trials', 'mixpanel_purchases', 'estimated_roas', 'profit'].includes(column.key) 
              ? 'font-bold text-green-600 dark:text-green-400' : '';

          // Check if the column should be grayed out based on event priority
          const shouldGrayOut = shouldGrayOutColumn(column.key, eventPriority);
          const grayedOutColor = shouldGrayOut ? 'text-gray-500 dark:text-gray-500' : finalColor;

          // Get column background class for visual differentiation
          const columnBackgroundClass = getColumnBackgroundClass(column.key);

                      return (
              <td key={column.key} className={`px-3 py-2 whitespace-nowrap text-right ${grayedOutColor} ${pipelineHighlight} ${isRoasColumn ? 'font-medium' : ''} ${columnBackgroundClass}`}>
                {renderCellValue(calculatedRow, column.key, isPipelineUpdated, eventPriority)}
              </td>
            );
        })}
      </tr>
    );

    if (isBreakdownExpanded && row.breakdowns) {
      rowNodes.push(...renderAllBreakdownRows(row, level));
    }

    if (isExpanded && row.children) {
      row.children.forEach(childRow => {
        rowNodes.push(...renderRow(childRow, level + 1));
      });
    }
    return rowNodes;
  };

  if (!data || data.length === 0) {
    return <div className="p-4 text-center text-gray-500 dark:text-gray-400">No data available.</div>;
  }

  // Prioritize column sorting over manual row ordering
  // If a column sort is active, use the sorted data directly
  // Otherwise, use manual row ordering if available
  const orderedData = (sortConfig.column && sortConfig.column !== null) 
    ? data // Use data as-is (already sorted by Dashboard component)
    : (rowOrder.length
        ? rowOrder.map(id => data.find(r => r.id === id)).filter(Boolean)
        : data);

  const allRows = orderedData.reduce(
    (acc, campaign) => acc.concat(renderRow(campaign, 0)),
    []
  );

  return (
    <div className="shadow-soft rounded-2xl">
      <div className="bg-white dark:bg-gray-800 px-4 py-2 flex space-x-4 border-b border-gray-200 dark:border-gray-700">
        <div className="inline-flex rounded-md shadow-sm">
          <button 
            onClick={expandAllRows} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-l-lg border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Layers size={14} className="mr-1" /> Expand All
          </button>
          <button 
            onClick={collapseAllRows} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-r-lg border border-l-0 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Table2 size={14} className="mr-1" /> Collapse All
          </button>
        </div>
        
        <div className="inline-flex rounded-md shadow-sm">
          <button 
            onClick={expandAllBreakdowns} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-l-lg border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <Search size={14} className="mr-1" /> Show Breakdowns
          </button>
          <button 
            onClick={collapseAllBreakdowns} 
            className="px-3 py-1.5 text-xs font-medium bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 rounded-r-lg border border-l-0 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 flex items-center"
          >
            <AlignJustify size={14} className="mr-1" /> Hide Breakdowns
          </button>
        </div>

        {/* Pipeline Status */}
        <div className="flex items-center space-x-4 ml-auto text-xs">
          {/* Running Pipelines Counter */}
          {(runningPipelines.size > 0 || pipelineQueue.length > 0) && (
            <div className="flex items-center space-x-2">
              {runningPipelines.size > 0 && (
                <div className="flex items-center bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded-full">
                  <div className="animate-spin rounded-full h-3 w-3 border-b border-blue-600 dark:border-blue-400 mr-2"></div>
                  <span className="text-blue-800 dark:text-blue-200 font-medium">
                    {runningPipelines.size} active
                  </span>
                </div>
              )}
              
              {pipelineQueue.length > 0 && (
                <div className="flex items-center bg-yellow-100 dark:bg-yellow-900 px-2 py-1 rounded-full">
                  <div className="animate-pulse rounded-full h-3 w-3 bg-yellow-600 dark:bg-yellow-400 mr-2"></div>
                  <span className="text-yellow-800 dark:text-yellow-200 font-medium">
                    {pipelineQueue.length} queued
                  </span>
                </div>
              )}
              
              <div className="text-gray-500 dark:text-gray-400 text-xs">
                ({activePipelineCount}/{maxConcurrentPipelines} concurrent)
              </div>
            </div>
          )}
          
          <div className="flex items-center">
            <span className="text-green-600 dark:text-green-400 mr-1">✨</span>
            <span className="text-gray-600 dark:text-gray-400">Pipeline Enhanced</span>
          </div>
        </div>
      </div>
      
      {/* Table container with proper horizontal scrolling */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
          <thead className="bg-gray-100 dark:bg-gray-800">
            <tr>
              {/* Name column - always visible and not draggable */}
              <th 
                scope="col" 
                className={`sticky left-0 px-3 py-3 text-left text-xs font-medium uppercase tracking-wider bg-gray-100 dark:bg-gray-800 z-20 cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors group
                  ${sortConfig.column === 'name' ? 'border-b-2 border-blue-500 dark:border-blue-400 bg-blue-50 dark:bg-blue-900/20' : ''}`}
                onClick={() => handleColumnHeaderClick('name')}
              >
                <div className="flex items-center">
                  <span>Name</span>
                  <SortIndicator column="name" sortConfig={sortConfig} />
                </div>
              </th>
              
              {/* Dynamic columns based on visibility - draggable and sortable */}
              {visibleColumns.slice(1).map((column) => {
                const columnType = getColumnType(column.key);
                const backgroundClass = getColumnBackgroundClass(column.key, dragOverColumn === column.key);
                
                return (
                  <th 
                    key={column.key} 
                    scope="col"
                    draggable={!column.alwaysVisible}
                    onDragStart={(e) => handleColumnDragStart(e, column.key)}
                    onDragOver={handleColumnDragOver}
                    onDragEnter={(e) => handleColumnDragEnter(e, column.key)}
                    onDragLeave={handleColumnDragLeave}
                    onDrop={(e) => handleColumnDrop(e, column.key)}
                    onDragEnd={handleColumnDragEnd}
                    onClick={() => handleColumnHeaderClick(column.key)}
                    className={`px-3 py-3 text-${column.key === 'campaign_name' || column.key === 'adset_name' ? 'left' : 'right'} text-xs font-medium uppercase tracking-wider ${getHeaderColor(column.key)} ${backgroundClass}
                      cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors duration-150 group
                      ${sortConfig.column === column.key ? 'bg-blue-50 dark:bg-blue-900/20 border-b-2 border-blue-500 dark:border-blue-400' : ''}
                      ${dragOverColumn === column.key && draggedColumn !== column.key ? 'bg-blue-100 dark:bg-blue-900 border-2 border-blue-300 dark:border-blue-600' : ''}
                      ${columnType === 'trial' ? 'border-l-2 border-blue-200 dark:border-blue-800' : ''}
                      ${columnType === 'purchase' ? 'border-l-2 border-green-200 dark:border-green-800' : ''}`}
                    title={`Click to sort by "${column.label}"${!column.alwaysVisible ? '. Drag to reorder column.' : ''}`}
                  >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center">
                      <span>{column.label}</span>
                      <SortIndicator column={column.key} sortConfig={sortConfig} />
                    </div>
                    {!column.alwaysVisible && (
                      <span className="ml-1 text-gray-400 dark:text-gray-500 text-lg leading-none hover:text-gray-600 dark:hover:text-gray-300 transition-colors duration-150" style={{ transform: 'rotate(90deg)' }}>⋮⋮</span>
                    )}
                  </div>
                </th>
              );
              })}
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
            {allRows.map(row => <Fragment key={row.key}>{row}</Fragment>)}
          </tbody>
        </table>
      </div>
    </div>
  );
}; 