import React, { useState, useRef, useCallback } from 'react';
import useOverviewChartData from '../../hooks/useOverviewChartData';

const OverviewSpendSparkline = React.memo(({ 
  dateRange,
  breakdown = 'all',
  hierarchy = 'campaign',
  refreshTrigger = 0,
  width = 120,
  height = 40
}) => {
  // Use shared overview chart data
  const { chartData, loading, error } = useOverviewChartData(dateRange, breakdown, hierarchy, refreshTrigger);
  
  const [hoveredPoint, setHoveredPoint] = useState(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const svgRef = useRef(null);

  // Get spend intensity color based on value
  const getSpendColor = (spend) => {
    const spendValue = parseFloat(spend) || 0;
    
    if (spendValue < 100) return 'text-blue-200';
    if (spendValue >= 100 && spendValue < 300) return 'text-blue-300';
    if (spendValue >= 300 && spendValue < 500) return 'text-blue-400';
    if (spendValue >= 500 && spendValue < 1000) return 'text-blue-500';
    if (spendValue >= 1000 && spendValue < 2000) return 'text-blue-600';
    return 'text-blue-700';
  };



  // Handle mouse move over SVG
  const handleMouseMove = useCallback((event) => {
    if (!svgRef.current || chartData.length < 2) return;
    
    const svgRect = svgRef.current.getBoundingClientRect();
    const mouseX = event.clientX - svgRect.left;
    
    const padding = 4;
    const dataWidth = width - 2 * padding;
    
    // Calculate which data point is closest
    const relativeX = Math.max(0, Math.min(dataWidth, mouseX - padding));
    const dataIndex = Math.round((relativeX / dataWidth) * (chartData.length - 1));
    
    if (dataIndex >= 0 && dataIndex < chartData.length) {
      setHoveredPoint(dataIndex);
      setTooltipPosition({ 
        x: event.clientX - 180,
        y: event.clientY - 160
      });
    }
  }, [chartData.length, width]);

  const handleMouseLeave = useCallback(() => {
    setHoveredPoint(null);
  }, []);

  // Format date for tooltip
  const formatDate = (dateStr) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { 
      weekday: 'short', 
      month: 'short', 
      day: 'numeric' 
    });
  };

  // Format spend value
  const formatSpend = (value) => {
    const spend = parseFloat(value) || 0;
    return spend.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
  };

  // Check if we have enough valid data for sparkline
  const hasEnoughData = chartData.length >= 2 && chartData.some(d => 
    parseFloat(d.rolling_1d_spend) > 0
  );
  
  // Debug logging
  console.log('📊 SpendSparkline data:', { 
    chartDataLength: chartData.length, 
    hasEnoughData, 
    loading, 
    error,
    sampleData: chartData.slice(0, 2)
  });

  if (loading) {
    return (
      <div className={`bg-blue-200 animate-pulse rounded flex items-center justify-center text-xs`} style={{width, height}}>
        ⏳
      </div>
    );
  }

  if (error || !hasEnoughData) {
    return (
      <div className={`flex items-center justify-center text-gray-400 text-xs border border-gray-300 rounded`} style={{width, height}}>
        <svg width={width} height={height} className="overflow-visible">
          <line
            x1="4"
            y1={height / 2}
            x2={width - 4}
            y2={height / 2}
            stroke="currentColor"
            strokeWidth="1"
            strokeDasharray="2,2"
            className="text-gray-300 dark:text-gray-600"
          />
        </svg>
      </div>
    );
  }

  // Render sparkline
  const padding = 4;
  const values = chartData.map(d => parseFloat(d.rolling_1d_spend) || 0);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = maxValue - minValue || 0.1;

  const points = values.map((value, index) => {
    const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
    const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
    return `${x},${y}`;
  });

  // Create colored segments
  const segments = [];
  for (let i = 0; i < points.length - 1; i++) {
    const [startX, startY] = points[i].split(',').map(Number);
    const [endX, endY] = points[i + 1].split(',').map(Number);
    
    const midX = (startX + endX) / 2;
    const midY = (startY + endY) / 2;
    const midPoint = `${midX},${midY}`;
    
    const startSpend = values[i];
    const endSpend = values[i + 1];
    
    const startColor = getSpendColor(startSpend);
    const endColor = getSpendColor(endSpend);
    
    segments.push({
      path: `M ${points[i]} L ${midPoint}`,
      color: startColor
    });
    
    segments.push({
      path: `M ${midPoint} L ${points[i + 1]}`,
      color: endColor
    });
  }

  return (
    <div className="relative">
      <svg 
        ref={svgRef}
        width={width} 
        height={height} 
        className="overflow-visible cursor-crosshair"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      >
        {/* Weekly reference lines */}
        {[7, 14, 21].map((day, index) => {
          const x = padding + (day / 28) * (width - 2 * padding);
          return (
            <line
              key={index}
              x1={x}
              y1={padding}
              x2={x}
              y2={height - padding}
              stroke="#FFFFFF"
              strokeWidth="1"
              strokeDasharray="2,2"
              style={{ opacity: 0.5 }}
            />
          );
        })}
        
        {/* Range indicators */}
        <text
          x={width - 2}
          y={padding + 8}
          fontSize="8"
          fill="currentColor"
          textAnchor="end"
          className="text-gray-400 dark:text-gray-500"
        >
          ${Math.round(maxValue).toLocaleString()}
        </text>
        <text
          x={width - 2}
          y={height - padding - 2}
          fontSize="8"
          fill="currentColor"
          textAnchor="end"
          className="text-gray-400 dark:text-gray-500"
        >
          ${Math.round(minValue).toLocaleString()}
        </text>

        {/* Render colored segments */}
        {segments.map((segment, index) => (
          <path
            key={index}
            d={segment.path}
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            className={segment.color}
          />
        ))}
        
        {/* Data points */}
        {values.map((value, index) => {
          const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
          const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
          const isHovered = hoveredPoint === index;
          const dayColor = getSpendColor(value);
          
          return (
            <circle
              key={index}
              cx={x}
              cy={y}
              r={isHovered ? "3" : "1.5"}
              fill="currentColor"
              className={dayColor}
              style={{
                filter: isHovered ? 'drop-shadow(0 0 3px rgba(0,0,0,0.3))' : 'none'
              }}
            />
          );
        })}
      </svg>
      
      {/* Tooltip */}
      {hoveredPoint !== null && (
        <div 
          className="fixed z-50 bg-gray-900 text-white text-xs rounded px-2 py-1 pointer-events-none shadow-lg border border-gray-700"
          style={{
            left: tooltipPosition.x,
            top: tooltipPosition.y,
            maxWidth: '200px'
          }}
        >
          {(() => {
            const dayData = chartData[hoveredPoint];
            const backendSpend = parseFloat(dayData.rolling_1d_spend) || 0;
            
            return (
              <>
                <div className="font-medium text-white">
                  {formatDate(dayData.date)}
                </div>
                <div className={getSpendColor(backendSpend).replace('text-', 'text-').replace(/\d00/, '300')}>
                  Daily Spend: {formatSpend(backendSpend)}
                </div>
                <div className="text-gray-300 text-xs">
                  Daily ROAS: {(parseFloat(dayData.rolling_1d_roas) || 0).toFixed(2)}
                </div>
                <div className="text-gray-300 text-xs">
                  Daily Est. Revenue: ${(parseFloat(dayData.rolling_1d_revenue) || 0).toFixed(2)}
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
});

export default OverviewSpendSparkline; 