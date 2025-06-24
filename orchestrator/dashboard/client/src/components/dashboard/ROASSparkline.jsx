import React, { useState, useEffect, useRef } from 'react';
import { dashboardApi } from '../../services/dashboardApi';

const ROASSparkline = ({ 
  entityType, 
  entityId, 
  currentROAS,
  currentPerformanceImpact = null,
  conversionCount = 0,
  breakdown = 'all',
  startDate,
  endDate,
  type = 'roas' // 'roas' or 'performance_impact'
}) => {
  const [chartData, setChartData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hoveredPoint, setHoveredPoint] = useState(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const svgRef = useRef(null);

  // Get ROAS performance color with intensity based on conversion count
  const getROASPerformanceColor = (roas, conversions = 0) => {
    const roasValue = parseFloat(roas) || 0;
    const hasSignificantData = conversions >= 5; // 5+ conversions = darker, <5 = lighter
    
    if (roasValue < 1.0) {
      return hasSignificantData ? 'text-red-600' : 'text-red-400';
    }
    if (roasValue >= 1.0 && roasValue < 1.5) {
      return hasSignificantData ? 'text-yellow-600' : 'text-yellow-400';
    }
    // >= 1.5
    return hasSignificantData ? 'text-green-600' : 'text-green-400';
  };

  // Get Performance Impact Score color (7-tier system matching main column)
  const getPerformanceImpactColor = (score) => {
    const value = parseFloat(score) || 0;
    
    if (value >= 7500) {
      return 'text-purple-600'; // Purple
    } else if (value >= 2500) {
      return 'text-blue-600'; // Blue  
    } else if (value >= 1000) {
      return 'text-green-600'; // Green
    } else if (value >= 500) {
      return 'text-yellow-600'; // Yellow
    } else if (value >= 200) {
      return 'text-orange-600'; // Orange
    } else if (value >= 50) {
      return 'text-red-600'; // Red
    } else {
      return 'text-gray-600'; // Grey
    }
  };

  // Load chart data
  useEffect(() => {
    const loadChartData = async () => {
      if (!entityId || !startDate || !endDate) {
        return;
      }
      
      setLoading(true);
      setError(null);
      
      try {
        const apiParams = {
          entity_type: entityType,
          entity_id: entityId,
          breakdown: breakdown,
          start_date: startDate,
          end_date: endDate
        };
        
        const response = await dashboardApi.getAnalyticsChartData(apiParams);
        
        if (response && response.success && response.chart_data) {
          setChartData(response.chart_data);
        } else {
          setError('Invalid API response');
        }
      } catch (error) {
        setError(error.message);
      } finally {
        setLoading(false);
      }
    };

    loadChartData();
  }, [entityType, entityId, breakdown, startDate, endDate]);

  // Handle mouse move over SVG
  const handleMouseMove = (event) => {
    if (!svgRef.current || chartData.length < 2) return;
    
    const svgRect = svgRef.current.getBoundingClientRect();
    const mouseX = event.clientX - svgRect.left;
    
    const width = 60;
    const padding = 2;
    const dataWidth = width - 2 * padding;
    
    // Calculate which data point is closest
    const relativeX = Math.max(0, Math.min(dataWidth, mouseX - padding));
    const dataIndex = Math.round((relativeX / dataWidth) * (chartData.length - 1));
    
    if (dataIndex >= 0 && dataIndex < chartData.length) {
      setHoveredPoint(dataIndex);
      setTooltipPosition({ 
        x: event.clientX - 180, // Offset 180px to the left
        y: event.clientY - 120  // Move tooltip much higher above cursor
      });
    }
  };

  const handleMouseLeave = () => {
    setHoveredPoint(null);
  };

  // Format date for tooltip
  const formatDate = (dateStr) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  // Format ROAS value
  const formatROAS = (value) => {
    const roas = parseFloat(value) || 0;
    return roas.toFixed(2);
  };

  // Format Performance Impact Score value
  const formatPerformanceImpact = (value) => {
    const score = parseFloat(value) || 0;
    return score.toLocaleString(undefined, { maximumFractionDigits: 0 });
  };

  // Get formatted value based on type
  const getFormattedValue = (value) => {
    return type === 'performance_impact' ? formatPerformanceImpact(value) : formatROAS(value);
  };

  // Calculate current value and color based on type
  const currentValue = type === 'performance_impact' ? currentPerformanceImpact : currentROAS;
  const colorClass = type === 'performance_impact' 
    ? getPerformanceImpactColor(currentValue)
    : getROASPerformanceColor(currentROAS, conversionCount);
  
  // Check if we have enough valid data for sparkline
  const hasEnoughData = chartData.length >= 2 && chartData.some(d => 
    parseFloat(d.daily_roas) > 0 || parseFloat(d.daily_spend) > 0 || parseFloat(d.daily_estimated_revenue) > 0
  );

  return (
    <div className="flex items-center space-x-3 min-w-[120px] relative">
      {/* Current Value Display */}
      <span className={`font-medium text-sm ${colorClass}`}>
        {getFormattedValue(currentValue)}
      </span>
      
      {/* Sparkline Area */}
      <div className="min-w-[60px] relative">
        {loading ? (
          <div className="w-[60px] h-[20px] bg-blue-200 animate-pulse rounded flex items-center justify-center text-xs">
            ⏳
          </div>
        ) : error ? (
          <div className="w-[60px] h-[20px] flex items-center justify-center text-red-400 text-xs border border-red-300">
            ❌
          </div>
        ) : hasEnoughData ? (
          // Real sparkline using actual data
          (() => {
            const width = 60;
            const height = 20;
            const padding = 2;
            
            // Calculate values based on type
            const values = chartData.map(d => {
              if (type === 'performance_impact') {
                const spend = parseFloat(d.daily_spend) || 0;
                const roas = parseFloat(d.daily_roas) || 0;
                return spend * (roas * roas); // spend × ROAS²
              }
              return parseFloat(d.daily_roas) || 0;
            });
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const range = maxValue - minValue || 0.1; // Prevent division by zero
            
            const points = values.map((value, index) => {
              const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
              const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
              return `${x},${y}`;
            });
            
            // Create colored segments with each half matching its respective endpoint
            const segments = [];
            for (let i = 0; i < points.length - 1; i++) {
              const [startX, startY] = points[i].split(',').map(Number);
              const [endX, endY] = points[i + 1].split(',').map(Number);
              
              // Calculate midpoint
              const midX = (startX + endX) / 2;
              const midY = (startY + endY) / 2;
              const midPoint = `${midX},${midY}`;
              
              // Get colors for start and end points based on type
              const startValue = values[i];
              const endValue = values[i + 1];
              const startConversions = parseInt(chartData[i].daily_mixpanel_purchases) || 0;
              const endConversions = parseInt(chartData[i + 1].daily_mixpanel_purchases) || 0;
              
              // Use grey styling for inactive periods (before first spend / after last spend)
              const startIsInactive = chartData[i].is_inactive || false;
              const endIsInactive = chartData[i + 1].is_inactive || false;
              
              const startColor = startIsInactive ? 
                'text-gray-300 dark:text-gray-600' : 
                (type === 'performance_impact' ? getPerformanceImpactColor(startValue) : getROASPerformanceColor(startValue, startConversions));
              const endColor = endIsInactive ? 
                'text-gray-300 dark:text-gray-600' : 
                (type === 'performance_impact' ? getPerformanceImpactColor(endValue) : getROASPerformanceColor(endValue, endConversions));
              
              // First half: from start point to midpoint (colored like start point)
              segments.push({
                path: `M ${points[i]} L ${midPoint}`,
                color: startColor,
                isInactive: startIsInactive
              });
              
              // Second half: from midpoint to end point (colored like end point)
              segments.push({
                path: `M ${midPoint} L ${points[i + 1]}`,
                color: endColor,
                isInactive: endIsInactive
              });
            }
            
            return (
              <>
                <svg 
                  ref={svgRef}
                  width="60" 
                  height="20" 
                  className="overflow-visible cursor-crosshair"
                  onMouseMove={handleMouseMove}
                  onMouseLeave={handleMouseLeave}
                >
                  {/* Center reference line - behind the main sparkline */}
                  <line
                    x1={width / 2}
                    y1={padding}
                    x2={width / 2}
                    y2={height - padding}
                    stroke="#FFFFFF"
                    strokeWidth="1"
                    strokeDasharray="2,2"
                    style={{ opacity: 0.5 }}
                  />
                  {/* Render each segment with its own color */}
                  {segments.map((segment, index) => (
                    <path
                      key={index}
                      d={segment.path}
                      fill="none"
                      stroke="currentColor"
                      strokeWidth={segment.isInactive ? "1" : "1.5"}
                      strokeDasharray={segment.isInactive ? "2,2" : "none"}
                      className={segment.color}
                      style={{
                        opacity: segment.isInactive ? 0.3 : 1
                      }}
                    />
                  ))}
                  {values.map((value, index) => {
                    const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
                    const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
                    const isHovered = hoveredPoint === index;
                    const dayConversions = parseInt(chartData[index].daily_mixpanel_purchases) || 0;
                    const isInactive = chartData[index].is_inactive || false;
                    const dayColor = isInactive ? 
                      'text-gray-300 dark:text-gray-600' : 
                      (type === 'performance_impact' ? getPerformanceImpactColor(value) : getROASPerformanceColor(value, dayConversions));
                    
                    return (
                      <circle
                        key={index}
                        cx={x}
                        cy={y}
                        r={isHovered ? "2.5" : (isInactive ? "0.5" : "1")}
                        fill="currentColor"
                        className={dayColor}
                        style={{
                          filter: isHovered ? 'drop-shadow(0 0 3px rgba(0,0,0,0.3))' : 'none',
                          opacity: isInactive ? 0.3 : 1
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
                      left: tooltipPosition.x, // Use the already offset position
                      top: tooltipPosition.y - 10, // Slight upward offset
                      transform: 'none',
                      maxWidth: '200px' // Prevent tooltip from being too wide
                    }}
                  >
                    {(() => {
                      const dayData = chartData[hoveredPoint];
                      const spend = parseFloat(dayData.daily_spend) || 0;
                      const backendROAS = parseFloat(dayData.daily_roas) || 0;
                      const performanceImpact = spend * (backendROAS * backendROAS);
                      
                      return (
                        <>
                          <div className="font-medium text-white">
                            {formatDate(dayData.date)}
                          </div>
                          {type === 'performance_impact' ? (
                            <div className={getPerformanceImpactColor(performanceImpact).replace('text-', 'text-').replace('600', '400')}>
                              Performance Impact: {formatPerformanceImpact(performanceImpact)}
                            </div>
                          ) : (
                            <div className={getROASPerformanceColor(backendROAS, dayData.daily_mixpanel_purchases || 0).replace('text-', 'text-').replace('600', '400')}>
                              ROAS: {formatROAS(backendROAS)}
                            </div>
                          )}
                          {type === 'performance_impact' && (
                            <div className="text-gray-300 text-xs">
                              ROAS: {formatROAS(backendROAS)} × Spend: ${spend.toFixed(2)}
                            </div>
                          )}
                        </>
                      );
                    })()}
                          <div className="text-gray-300 text-xs">
                            Spend: ${(parseFloat(chartData[hoveredPoint].daily_spend) || 0).toFixed(2)}
                          </div>
                          <div className="text-gray-300 text-xs">
                            Revenue: ${(parseFloat(chartData[hoveredPoint].daily_estimated_revenue) || 0).toFixed(2)}
                          </div>
                          {/* Display accuracy ratio using same logic as main dashboard */}
                          {chartData[hoveredPoint].period_accuracy_ratio && chartData[hoveredPoint].period_accuracy_ratio !== 1.0 && (
                            <div className="text-gray-400 text-xs">
                              {(() => {
                                const eventPriority = chartData[hoveredPoint].event_priority || 'trials';
                                const ratioLabel = eventPriority === 'purchases' ? 'Purchase Accuracy' : 'Trial Accuracy';
                                const accuracyRatio = (parseFloat(chartData[hoveredPoint].period_accuracy_ratio) * 100).toFixed(1);
                                
                                return `${ratioLabel}: ${accuracyRatio}% (MP/Meta)`;
                              })()}
                            </div>
                          )}
          {/* Show conversion counts for confidence assessment */}
          <div className="text-green-300 text-xs">
            Conversions: {chartData[hoveredPoint].daily_mixpanel_purchases || 0}
          </div>
          <div className="text-blue-300 text-xs">
            Trials: {chartData[hoveredPoint].daily_mixpanel_trials || 0}
          </div>
          {/* Show Meta comparison if available */}
          {chartData[hoveredPoint].daily_meta_trials && (
            <div className="text-gray-400 text-xs">
              Meta Trials: {chartData[hoveredPoint].daily_meta_trials}
            </div>
          )}
                  </div>
                )}
              </>
            );
          })()
        ) : (
          // No data placeholder - subtle dashed gray line
          <div className="w-[60px] h-[20px] flex items-center justify-center">
            <svg width="60" height="20" className="overflow-visible">
              <line
                x1="2"
                y1="10"
                x2="58"
                y2="10"
                stroke="currentColor"
                strokeWidth="1"
                strokeDasharray="2,2"
                className="text-gray-300 dark:text-gray-600"
              />
            </svg>
          </div>
        )}
      </div>
    </div>
  );
};

export default ROASSparkline; 