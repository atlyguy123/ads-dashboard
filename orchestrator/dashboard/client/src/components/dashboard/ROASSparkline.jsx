import React, { useState, useEffect, useRef } from 'react';
import { dashboardApi } from '../../services/dashboardApi';

const ROASSparkline = ({ 
  entityType, 
  entityId, 
  currentROAS,
  conversionCount = 0,
  breakdown = 'all',
  startDate,
  endDate 
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

  // Calculate limited date range (max 14 days)
  const getLimitedDateRange = (start, end) => {
    const startDate = new Date(start);
    const endDate = new Date(end);
    const daysDiff = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24));
    
    if (daysDiff <= 14) {
      return { start, end };
    }
    
    // Take the last 14 days from the end date
    const limitedStart = new Date(endDate);
    limitedStart.setDate(limitedStart.getDate() - 13); // 13 days back + end date = 14 days
    
    return {
      start: limitedStart.toISOString().split('T')[0],
      end
    };
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
        // Limit to 14 days maximum
        const { start: limitedStart, end: limitedEnd } = getLimitedDateRange(startDate, endDate);
        
        const apiParams = {
          entity_type: entityType,
          entity_id: entityId,
          breakdown: breakdown,
          start_date: limitedStart,
          end_date: limitedEnd
        };
        
        const response = await dashboardApi.getAnalyticsChartData(apiParams);
        
        if (response && response.success && response.chart_data) {
          setChartData(response.chart_data);
        } else {
          console.error('ROASSparkline: Invalid API response for', entityId, response);
          setError('Invalid API response');
        }
      } catch (error) {
        console.error('ROASSparkline: API call failed for', entityId, error.message);
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
        x: event.clientX, 
        y: event.clientY - 60 
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

  // Calculate current ROAS color (using conversionCount passed as prop)
  const colorClass = getROASPerformanceColor(currentROAS, conversionCount);
  
  // Check if we have enough valid data for sparkline
  const hasEnoughData = chartData.length >= 2 && chartData.some(d => 
    parseFloat(d.daily_roas) > 0 || parseFloat(d.daily_spend) > 0 || parseFloat(d.daily_estimated_revenue) > 0
  );

  return (
    <div className="flex items-center space-x-3 min-w-[120px] relative">
      {/* ROAS Value */}
      <span className={`font-medium text-sm ${colorClass}`}>
        {formatROAS(currentROAS)}
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
            
            const values = chartData.map(d => parseFloat(d.daily_roas) || 0);
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const range = maxValue - minValue || 0.1; // Prevent division by zero
            
            const points = values.map((value, index) => {
              const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
              const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
              return `${x},${y}`;
            });
            
            // Create colored segments using midpoint approach
            const segments = [];
            for (let i = 0; i < points.length - 1; i++) {
              // Calculate midpoint ROAS and conversion values for segment coloring
              const startROAS = values[i];
              const endROAS = values[i + 1];
              const midpointROAS = (startROAS + endROAS) / 2;
              
              const startConversions = parseInt(chartData[i].daily_mixpanel_purchases) || 0;
              const endConversions = parseInt(chartData[i + 1].daily_mixpanel_purchases) || 0;
              const midpointConversions = Math.round((startConversions + endConversions) / 2);
              
              const segmentColor = getROASPerformanceColor(midpointROAS, midpointConversions);
              segments.push({
                path: `M ${points[i]} L ${points[i + 1]}`,
                color: segmentColor
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
                  {/* Render each segment with its own color */}
                  {segments.map((segment, index) => (
                    <path
                      key={index}
                      d={segment.path}
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      className={segment.color}
                    />
                  ))}
                  {values.map((value, index) => {
                    const x = padding + (index / (values.length - 1)) * (width - 2 * padding);
                    const y = height - padding - ((value - minValue) / range) * (height - 2 * padding);
                    const isHovered = hoveredPoint === index;
                    const dayConversions = parseInt(chartData[index].daily_mixpanel_purchases) || 0;
                    const dayColor = getROASPerformanceColor(value, dayConversions);
                    
                    return (
                      <circle
                        key={index}
                        cx={x}
                        cy={y}
                        r={isHovered ? "2.5" : "1"}
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
                    className="fixed z-50 bg-gray-900 text-white text-xs rounded px-2 py-1 pointer-events-none shadow-lg"
                    style={{
                      left: tooltipPosition.x - 120, // Move tooltip to the left of cursor
                      top: tooltipPosition.y,
                      transform: 'none' // Remove centering transform
                    }}
                  >
                    {(() => {
                      const dayData = chartData[hoveredPoint];
                      const spend = parseFloat(dayData.daily_spend) || 0;
                      const revenue = parseFloat(dayData.daily_estimated_revenue) || 0;
                      const backendROAS = parseFloat(dayData.daily_roas) || 0;
                      
                      // Verify ROAS calculation
                      const calculatedROAS = spend > 0 ? revenue / spend : 0;
                      const roasMatch = Math.abs(backendROAS - calculatedROAS) < 0.01;
                      
                      return (
                        <>
                          <div className="font-medium text-white">
                            {formatDate(dayData.date)}
                          </div>
                          <div className={getROASPerformanceColor(backendROAS, dayData.daily_mixpanel_purchases || 0).replace('text-', 'text-').replace('600', '400')}>
                            ROAS: {formatROAS(backendROAS)}
                            {!roasMatch && (
                              <span className="text-yellow-400 ml-1" title={`Calculated: ${calculatedROAS.toFixed(2)}`}>
                                ⚠️
                              </span>
                            )}
                          </div>
                        </>
                      );
                    })()}
                          <div className="text-gray-300 text-xs">
                            Spend: ${(parseFloat(chartData[hoveredPoint].daily_spend) || 0).toFixed(2)}
                          </div>
                          <div className="text-gray-300 text-xs">
                            Revenue: ${(parseFloat(chartData[hoveredPoint].daily_estimated_revenue) || 0).toFixed(2)}
                          </div>
                          {(() => {
                            const spend = parseFloat(chartData[hoveredPoint].daily_spend) || 0;
                            const revenue = parseFloat(chartData[hoveredPoint].daily_estimated_revenue) || 0;
                            const accuracyRatio = parseFloat(chartData[hoveredPoint].period_accuracy_ratio) || 0;
                            
                            if (spend > 0) {
                              const baseROAS = (revenue / spend).toFixed(2);
                              
                              if (accuracyRatio > 0 && accuracyRatio !== 1.0) {
                                const adjustedRevenue = revenue / accuracyRatio;
                                const adjustedROAS = (adjustedRevenue / spend).toFixed(2);
                                return (
                                  <div className="text-gray-400 text-xs">
                                    <div>Base: ${revenue.toFixed(2)} ÷ ${spend.toFixed(2)} = {baseROAS}</div>
                                    <div>Ratio: {(accuracyRatio * 100).toFixed(1)}% (MP/Meta)</div>
                                    <div>Adj: ${adjustedRevenue.toFixed(2)} ÷ ${spend.toFixed(2)} = {adjustedROAS}</div>
                                  </div>
                                );
                              } else {
                                return (
                                  <div className="text-gray-400 text-xs">
                                    Calc: ${revenue.toFixed(2)} ÷ ${spend.toFixed(2)} = {baseROAS}
                                  </div>
                                );
                              }
                            }
                            return null;
                          })()}
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