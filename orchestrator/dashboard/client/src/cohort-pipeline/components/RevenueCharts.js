import React, { useState } from 'react';

const RevenueCharts = ({ data }) => {
    const [selectedChart, setSelectedChart] = useState('daily');

    if (!data?.data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No data available for revenue charts. Run an analysis to see results.
                </p>
            </div>
        );
    }

    const timelineData = data.data.timeline_data;
    const arpuData = data.data.arpu_data;

    // Prepare daily revenue data
    const dailyRevenueData = timelineData?.dates?.map(date => ({
        date,
        revenue: timelineData.daily_metrics?.[date]?.revenue || 0,
        trial_events: timelineData.daily_metrics?.[date]?.trial_events || 0,
        purchase_events: timelineData.daily_metrics?.[date]?.purchase_events || 0
    })) || [];

    // Prepare ARPU by product data
    const productARPUData = arpuData?.per_product ? 
        Object.entries(arpuData.per_product).map(([productId, productData]) => ({
            productId: productId.length > 20 ? productId.substring(0, 20) + '...' : productId,
            fullProductId: productId,
            arpu: productData.arpu || 0,
            totalRevenue: productData.total_revenue || 0,
            payingUsers: productData.paying_users || 0
        })).sort((a, b) => b.arpu - a.arpu) : [];

    const formatCurrency = (value) => {
        if (value === null || value === undefined) return '$0.00';
        return `$${Number(value).toFixed(2)}`;
    };

    const formatDate = (dateStr) => {
        return new Date(dateStr).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    };

    // Simple bar chart component
    const BarChart = ({ data, valueKey, labelKey, title, color = 'blue' }) => {
        if (!data || data.length === 0) return null;

        const maxValue = Math.max(...data.map(item => item[valueKey]));
        const colorClasses = {
            blue: 'bg-blue-500',
            green: 'bg-green-500',
            purple: 'bg-purple-500',
            orange: 'bg-orange-500'
        };

        return (
            <div className="space-y-4">
                <h4 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h4>
                <div className="space-y-2">
                    {data.slice(0, 10).map((item, index) => {
                        const percentage = maxValue > 0 ? (item[valueKey] / maxValue) * 100 : 0;
                        return (
                            <div key={index} className="flex items-center space-x-3">
                                <div className="w-24 text-xs text-gray-600 dark:text-gray-400 truncate">
                                    {item[labelKey]}
                                </div>
                                <div className="flex-1 bg-gray-200 dark:bg-gray-700 rounded-full h-6 relative">
                                    <div 
                                        className={`${colorClasses[color]} h-6 rounded-full transition-all duration-300`}
                                        style={{ width: `${percentage}%` }}
                                    ></div>
                                    <div className="absolute inset-0 flex items-center justify-center text-xs font-medium text-white">
                                        {valueKey === 'revenue' || valueKey === 'arpu' || valueKey === 'totalRevenue' ? 
                                            formatCurrency(item[valueKey]) : 
                                            item[valueKey]
                                        }
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    };

    // Simple line chart component
    const LineChart = ({ data, valueKey, title, color = 'blue' }) => {
        if (!data || data.length === 0) return null;

        const maxValue = Math.max(...data.map(item => item[valueKey]));
        const minValue = Math.min(...data.map(item => item[valueKey]));
        const range = maxValue - minValue;

        const colorClasses = {
            blue: 'stroke-blue-500 fill-blue-100',
            green: 'stroke-green-500 fill-green-100',
            purple: 'stroke-purple-500 fill-purple-100',
            orange: 'stroke-orange-500 fill-orange-100'
        };

        return (
            <div className="space-y-4">
                <h4 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h4>
                <div className="bg-white dark:bg-gray-800 p-4 rounded-lg border border-gray-200 dark:border-gray-700">
                    <svg width="100%" height="200" className="overflow-visible">
                        {/* Grid lines */}
                        {[0, 25, 50, 75, 100].map(y => (
                            <line 
                                key={y} 
                                x1="0" 
                                y1={y * 2} 
                                x2="100%" 
                                y2={y * 2} 
                                stroke="#e5e7eb" 
                                strokeWidth="1"
                            />
                        ))}
                        
                        {/* Data line */}
                        <polyline
                            fill="none"
                            stroke={color === 'blue' ? '#3b82f6' : color === 'green' ? '#10b981' : color === 'purple' ? '#8b5cf6' : '#f59e0b'}
                            strokeWidth="2"
                            points={data.map((item, index) => {
                                const x = (index / (data.length - 1)) * 100;
                                const y = range > 0 ? 200 - ((item[valueKey] - minValue) / range) * 180 : 100;
                                return `${x}%,${y}`;
                            }).join(' ')}
                        />
                        
                        {/* Data points */}
                        {data.map((item, index) => {
                            const x = (index / (data.length - 1)) * 100;
                            const y = range > 0 ? 200 - ((item[valueKey] - minValue) / range) * 180 : 100;
                            return (
                                <circle
                                    key={index}
                                    cx={`${x}%`}
                                    cy={y}
                                    r="4"
                                    fill={color === 'blue' ? '#3b82f6' : color === 'green' ? '#10b981' : color === 'purple' ? '#8b5cf6' : '#f59e0b'}
                                    className="hover:r-6 transition-all"
                                >
                                    <title>{`${item.date}: ${valueKey === 'revenue' ? formatCurrency(item[valueKey]) : item[valueKey]}`}</title>
                                </circle>
                            );
                        })}
                    </svg>
                    
                    {/* X-axis labels */}
                    <div className="flex justify-between mt-2 text-xs text-gray-500 dark:text-gray-400">
                        {data.filter((_, index) => index % Math.ceil(data.length / 5) === 0).map((item, index) => (
                            <span key={index}>{formatDate(item.date)}</span>
                        ))}
                    </div>
                </div>
            </div>
        );
    };

    return (
        <div className="space-y-6">
            {/* Chart Type Selector */}
            <div className="flex space-x-4 border-b border-gray-200 dark:border-gray-700">
                {[
                    { id: 'daily', label: 'Daily Revenue' },
                    { id: 'arpu', label: 'ARPU by Product' },
                    { id: 'events', label: 'Event Trends' }
                ].map(chart => (
                    <button
                        key={chart.id}
                        onClick={() => setSelectedChart(chart.id)}
                        className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                            selectedChart === chart.id
                                ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                                : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
                        }`}
                    >
                        {chart.label}
                    </button>
                ))}
            </div>

            {/* Chart Content */}
            <div className="min-h-96">
                {selectedChart === 'daily' && (
                    <LineChart
                        data={dailyRevenueData}
                        valueKey="revenue"
                        title="Daily Revenue Trend"
                        color="green"
                    />
                )}

                {selectedChart === 'arpu' && (
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        <BarChart
                            data={productARPUData}
                            valueKey="arpu"
                            labelKey="productId"
                            title="ARPU by Product"
                            color="purple"
                        />
                        <BarChart
                            data={productARPUData}
                            valueKey="totalRevenue"
                            labelKey="productId"
                            title="Total Revenue by Product"
                            color="green"
                        />
                    </div>
                )}

                {selectedChart === 'events' && (
                    <div className="space-y-6">
                        <LineChart
                            data={dailyRevenueData}
                            valueKey="trial_events"
                            title="Daily Trial Events"
                            color="blue"
                        />
                        <LineChart
                            data={dailyRevenueData}
                            valueKey="purchase_events"
                            title="Daily Purchase Events"
                            color="orange"
                        />
                    </div>
                )}
            </div>

            {/* Summary Statistics */}
            <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
                    Revenue Summary
                </h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                    <div>
                        <div className="text-gray-600 dark:text-gray-400">Total Revenue</div>
                        <div className="font-semibold text-gray-900 dark:text-white">
                            {formatCurrency(arpuData?.cohort_wide?.total_revenue)}
                        </div>
                    </div>
                    <div>
                        <div className="text-gray-600 dark:text-gray-400">Average ARPU</div>
                        <div className="font-semibold text-gray-900 dark:text-white">
                            {formatCurrency(arpuData?.cohort_wide?.arpu)}
                        </div>
                    </div>
                    <div>
                        <div className="text-gray-600 dark:text-gray-400">Paying Users</div>
                        <div className="font-semibold text-gray-900 dark:text-white">
                            {arpuData?.cohort_wide?.paying_users || 0}
                        </div>
                    </div>
                    <div>
                        <div className="text-gray-600 dark:text-gray-400">Products</div>
                        <div className="font-semibold text-gray-900 dark:text-white">
                            {productARPUData.length}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default RevenueCharts; 