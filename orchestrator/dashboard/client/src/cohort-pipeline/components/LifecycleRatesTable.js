import React, { useState } from 'react';
import { LineChart, X, HelpCircle } from 'lucide-react';
import { LineChart as RechartsLineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const LifecycleRatesTable = ({ data }) => {
    const [globalSelectedProduct, setGlobalSelectedProduct] = useState(null);
    const [showChartModal, setShowChartModal] = useState(false);
    const [chartModalData, setChartModalData] = useState(null);
    const [showBreakdownModal, setShowBreakdownModal] = useState(false);
    const [breakdownModalData, setBreakdownModalData] = useState(null);

    // Handle different data structures - debug mode vs regular mode
    let lifecycleData = null;

    // Debug: Log the actual data structure
    console.log('LifecycleRatesTable received data:', data);

    if (data?.data?.lifecycle_rates) {
        // Regular mode: data.data.lifecycle_rates
        lifecycleData = data.data.lifecycle_rates;
        console.log('Using regular mode lifecycle_rates:', lifecycleData);
    } else if (data?.data?.structured_format?.lifecycle_analysis) {
        // Debug mode: data.data.structured_format.lifecycle_analysis
        lifecycleData = data.data.structured_format.lifecycle_analysis;
        console.log('Using debug mode structured_format.lifecycle_analysis:', lifecycleData);
    } else if (data?.data?.legacy_format) {
        // Debug mode: data.data.legacy_format (fallback)
        const legacyData = data.data.legacy_format;
        console.log('Using debug mode legacy_format:', legacyData);
        lifecycleData = {
            aggregate: {
                trial_to_outcome: {
                    rates_by_day: legacyData.lifecycle_rates || {},
                    smoothing_method: legacyData.smoothing_method || 'none',
                    smoothing_quality: legacyData.smoothing_quality || 'N/A',
                    total_sample_size: legacyData.total_sample_size || 0
                }
            },
            per_product: {},
            metadata: {
                calculation_method: 'legacy_format',
                message: 'Data from legacy format'
            }
        };
    } else if (data?.structured_format?.lifecycle_analysis) {
        // Debug mode: structured_format.lifecycle_analysis (direct)
        lifecycleData = data.structured_format.lifecycle_analysis;
        console.log('Using direct structured_format.lifecycle_analysis:', lifecycleData);
    } else if (data?.legacy_format) {
        // Debug mode: legacy_format (direct)
        const legacyData = data.legacy_format;
        console.log('Using direct legacy_format:', legacyData);
        lifecycleData = {
            aggregate: {
                trial_to_outcome: {
                    rates_by_day: legacyData.lifecycle_rates || {},
                    smoothing_method: legacyData.smoothing_method || 'none',
                    smoothing_quality: legacyData.smoothing_quality || 'N/A',
                    total_sample_size: legacyData.total_sample_size || 0
                }
            },
            per_product: {},
            metadata: {
                calculation_method: 'legacy_format',
                message: 'Data from legacy format'
            }
        };
    }

    console.log('Final lifecycleData:', lifecycleData);

    if (!lifecycleData) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No lifecycle data available. Run an analysis to see results.
                </p>
            </div>
        );
    }

    // The backend returns: { aggregate: { trial_to_outcome: {...}, ... }, per_product: { product_id: { trial_to_outcome: {...}, ... } } }
    const aggregate = lifecycleData.aggregate || {};
    const perProduct = lifecycleData.per_product || {};

    console.log('Aggregate data:', aggregate);
    console.log('Per product data:', perProduct);

    // Function to extract lookback information from rate calculation data
    const extractLookbackInfo = (rateCalculationData, tableTitle) => {
        // The backend stores lookback info in the rate calculation results
        // Look for extension_days_used and user count breakdown
        
        if (!rateCalculationData) return null;
        
        // Check if this is the new product-based format from lifecycle_calculator.py
        const aggregateRates = rateCalculationData.aggregate_rates;
        const extensionInfo = rateCalculationData.extension_info;
        
        if (aggregateRates && extensionInfo) {
            // Extract extension information from the backend data
            const extensionDays = aggregateRates.extension_days_used || extensionInfo?.extension_days_used || 0;
            const totalSampleSize = aggregateRates.total_sample_size || extensionInfo?.total_sample_size || 0;
            const attempts = aggregateRates.extension_attempts || extensionInfo?.attempts_made || 1;
            const perProductInfo = extensionInfo?.per_product_info || {};
            
            if (extensionDays > 0 && Object.keys(perProductInfo).length > 0) {
                // Use real per-product breakdown from backend
                let breakdown = {};
                
                // Aggregate the breakdown from per-product info
                Object.values(perProductInfo).forEach(productInfo => {
                    const extensionDays = productInfo.extension_days_used || 0;
                    const sampleSize = productInfo.final_sample_size || 0;
                    
                    if (extensionDays === 0) {
                        breakdown['0'] = (breakdown['0'] || 0) + sampleSize;
                    } else if (extensionDays === 30) {
                        breakdown['30'] = (breakdown['30'] || 0) + sampleSize;
                    } else if (extensionDays === 60) {
                        breakdown['60'] = (breakdown['60'] || 0) + sampleSize;
                    }
                });
                
                return {
                    extensionDays,
                    totalSize: totalSampleSize,
                    breakdown,
                    attempts,
                    perProductInfo
                };
            }
        }
        
        return null;
    };

    // Function to simulate lookback information based on typical patterns (fallback only)
    const simulateLookbackInfo = (ratesData, tableTitle) => {
        // This is now only used as a fallback when backend data is not available
        const sampleSize = ratesData?.aggregate_rates?.total_sample_size || ratesData?.total_sample_size || 0;
        
        // Only simulate for very specific cases where we know lookback is typically needed
        if (tableTitle.includes('Renewal') && sampleSize < 10) {
            return {
                extensionDays: 60,
                totalSize: sampleSize,
                breakdown: {
                    '0': Math.floor(sampleSize * 0.3),
                    '30': Math.floor(sampleSize * 0.3),
                    '60': Math.floor(sampleSize * 0.4)
                }
            };
        }
        
        return null;
    };

    // Function to render sample size with lookback information
    const renderSampleSizeInfo = (displayRates, globalSelectedProduct, tableTitle, rateCalculationData, isShowingProductData, isShowingFallbackData) => {
        const sampleSize = displayRates.total_sample_size || 0;
        const lookbackInfo = extractLookbackInfo(rateCalculationData, tableTitle);

        // Check if we have product data but it's below threshold
        let productSampleSize = 0;
        let isBelowThreshold = false;
        if (globalSelectedProduct && rateCalculationData.rates_by_product && rateCalculationData.rates_by_product[globalSelectedProduct]) {
            productSampleSize = rateCalculationData.rates_by_product[globalSelectedProduct].total_sample_size || 0;
            isBelowThreshold = productSampleSize > 0 && productSampleSize < 10;
        }

        // Get the actual extension days used for the current display
        let actualExtensionDays = 0;
        if (isShowingProductData && globalSelectedProduct && rateCalculationData.rates_by_product && rateCalculationData.rates_by_product[globalSelectedProduct]) {
            actualExtensionDays = rateCalculationData.rates_by_product[globalSelectedProduct].extension_days_used || 0;
        } else if (!isShowingProductData && rateCalculationData.aggregate_rates) {
            actualExtensionDays = rateCalculationData.aggregate_rates.extension_days_used || 0;
        }

    return (
            <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                <div className="flex items-center justify-between">
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                        Based on <span className="font-semibold text-gray-900 dark:text-white">{sampleSize}</span> users
                        {isShowingProductData && (
                            <span className="ml-2 text-green-600 dark:text-green-400">
                                (Product: {globalSelectedProduct})
                            </span>
                        )}
                        {isShowingFallbackData && !isBelowThreshold && (
                            <span className="ml-2 text-orange-600 dark:text-orange-400">
                                (Aggregate data - {globalSelectedProduct} has no data for this lifecycle)
                            </span>
                        )}
                        {isShowingFallbackData && isBelowThreshold && (
                            <span className="ml-2 text-orange-600 dark:text-orange-400">
                                (Aggregate data - {globalSelectedProduct} has only {productSampleSize} users, minimum 10 required)
                            </span>
                        )}
                        {!globalSelectedProduct && (
                            <span className="ml-2 text-blue-600 dark:text-blue-400">
                                (All Products - Aggregate)
                            </span>
                        )}
                    </p>
                    
                    <button
                        onClick={() => openBreakdownModal(tableTitle, rateCalculationData)}
                        className="ml-2 p-1 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors"
                        title="View detailed breakdown by product"
                    >
                        <HelpCircle size={16} />
                    </button>
                </div>
                
                {actualExtensionDays > 0 && (
                    <div className="mt-2 p-2 bg-yellow-50 dark:bg-yellow-900/30 rounded border border-yellow-200 dark:border-yellow-700">
                        <p className="text-xs text-yellow-800 dark:text-yellow-200">
                            <strong>Lookback Applied:</strong> Extended {actualExtensionDays} days to capture more starter events
                        </p>
                        {lookbackInfo && lookbackInfo.breakdown && (
                            <div className="mt-1 text-xs text-yellow-700 dark:text-yellow-300">
                                {Object.entries(lookbackInfo.breakdown).map(([days, count]) => (
                                    <span key={days} className="mr-3">
                                        {days === '0' ? 'Base period' : `+${days} days`}: {count} users
                                    </span>
                                ))}
                        </div>
                        )}
                    </div>
                )}
                        </div>
        );
    };

    // Get all available product IDs from lifecycle rates
    const getAllAvailableProducts = () => {
        const allAvailableProducts = new Set();
        
        // Check per_product structure first (correct structure from lifecycle_calculator.py)
        if (perProduct && Object.keys(perProduct).length > 0) {
            Object.keys(perProduct).forEach(productId => {
                allAvailableProducts.add(productId);
            });
        }
        
        // Also check aggregate rates for product-based data
        if (aggregate && typeof aggregate === 'object') {
            Object.values(aggregate).forEach(rateData => {
                if (rateData && rateData.rates_by_product) {
                    Object.keys(rateData.rates_by_product).forEach(productId => {
                        allAvailableProducts.add(productId);
                    });
                }
            });
        }
        
        return Array.from(allAvailableProducts).sort();
    };

    const productList = getAllAvailableProducts();

    // Function to open chart modal
    const openChartModal = (title, ratesData) => {
        setChartModalData({ title, ratesData });
        setShowChartModal(true);
    };

    // Function to close chart modal
    const closeChartModal = () => {
        setChartModalData(null);
        setShowChartModal(false);
    };

    // Function to open breakdown modal
    const openBreakdownModal = (title, rateCalculationData) => {
        setBreakdownModalData({ title, rateCalculationData });
        setShowBreakdownModal(true);
    };

    // Function to close breakdown modal
    const closeBreakdownModal = () => {
        setBreakdownModalData(null);
        setShowBreakdownModal(false);
    };

    // Function to render lifecycle table with precise requirements
    const renderLifecycleTable = (title, ratesData, denominatorKey = "rate_denominator_count") => {
        if (!ratesData) {
            return (
                <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                    <div className="flex items-center justify-between">
                        <h4 className="text-sm font-medium text-gray-900 dark:text-white">{title}</h4>
                        <span className="text-xs text-gray-500 dark:text-gray-400">No data available</span>
                    </div>
                </div>
            );
        }

        // Determine table type from title to show only relevant metrics
        const isCancellationTable = title.toLowerCase().includes('cancelled');
        const isRefundTable = title.toLowerCase().includes('refund');
        const isTrialOutcomeTable = title.toLowerCase().includes('trial started');

        // Handle the correct data structure from lifecycle_calculator.py
        // The backend now returns complete rate calculation data including extension_info
        
        console.log('renderLifecycleTable - title:', title, 'ratesData:', ratesData);
        
        // Check if we have the new complete structure or the old direct structure
        let aggregateRates, ratesByProduct, rateExtensionInfo;
        
        if (ratesData.aggregate_rates) {
            // New complete structure
            aggregateRates = ratesData.aggregate_rates;
            ratesByProduct = ratesData.rates_by_product || {};
            rateExtensionInfo = ratesData.extension_info || {};
        } else {
            // Old direct structure (fallback)
            aggregateRates = ratesData;
            ratesByProduct = {};
            rateExtensionInfo = {};
        }
        
        // Get per-product data for this specific rate type
        const currentTableKey = title.toLowerCase().includes('trial started') ? 'trial_to_outcome' :
                               title.toLowerCase().includes('trial converted') && title.toLowerCase().includes('cancelled') ? 'trial_converted_to_cancellation' :
                               title.toLowerCase().includes('trial converted') && title.toLowerCase().includes('refund') ? 'trial_converted_to_refund' :
                               title.toLowerCase().includes('initial purchase') && title.toLowerCase().includes('cancelled') ? 'initial_purchase_to_cancellation' :
                               title.toLowerCase().includes('initial purchase') && title.toLowerCase().includes('refund') ? 'initial_purchase_to_refund' :
                               title.toLowerCase().includes('renewal') && title.toLowerCase().includes('cancelled') ? 'renewal_to_cancellation' :
                               title.toLowerCase().includes('renewal') && title.toLowerCase().includes('refund') ? 'renewal_to_refund' : null;
        
        // For the new structure, we already have the rates_by_product for this specific lifecycle type
        // For the old structure, we need to get it from the per-product data
        if (!ratesData.aggregate_rates && currentTableKey) {
            Object.keys(perProduct).forEach(productId => {
                if (perProduct[productId][currentTableKey]) {
                    ratesByProduct[productId] = perProduct[productId][currentTableKey];
                }
            });
        }
        
        if (!aggregateRates || !aggregateRates.rates_by_day || Object.keys(aggregateRates.rates_by_day).length === 0) {
            return (
                <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                    <div className="flex items-center justify-between">
                        <h4 className="text-sm font-medium text-gray-900 dark:text-white">{title}</h4>
                        <span className="text-xs text-gray-500 dark:text-gray-400">No data available</span>
                    </div>
                </div>
            );
        }
        
        // Use global product selection to determine which rates to display
        // If a product is selected but doesn't have data for this lifecycle type, fall back to aggregate
        let displayRates = aggregateRates;
        let isShowingProductData = false;
        let isShowingFallbackData = false;
        
        if (globalSelectedProduct) {
            // Add null checks to prevent undefined access errors
            if (ratesByProduct && ratesByProduct[globalSelectedProduct]) {
                const productSampleSize = ratesByProduct[globalSelectedProduct].total_sample_size || 0;
                
                // Only show product-specific data if it has at least 10 users
                if (productSampleSize >= 10) {
                    // Product has sufficient data for this lifecycle type
                    displayRates = ratesByProduct[globalSelectedProduct];
                    isShowingProductData = true;
                } else {
                    // Product has data but insufficient sample size - use aggregate as fallback
                    displayRates = aggregateRates;
                    isShowingFallbackData = true;
                }
            } else {
                // Product selected but no data for this lifecycle type - use aggregate as fallback
                displayRates = aggregateRates;
                isShowingFallbackData = true;
            }
        }
        
        const ratesByDay = displayRates.rates_by_day;
        const sortedDays = Object.keys(ratesByDay).map(Number).sort((a, b) => a - b);
        
        // Get sample size for display
        const sampleSize = displayRates.total_sample_size || 0;
        
        // Create a mock rateCalculationData structure for the lookback info function
        // But use real extension info from the backend if available
        let extensionInfo = {
            extension_days_used: displayRates.extension_days_used || 0,
            total_sample_size: sampleSize,
            attempts_made: displayRates.extension_attempts || 1,
            per_product_info: {}
        };
        
        // Use real extension info from the backend if available
        if (rateExtensionInfo && Object.keys(rateExtensionInfo).length > 0) {
            extensionInfo = rateExtensionInfo;
        }
        
        // If we have a selected product, make sure its info is included
        if (globalSelectedProduct && ratesByProduct[globalSelectedProduct]) {
            const productRates = ratesByProduct[globalSelectedProduct];
            if (!extensionInfo.per_product_info[globalSelectedProduct]) {
                extensionInfo.per_product_info[globalSelectedProduct] = {
                    extension_days_used: productRates.extension_days_used || 0,
                    final_sample_size: productRates.total_sample_size || 0,
                    attempts_made: productRates.extension_attempts || 1,
                    user_counts_by_step: productRates.user_counts_by_step || {}
                };
            }
        }
        
        const mockRateCalculationData = {
            aggregate_rates: displayRates,
            rates_by_product: ratesByProduct,
            extension_info: extensionInfo
        };
        
        // Prepare row data based on table type - only 4 rows per table
        let rowData = [];
        
        if (isTrialOutcomeTable) {
            // Trial Started table - show only cancellation metrics
            rowData = [
                {
                    metric: "Raw Daily Cancellation %",
                    dataKey: "raw_daily_cancellation_rate",
                    bgClass: "bg-red-50 dark:bg-red-900/20",
                    textClass: "text-red-700 dark:text-red-300"
                },
                {
                    metric: "Smoothed Daily Cancellation %", 
                    dataKey: "smoothed_daily_cancellation_rate",
                    bgClass: "bg-red-100 dark:bg-red-900/30",
                    textClass: "text-red-800 dark:text-red-200"
                },
                {
                    metric: "Raw Cumulative Cancellation %",
                    dataKey: "raw_cumulative_cancellation_rate",
                    bgClass: "bg-red-150 dark:bg-red-900/40",
                    textClass: "text-red-800 dark:text-red-200"
                },
                {
                    metric: "Smoothed Cumulative Cancellation %",
                    dataKey: "smoothed_cumulative_cancellation_rate",
                    bgClass: "bg-red-200 dark:bg-red-900/50",
                    textClass: "text-red-900 dark:text-red-100"
                }
            ];
        } else if (isCancellationTable) {
            // Cancellation table - red theme
            rowData = [
                {
                    metric: "Raw Daily Cancellation %",
                    dataKey: "raw_daily_cancellation_rate",
                    bgClass: "bg-red-50 dark:bg-red-900/20",
                    textClass: "text-red-700 dark:text-red-300"
                },
                {
                    metric: "Smoothed Daily Cancellation %", 
                    dataKey: "smoothed_daily_cancellation_rate",
                    bgClass: "bg-red-100 dark:bg-red-900/30",
                    textClass: "text-red-800 dark:text-red-200"
                },
                {
                    metric: "Raw Cumulative Cancellation %",
                    dataKey: "raw_cumulative_cancellation_rate",
                    bgClass: "bg-red-150 dark:bg-red-900/40",
                    textClass: "text-red-800 dark:text-red-200"
                },
                {
                    metric: "Smoothed Cumulative Cancellation %",
                    dataKey: "smoothed_cumulative_cancellation_rate",
                    bgClass: "bg-red-200 dark:bg-red-900/50",
                    textClass: "text-red-900 dark:text-red-100"
                }
            ];
        } else if (isRefundTable) {
            // Refund table - blue theme
            rowData = [
                {
                    metric: "Raw Daily Refund %",
                    dataKey: "raw_daily_refund_rate",
                    bgClass: "bg-blue-50 dark:bg-blue-900/20",
                    textClass: "text-blue-700 dark:text-blue-300"
                },
                {
                    metric: "Smoothed Daily Refund %",
                    dataKey: "smoothed_daily_refund_rate",
                    bgClass: "bg-blue-100 dark:bg-blue-900/30",
                    textClass: "text-blue-800 dark:text-blue-200"
                },
                {
                    metric: "Raw Cumulative Refund %",
                    dataKey: "raw_cumulative_refund_rate",
                    bgClass: "bg-blue-150 dark:bg-blue-900/40",
                    textClass: "text-blue-800 dark:text-blue-200"
                },
                {
                    metric: "Smoothed Cumulative Refund %",
                    dataKey: "smoothed_cumulative_refund_rate",
                    bgClass: "bg-blue-200 dark:bg-blue-900/50",
                    textClass: "text-blue-900 dark:text-blue-100"
                }
            ];
        }

        return (
            <div className="mb-6">
                <div className="flex items-center justify-between mb-4">
                    <h4 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h4>
                    <button
                        onClick={() => openChartModal(title, ratesData)}
                        className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-md transition-colors duration-200 flex items-center space-x-1"
                    >
                        <LineChart size={16} />
                        <span>Chart</span>
                    </button>
                </div>
                
                {renderSampleSizeInfo(displayRates, globalSelectedProduct, title, mockRateCalculationData, isShowingProductData, isShowingFallbackData)}

                {/* Smoothing Information */}
                <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
                    <h5 className="font-semibold text-blue-800 dark:text-blue-200 mb-2">Smoothing Method Information</h5>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-blue-700 dark:text-blue-300">
                        <div>
                            <p><strong>Method:</strong> {displayRates.smoothing_method || 'none'}</p>
                            <p><strong>Quality:</strong> {displayRates.smoothing_quality || 'N/A'}</p>
                            <p><strong>Sample Size:</strong> {sampleSize}</p>
                        </div>
                        {displayRates.smoothing_details && (
                            <div>
                                <p><strong>Data Characteristics:</strong></p>
                                <ul className="ml-4 mt-1">
                                    {displayRates.smoothing_details.cancellation && (
                                        <>
                                            <li>Early Concentration: {displayRates.smoothing_details.cancellation.early_concentration}</li>
                                            <li>Sparsity: {displayRates.smoothing_details.cancellation.sparsity}</li>
                                            <li>Max Rate: {displayRates.smoothing_details.cancellation.max_rate}</li>
                                        </>
                                    )}
                                </ul>
                        </div>
                        )}
                    </div>
                </div>
                
                {/* Transposed Table with Frozen First Column and Color Coordination */}
                <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                    <div className="flex">
                        {/* Frozen First Column */}
                        <div className="bg-gray-50 dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700">
                            <div className="px-4 py-3 font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700">
                                Metric
                            </div>
                            {rowData.map((row, index) => (
                                <div key={index} className={`px-4 py-3 text-sm border-b border-gray-200 dark:border-gray-700 ${row.bgClass} ${row.textClass} font-medium`}>
                                    {row.metric}
                                </div>
                            ))}
                        </div>

                        {/* Scrollable Data Columns */}
                        <div className="flex-1 overflow-x-auto">
                            <div className="flex">
                                {sortedDays.map((day) => {
                                    const dayStr = day.toString();
                                    const dayData = ratesByDay[dayStr];
                                    
                                    return (
                                        <div key={dayStr} className="min-w-[100px] border-r border-gray-200 dark:border-gray-700 last:border-r-0">
                                            {/* Header */}
                                            <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 font-medium text-gray-900 dark:text-white text-center border-b border-gray-200 dark:border-gray-700">
                                                Day {day}
                                            </div>
                                            
                                            {/* Data Cells */}
                                            {rowData.map((row, index) => (
                                                <div key={index} className={`px-4 py-3 text-sm text-center border-b border-gray-200 dark:border-gray-700 ${row.bgClass} ${row.textClass}`}>
                                                    {dayData ? (dayData[row.dataKey] || 0).toFixed(2) : '0.00'}%
                                                </div>
                                            ))}
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    };

    // Chart Modal Component
    const ChartModal = () => {
        if (!chartModalData) return null;

        const { title, ratesData } = chartModalData;
        
        // Determine table type from title
        const isCancellationTable = title.toLowerCase().includes('cancelled');
        const isRefundTable = title.toLowerCase().includes('refund');
        const isTrialOutcomeTable = title.toLowerCase().includes('trial started');
        
        const prepareChartData = () => {
            // Handle the correct data structure from lifecycle_calculator.py
            // ratesData might be the complete structure with aggregate_rates, rates_by_product, etc.
            // OR it might be the direct rates data
            
            console.log('Chart ratesData:', ratesData);
            
            // Determine the correct rates to use based on current product selection and data structure
            let displayRates = null;
            
            if (ratesData.aggregate_rates) {
                // New complete structure - use the same logic as the table
                const aggregateRates = ratesData.aggregate_rates;
                const ratesByProduct = ratesData.rates_by_product || {};
                
                if (globalSelectedProduct && ratesByProduct[globalSelectedProduct]) {
                    const productSampleSize = ratesByProduct[globalSelectedProduct].total_sample_size || 0;
                    if (productSampleSize >= 10) {
                        displayRates = ratesByProduct[globalSelectedProduct];
                    } else {
                        displayRates = aggregateRates;
                    }
                } else {
                    displayRates = aggregateRates;
                }
            } else if (ratesData.rates_by_day) {
                // Direct rates structure
                displayRates = ratesData;
            } else {
                console.log('No valid rates structure found in:', ratesData);
                return [];
            }
            
            if (!displayRates || !displayRates.rates_by_day) {
                console.log('No rates_by_day found in displayRates:', displayRates);
                return [];
            }
            
            const ratesByDay = displayRates.rates_by_day;
            const sortedDays = Object.keys(ratesByDay).map(Number).sort((a, b) => a - b);
            
            console.log('Chart sortedDays:', sortedDays);
            console.log('Chart ratesByDay sample:', ratesByDay[sortedDays[0]]);
            
            return sortedDays.map(day => {
                const dayData = ratesByDay[day.toString()];
                const chartPoint = { day };
                
                if (isTrialOutcomeTable) {
                    // Trial Started table - show only cancellation metrics
                    chartPoint.rawDailyCancellation = dayData?.raw_daily_cancellation_rate || 0;
                    chartPoint.smoothedDailyCancellation = dayData?.smoothed_daily_cancellation_rate || 0;
                    chartPoint.rawCumulativeCancellation = dayData?.raw_cumulative_cancellation_rate || 0;
                    chartPoint.smoothedCumulativeCancellation = dayData?.smoothed_cumulative_cancellation_rate || 0;
                    
                    // Add conversion probability data for trial-to-outcome tables
                    chartPoint.conversionProbabilityFromDayOnward = (dayData?.conversion_probability_from_day_onward || 0) * 100; // Convert to percentage
                    chartPoint.survivalProbabilityThisDay = (dayData?.survival_probability_this_day || 0) * 100; // Convert to percentage
                } else if (isCancellationTable) {
                    // Only cancellation metrics for cancellation tables
                    chartPoint.rawDailyCancellation = dayData?.raw_daily_cancellation_rate || 0;
                    chartPoint.smoothedDailyCancellation = dayData?.smoothed_daily_cancellation_rate || 0;
                    chartPoint.rawCumulativeCancellation = dayData?.raw_cumulative_cancellation_rate || 0;
                    chartPoint.smoothedCumulativeCancellation = dayData?.smoothed_cumulative_cancellation_rate || 0;
                } else if (isRefundTable) {
                    // Only refund metrics for refund tables
                    chartPoint.rawDailyRefund = dayData?.raw_daily_refund_rate || 0;
                    chartPoint.smoothedDailyRefund = dayData?.smoothed_daily_refund_rate || 0;
                    chartPoint.rawCumulativeRefund = dayData?.raw_cumulative_refund_rate || 0;
                    chartPoint.smoothedCumulativeRefund = dayData?.smoothed_cumulative_refund_rate || 0;
                    
                    // Add refund probability data for refund tables
                    chartPoint.totalRefundProbability = (dayData?.total_refund_probability || 0) * 100; // Convert to percentage
                    chartPoint.noRefundProbability = (dayData?.no_refund_probability || 0) * 100; // Convert to percentage
                }
                
                return chartPoint;
            });
        };

        const customTooltip = ({ active, payload, label }) => {
            if (active && payload && payload.length) {
                return (
                    <div className="bg-white dark:bg-gray-800 p-3 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg">
                        <p className="font-semibold text-gray-900 dark:text-white">{`Day ${label}`}</p>
                        {payload.map((entry, index) => (
                            <p key={index} style={{ color: entry.color }} className="text-sm">
                                {`${entry.name}: ${entry.value.toFixed(2)}%`}
                            </p>
                        ))}
                            </div>
                );
            }
            return null;
        };

        const chartData = prepareChartData();
        
        // Define lines based on table type
        const getChartLines = () => {
            if (isTrialOutcomeTable) {
                return [
                    { dataKey: 'rawDailyCancellation', name: 'Raw Daily Cancellation', stroke: '#dc2626', strokeDasharray: '0' },
                    { dataKey: 'smoothedDailyCancellation', name: 'Smoothed Daily Cancellation', stroke: '#dc2626', strokeDasharray: '5 5' },
                    { dataKey: 'rawCumulativeCancellation', name: 'Raw Cumulative Cancellation', stroke: '#b91c1c', strokeDasharray: '0' },
                    { dataKey: 'smoothedCumulativeCancellation', name: 'Smoothed Cumulative Cancellation', stroke: '#b91c1c', strokeDasharray: '5 5' }
                ];
            } else if (isCancellationTable) {
                return [
                    { dataKey: 'rawDailyCancellation', name: 'Raw Daily Cancellation', stroke: '#dc2626', strokeDasharray: '0' },
                    { dataKey: 'smoothedDailyCancellation', name: 'Smoothed Daily Cancellation', stroke: '#dc2626', strokeDasharray: '5 5' },
                    { dataKey: 'rawCumulativeCancellation', name: 'Raw Cumulative Cancellation', stroke: '#b91c1c', strokeDasharray: '0' },
                    { dataKey: 'smoothedCumulativeCancellation', name: 'Smoothed Cumulative Cancellation', stroke: '#b91c1c', strokeDasharray: '5 5' }
                ];
            } else if (isRefundTable) {
                return [
                    { dataKey: 'rawDailyRefund', name: 'Raw Daily Refund', stroke: '#2563eb', strokeDasharray: '0' },
                    { dataKey: 'smoothedDailyRefund', name: 'Smoothed Daily Refund', stroke: '#2563eb', strokeDasharray: '5 5' },
                    { dataKey: 'rawCumulativeRefund', name: 'Raw Cumulative Refund', stroke: '#1d4ed8', strokeDasharray: '0' },
                    { dataKey: 'smoothedCumulativeRefund', name: 'Smoothed Cumulative Refund', stroke: '#1d4ed8', strokeDasharray: '5 5' }
                ];
            }
            
            return [];
        };

        // Define conversion probability lines for trial-to-outcome tables
        const getConversionProbabilityLines = () => {
            if (isTrialOutcomeTable) {
                return [
                    { dataKey: 'conversionProbabilityFromDayOnward', name: 'Conversion Probability from Day Onward', stroke: '#059669', strokeDasharray: '0', strokeWidth: 3 },
                    { dataKey: 'survivalProbabilityThisDay', name: 'Daily Survival Probability', stroke: '#10b981', strokeDasharray: '5 5', strokeWidth: 2 }
                ];
            }
            return [];
        };

        // Define refund probability lines for refund tables
        const getRefundProbabilityLines = () => {
            if (isRefundTable) {
                return [
                    { dataKey: 'totalRefundProbability', name: 'Total Refund Probability', stroke: '#dc2626', strokeDasharray: '0', strokeWidth: 3 },
                    { dataKey: 'noRefundProbability', name: 'No Refund Probability (Retention)', stroke: '#059669', strokeDasharray: '5 5', strokeWidth: 3 }
                ];
            }
            return [];
        };

        const chartLines = getChartLines();
        const conversionProbabilityLines = getConversionProbabilityLines();
        const refundProbabilityLines = getRefundProbabilityLines();

        return (
            <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-6xl w-full max-h-[90vh] overflow-y-auto">
                    <div className="p-6">
                        <div className="flex justify-between items-center mb-6">
                            <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                                {title} - Lifecycle Charts
                            </h3>
                            <button
                                onClick={closeChartModal}
                                className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                            >
                                <X size={24} />
                            </button>
                        </div>

                        {/* Product Filter Display */}
                        <div className="mb-6 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium text-gray-900 dark:text-white">
                                    Current Filter: {globalSelectedProduct || 'All Products (Aggregate)'}
                                </span>
                                <span className="text-xs text-gray-600 dark:text-gray-400">
                                    Use the main product toggle to change filter
                                </span>
                            </div>
                        </div>

                        {chartData.length > 0 ? (
                            <div className="space-y-8">
                                {/* Conversion Probability Chart - Only for Trial-to-Outcome */}
                                {isTrialOutcomeTable && conversionProbabilityLines.length > 0 && (
                                    <div>
                                        <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                                            🎯 Revenue Estimation: Conversion Probabilities
                                        </h4>
                                        <div className="mb-4 p-3 bg-green-50 dark:bg-green-900/30 rounded-lg">
                                            <p className="text-sm text-green-800 dark:text-green-200">
                                                <strong>Conversion Probability from Day Onward:</strong> Shows the likelihood that a user on day X will convert by the end of the trial.
                                                This is used in revenue estimation: <code>Expected Revenue = P_convert(x) × P_not_refunded × ARPU</code>
                                            </p>
                                        </div>
                                        <div className="h-80">
                                            <ResponsiveContainer width="100%" height="100%">
                                                <RechartsLineChart data={chartData}>
                                                    <CartesianGrid strokeDasharray="3 3" />
                                                    <XAxis 
                                                        dataKey="day" 
                                                        label={{ value: 'Trial Day', position: 'insideBottom', offset: -5 }}
                                                    />
                                                    <YAxis 
                                                        label={{ value: 'Probability (%)', angle: -90, position: 'insideLeft' }}
                                                        domain={[0, 100]}
                                                    />
                                                    <Tooltip content={customTooltip} />
                                                    <Legend />
                                                    {conversionProbabilityLines.map((line, index) => (
                                                        <Line
                                                            key={index}
                                                            type="monotone"
                                                            dataKey={line.dataKey}
                                                            stroke={line.stroke}
                                                            strokeDasharray={line.strokeDasharray}
                                                            name={line.name}
                                                            strokeWidth={line.strokeWidth || 2}
                                                            dot={{ r: 4 }}
                                                        />
                                                    ))}
                                                </RechartsLineChart>
                                            </ResponsiveContainer>
                                        </div>
                                    </div>
                                )}

                                {/* Refund Probability Chart - Only for Refund Tables */}
                                {isRefundTable && refundProbabilityLines.length > 0 && (
                                    <div>
                                        <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                                            💰 Revenue Estimation: Refund Probabilities
                                        </h4>
                                        <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
                                            <p className="text-sm text-blue-800 dark:text-blue-200">
                                                <strong>Total Refund Probability:</strong> The total likelihood of refunding within the refund window.
                                                <strong> No Refund Probability:</strong> Used in revenue estimation as the retention component.
                                            </p>
                                        </div>
                                        <div className="h-80">
                                            <ResponsiveContainer width="100%" height="100%">
                                                <RechartsLineChart data={chartData}>
                                                    <CartesianGrid strokeDasharray="3 3" />
                                                    <XAxis 
                                                        dataKey="day" 
                                                        label={{ value: 'Days Since Conversion', position: 'insideBottom', offset: -5 }}
                                                    />
                                                    <YAxis 
                                                        label={{ value: 'Probability (%)', angle: -90, position: 'insideLeft' }}
                                                        domain={[0, 100]}
                                                    />
                                                    <Tooltip content={customTooltip} />
                                                    <Legend />
                                                    {refundProbabilityLines.map((line, index) => (
                                                        <Line
                                                            key={index}
                                                            type="monotone"
                                                            dataKey={line.dataKey}
                                                            stroke={line.stroke}
                                                            strokeDasharray={line.strokeDasharray}
                                                            name={line.name}
                                                            strokeWidth={line.strokeWidth || 2}
                                                            dot={{ r: 4 }}
                                                        />
                                                    ))}
                                                </RechartsLineChart>
                                            </ResponsiveContainer>
                                        </div>
                                    </div>
                                )}

                                {/* Daily and Cumulative Charts for all tables */}
                                {/* Daily Rates Chart */}
                        <div>
                                    <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                                        Daily Rates Chart
                                    </h4>
                                    <div className="h-80">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <RechartsLineChart data={chartData}>
                                                <CartesianGrid strokeDasharray="3 3" />
                                                <XAxis 
                                                    dataKey="day" 
                                                    label={{ value: 'Days', position: 'insideBottom', offset: -5 }}
                                                />
                                                <YAxis 
                                                    label={{ value: 'Rate (%)', angle: -90, position: 'insideLeft' }}
                                                />
                                                <Tooltip content={customTooltip} />
                                                <Legend />
                                                {chartLines
                                                    .filter(line => line.dataKey.includes('Daily') || line.dataKey.includes('daily'))
                                                    .map((line, index) => (
                                                        <Line
                                                            key={index}
                                                            type="monotone"
                                                            dataKey={line.dataKey}
                                                            stroke={line.stroke}
                                                            strokeDasharray={line.strokeDasharray}
                                                            name={line.name}
                                                            strokeWidth={2}
                                                            dot={{ r: 3 }}
                                                        />
                                                    ))
                                                }
                                            </RechartsLineChart>
                                        </ResponsiveContainer>
                            </div>
                        </div>

                                {/* Cumulative Rates Chart */}
                        <div>
                                    <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                                        Cumulative Rates Chart
                                    </h4>
                                    <div className="h-80">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <RechartsLineChart data={chartData}>
                                                <CartesianGrid strokeDasharray="3 3" />
                                                <XAxis 
                                                    dataKey="day" 
                                                    label={{ value: 'Days', position: 'insideBottom', offset: -5 }}
                                                />
                                                <YAxis 
                                                    label={{ value: 'Rate (%)', angle: -90, position: 'insideLeft' }}
                                                />
                                                <Tooltip content={customTooltip} />
                                                <Legend />
                                                {chartLines
                                                    .filter(line => line.dataKey.includes('Cumulative') || line.dataKey.includes('cumulative'))
                                                    .map((line, index) => (
                                                        <Line
                                                            key={index}
                                                            type="monotone"
                                                            dataKey={line.dataKey}
                                                            stroke={line.stroke}
                                                            strokeDasharray={line.strokeDasharray}
                                                            name={line.name}
                                                            strokeWidth={2}
                                                            dot={{ r: 3 }}
                                                        />
                                                    ))
                                                }
                                            </RechartsLineChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>

                                {/* Chart Information */}
                                <div className="mt-6 p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
                                    <h5 className="font-semibold text-blue-800 dark:text-blue-200 mb-2">Chart Information</h5>
                                    <div className="text-sm text-blue-700 dark:text-blue-300 space-y-1">
                                        <p><strong>Solid lines:</strong> Raw data points</p>
                                        <p><strong>Dashed lines:</strong> Smoothed data (when available)</p>
                                        {isTrialOutcomeTable && (
                                            <>
                                                <p><strong>Red colors:</strong> Cancellation metrics (Trial Started → Cancelled)</p>
                                                <p><strong>Green colors:</strong> Conversion probabilities (used for revenue estimation)</p>
                                                <p><strong>Thick green line:</strong> Pre-calculated conversion probability from each day onward</p>
                                            </>
                                        )}
                                        {isCancellationTable && <p><strong>Red colors:</strong> Cancellation metrics</p>}
                                        {isRefundTable && (
                                            <>
                                                <p><strong>Blue colors:</strong> Refund metrics</p>
                                                <p><strong>Red/Green colors:</strong> Refund probabilities (used for revenue estimation)</p>
                                                <p><strong>Thick lines:</strong> Pre-calculated total refund probabilities</p>
                                            </>
                                        )}
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="text-center py-8">
                                <p className="text-gray-500 dark:text-gray-400">
                                    No chart data available for this lifecycle analysis.
                                </p>
                        </div>
                        )}
                    </div>
                </div>
            </div>
        );
    };

    // Breakdown Modal Component
    const BreakdownModal = () => {
        if (!breakdownModalData) return null;

        const { title, rateCalculationData } = breakdownModalData;
        
        // Extract per-product information from the rate calculation data
        const extensionInfo = rateCalculationData.extension_info;
        const perProductInfo = extensionInfo?.per_product_info || {};
        const aggregateRates = rateCalculationData.aggregate_rates;
        const ratesByProduct = rateCalculationData.rates_by_product || {};
        
        // Get ALL products from the global product list, not just ones with data for this lifecycle
        const allAvailableProducts = getAllAvailableProducts();

        return (
            <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-y-auto">
                    <div className="p-6">
                        <div className="flex justify-between items-center mb-6">
                            <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                                {title} - Product Breakdown
                            </h3>
                            <button
                                onClick={closeBreakdownModal}
                                className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                            >
                                <X size={24} />
                            </button>
                        </div>

                        {/* Aggregate Summary */}
                        <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
                            <h4 className="text-lg font-semibold text-blue-800 dark:text-blue-200 mb-2">
                                Aggregate Summary
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                                <div>
                                    <p className="text-blue-700 dark:text-blue-300">
                                        <strong>Total Users:</strong> {aggregateRates?.total_sample_size || 0}
                                    </p>
                                </div>
                                <div>
                                    <p className="text-blue-700 dark:text-blue-300">
                                        <strong>Extension Used:</strong> {aggregateRates?.extension_days_used || 0} days
                                    </p>
                                </div>
                                <div>
                                    <p className="text-blue-700 dark:text-blue-300">
                                        <strong>Products with Data:</strong> {Object.keys(ratesByProduct).length}
                                    </p>
                                </div>
                            </div>
                        </div>

                        {/* Per-Product Breakdown */}
                        <div className="space-y-4">
                            <h4 className="text-lg font-semibold text-gray-900 dark:text-white">
                                Per-Product Details ({allAvailableProducts.length} products)
                            </h4>
                            
                            {allAvailableProducts.map(productId => {
                                const productInfo = perProductInfo[productId] || {};
                                const productRates = ratesByProduct[productId] || {};
                                const finalSampleSize = productInfo.final_sample_size || productRates.total_sample_size || 0;
                                const extensionDays = productInfo.extension_days_used || productRates.extension_days_used || 0;
                                const attempts = productInfo.attempts_made || productRates.extension_attempts || 1;
                                
                                // Determine status
                                let status = 'No Data';
                                let statusColor = 'text-gray-500';
                                let bgColor = 'bg-gray-50 dark:bg-gray-700';
                                
                                if (finalSampleSize >= 10) {
                                    status = 'Sufficient Data';
                                    statusColor = 'text-green-600 dark:text-green-400';
                                    bgColor = 'bg-green-50 dark:bg-green-900/20';
                                } else if (finalSampleSize > 0) {
                                    status = 'Insufficient Data (<10 users)';
                                    statusColor = 'text-orange-600 dark:text-orange-400';
                                    bgColor = 'bg-orange-50 dark:bg-orange-900/20';
                                }
                                
                                // Create lookback breakdown text
                                let lookbackText = 'No lookback data available';
                                if (productInfo.final_sample_size !== undefined || productRates.total_sample_size !== undefined) {
                                    // Check if we have detailed step-by-step breakdown from backend
                                    const stepCounts = productInfo.user_counts_by_step;
                                    if (stepCounts) {
                                        const base = stepCounts.base || 0;
                                        const thirtyDays = stepCounts["30_days"] || 0;
                                        const sixtyDays = stepCounts["60_days"] || 0;
                                        
                                        if (extensionDays === 0) {
                                            lookbackText = `Base: ${base}`;
                                        } else if (extensionDays === 30) {
                                            lookbackText = `Base: ${base}, +0-30: ${thirtyDays}`;
                                        } else if (extensionDays === 60) {
                                            lookbackText = `Base: ${base}, +0-30: ${thirtyDays}, +31-60: ${sixtyDays}`;
                                        }
                                    } else {
                                        // Fallback to old format if detailed breakdown not available
                                        if (extensionDays === 0) {
                                            lookbackText = `Base: ${finalSampleSize}`;
                                        } else if (extensionDays === 30) {
                                            lookbackText = `Base: ?, +0-30: ${finalSampleSize} (final)`;
                                        } else if (extensionDays === 60) {
                                            lookbackText = `Base: ?, +0-30: ?, +31-60: ${finalSampleSize} (final)`;
                                        }
                                    }
                                }
                                
                                return (
                                    <div key={productId} className={`p-4 rounded-lg border ${bgColor} border-gray-200 dark:border-gray-600`}>
                                        <div className="flex items-center justify-between mb-2">
                                            <h5 className="font-medium text-gray-900 dark:text-white">
                                                {productId}
                                            </h5>
                                            <span className={`text-sm font-medium ${statusColor}`}>
                                                {status}
                                            </span>
                                        </div>
                                        
                                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm text-gray-600 dark:text-gray-400">
                                            <div>
                                                <p><strong>Final Users:</strong> {finalSampleSize}</p>
                                            </div>
                                            <div>
                                                <p><strong>Extension:</strong> {extensionDays} days</p>
                                            </div>
                                            <div>
                                                <p><strong>Attempts:</strong> {attempts}</p>
                                            </div>
                                            <div>
                                                <p><strong>Used in Display:</strong> {
                                                    globalSelectedProduct === productId 
                                                        ? (finalSampleSize >= 10 ? 'Yes (Product-specific)' : 'No (Fallback to aggregate)')
                                                        : 'N/A'
                                                }</p>
                                            </div>
                                        </div>
                                        
                                        <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                                            <p><strong>Lookback breakdown:</strong> {lookbackText}</p>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>

                        {/* Explanation */}
                        <div className="mt-6 p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
                            <h5 className="font-semibold text-gray-800 dark:text-gray-200 mb-2">How This Works</h5>
                            <div className="text-sm text-gray-600 dark:text-gray-400 space-y-1">
                                <p><strong>Lookback Strategy:</strong> Each product tries to reach 100+ users by extending lookback period (Base → +0-30 days → +31-60 days)</p>
                                <p><strong>Display Logic:</strong> Product-specific data shown only if ≥10 users, otherwise falls back to aggregate</p>
                                <p><strong>Aggregate:</strong> Combined data from all products that have sufficient data</p>
                                <p><strong>Current Filter:</strong> {globalSelectedProduct || 'All Products (Aggregate)'}</p>
                                <p><strong>Note:</strong> Products with "No Data" for this lifecycle type don't participate in the aggregate calculation</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    };

    if (!data) {
        return (
            <div className="p-6 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                <p className="text-gray-500 dark:text-gray-400">
                    No lifecycle rate data available.
                </p>
            </div>
        );
    }

    // Define the exact order of tables as requested
    const lifecycleTableOrder = [
        {
            key: 'trial_to_outcome',
            title: 'Trial Started → Cancelled',
            dataKey: 'trial_to_outcome'
        },
        {
            key: 'trial_converted_to_cancellation',
            title: 'Trial Converted → Cancelled',
            dataKey: 'trial_converted_to_cancellation'
        },
        {
            key: 'trial_converted_to_refund',
            title: 'Trial Converted → Refunded',
            dataKey: 'trial_converted_to_refund'
        },
        {
            key: 'initial_purchase_to_cancellation',
            title: 'Initial Purchase → Cancelled',
            dataKey: 'initial_purchase_to_cancellation'
        },
        {
            key: 'initial_purchase_to_refund',
            title: 'Initial Purchase → Refunded',
            dataKey: 'initial_purchase_to_refund'
        },
        {
            key: 'renewal_to_cancellation',
            title: 'Renewal → Cancelled',
            dataKey: 'renewal_to_cancellation'
        },
        {
            key: 'renewal_to_refund',
            title: 'Renewal → Refunded',
            dataKey: 'renewal_to_refund'
        }
    ];

    return (
        <div className="space-y-6">
            {/* Global Product Toggle */}
            <div className="p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg border border-blue-200 dark:border-blue-700">
                <div className="flex items-center justify-between">
                    <div>
                        <h3 className="text-lg font-semibold text-blue-800 dark:text-blue-200">
                            Lifecycle Analysis - Product Filter
                        </h3>
                        <p className="text-sm text-blue-600 dark:text-blue-300">
                            {globalSelectedProduct 
                                ? `Showing data for: ${globalSelectedProduct}` 
                                : 'Showing aggregate data across all products'
                            }
                        </p>
                        <p className="text-xs text-blue-500 dark:text-blue-400 mt-1">
                            Note: Product-specific data requires minimum 10 users per lifecycle type. 
                            Tables with fewer users will show aggregate data.
                        </p>
                    </div>
                    <select
                        value={globalSelectedProduct || ''}
                        onChange={(e) => setGlobalSelectedProduct(e.target.value || null)}
                        className="px-4 py-2 border border-blue-300 dark:border-blue-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    >
                        <option value="">All Products (Aggregate)</option>
                        {productList.map(productId => (
                            <option key={productId} value={productId}>
                                {productId}
                            </option>
                        ))}
                    </select>
                </div>
            </div>

            {/* Render tables in the specified order */}
            <div className="space-y-8">
                {lifecycleTableOrder.map((tableConfig) => {
                    // Add safety check for aggregate data
                    const ratesData = aggregate && aggregate[tableConfig.dataKey] ? aggregate[tableConfig.dataKey] : null;
                    
                    if (!ratesData) {
                        return (
                            <div key={tableConfig.key} className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                                <div className="flex items-center justify-between">
                                    <h4 className="text-sm font-medium text-gray-900 dark:text-white">{tableConfig.title}</h4>
                                    <span className="text-xs text-gray-500 dark:text-gray-400">No data available</span>
                                </div>
                            </div>
                        );
                    }

                    return renderLifecycleTable(tableConfig.title, ratesData);
                })}
            </div>

            {/* Chart Modal */}
            {showChartModal && <ChartModal />}

            {/* Breakdown Modal */}
            {showBreakdownModal && <BreakdownModal />}
        </div>
    );
};

export default LifecycleRatesTable; 