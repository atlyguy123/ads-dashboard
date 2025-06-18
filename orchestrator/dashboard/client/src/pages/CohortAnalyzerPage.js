// client/src/pages/CohortAnalyzerPage.js
// COHORT DEFINITION:
// A user is part of the cohort ONLY if they:
// 1. Exist in the database
// 2. Have a "trial started" event within the specified date range
// This is enforced using the enforce_trial_started flag in API calls
// IMPORTANT: Users who do not meet both criteria are EXCLUDED from all analysis.
// All events analyzed are ONLY from users in this cohort.

import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../services/api'; // Assuming your API service is here
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { ChevronDown, ChevronRight, PlusCircle, XCircle, AlertTriangle, Loader, Filter } from 'lucide-react';
import Select from 'react-select';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip as ChartTooltip,
    Legend as ChartLegend,
} from 'chart.js';
import { Line as ChartLine } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    ChartTooltip,
    ChartLegend
);

// Custom styles for react-select
const customSelectStyles = {
  control: (base, state) => ({
    ...base,
    background: '#FFFFFF',
    borderColor: state.isFocused ? '#4F46E5' : '#CBD5E0',
    boxShadow: state.isFocused ? '0 0 0 1px #4F46E5' : 'none',
    '&:hover': {
      borderColor: '#4F46E5',
    },
    padding: '2px',
    fontSize: '0.9rem',
  }),
  option: (base, state) => ({
    ...base,
    backgroundColor: state.isSelected 
      ? '#4F46E5' 
      : state.isFocused 
        ? '#EEF2FF' 
        : 'transparent',
    color: state.isSelected ? 'white' : '#1A202C',
    '&:hover': {
      backgroundColor: state.isSelected ? '#4F46E5' : '#EEF2FF',
    },
    cursor: 'pointer',
    fontSize: '0.9rem',
  }),
  multiValue: (base) => ({
    ...base,
    backgroundColor: '#EEF2FF',
    border: '1px solid #E2E8F0',
  }),
  multiValueLabel: (base) => ({
    ...base,
    color: '#4F46E5',
    fontWeight: 500,
  }),
  multiValueRemove: (base) => ({
    ...base,
    color: '#4F46E5',
    '&:hover': {
      backgroundColor: '#4F46E5',
      color: 'white',
    },
  }),
  menu: (base) => ({
    ...base,
    zIndex: 100,
    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
    backgroundColor: '#FFFFFF',
  }),
  placeholder: (base) => ({
    ...base,
    color: '#718096',
  }),
  input: (base) => ({
    ...base,
    color: '#1A202C',
  }),
  singleValue: (base) => ({
    ...base,
    color: '#1A202C',
  }),
};

// For dark mode
const darkModeSelectStyles = {
  ...customSelectStyles,
  control: (base, state) => ({
    ...customSelectStyles.control(base, state),
    background: '#1F2937',
    borderColor: state.isFocused ? '#4F46E5' : '#4B5563',
    '&:hover': {
      borderColor: '#4F46E5',
    },
  }),
  option: (base, state) => ({
    ...customSelectStyles.option(base, state),
    backgroundColor: state.isSelected 
      ? '#4F46E5' 
      : state.isFocused 
        ? '#374151' 
        : '#1F2937',
    color: state.isSelected ? 'white' : '#F3F4F6',
  }),
  multiValue: (base) => ({
    ...customSelectStyles.multiValue(base),
    backgroundColor: '#374151',
    border: '1px solid #4B5563',
  }),
  multiValueLabel: (base) => ({
    ...customSelectStyles.multiValueLabel(base),
    color: '#F3F4F6',
  }),
  multiValueRemove: (base) => ({
    ...customSelectStyles.multiValueRemove(base),
    color: '#F3F4F6',
    '&:hover': {
      backgroundColor: '#4F46E5',
      color: 'white',
    },
  }),
  menu: (base) => ({
    ...customSelectStyles.menu(base),
    backgroundColor: '#1F2937',
    border: '1px solid #4B5563',
  }),
  placeholder: (base) => ({
    ...customSelectStyles.placeholder(base),
    color: '#9CA3AF',
  }),
  input: (base) => ({
    ...base,
    color: '#F3F4F6',
  }),
  singleValue: (base) => ({
    ...base,
    color: '#F3F4F6',
  }),
};

// Define the color coding for the user timeline table
const timelineColors = {
  default: {
    bg: 'bg-gray-100 dark:bg-gray-700',
    text: 'text-gray-500 dark:text-gray-400'
  },
  trial_started: {
    bg: 'bg-yellow-100 dark:bg-yellow-900/50',
    text: 'text-yellow-800 dark:text-yellow-200'
  },
  trial_converted: {
    bg: 'bg-green-100 dark:bg-green-900/50',
    text: 'text-green-800 dark:text-green-200'
  },
  trial_cancelled: {
    bg: 'bg-red-100 dark:bg-red-900/50',
    text: 'text-red-800 dark:text-red-200'
  },
  conversion_cancelled: {
    bg: 'bg-green-50 dark:bg-green-900/30',
    text: 'text-green-600 dark:text-green-300'
  },
  refunded: {
    bg: 'bg-red-50 dark:bg-red-900/30',
    text: 'text-red-600 dark:text-red-300'
  }
};

// New component for displaying filter stats
const FilterStatsDisplay = ({ filterStats }) => {
    if (!filterStats) return null;
    
    const { filtering_steps, base_cohort_count, final_cohort_count } = filterStats;
    
    // Calculate percentages
    const calculatePercentage = (count, total) => {
        if (!total) return 0;
        return ((count / total) * 100).toFixed(1);
    };
    
    return (
        <div className="mb-6 p-4 border rounded-lg bg-white dark:bg-gray-700 shadow-sm">
            <div className="flex items-center gap-2 mb-3">
                <Filter size={18} className="text-indigo-600 dark:text-indigo-400" />
                <h3 className="text-lg font-semibold text-gray-700 dark:text-white">Cohort Filtering Statistics</h3>
            </div>
            
            <div className="flex flex-col md:flex-row items-center justify-between mb-4 p-3 bg-indigo-50 dark:bg-indigo-900/30 rounded-md">
                <div className="text-center">
                    <div className="text-sm text-gray-600 dark:text-gray-300">Base Cohort</div>
                    <div className="text-xl font-bold text-indigo-600 dark:text-indigo-300">{base_cohort_count}</div>
                    <div className="text-xs text-gray-500 dark:text-gray-400">100%</div>
                </div>
                
                <div className="hidden md:block text-gray-400">→</div>
                
                {filtering_steps.slice(1).map((step, index) => (
                    <div key={index} className="text-center">
                        <div className="text-sm text-gray-600 dark:text-gray-300">After {step.step}</div>
                        <div className="text-xl font-bold text-indigo-600 dark:text-indigo-300">{step.users_count}</div>
                        <div className="text-xs text-gray-500 dark:text-gray-400">
                            {calculatePercentage(step.users_count, base_cohort_count)}% of base
                        </div>
                    </div>
                ))}
                
                {/* If there's a significant difference between final count and last step */}
                {final_cohort_count !== (filtering_steps[filtering_steps.length - 1]?.users_count) && (
                    <>
                        <div className="hidden md:block text-gray-400">→</div>
                        <div className="text-center">
                            <div className="text-sm text-gray-600 dark:text-gray-300">Final Cohort</div>
                            <div className="text-xl font-bold text-indigo-600 dark:text-indigo-300">{final_cohort_count}</div>
                            <div className="text-xs text-gray-500 dark:text-gray-400">
                                {calculatePercentage(final_cohort_count, base_cohort_count)}% of base
                            </div>
                        </div>
                    </>
                )}
            </div>
            
            <div className="overflow-x-auto">
                <table className="min-w-full text-sm border dark:border-gray-600">
                    <thead className="bg-gray-50 dark:bg-gray-700">
                        <tr>
                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Filter Step</th>
                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Description</th>
                            <th className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">Users Count</th>
                            <th className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">Users Filtered</th>
                            <th className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white">% of Base</th>
                        </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-800">
                        {filtering_steps.map((step, index) => (
                            <tr key={index} className={index % 2 === 0 ? '' : 'bg-gray-50 dark:bg-gray-700'}>
                                <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">{step.step}</td>
                                <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">{step.description}</td>
                                <td className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">{step.users_count}</td>
                                <td className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                    {step.users_filtered || 0}
                                </td>
                                <td className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300">
                                    {calculatePercentage(step.users_count, base_cohort_count)}%
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

// Add this Tooltip component before the main CohortAnalyzerPage component
const EstimatedRevenueTooltip = ({ data, position, onClose }) => {
    if (!data) return null;
    
    return (
        <div 
            className="fixed z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg p-4 max-w-md"
            style={{
                left: Math.min(position.x, window.innerWidth - 400),
                top: Math.max(position.y - 100, 10),
            }}
        >
            <div className="flex justify-between items-start mb-2">
                <h4 className="font-semibold text-gray-800 dark:text-white">Estimated Revenue Calculation</h4>
                <button onClick={onClose} className="text-gray-500 hover:text-gray-700">×</button>
            </div>
            
            <div className="space-y-3 text-sm">
                {/* NEW: Refund Rate Ratios - Show prominently at the top */}
                {(() => {
                    const breakdown = data.breakdown || {};
                    const trialConverted = breakdown.trial_converted || {};
                    const initialPurchase = breakdown.initial_purchase || {};
                    
                    const showTrialRatio = trialConverted.actual_revenue > 0;
                    const showInitialRatio = initialPurchase.actual_revenue > 0;
                    
                    if (!showTrialRatio && !showInitialRatio) return null;
                    
                    return (
                        <div className="bg-blue-50 dark:bg-blue-900 p-3 rounded-lg border border-blue-200 dark:border-blue-700 mb-3">
                            <div className="text-sm font-medium text-blue-800 dark:text-blue-200 mb-2">
                                🎯 Refund Rate Analysis
                            </div>
                            
                            {showTrialRatio && (
                                <div className="mb-2">
                                    <div className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                        Trial Conversion Refund Rate:
                                    </div>
                                    <div className="text-sm font-bold text-blue-900 dark:text-blue-100">
                                        {(() => {
                                            const revenue = trialConverted.actual_revenue || 0;
                                            const estimatedRefunds = trialConverted.estimated_refunds || 0;
                                            const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                            return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                        })()}
                                    </div>
                                    <div className="text-xs text-blue-600 dark:text-blue-400">
                                        ${(trialConverted.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(trialConverted.actual_revenue || 0).toFixed(2)} revenue
                                    </div>
                                </div>
                            )}
                            
                            {showInitialRatio && (
                                <div>
                                    <div className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                        Initial Purchase Refund Rate:
                                    </div>
                                    <div className="text-sm font-bold text-blue-900 dark:text-blue-100">
                                        {(() => {
                                            const revenue = initialPurchase.actual_revenue || 0;
                                            const estimatedRefunds = initialPurchase.estimated_refunds || 0;
                                            const refundRate = revenue > 0 ? (estimatedRefunds / revenue) * 100 : 0;
                                            return `${estimatedRefunds.toFixed(2)} / ${revenue.toFixed(2)} = ${refundRate.toFixed(1)}%`;
                                        })()}
                                    </div>
                                    <div className="text-xs text-blue-600 dark:text-blue-400">
                                        ${(initialPurchase.estimated_refunds || 0).toFixed(2)} estimated refunds on ${(initialPurchase.actual_revenue || 0).toFixed(2)} revenue
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })()}
                
                <div>
                    <div className="font-medium text-gray-700 dark:text-gray-300">Formula:</div>
                    <div className="font-mono text-xs bg-gray-100 dark:bg-gray-700 p-2 rounded">
                        {data.formula}
                    </div>
                </div>
                
                <div>
                    <div className="font-medium text-gray-700 dark:text-gray-300">Calculation:</div>
                    <div className="font-mono text-xs bg-gray-100 dark:bg-gray-700 p-2 rounded">
                        {data.calculation}
                    </div>
                </div>
                
                <div>
                    <div className="font-medium text-gray-700 dark:text-gray-300">Components:</div>
                    <div className="text-xs space-y-1">
                        {Object.entries(data.components || {}).map(([key, value]) => (
                            <div key={key} className="flex justify-between">
                                <span className="text-gray-600 dark:text-gray-400">{key}:</span>
                                <span className="font-mono">{value}</span>
                            </div>
                        ))}
                    </div>
                </div>
                
                <div className="border-t pt-2">
                    <div className="flex justify-between font-medium">
                        <span>Result:</span>
                        <span className="text-green-600 dark:text-green-400">{data.result}</span>
                    </div>
                </div>
            </div>
        </div>
    );
};

const CohortAnalyzerPage = () => {
    const initialFilters = {
        date_from_str: '',
        date_to_str: '',
        optional_filters: [], // [{id: uuid, property_name: '', property_values: [], property_source: 'event/user'}]
    };

    const [filters, setFilters] = useState(initialFilters);
    const [availableEventProperties, setAvailableEventProperties] = useState([]);
    const [availableUserProperties, setAvailableUserProperties] = useState([]);
    const [propertyValueOptions, setPropertyValueOptions] = useState({}); // { 'prop_key': ['val1', 'val2'] }

    const [isLoading, setIsLoading] = useState(false);
    const [analysisResult, setAnalysisResult] = useState(null);
    const [error, setError] = useState(null);
    
    const [showLifecycleTables, setShowLifecycleTables] = useState(false);
    const [activePropertyDiscovery, setActivePropertyDiscovery] = useState(false);
    const [propertyDiscoveryMessage, setPropertyDiscoveryMessage] = useState('');
    const [isInitializing, setIsInitializing] = useState(true);
    
    // User timeline state
    const [userTimelineData, setUserTimelineData] = useState(null);
    const [isLoadingTimeline, setIsLoadingTimeline] = useState(false);
    const [timelineError, setTimelineError] = useState(null);
    
    // Event Revenue Timeline state - NEW FEATURE
    const [eventRevenueTimelineData, setEventRevenueTimelineData] = useState(null);
    const [isLoadingEventRevenueTimeline, setIsLoadingEventRevenueTimeline] = useState(false);
    const [eventRevenueTimelineError, setEventRevenueTimelineError] = useState(null);
    const [selectedUserId, setSelectedUserId] = useState('');
    const [selectedProductId, setSelectedProductId] = useState('');
    const [availableUsers, setAvailableUsers] = useState([]);
    const [availableProducts, setAvailableProducts] = useState([]);
    
    // Add state for cohort definition warning
    const [cohortDefinitionWarning, setCohortDefinitionWarning] = useState(null);
    
    // Add state for tooltip
    const [tooltipData, setTooltipData] = useState(null);
    const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

    // Add state for product selection at component level
    const [selectedProducts, setSelectedProducts] = React.useState({});

    // State for lifecycle table global product selection (single selector for all tables)
    const [globalSelectedProduct, setGlobalSelectedProduct] = React.useState("");
    
    // State for chart modal
    const [showChartModal, setShowChartModal] = React.useState(false);

    // Load saved filters from localStorage on component mount
    useEffect(() => {
        const savedFilters = localStorage.getItem('cohortAnalyzerFilters');
        if (savedFilters) {
            try {
                const parsedFilters = JSON.parse(savedFilters);
                setFilters(prev => ({
                    ...prev,
                    ...parsedFilters,
                    optional_filters: parsedFilters.optional_filters || [] // Ensure optional_filters exists
                }));
            } catch (error) {
                console.error('Error loading saved filters:', error);
            }
        }
    }, []);

    // Save filters to localStorage whenever they change
    useEffect(() => {
        const filtersToSave = {
            date_from_str: filters.date_from_str,
            date_to_str: filters.date_to_str,
            optional_filters: filters.optional_filters
        };
        localStorage.setItem('cohortAnalyzerFilters', JSON.stringify(filtersToSave));
    }, [filters.date_from_str, filters.date_to_str, filters.optional_filters]);

    // Define fetchDiscoverableProperties BEFORE it's used in the useEffect
    const fetchDiscoverableProperties = useCallback(async () => {
        try {
            const data = await api.getDiscoverableCohortProperties();
            setAvailableEventProperties(data.event_properties || []);
            setAvailableUserProperties(data.user_properties || []);
            
            // Check if we got an empty response and need to enable properties
            if ((!data.event_properties || data.event_properties.length === 0) && 
                (!data.user_properties || data.user_properties.length === 0)) {
                if (data.status === "discovery_initiated") {
                    setPropertyDiscoveryMessage(data.message || "Property discovery auto-triggered. Please wait...");
                    setActivePropertyDiscovery(true);
                    // Poll for completion
                    const checkDiscoveryInterval = setInterval(async () => {
                        try {
                            const status = await api.getPropertyDiscoveryStatus();
                            if (status.status === 'completed') {
                                clearInterval(checkDiscoveryInterval);
                                setActivePropertyDiscovery(false);
                                setPropertyDiscoveryMessage("Discovery completed! Refreshing properties...");
                                // Refresh properties after a delay
                                setTimeout(fetchDiscoverableProperties, 1000);
                            }
                        } catch (err) {
                            console.error("Error checking discovery status:", err);
                        }
                    }, 3000);
                }
            }
        } catch (err) {
            console.error("Error fetching discoverable properties:", err);
            if (err.code === "tables_missing") {
                // Specific error for missing tables - try to enable properties automatically
                try {
                    await api.enableCohortProperties();
                    setPropertyDiscoveryMessage("Property system initialized. Please wait while we discover properties...");
                    // Give time for the initialization and then try again
                    setTimeout(fetchDiscoverableProperties, 2000);
                } catch (enableErr) {
                    setError("Could not initialize property system. Please try again or check server logs.");
                }
            } else {
                setError("Could not load filter property lists. Try triggering discovery or check console.");
            }
        }
    }, []);

    const getPropertyDiscoveryStatus = useCallback(async () => {
        try {
            const status = await api.getPropertyDiscoveryStatus();
            if (status.active) {
                setActivePropertyDiscovery(true);
                setPropertyDiscoveryMessage(status.message || "Property discovery in progress...");
            } else if (status.status === 'completed') {
                setActivePropertyDiscovery(false);
            }
        } catch (err) {
            console.error("Error checking discovery status:", err);
        }
    }, []);

    // Initialize properties and fetch discoverable properties on mount
    useEffect(() => {
        const initializeProperties = async () => {
            setIsInitializing(true);
            try {
                // First try to enable properties to ensure tables exist
                await api.enableCohortProperties();
                // Check discovery status
                await getPropertyDiscoveryStatus();
                // Then fetch the properties
                await fetchDiscoverableProperties();
            } catch (err) {
                console.error("Error initializing properties:", err);
                setError("Could not initialize property system. Please check server logs.");
            } finally {
                setIsInitializing(false);
            }
        };
        
        initializeProperties();
    }, [fetchDiscoverableProperties, getPropertyDiscoveryStatus]);

    const handleFilterChange = (type, value, index = -1) => {
        setFilters(prev => {
            const newFilters = { ...prev };
            if (type === 'date_from_str' || type === 'date_to_str') {
                newFilters[type] = value;
            } else if (type === 'optional_filter_property') {
                newFilters.optional_filters[index].property_name = value;
                newFilters.optional_filters[index].property_values = []; // Reset values
                if (value) fetchPropertyValues(value, 'user', 'optional_filter_' + index);
            } else if (type === 'optional_filter_values') {
                // Assuming value is an array from a multi-select component
                newFilters.optional_filters[index].property_values = value;
            } else if (type.startsWith('optional_filter_')) {
                const field = type.substring('optional_filter_'.length); // 'property', 'values', 'source'
                if (field === 'property') {
                    newFilters.optional_filters[index].property_name = value;
                    newFilters.optional_filters[index].property_values = []; // Reset values
                    if (value && newFilters.optional_filters[index].property_source) {
                        fetchPropertyValues(value, newFilters.optional_filters[index].property_source, `optional_filter_${index}`);
                    }
                } else if (field === 'values') {
                    newFilters.optional_filters[index].property_values = value; // Assuming array
                } else if (field === 'source') {
                    newFilters.optional_filters[index].property_source = value;
                    // If property name already exists, re-fetch values for new source
                    if (newFilters.optional_filters[index].property_name) {
                         fetchPropertyValues(newFilters.optional_filters[index].property_name, value, `optional_filter_${index}`);
                    }
                }
            }
            return newFilters;
        });
    };

    const fetchPropertyValues = async (propertyName, propertySource, filterIdentifier) => {
        if (!propertyName || !propertySource) return;
        try {
            const data = await api.getDiscoverableCohortPropertyValues(propertyName, propertySource);
            setPropertyValueOptions(prev => ({ ...prev, [filterIdentifier + '_' + propertyName]: data.values || [] }));
        } catch (err) {
            console.error(`Error fetching values for ${propertyName}:`, err);
             setPropertyValueOptions(prev => ({ ...prev, [filterIdentifier + '_' + propertyName]: [] }));
        }
    };

    const addOptionalFilter = () => {
        setFilters(prev => ({
            ...prev,
            optional_filters: [...prev.optional_filters, { id: Date.now().toString(), property_name: '', property_values: [], property_source: 'event' }]
        }));
    };

    const removeOptionalFilter = (idToRemove) => {
        setFilters(prev => ({
            ...prev,
            optional_filters: prev.optional_filters.filter(f => f.id !== idToRemove)
        }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setIsLoading(true);
        setError(null);
        setAnalysisResult(null);
        setUserTimelineData(null);
        setTimelineError(null);
        
        // Reset event revenue timeline state
        setEventRevenueTimelineData(null);
        setEventRevenueTimelineError(null);
        setSelectedUserId('');
        setSelectedProductId('');
        
        // Reset warning state
        setCohortDefinitionWarning(null);
        
        try {
            // Basic validation
            if (!filters.date_from_str || !filters.date_to_str) {
                throw new Error("Start Date and End Date are required.");
            }

            // Remove the check that requires primary filter when secondary filters exist
            // We'll allow analysis without a primary filter
            
            // Check primary filter has values if property is selected
            if (filters.optional_filters.length > 0 && filters.optional_filters[0].property_name && filters.optional_filters[0].property_values.length === 0) {
                throw new Error("Please select values for the optional filters or clear the property selection.");
            }

            // Validate secondary filters
            for (const filter of filters.optional_filters) {
                if (!filter.property_name || filter.property_values.length === 0) {
                    throw new Error(`Optional filter is incomplete. Please select both property and values for all filters or remove unused ones.`);
                }
            }

            // Define precise cohort filtering requirements for the backend
            // Define precise cohort filtering requirements for the backend
            // This ensures ONLY users with a trial started event in the specified date range are included
            const cohortFilters = {
                ...filters,
                enforce_trial_started: true, // Add flag to inform backend to enforce trial started requirement
                cohort_definition: {
                    required_event: "RC Trial started",
                    date_range: {
                        from: filters.date_from_str,
                        to: filters.date_to_str
                    },
                    strict_filtering: true // Only include events from users in this cohort
                }
            };

            const result = await api.analyzeCohortData(cohortFilters);
            if (result.error) {
                setError(result.error);
            } else {
                // Verify that the backend properly applied our cohort definition
                // by checking if trial_started user count matches the total cohort users
                if (result.cohort_event_occurrence && 
                    result.cohort_event_occurrence.trial_started && 
                    result.summary_stats && 
                    result.summary_stats.total_users_in_final_cohort) {
                    
                    const trialStartedUsers = result.cohort_event_occurrence.trial_started.user_count;
                    const totalCohortUsers = result.summary_stats.total_users_in_final_cohort;
                    
                    // The counts should match if cohort definition was applied correctly
                    if (trialStartedUsers !== totalCohortUsers) {
                        const warningMessage = 
                            `Warning: Cohort definition may not be correctly applied. ` +
                            `Cohort users (${totalCohortUsers}) should equal ` +
                            `trial started users (${trialStartedUsers}).`;
                        
                        console.warn(warningMessage);
                        setCohortDefinitionWarning(warningMessage);
                    }
                }
                
                setAnalysisResult(result);
                // Fetch timeline data after successful analysis
                fetchUserTimeline();
                // Fetch event revenue timeline data (aggregate mode)
                fetchEventRevenueTimeline();
            }
        } catch (err) {
            console.error("Analysis error:", err);
            setError(err.message || "Analysis failed. Please check that you have selected valid filter criteria.");
        }
        setIsLoading(false);
    };

    const fetchUserTimeline = async () => {
        setIsLoadingTimeline(true);
        try {
            console.log('[DEBUG] fetchUserTimeline called with filters:', filters);
            // Use only the basic filters that the unified pipeline expects
            const timelineData = await api.getCohortUserTimeline(filters);
            console.log('[DEBUG] fetchUserTimeline response:', timelineData);
            if (timelineData.error) {
                console.log('[DEBUG] fetchUserTimeline error:', timelineData.error);
                setTimelineError(timelineData.error);
            } else {
                console.log('[DEBUG] Setting userTimelineData:', timelineData);
                setUserTimelineData(timelineData);
            }
        } catch (err) {
            console.error("Timeline fetch error:", err);
            setTimelineError(err.message || "Could not load user timeline data.");
        }
        setIsLoadingTimeline(false);
    };
    
    const fetchEventRevenueTimeline = async (distinctId = null, productId = null) => {
        setIsLoadingEventRevenueTimeline(true);
        setEventRevenueTimelineError(null);
        
        try {
            console.log('[DEBUG] fetchEventRevenueTimeline called with filters:', filters, 'distinctId:', distinctId, 'productId:', productId);
            // Use only the basic filters that the unified pipeline expects
            const timelineData = await api.getUserEventRevenueTimeline(filters, distinctId, productId);
            console.log('[DEBUG] fetchEventRevenueTimeline response:', timelineData);
            if (timelineData.error) {
                console.log('[DEBUG] fetchEventRevenueTimeline error:', timelineData.error);
                setEventRevenueTimelineError(timelineData.error);
            } else {
                console.log('[DEBUG] Setting eventRevenueTimelineData:', timelineData);
                setEventRevenueTimelineData(timelineData);
                
                // Update available users and products from the response
                if (timelineData.available_users) {
                    setAvailableUsers(timelineData.available_users);
                }
                if (timelineData.available_products) {
                    setAvailableProducts(timelineData.available_products);
                }
            }
        } catch (err) {
            console.error("Event revenue timeline fetch error:", err);
            setEventRevenueTimelineError(err.message || "Could not load event revenue timeline data.");
        }
        setIsLoadingEventRevenueTimeline(false);
    };
    
    // Function to determine cell state based on user's event history
    // Returns the event state for a day (used for cell background color)
    const getCellState = (userProductKey, date, eventMap) => {
        // If this user has no events on this date, return default state
        if (!eventMap[userProductKey] || !eventMap[userProductKey][date]) {
            // Look for the most recent event before this date to maintain state
            let mostRecentEventDate = null;
            let mostRecentState = 'default';
            
            if (eventMap[userProductKey]) {
                // Find the most recent event before this date
                for (const eventDate in eventMap[userProductKey]) {
                    if (eventDate <= date && (!mostRecentEventDate || eventDate > mostRecentEventDate)) {
                        mostRecentEventDate = eventDate;
                        // Get the last event of that day which would determine state going forward
                        const events = eventMap[userProductKey][eventDate];
                        const lastEvent = events[events.length - 1];
                        
                        // Determine the state based on the last event
                        if (lastEvent.type === 'RC Trial started') {
                            mostRecentState = 'trial_started';
                        } else if (lastEvent.type === 'RC Trial converted') {
                            mostRecentState = 'trial_converted';
                        } else if (lastEvent.type === 'RC Trial cancelled') {
                            mostRecentState = 'trial_cancelled';
                        } else if (lastEvent.type === 'RC Initial purchase') {
                            mostRecentState = 'trial_converted';  // Use same color as trial converted
                        } else if (lastEvent.type === 'RC Renewal') {
                            mostRecentState = 'trial_converted';  // Use same color for renewals
                        } else if (lastEvent.type === 'RC Cancellation') {
                            mostRecentState = lastEvent.has_refund ? 'refunded' : 'conversion_cancelled';
                        }
                    }
                }
            }
            
            return mostRecentState;
        }
        
        // Get the first event for this user on this date (which determines cell color)
        const firstEvent = eventMap[userProductKey][date][0];
        
        // Return cell state based on the first event type
        if (firstEvent.type === 'RC Trial started') {
            return 'trial_started';
        } else if (firstEvent.type === 'RC Trial converted') {
            return 'trial_converted';
        } else if (firstEvent.type === 'RC Trial cancelled') {
            return 'trial_cancelled';
        } else if (firstEvent.type === 'RC Initial purchase') {
            return 'trial_converted';  // Use same color as trial converted for initial purchase
        } else if (firstEvent.type === 'RC Renewal') {
            return 'trial_converted';  // Use same color for renewals
        } else if (firstEvent.type === 'RC Cancellation') {
            // If cancellation has refund flag, use refunded color
            return firstEvent.has_refund ? 'refunded' : 'conversion_cancelled';
        }
        
        return 'default';
    };
    
    // Function to get events for a specific date (returns an array of events)
    const getEventsForDate = (userProductKey, date, eventMap) => {
        if (!eventMap[userProductKey] || !eventMap[userProductKey][date]) {
            return [];
        }
        return eventMap[userProductKey][date];
    };

    const triggerPropertyDiscovery = async () => {
        setActivePropertyDiscovery(true);
        setPropertyDiscoveryMessage('Property discovery initiated...');
        try {
            const result = await api.triggerCohortPropertyDiscovery(); // Updated API method name
            setPropertyDiscoveryMessage(result.message + " Refresh page after a few moments to see updated filter options.");
            // Optionally, could poll for completion or auto-refresh discoverable props
            setTimeout(() => fetchDiscoverableProperties(), 5000); // Re-fetch after a delay
        } catch (err) {
             setPropertyDiscoveryMessage('Error triggering discovery: ' + (err.message || 'Unknown error'));
        }
        // setActivePropertyDiscovery(false); // Keep message displayed
    }

    const renderLifecycleTable = (title, ratesData, denominatorKey = "rate_denominator_count") => {
        // Add debugging
        console.log(`[DEBUG] renderLifecycleTable called for "${title}"`);
        console.log(`[DEBUG] ratesData:`, ratesData);
        
        if (!ratesData) {
            console.log(`[DEBUG] No ratesData for "${title}"`);
            return <p className="text-sm text-gray-500 dark:text-gray-300">Lifecycle rate data not available for {title}.</p>;
        }
        
        // Handle new product-based format
        const isNewFormat = ratesData.rates_by_product || ratesData.aggregate_rates;
        console.log(`[DEBUG] isNewFormat: ${isNewFormat} for "${title}"`);
        
        if (isNewFormat) {
            const aggregateRates = ratesData.aggregate_rates;
            const productRates = ratesData.rates_by_product || {};
            
            console.log(`[DEBUG] aggregateRates:`, aggregateRates);
            console.log(`[DEBUG] productRates:`, productRates);
            
            if (!aggregateRates || !aggregateRates.rates_by_day || Object.keys(aggregateRates.rates_by_day).length === 0) {
                console.log(`[DEBUG] No valid aggregateRates for "${title}"`);
                
                // Show raw data for debugging
                return (
                    <div className="mb-6 p-4 bg-yellow-50 dark:bg-yellow-900/30 rounded-lg">
                        <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">{title} - Debug Info</h4>
                        <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">Raw data structure (for debugging):</p>
                        <pre className="text-xs bg-gray-100 dark:bg-gray-800 p-2 rounded overflow-x-auto">
                            {JSON.stringify(ratesData, null, 2)}
                        </pre>
                    </div>
                );
            }
            
            // Determine which rates to display based on global product selector
            const displayRates = globalSelectedProduct && productRates[globalSelectedProduct] 
                ? productRates[globalSelectedProduct] 
                : aggregateRates;
            
            const ratesByDay = displayRates.rates_by_day;
            const sortedDays = Object.keys(ratesByDay).map(Number).sort((a, b) => a - b);
            
            // Prepare row data for the transposed table
            const rowData = [
                {
                    metric: "Raw Daily Cancellation %",
                    dataKey: "raw_daily_cancellation_rate"
                },
                {
                    metric: "Smoothed Daily Cancellation %", 
                    dataKey: "smoothed_daily_cancellation_rate"
                },
                {
                    metric: "Raw Cumulative Cancellation %",
                    dataKey: "raw_cumulative_cancellation_rate"
                },
                {
                    metric: "Smoothed Cumulative Cancellation %",
                    dataKey: "smoothed_cumulative_cancellation_rate"
                },
                {
                    metric: "Raw Daily Refund %",
                    dataKey: "raw_daily_refund_rate"
                },
                {
                    metric: "Smoothed Daily Refund %",
                    dataKey: "smoothed_daily_refund_rate"
                },
                {
                    metric: "Raw Cumulative Refund %",
                    dataKey: "raw_cumulative_refund_rate"
                },
                {
                    metric: "Smoothed Cumulative Refund %",
                    dataKey: "smoothed_cumulative_refund_rate"
                }
            ];
            
            return (
                <div className="mb-6">
                    <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">{title}</h4>
                    
                    {/* Smoothing Information */}
                    <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
                        <h5 className="font-semibold text-blue-800 dark:text-blue-200 mb-2">Smoothing Method Information</h5>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-blue-700 dark:text-blue-300">
                            <div>
                                <p><strong>Method:</strong> {displayRates.smoothing_method || 'none'}</p>
                                <p><strong>Quality:</strong> {displayRates.smoothing_quality || 'N/A'}</p>
                                <p><strong>Sample Size:</strong> {displayRates.total_sample_size || 0}</p>
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
                    
                    {/* Transposed Table with Frozen First Column */}
                    <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                        <div className="flex">
                            {/* Frozen First Column */}
                            <div className="bg-gray-50 dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700">
                                <div className="px-4 py-3 font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700">
                                    Metric
                                </div>
                                {rowData.map((row, index) => (
                                    <div key={index} className={`px-4 py-3 text-sm border-b border-gray-200 dark:border-gray-700 ${
                                        index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800'
                                    } text-gray-900 dark:text-white font-medium`}>
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
                                                    <div key={index} className={`px-4 py-3 text-sm text-center border-b border-gray-200 dark:border-gray-700 ${
                                                        index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800'
                                                    } text-gray-500 dark:text-gray-300`}>
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
        }
        
        // Fall back to legacy format handling if needed
        if (!ratesData.rates_by_day || Object.keys(ratesData.rates_by_day).length === 0) {
            return <p className="text-sm text-gray-500 dark:text-gray-300">No lifecycle rate data available for {title}.</p>;
        }
        
        const sortedDays = Object.keys(ratesData.rates_by_day).map(Number).sort((a, b) => a - b);
        const ratesDenominator = ratesData[denominatorKey] || 0;
        
        // Legacy format with transposed layout
        const legacyRowData = [
            { metric: "Daily Cancel %", dataKey: "cancelled_daily_pct" },
            { metric: "Cumulative Cancel %", dataKey: "cancelled_cumulative_pct" },
            { metric: "Daily Convert %", dataKey: "converted_daily_pct" },
            { metric: "Cumulative Convert %", dataKey: "converted_cumulative_pct" }
        ];
        
        return (
            <div className="mb-6">
                <h4 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">{title}</h4>
                <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
                    Based on {ratesDenominator} users. {ratesData.message}
                </p>
                
                {/* Transposed Legacy Table */}
                <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                    <div className="flex">
                        {/* Frozen First Column */}
                        <div className="bg-gray-50 dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700">
                            <div className="px-4 py-3 font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700">
                                Metric
                            </div>
                            {legacyRowData.map((row, index) => (
                                <div key={index} className={`px-4 py-3 text-sm border-b border-gray-200 dark:border-gray-700 ${
                                    index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800'
                                } text-gray-900 dark:text-white font-medium`}>
                                    {row.metric}
                                </div>
                            ))}
                        </div>
                        
                        {/* Scrollable Data Columns */}
                        <div className="flex-1 overflow-x-auto">
                            <div className="flex">
                                {sortedDays.map((day) => {
                                    const dayStr = day.toString();
                                    const dayData = ratesData.rates_by_day[dayStr];
                                    
                                    return (
                                        <div key={dayStr} className="min-w-[100px] border-r border-gray-200 dark:border-gray-700 last:border-r-0">
                                            {/* Header */}
                                            <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 font-medium text-gray-900 dark:text-white text-center border-b border-gray-200 dark:border-gray-700">
                                                Day {day}
                                            </div>
                                            
                                            {/* Data Cells */}
                                            {legacyRowData.map((row, index) => (
                                                <div key={index} className={`px-4 py-3 text-sm text-center border-b border-gray-200 dark:border-gray-700 ${
                                                    index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800'
                                                } text-gray-500 dark:text-gray-300`}>
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

    const formatCurrency = (value) => {
        if (value === null || value === undefined) return "N/A";
        return value.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
    };
    
    // Prepare data for the chart
    const chartData = analysisResult?.daily_data.map(d => {
        const actualNetRevenue = d.cumulative_revenue_actual_net || 0;
        // Dotted line: actual net cumulative revenue + (estimated additional from pending trials) + (net effect of estimated future refunds on actual revenue)
        // The estimated_future_refunds_on_actual_revenue_amount is an *amount* to be subtracted.
        const projectedNetRevenue = actualNetRevenue + 
                                    (d.estimated_additional_revenue_from_pending_trials_net_refund || 0) -
                                    (d.estimated_future_refunds_on_actual_revenue_amount || 0);
        
        const trialsStarted = d.daily_trials_started || 0;
        const trialsConverted = d.daily_trials_converted_actual || 0;
        const trialsCancelled = d.daily_trials_cancelled_actual || 0;
        const trialsEnded = trialsConverted + trialsCancelled;
        const trialsPending = d.daily_trials_started - trialsEnded;
        
        return {
            date: d.date,
            // Absolute values - Daily
            daily_trials_started: trialsStarted,
            daily_trials_ended: trialsEnded,
            daily_trials_pending: trialsPending,
            daily_conversions: trialsConverted,
            daily_conversions_net_refunds: trialsConverted - (d.daily_refund_count_actual || 0),
            daily_estimated_conversions_net_refunds: trialsConverted - (d.daily_estimated_refunds || 0),
            daily_refunds: d.daily_refund_count_actual || 0,
            
            // Money values - Daily
            daily_revenue: d.daily_revenue_actual || 0,
            daily_revenue_net: d.daily_revenue_actual_net || 0,
            daily_estimated_revenue: d.daily_estimated_revenue || 0,
            
            // Absolute values - Cumulative
            cumulative_trials_started: d.cumulative_trials_started || 0,
            cumulative_trials_ended: (d.cumulative_trials_converted_actual || 0) + (d.cumulative_trials_cancelled_actual || 0),
            cumulative_trials_pending: d.cumulative_trials_started - ((d.cumulative_trials_converted_actual || 0) + (d.cumulative_trials_cancelled_actual || 0)),
            cumulative_conversions: d.cumulative_trials_converted_actual || 0,
            cumulative_conversions_net_refunds: (d.cumulative_trials_converted_actual || 0) - (d.cumulative_refund_count_actual || 0),
            cumulative_estimated_conversions_net_refunds: (d.cumulative_trials_converted_actual || 0) - (d.cumulative_estimated_refunds || 0),
            cumulative_refunds: d.cumulative_refund_count_actual || 0,
            
            // Money values - Cumulative
            cumulative_revenue: d.cumulative_revenue_actual || 0,
            cumulative_revenue_net: actualNetRevenue,
            cumulative_estimated_revenue: projectedNetRevenue,
        };
    }) || [];
    
    const [visibleAbsoluteMetrics, setVisibleAbsoluteMetrics] = useState({
        daily_trials_started: true,
        daily_trials_ended: false,
        daily_trials_pending: false,
        daily_conversions: true,
        daily_conversions_net_refunds: false,
        daily_estimated_conversions_net_refunds: false,
        daily_refunds: false,
        cumulative_trials_started: true,
        cumulative_trials_ended: false,
        cumulative_trials_pending: false,
        cumulative_conversions: true,
        cumulative_conversions_net_refunds: false,
        cumulative_estimated_conversions_net_refunds: false,
        cumulative_refunds: false,
    });
    
    const [visibleMoneyMetrics, setVisibleMoneyMetrics] = useState({
        daily_revenue: true,
        daily_revenue_net: false,
        daily_estimated_revenue: false,
        cumulative_revenue: false,
        cumulative_revenue_net: true,
        cumulative_estimated_revenue: true,
    });

    const toggleAbsoluteMetric = (metricKey) => {
        setVisibleAbsoluteMetrics(prev => ({...prev, [metricKey]: !prev[metricKey]}));
    };
    
    const toggleMoneyMetric = (metricKey) => {
        setVisibleMoneyMetrics(prev => ({...prev, [metricKey]: !prev[metricKey]}));
    };

    // Add this function to determine which styles to use based on dark mode detection
    const getSelectTheme = () => {
        const isDarkMode = document.documentElement.classList.contains('dark') || 
                          window.matchMedia('(prefers-color-scheme: dark)').matches;
        return isDarkMode ? darkModeSelectStyles : customSelectStyles;
    };

    // Chart Modal Component
    const ChartModal = () => {
        if (!showChartModal || !analysisResult?.lifecycle_rates) return null;

        const lifecycleRates = analysisResult.lifecycle_rates;
        
        // Get chart data for different scenarios
        const getChartData = (ratesData, title, type = 'cancellation_refund') => {
            if (!ratesData?.rates_by_day) return null;

            const days = Object.keys(ratesData.rates_by_day).map(Number).sort((a, b) => a - b);
            
            let datasets = [];
            
            if (type === 'cancellation_refund') {
                // Cancellation/Refund scenario
                datasets = [
                    {
                        label: 'Raw Cumulative Cancellation %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.raw_cumulative_cancellation_rate || 0),
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        borderDash: [],
                        tension: 0.1
                    },
                    {
                        label: 'Smoothed Cumulative Cancellation %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.smoothed_cumulative_cancellation_rate || 0),
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.3)',
                        borderDash: [5, 5],
                        tension: 0.1
                    },
                    {
                        label: 'Raw Cumulative Refund %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.raw_cumulative_refund_rate || 0),
                        borderColor: 'rgb(54, 162, 235)',
                        backgroundColor: 'rgba(54, 162, 235, 0.1)',
                        borderDash: [],
                        tension: 0.1
                    },
                    {
                        label: 'Smoothed Cumulative Refund %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.smoothed_cumulative_refund_rate || 0),
                        borderColor: 'rgb(54, 162, 235)',
                        backgroundColor: 'rgba(54, 162, 235, 0.3)',
                        borderDash: [5, 5],
                        tension: 0.1
                    }
                ];
            } else if (type === 'conversion_cancellation') {
                // Trial Conversion/Cancellation scenario
                datasets = [
                    {
                        label: 'Raw Cumulative Conversion %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.raw_cumulative_conversion_rate || 0),
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        borderDash: [],
                        tension: 0.1
                    },
                    {
                        label: 'Smoothed Cumulative Conversion %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.smoothed_cumulative_conversion_rate || 0),
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.3)',
                        borderDash: [5, 5],
                        tension: 0.1
                    },
                    {
                        label: 'Raw Cumulative Cancellation %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.raw_cumulative_cancellation_rate || 0),
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        borderDash: [],
                        tension: 0.1
                    },
                    {
                        label: 'Smoothed Cumulative Cancellation %',
                        data: days.map(day => ratesData.rates_by_day[day.toString()]?.smoothed_cumulative_cancellation_rate || 0),
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.3)',
                        borderDash: [5, 5],
                        tension: 0.1
                    }
                ];
            }

            return {
                labels: days.map(day => `Day ${day}`),
                datasets: datasets
            };
        };

        const chartOptions = {
            responsive: true,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: true,
                    text: 'Raw vs Smoothed Cumulative Rates'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Percentage (%)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Days'
                    }
                }
            }
        };

        // Determine which rates to use based on global product selector
        const getDisplayRates = (ratesData) => {
            if (!ratesData) return null;
            if (globalSelectedProduct && ratesData.rates_by_product && ratesData.rates_by_product[globalSelectedProduct]) {
                return ratesData.rates_by_product[globalSelectedProduct];
            }
            return ratesData.aggregate_rates || ratesData;
        };

        const trialToOutcomeRates = getDisplayRates(lifecycleRates.trial_to_outcome_rates);
        const cancellationRefundRates = getDisplayRates(lifecycleRates.trial_to_cancellation_rates || lifecycleRates.trial_to_refund_rates);

        return (
            <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
                <div className="bg-white dark:bg-gray-800 rounded-lg max-w-6xl w-full max-h-[90vh] overflow-y-auto">
                    <div className="p-6">
                        <div className="flex justify-between items-center mb-6">
                            <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                                Lifecycle Rate Charts
                            </h2>
                            <button
                                onClick={() => setShowChartModal(false)}
                                className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
                            >
                                <XCircle size={24} />
                            </button>
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            {/* Trial to Outcome Chart */}
                            {trialToOutcomeRates && (
                                <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                                    <h3 className="text-lg font-semibold mb-4 text-gray-900 dark:text-white">
                                        Trial to Outcome (Conversion vs Cancellation)
                                    </h3>
                                    <ChartLine 
                                        data={getChartData(trialToOutcomeRates, 'Trial to Outcome', 'conversion_cancellation')}
                                        options={chartOptions}
                                    />
                                </div>
                            )}

                            {/* Cancellation/Refund Chart */}
                            {cancellationRefundRates && (
                                <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                                    <h3 className="text-lg font-semibold mb-4 text-gray-900 dark:text-white">
                                        Cancellation vs Refund Rates
                                    </h3>
                                    <ChartLine 
                                        data={getChartData(cancellationRefundRates, 'Cancellation vs Refund', 'cancellation_refund')}
                                        options={chartOptions}
                                    />
                                </div>
                            )}
                        </div>

                        <div className="mt-6 text-sm text-gray-600 dark:text-gray-400">
                            <p><strong>Legend:</strong></p>
                            <ul className="list-disc list-inside mt-2">
                                <li>Solid lines represent raw data</li>
                                <li>Dashed lines represent smoothed curves</li>
                                <li>The smoothing algorithm first smooths cumulative rates, then derives daily rates</li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        );
    };

    return (
        <div className="p-6 max-w-full mx-auto bg-white dark:bg-gray-800 min-h-screen">
            <h1 className="text-3xl font-bold mb-6 text-gray-800 dark:text-white border-b pb-2 border-gray-200 dark:border-gray-600">Cohort Analyzer</h1>

            {isInitializing ? (
                <div className="flex items-center justify-center p-8 bg-white dark:bg-gray-700 rounded-lg shadow-md">
                    <Loader className="animate-spin mr-2 text-indigo-600 dark:text-indigo-400" />
                    <p className="text-gray-700 dark:text-white">Initializing cohort analyzer...</p>
                </div>
            ) : (
                <form onSubmit={handleSubmit} className="bg-white dark:bg-gray-700 p-6 rounded-lg shadow-md mb-8">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6 border-b border-gray-200 dark:border-gray-600 pb-6">
                        <div>
                            <label htmlFor="date_from_str" className="block text-sm font-medium text-gray-700 dark:text-white mb-2">Start Date *</label>
                            <input type="date" id="date_from_str" value={filters.date_from_str} onChange={e => handleFilterChange('date_from_str', e.target.value)}
                                className="mt-1 block w-full p-2.5 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-600 text-gray-900 dark:text-white" required />
                        </div>
                        <div>
                            <label htmlFor="date_to_str" className="block text-sm font-medium text-gray-700 dark:text-white mb-2">End Date *</label>
                            <input type="date" id="date_to_str" value={filters.date_to_str} onChange={e => handleFilterChange('date_to_str', e.target.value)}
                                className="mt-1 block w-full p-2.5 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-600 text-gray-900 dark:text-white" required />
                        </div>
                    </div>

                    {/* Optional Filters Section */}
                    <div className="mb-6 p-5 border rounded-md border-teal-200 dark:border-teal-500 bg-teal-50 dark:bg-teal-900/40">
                        <div className="flex justify-between items-center mb-3">
                            <h3 className="text-lg font-semibold text-gray-800 dark:text-white">Additional Event/User Property Filters</h3>
                            <button 
                                type="button" 
                                onClick={addOptionalFilter}
                                className="p-1.5 bg-teal-100 dark:bg-teal-600 text-teal-700 dark:text-white rounded-full hover:bg-teal-200 dark:hover:bg-teal-500 transition-colors"
                            >
                                <PlusCircle size={18} />
                            </button>
                        </div>
                        {filters.optional_filters.length === 0 && (
                            <p className="text-sm text-gray-600 dark:text-gray-300 italic mb-2">No additional filters. Click the + button to add a filter.</p>
                        )}
                        {filters.optional_filters.map((filter, index) => (
                            <div key={filter.id} className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end mb-4 p-4 border-t dark:border-gray-600 bg-white dark:bg-gray-600 rounded-md shadow-sm">
                                <div>
                                    <label className="block text-xs font-medium text-gray-600 dark:text-white mb-1">Source Type</label>
                                    <select value={filter.property_source}
                                            onChange={e => handleFilterChange(`optional_filter_source`, e.target.value, index)}
                                            className="block w-full p-2 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-teal-500 focus:border-teal-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                                        <option value="event">Event Property</option>
                                        <option value="user">User Property</option>
                                    </select>
                                </div>
                                <div>
                                    <label className="block text-xs font-medium text-gray-600 dark:text-white mb-1">Property Name</label>
                                    <select value={filter.property_name}
                                            onChange={e => handleFilterChange(`optional_filter_property`, e.target.value, index)}
                                            className="block w-full p-2 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-teal-500 focus:border-teal-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                                        <option value="">-- Select Property --</option>
                                        {(filter.property_source === 'event' ? availableEventProperties : availableUserProperties).map(p => <option key={p.key} value={p.key}>{p.display_name || p.key}</option>)}
                                    </select>
                                </div>
                                <div>
                                    <label className="block text-xs font-medium text-gray-600 dark:text-white mb-1">Property Value(s)</label>
                                    <Select
                                        isMulti
                                        isDisabled={!filter.property_name || !filter.property_source}
                                        options={(propertyValueOptions[`optional_filter_${index}_` + filter.property_name] || []).map(val => ({ value: val, label: val }))}
                                        value={filter.property_values.map(val => ({ value: val, label: val }))}
                                        onChange={(selected) => handleFilterChange(`optional_filter_values`, selected ? selected.map(option => option.value) : [], index)}
                                        styles={getSelectTheme()}
                                        placeholder="Select values..."
                                    />
                                </div>
                                <div className="flex justify-end">
                                    <button type="button" onClick={() => removeOptionalFilter(filter.id)}
                                            className="p-1.5 bg-red-100 dark:bg-red-700 text-red-600 dark:text-white rounded-full hover:bg-red-200 dark:hover:bg-red-600 transition-colors">
                                        <XCircle size={18} />
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>

                    <div className="flex items-center justify-between mt-8">
                        <button type="submit" disabled={isLoading}
                                className="px-6 py-2.5 bg-indigo-600 text-white font-semibold rounded-md shadow hover:bg-indigo-700 disabled:opacity-50 flex items-center transition-colors">
                            {isLoading ? (
                                <>
                                    <Loader className="animate-spin mr-2" size={16} />
                                    Analyzing...
                                </>
                            ) : 'Analyze Cohort'}
                        </button>
                        <button type="button" onClick={triggerPropertyDiscovery} disabled={activePropertyDiscovery}
                                className="px-5 py-2.5 bg-teal-600 text-white text-sm font-medium rounded-md hover:bg-teal-700 disabled:opacity-50 flex items-center transition-colors">
                            {activePropertyDiscovery ? (
                                <>
                                    <Loader className="animate-spin mr-2" size={16} />
                                    Discovery Running...
                                </>
                            ) : "Discover Filter Properties"}
                        </button>
                    </div>
                    {propertyDiscoveryMessage && (
                        <div className="mt-3 p-3 bg-teal-50 dark:bg-teal-900/40 border border-teal-200 dark:border-teal-600 rounded-md">
                            <p className="text-sm text-teal-700 dark:text-teal-300">{propertyDiscoveryMessage}</p>
                        </div>
                    )}
                </form>
            )}

            {error && (
                <div className="my-4 p-4 bg-red-100 dark:bg-red-800 text-red-700 dark:text-white rounded-md shadow-md border border-red-300 dark:border-red-600">
                    <div className="flex items-center">
                        <AlertTriangle className="mr-2 flex-shrink-0" />
                        <div>
                            <span className="font-medium block">Analysis Error:</span>
                            <span className="block mt-1">{error}</span>
                        </div>
                    </div>
                </div>
            )}
            
            {cohortDefinitionWarning && (
                <div className="my-4 p-4 bg-yellow-100 dark:bg-yellow-800 text-yellow-700 dark:text-white rounded-md shadow-md border border-yellow-300 dark:border-yellow-600">
                    <div className="flex items-center">
                        <AlertTriangle className="mr-2 flex-shrink-0" />
                        <div>
                            <span className="font-medium block">Cohort Definition Warning:</span>
                            <span className="block mt-1">{cohortDefinitionWarning}</span>
                            <span className="block mt-1 text-sm italic">The backend may not be correctly filtering users based on trial started events.</span>
                        </div>
                    </div>
                </div>
            )}

            {analysisResult && (
                <div className="mt-8 bg-white dark:bg-gray-700 p-6 rounded-lg shadow-md">
                    <h2 className="text-2xl font-bold mb-4 text-gray-800 dark:text-white">Analysis Results</h2>
                    
                    {/* Add the filter stats display component */}
                    {analysisResult.filter_stats && (
                        <FilterStatsDisplay filterStats={analysisResult.filter_stats} />
                    )}
                    
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                        <div className="p-4 bg-gray-50 dark:bg-gray-600 rounded-md">
                            <h3 className="font-semibold text-gray-700 dark:text-white">Cohort Users (Trial Started)</h3>
                            <p className="text-2xl font-bold text-indigo-600 dark:text-indigo-300">{analysisResult.summary_stats?.total_users_in_final_cohort || 0}</p>
                            <p className="text-xs text-gray-500 dark:text-gray-300">Unique users who had an 'RC Trial started' event in the selected date range (and match primary filter if applied).</p>
                        </div>
                        <div className="p-4 bg-gray-50 dark:bg-gray-600 rounded-md">
                            <h3 className="font-semibold text-gray-700 dark:text-white">Events Analyzed (Cohort Only)</h3>
                            <p className="text-2xl font-bold text-indigo-600 dark:text-indigo-300">{analysisResult.summary_stats?.total_events_analyzed || 0}</p>
                            <p className="text-xs text-gray-500 dark:text-gray-300">Events from users in the trial started cohort</p>
                        </div>
                        <div className="p-4 bg-gray-50 dark:bg-gray-600 rounded-md">
                            <h3 className="font-semibold text-gray-700 dark:text-white">Cohort ARPC (Overall)</h3>
                            <p className="text-2xl font-bold text-indigo-600 dark:text-indigo-300">{formatCurrency(analysisResult.summary_stats?.arpc_cohort)}</p>
                            <p className="text-xs text-gray-500 dark:text-gray-300">Basis: {analysisResult.summary_stats?.arpc_datapoints} users. {analysisResult.summary_stats?.arpc_calculation_basis}</p>
                            <p className="text-xs text-gray-500 dark:text-gray-300 italic">See per-product breakdown below</p>
                        </div>
                    </div>

                    {/* Per-Product ARPC Breakdown - REDESIGNED FOR COMPACTNESS */}
                    {eventRevenueTimelineData && eventRevenueTimelineData.arpc_per_product && Object.keys(eventRevenueTimelineData.arpc_per_product).length > 0 && (
                        <div className="mb-6">
                            <h3 className="font-semibold text-gray-700 dark:text-white mb-3">ARPC Per Product</h3>
                            <div className="bg-gray-50 dark:bg-gray-600 rounded-lg p-3">
                                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
                                    {Object.entries(eventRevenueTimelineData.arpc_per_product).map(([productId, arpcData]) => (
                                        <div key={productId} className="bg-white dark:bg-gray-700 rounded-md p-3 border border-gray-200 dark:border-gray-600">
                                            <div className="text-xs font-medium text-gray-600 dark:text-gray-300 truncate" title={productId}>
                                                {productId}
                                            </div>
                                            <div className="text-lg font-bold text-green-600 dark:text-green-400 leading-tight">
                                                {typeof arpcData === 'object' ? formatCurrency(arpcData.arpc) : formatCurrency(arpcData)}
                                            </div>
                                            {typeof arpcData === 'object' && (
                                                <div className="text-xs text-gray-500 dark:text-gray-400 mt-1 space-y-0.5">
                                                    <div>{arpcData.event_count} events</div>
                                                    <div>{arpcData.unique_users} users</div>
                                                    <div className="font-medium">{formatCurrency(arpcData.total_revenue)}</div>
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    )}

                    {analysisResult.event_user_counts && (
                        <div className="mb-6">
                            <h3 className="font-semibold text-gray-700 dark:text-white mb-3">Unique Users Per Event Type</h3>
                            <div className="overflow-x-auto">
                                <table className="min-w-full text-sm border dark:border-gray-600">
                                    <thead className="bg-gray-50 dark:bg-gray-700">
                                        <tr>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Event Type</th>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Unique Users</th>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Total Events</th>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Events Per User</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white dark:bg-gray-800">
                                        {Object.entries(analysisResult.event_user_counts).map(([eventType, counts], index) => (
                                            <tr key={eventType} className={index % 2 === 0 ? '' : 'bg-gray-50 dark:bg-gray-700'}>
                                                <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">{eventType}</td>
                                                <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">{counts.unique_users || 0}</td>
                                                <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">{counts.total_events || 0}</td>
                                                <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                    {counts.unique_users > 0 ? (counts.total_events / counts.unique_users).toFixed(2) : '0.00'}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Cohort Event Occurrence Table */}
                    {analysisResult.cohort_event_occurrence && (
                        <div className="mb-6">
                            <h3 className="font-semibold text-gray-700 dark:text-white mb-3">Cohort Event Occurrence</h3>
                            <p className="text-sm text-gray-600 dark:text-gray-300 mb-2">
                                Number of users in the cohort who have had each event at least once. 
                                <span className="italic"> Note: Cohort is defined as users in the database who have a "Trial started" event within the selected date range. All other users are excluded.</span>
                            </p>
                            <div className="overflow-x-auto">
                                <table className="min-w-full text-sm border dark:border-gray-600">
                                    <thead className="bg-gray-50 dark:bg-gray-700">
                                        <tr>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">Event</th>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">User Count</th>
                                            <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white">% of Cohort</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white dark:bg-gray-800">
                                        {/* Trial Started */}
                                        <tr className="bg-indigo-50 dark:bg-indigo-900/30">
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-white">Trial Started</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_started?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                100%
                                            </td>
                                        </tr>
                                        
                                        {/* Trial Ended */}
                                        <tr>
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">Trial Ended (Total)</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_ended?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_ended?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.trial_ended.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* Trial Converted */}
                                        <tr className="bg-gray-50 dark:bg-gray-700 pl-4">
                                            <td className="p-2 border-b dark:border-gray-600 pl-8 text-gray-700 dark:text-gray-300">→ Trial Converted</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_converted?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_converted?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.trial_converted.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* Trial Cancelled */}
                                        <tr className="bg-gray-50 dark:bg-gray-700">
                                            <td className="p-2 border-b dark:border-gray-600 pl-8 text-gray-700 dark:text-gray-300">→ Trial Cancelled</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_cancelled?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.trial_cancelled?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.trial_cancelled.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* Initial Purchase */}
                                        <tr>
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">Initial Purchase</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.initial_purchase?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.initial_purchase?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.initial_purchase.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* Renewal */}
                                        <tr className="bg-gray-50 dark:bg-gray-700">
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">Renewal</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.renewal?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.renewal?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.renewal.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* RC Cancellation */}
                                        <tr>
                                            <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300">RC Cancellation (Total)</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_cancellation?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_cancellation?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.rc_cancellation.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* RC Refund (revenue < 0) */}
                                        <tr className="bg-gray-50 dark:bg-gray-700">
                                            <td className="p-2 border-b dark:border-gray-600 pl-8 text-gray-700 dark:text-gray-300">→ Refund (revenue &lt; 0)</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_refund?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_refund?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.rc_refund.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                        
                                        {/* RC Cancelled (revenue = 0) */}
                                        <tr className="bg-gray-50 dark:bg-gray-700">
                                            <td className="p-2 border-b dark:border-gray-600 pl-8 text-gray-700 dark:text-gray-300">→ Cancelled (revenue = 0)</td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_cancel_no_refund?.user_count?.toLocaleString() || '0'}
                                            </td>
                                            <td className="p-2 border-b dark:border-gray-600 text-gray-700 dark:text-gray-300">
                                                {analysisResult.cohort_event_occurrence.rc_cancel_no_refund?.percentage ? 
                                                    analysisResult.cohort_event_occurrence.rc_cancel_no_refund.percentage.toFixed(1) + '%' : '0%'}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    <div className="mb-4">
                        <button onClick={() => setShowLifecycleTables(!showLifecycleTables)}
                                className="text-indigo-600 dark:text-indigo-300 hover:underline text-sm mb-2">
                            {showLifecycleTables ? <ChevronDown className="inline mr-1" size={16}/> : <ChevronRight className="inline mr-1" size={16}/>}
                            Show/Hide Lifecycle Rate Tables
                        </button>
                        {/* DEBUG: Moved outside JSX */}
                        {(() => {
                            console.log('[DEBUG] showLifecycleTables:', showLifecycleTables, 'lifecycle_rates exists:', !!analysisResult?.lifecycle_rates);
                            if (showLifecycleTables && analysisResult?.lifecycle_rates) {
                                console.log('[DEBUG] Lifecycle tables section should render!', analysisResult.lifecycle_rates);
                                console.log('[DEBUG] Available lifecycle rate keys:', Object.keys(analysisResult.lifecycle_rates));
                            }
                            return null;
                        })()}
                    </div>

                    {/* Fixed: Replaced IIFE with direct conditional rendering */}
                    {showLifecycleTables && analysisResult?.lifecycle_rates && (
                        <div className="space-y-6 mb-8 p-4 border-t dark:border-gray-600">
                            {/* System Information Banner */}
                            <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg border border-blue-200 dark:border-blue-700">
                                <h3 className="text-lg font-semibold text-blue-800 dark:text-blue-200 mb-3">
                                    Enhanced Product-Based Lifecycle Analysis System
                                </h3>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-blue-700 dark:text-blue-300">
                                    <div>
                                        <h4 className="font-semibold mb-2">Key Improvements:</h4>
                                        <ul className="list-disc ml-5 space-y-1">
                                            <li><strong>Fixed Cumulative Calculation:</strong> Now shows actual percentage of cohort that performs action, not forced 100%</li>
                                            <li><strong>Product-Level Granularity:</strong> Analyze lifecycle patterns per product ID</li>
                                            <li><strong>Intelligent Smoothing:</strong> Adaptive smoothing that handles high-early, low-later patterns effectively</li>
                                            <li><strong>Extended Lookback:</strong> Automatically extends lookback 30-60 days when sample size &lt; 100</li>
                                        </ul>
                                    </div>
                                    <div>
                                        <h4 className="font-semibold mb-2">Metrics Displayed:</h4>
                                        <ul className="list-disc ml-5 space-y-1">
                                            <li><strong>Raw Daily Rates:</strong> Actual percentage of cohort performing action on specific day</li>
                                            <li><strong>Smoothed Daily Rates:</strong> Algorithm-enhanced rates accounting for data sparsity</li>
                                            <li><strong>Cumulative Rates:</strong> Total percentage who have performed action by day X (using smoothed data)</li>
                                            <li><strong>Separate Tracking:</strong> 14 days for refunds, 400 days for cancellations</li>
                                        </ul>
                                    </div>
                                </div>
                            </div>

                            {/* Global Product Selector for Lifecycle Tables */}
                            {(() => {
                                // Get all available product IDs from lifecycle rates
                                const allAvailableProducts = new Set();
                                const lifecycleRates = analysisResult.lifecycle_rates || {};
                                
                                Object.values(lifecycleRates).forEach(rateData => {
                                    if (rateData && rateData.rates_by_product) {
                                        Object.keys(rateData.rates_by_product).forEach(productId => {
                                            allAvailableProducts.add(productId);
                                        });
                                    }
                                });
                                
                                const productList = Array.from(allAvailableProducts).sort();
                                
                                if (productList.length > 0) {
                                    return (
                                        <div className="mb-6 p-4 bg-green-50 dark:bg-green-900/30 rounded-lg border border-green-200 dark:border-green-700">
                                            <div className="flex items-center justify-between">
                                                <div>
                                                    <h4 className="font-semibold text-green-800 dark:text-green-200 mb-1">Product Analysis Filter</h4>
                                                    <p className="text-sm text-green-700 dark:text-green-300">
                                                        Select a specific product to analyze, or view aggregate data across all products.
                                                    </p>
                                                </div>
                                                <div className="flex items-center space-x-3">
                                                    <label className="text-sm font-medium text-green-700 dark:text-green-300">
                                                        Product:
                                                    </label>
                                                    <select 
                                                        value={globalSelectedProduct}
                                                        onChange={(e) => setGlobalSelectedProduct(e.target.value)}
                                                        className="px-4 py-2 border border-green-300 dark:border-green-600 rounded-md text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-green-500 focus:border-green-500"
                                                    >
                                                        <option value="">All Products (Aggregate)</option>
                                                        {productList.map(productId => (
                                                            <option key={productId} value={productId}>
                                                                Product {productId}
                                                            </option>
                                                        ))}
                                                    </select>
                                                    <button
                                                        onClick={() => setShowChartModal(true)}
                                                        className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-md transition-colors duration-200 flex items-center space-x-2"
                                                    >
                                                        <span>📊</span>
                                                        <span>View Charts</span>
                                                    </button>
                                                </div>
                                            </div>
                                        </div>
                                    );
                                }
                                return null;
                            })()}

                            {/* Lifecycle Rate Tables */}
                            <div className="space-y-8">
                                {/* Trial Conversion to Cancellation/Refund */}
                                {analysisResult.lifecycle_rates.trial_to_cancellation_rates && (
                                    renderLifecycleTable("Trial Converted -> Cancellation Rates", analysisResult.lifecycle_rates.trial_to_cancellation_rates)
                                )}
                                
                                {analysisResult.lifecycle_rates.trial_to_refund_rates && (
                                    renderLifecycleTable("Trial Converted -> Refund Rates", analysisResult.lifecycle_rates.trial_to_refund_rates)
                                )}
                                
                                {/* Initial Purchase to Cancellation/Refund */}
                                {analysisResult.lifecycle_rates.initial_purchase_to_cancellation_rates && (
                                    renderLifecycleTable("Initial Purchase -> Cancellation Rates", analysisResult.lifecycle_rates.initial_purchase_to_cancellation_rates)
                                )}
                                
                                {analysisResult.lifecycle_rates.initial_purchase_to_refund_rates && (
                                    renderLifecycleTable("Initial Purchase -> Refund Rates", analysisResult.lifecycle_rates.initial_purchase_to_refund_rates)
                                )}
                                
                                {/* Renewal to Cancellation/Refund */}
                                {analysisResult.lifecycle_rates.renewal_to_cancellation_rates && (
                                    renderLifecycleTable("Renewal -> Cancellation Rates", analysisResult.lifecycle_rates.renewal_to_cancellation_rates)
                                )}
                                
                                {analysisResult.lifecycle_rates.renewal_to_refund_rates && (
                                    renderLifecycleTable("Renewal -> Refund Rates", analysisResult.lifecycle_rates.renewal_to_refund_rates)
                                )}
                                
                                {/* Legacy format support */}
                                {analysisResult.lifecycle_rates.trial_to_outcome_rates && (
                                    renderLifecycleTable("Trial to Outcome Rates", analysisResult.lifecycle_rates.trial_to_outcome_rates)
                                )}
                            </div>
                        </div>
                    )}

                    <h3 className="text-xl font-semibold mb-3 text-gray-700 dark:text-white">Daily Performance Charts</h3>
                    
                    {/* Absolute Values Chart */}
                    <div className="mb-6">
                        <h4 className="font-semibold text-gray-700 dark:text-white mb-2">Absolute Values</h4>
                        <div className="mb-4 flex flex-wrap gap-2">
                            {Object.keys(visibleAbsoluteMetrics).map(key => (
                                <button key={key} onClick={() => toggleAbsoluteMetric(key)}
                                        className={`px-3 py-1 text-xs rounded-full ${visibleAbsoluteMetrics[key] ? 'bg-blue-500 text-white' : 'bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-white'}`}>
                                    {key.split('_').join(' ')}
                                </button>
                            ))}
                        </div>
                        <div className="h-80 w-full mb-8 bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="date" />
                                    <YAxis />
                                    <Tooltip formatter={(value) => typeof value === 'number' ? value.toLocaleString() : value} />
                                    <ChartLegend />
                                    
                                    {/* Daily metrics */}
                                    {visibleAbsoluteMetrics.daily_trials_started && (
                                        <Line type="monotone" dataKey="daily_trials_started" stroke="#8884d8" name="Daily Trials Started" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_trials_ended && (
                                        <Line type="monotone" dataKey="daily_trials_ended" stroke="#82ca9d" name="Daily Trials Ended" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_trials_pending && (
                                        <Line type="monotone" dataKey="daily_trials_pending" stroke="#ffc658" name="Daily Trials Pending" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_conversions && (
                                        <Line type="monotone" dataKey="daily_conversions" stroke="#ff7300" name="Daily Conversions" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_conversions_net_refunds && (
                                        <Line type="monotone" dataKey="daily_conversions_net_refunds" stroke="#ff0000" name="Daily Conv. Net Refunds" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_estimated_conversions_net_refunds && (
                                        <Line type="monotone" dataKey="daily_estimated_conversions_net_refunds" stroke="#b967ff" name="Daily Est. Conv. Net Refunds" />
                                    )}
                                    {visibleAbsoluteMetrics.daily_refunds && (
                                        <Line type="monotone" dataKey="daily_refunds" stroke="#ff67b3" name="Daily Refunds" />
                                    )}
                                    
                                    {/* Cumulative metrics */}
                                    {visibleAbsoluteMetrics.cumulative_trials_started && (
                                        <Line type="monotone" dataKey="cumulative_trials_started" stroke="#8884d8" strokeDasharray="5 5" name="Cum. Trials Started" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_trials_ended && (
                                        <Line type="monotone" dataKey="cumulative_trials_ended" stroke="#82ca9d" strokeDasharray="5 5" name="Cum. Trials Ended" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_trials_pending && (
                                        <Line type="monotone" dataKey="cumulative_trials_pending" stroke="#ffc658" strokeDasharray="5 5" name="Cum. Trials Pending" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_conversions && (
                                        <Line type="monotone" dataKey="cumulative_conversions" stroke="#ff7300" strokeDasharray="5 5" name="Cum. Conversions" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_conversions_net_refunds && (
                                        <Line type="monotone" dataKey="cumulative_conversions_net_refunds" stroke="#ff0000" strokeDasharray="5 5" name="Cum. Conv. Net Refunds" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_estimated_conversions_net_refunds && (
                                        <Line type="monotone" dataKey="cumulative_estimated_conversions_net_refunds" stroke="#b967ff" strokeDasharray="5 5" name="Cum. Est. Conv. Net Refunds" />
                                    )}
                                    {visibleAbsoluteMetrics.cumulative_refunds && (
                                        <Line type="monotone" dataKey="cumulative_refunds" stroke="#ff67b3" strokeDasharray="5 5" name="Cum. Refunds" />
                                    )}
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                    
                    {/* Money Values Chart */}
                    <div className="mb-6">
                        <h4 className="font-semibold text-gray-700 dark:text-white mb-2">Revenue Values</h4>
                        <div className="mb-4 flex flex-wrap gap-2">
                            {Object.keys(visibleMoneyMetrics).map(key => (
                                <button key={key} onClick={() => toggleMoneyMetric(key)}
                                        className={`px-3 py-1 text-xs rounded-full ${visibleMoneyMetrics[key] ? 'bg-green-500 text-white' : 'bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-white'}`}>
                                    {key.split('_').join(' ')}
                                </button>
                            ))}
                        </div>
                        <div className="h-80 w-full mb-8 bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="date" />
                                    <YAxis />
                                    <Tooltip formatter={(value) => formatCurrency(value)} />
                                    <ChartLegend />
                                    
                                    {/* Daily metrics */}
                                    {visibleMoneyMetrics.daily_revenue && (
                                        <Line type="monotone" dataKey="daily_revenue" stroke="#00b300" name="Daily Revenue" />
                                    )}
                                    {visibleMoneyMetrics.daily_revenue_net && (
                                        <Line type="monotone" dataKey="daily_revenue_net" stroke="#00e600" name="Daily Net Revenue" />
                                    )}
                                    {visibleMoneyMetrics.daily_estimated_revenue && (
                                        <Line type="monotone" dataKey="daily_estimated_revenue" stroke="#4dff4d" name="Daily Est. Revenue" />
                                    )}
                                    
                                    {/* Cumulative metrics */}
                                    {visibleMoneyMetrics.cumulative_revenue && (
                                        <Line type="monotone" dataKey="cumulative_revenue" stroke="#009900" strokeDasharray="5 5" name="Cum. Revenue" />
                                    )}
                                    {visibleMoneyMetrics.cumulative_revenue_net && (
                                        <Line type="monotone" dataKey="cumulative_revenue_net" stroke="#00cc00" strokeDasharray="5 5" name="Cum. Net Revenue" />
                                    )}
                                    {visibleMoneyMetrics.cumulative_estimated_revenue && (
                                        <Line type="monotone" dataKey="cumulative_estimated_revenue" stroke="#00e600" strokeDasharray="5 5" name="Cum. Est. Revenue" />
                                    )}
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                </div>
            )}
                    
            {/* Event Revenue Timeline Table - NEW FEATURE - MOVED ABOVE USER TIMELINE - ENHANCED WITH ALL METRICS */}
            {eventRevenueTimelineData ? (
                <div className="mb-8 bg-white dark:bg-gray-700 p-6 rounded-lg shadow-md">
                    <div className="flex justify-between items-center mb-4">
                        <h2 className="text-2xl font-bold text-gray-800 dark:text-white">Daily Event & Revenue Timeline</h2>
                        <div className="flex gap-2">
                            <span className="text-sm font-medium px-3 py-1 bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100 rounded-full">
                                {selectedUserId && selectedProductId ? 'Detailed View' : 'Aggregate View'}
                            </span>
                        </div>
                    </div>
                    
                    {/* User and Product Selection */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6 p-4 border rounded-lg bg-gray-50 dark:bg-gray-600">
                        <div>
                            <label htmlFor="user-select" className="block text-sm font-medium text-gray-700 dark:text-white mb-2">
                                Select User (Optional)
                            </label>
                            <Select
                                id="user-select"
                                isClearable
                                isSearchable
                                options={availableUsers.map(user => ({ 
                                    value: user, 
                                    label: user 
                                }))}
                                value={selectedUserId ? { value: selectedUserId, label: selectedUserId } : null}
                                onChange={(selected) => {
                                    const newUserId = selected ? selected.value : '';
                                    setSelectedUserId(newUserId);
                                    fetchEventRevenueTimeline(newUserId, selectedProductId);
                                }}
                                styles={getSelectTheme()}
                                placeholder="Type to search users..."
                            />
                        </div>
                        <div>
                            <label htmlFor="product-select" className="block text-sm font-medium text-gray-700 dark:text-white mb-2">
                                Select Product (Optional)
                            </label>
                            <Select
                                id="product-select"
                                isClearable
                                isSearchable
                                options={availableProducts.map(product => ({ 
                                    value: product, 
                                    label: product 
                                }))}
                                value={selectedProductId ? { value: selectedProductId, label: selectedProductId } : null}
                                onChange={(selected) => {
                                    const newProductId = selected ? selected.value : '';
                                    setSelectedProductId(newProductId);
                                    fetchEventRevenueTimeline(selectedUserId, newProductId);
                                }}
                                styles={getSelectTheme()}
                                placeholder="Type to search products..."
                            />
                        </div>
                    </div>
                    
                    {/* Enhanced Event Revenue Timeline Table */}
                    <div className="overflow-x-auto">
                        <table className="min-w-full text-sm border dark:border-gray-600">
                            <thead className="bg-gray-50 dark:bg-gray-700">
                                <tr>
                                    <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white sticky left-0 bg-gray-50 dark:bg-gray-700 z-10 min-w-[200px]">
                                        Metric
                                    </th>
                                    {eventRevenueTimelineData.dates.map(date => (
                                        <th key={date} className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white whitespace-nowrap min-w-[80px]">
                                            {date}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="bg-white dark:bg-gray-800">
                                {(() => {
                                    // Define all metrics with their categories and data extraction logic
                                    const metrics = [
                                        // Daily Absolute Values
                                        {
                                            key: 'daily_trial_started',
                                            label: 'Trial Started',
                                            category: 'daily_absolute',
                                            categoryLabel: 'Daily Absolute Values',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    (eventRows.trial_started?.[date] || 0) : 
                                                    (eventRows.trial_started?.[date] || chartDay?.daily_trials_started || 0);
                                            }
                                        },
                                        {
                                            key: 'daily_trial_pending',
                                            label: 'Trial Pending',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                // FIXED: Use the backend-calculated trial_pending value directly
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.trial_pending?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_trial_ended',
                                            label: 'Trial Ended',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.trial_ended?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_trial_cancelled',
                                            label: 'Trial Cancelled',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.trial_canceled?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_trial_converted',
                                            label: 'Trial Converted',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    (eventRows.trial_converted?.[date] || 0) : 
                                                    (eventRows.trial_converted?.[date] || chartDay?.daily_conversions || 0);
                                            }
                                        },
                                        {
                                            key: 'daily_initial_purchase',
                                            label: 'Initial Purchase',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.initial_purchase?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_subscription_active',
                                            label: 'Subscription Active',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                // FIXED: Use the backend-calculated subscription_active value directly
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.subscription_active?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_subscription_cancelled',
                                            label: 'Subscription Cancelled',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.subscription_cancelled?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'daily_refund',
                                            label: 'Refund',
                                            category: 'daily_absolute',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    (eventRows.refund?.[date] || 0) : 
                                                    (eventRows.refund?.[date] || chartDay?.daily_refunds || 0);
                                            }
                                        },
                                        
                                        // Daily Revenue
                                        {
                                            key: 'daily_revenue',
                                            label: 'Revenue (Actual)',
                                            category: 'daily_revenue',
                                            categoryLabel: 'Daily Revenue',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const value = (selectedUserId || selectedProductId) ? 
                                                    (estimateRows.current_revenue?.[date] || 0) : 
                                                    (estimateRows.current_revenue?.[date] || chartDay?.daily_revenue || 0);
                                                return formatCurrency(value);
                                            }
                                        },
                                        {
                                            key: 'daily_est_revenue',
                                            label: 'Est. Revenue',
                                            category: 'daily_revenue',
                                            isEstimatedRevenue: true,
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const value = (selectedUserId || selectedProductId) ? 
                                                    (estimateRows.estimated_net_revenue?.[date] || 0) : 
                                                    (estimateRows.estimated_revenue?.[date] || chartDay?.daily_estimated_revenue || 0);
                                                return formatCurrency(value);
                                            },
                                            getTooltipData: (date, index) => {
                                                // Mock calculation data - this would come from the backend in reality
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const actualValue = (selectedUserId || selectedProductId) ? 
                                                    (estimateRows.estimated_net_revenue?.[date] || 0) : 
                                                    (estimateRows.estimated_revenue?.[date] || chartDay?.daily_estimated_revenue || 0);
                                                
                                                // Mock data structure - backend should provide this
                                                return {
                                                    formula: "Est_Revenue = (Active_Trials × Trial_Conv_Rate × Avg_Revenue) + (Active_Subs × (1 - Churn_Rate) × Avg_Revenue) - (Expected_Refunds × Avg_Refund)",
                                                    calculation: `(${chartDay?.daily_trials_pending || 0} × 0.15 × $9.99) + (${chartDay?.daily_trials_pending || 0} × 0.95 × $9.99) - (${chartDay?.daily_refunds || 0} × $9.99)`,
                                                    components: {
                                                        "Active Trials": chartDay?.daily_trials_pending || 0,
                                                        "Trial Conversion Rate": "15%",
                                                        "Active Subscriptions": "N/A",
                                                        "Churn Rate": "5%",
                                                        "Avg Revenue": "$9.99",
                                                        "Expected Refunds": chartDay?.daily_refunds || 0,
                                                        "Avg Refund": "$9.99"
                                                    },
                                                    result: formatCurrency(actualValue)
                                                };
                                            }
                                        },
                                        
                                        // Cumulative Absolute Values
                                        {
                                            key: 'cumulative_trial_started',
                                            label: 'Trial Started',
                                            category: 'cumulative_absolute',
                                            categoryLabel: 'Cumulative Absolute Values',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.trial_started?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_trials_started || 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_trial_pending',
                                            label: 'Trial Pending',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                // This is a point-in-time value, same as daily pending
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.trial_pending?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'cumulative_trial_ended',
                                            label: 'Trial Ended',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.trial_ended?.[d] || 0), 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_trial_cancelled',
                                            label: 'Trial Cancelled',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.trial_canceled?.[d] || 0), 0) : 
                                                    (chartData ? chartData.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.trial_canceled?.[d.date] || 0), 0) : 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_trial_converted',
                                            label: 'Trial Converted',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.trial_converted?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_conversions || 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_initial_purchase',
                                            label: 'Initial Purchase',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.initial_purchase?.[d] || 0), 0) : 
                                                    (eventRows.cumulative_initial_purchase?.[date] || 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_subscription_active',
                                            label: 'Subscription Active',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                // This is a point-in-time value, same as daily subscription_active
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRows.subscription_active?.[date] || 0;
                                            }
                                        },
                                        {
                                            key: 'cumulative_subscription_cancelled',
                                            label: 'Subscription Cancelled',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.subscription_cancelled?.[d] || 0), 0);
                                            }
                                        },
                                        {
                                            key: 'cumulative_refund',
                                            label: 'Refund',
                                            category: 'cumulative_absolute',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const eventRows = eventRevenueTimelineData.event_rows || {};
                                                return (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (eventRows.refund?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_refunds || 0);
                                            }
                                        },
                                        
                                        // Cumulative Revenue
                                        {
                                            key: 'cumulative_revenue',
                                            label: 'Revenue (Actual)',
                                            category: 'cumulative_revenue',
                                            categoryLabel: 'Cumulative Revenue',
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const value = (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (estimateRows.current_revenue?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_revenue || 0);
                                                return formatCurrency(value);
                                            }
                                        },
                                        {
                                            key: 'cumulative_est_revenue',
                                            label: 'Est. Revenue',
                                            category: 'cumulative_revenue',
                                            isEstimatedRevenue: true,
                                            getValue: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const value = (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (estimateRows.estimated_net_revenue?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_estimated_revenue || 0);
                                                return formatCurrency(value);
                                            },
                                            getTooltipData: (date, index) => {
                                                const chartDay = chartData ? chartData.find(d => d.date === date) : null;
                                                const estimateRows = eventRevenueTimelineData.estimate_rows || {};
                                                const actualValue = (selectedUserId || selectedProductId) ? 
                                                    eventRevenueTimelineData.dates.slice(0, index + 1).reduce((sum, d) => sum + (estimateRows.estimated_net_revenue?.[d] || 0), 0) : 
                                                    (chartDay?.cumulative_estimated_revenue || 0);
                                                
                                                return {
                                                    formula: "Cumulative_Est_Revenue = Σ(Daily_Est_Revenue) from start to date",
                                                    calculation: `Sum of daily estimated revenue from start date to ${date}`,
                                                    components: {
                                                        "Period": `Start to ${date}`,
                                                        "Days": index + 1,
                                                        "Avg Daily Est": formatCurrency(actualValue / (index + 1)),
                                                    },
                                                    result: formatCurrency(actualValue)
                                                };
                                            }
                                        }
                                    ];

                                    // Group metrics by category for styling
                                    const categoryColors = {
                                        daily_absolute: 'bg-blue-50 dark:bg-blue-900/30',
                                        daily_revenue: 'bg-green-50 dark:bg-green-900/30', 
                                        cumulative_absolute: 'bg-indigo-50 dark:bg-indigo-900/30',
                                        cumulative_revenue: 'bg-emerald-50 dark:bg-emerald-900/30'
                                    };

                                    return metrics.map((metric, metricIndex) => {
                                        const isFirstInCategory = metricIndex === 0 || metrics[metricIndex - 1].category !== metric.category;
                                        
                                        return (
                                            <React.Fragment key={metric.key}>
                                                {/* Category header row */}
                                                {isFirstInCategory && metric.categoryLabel && (
                                                    <tr className={`${categoryColors[metric.category]} border-t-2 border-gray-300 dark:border-gray-500`}>
                                                        <td colSpan={eventRevenueTimelineData.dates.length + 1} className="p-2 border-b dark:border-gray-600 text-center font-semibold text-gray-700 dark:text-white">
                                                            {metric.categoryLabel}
                                                        </td>
                                                    </tr>
                                                )}
                                                
                                                {/* Metric data row */}
                                                <tr className={metricIndex % 2 === 0 ? '' : 'bg-gray-50 dark:bg-gray-700'}>
                                                    <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300 sticky left-0 z-10 bg-inherit">
                                                        {metric.label}
                                                    </td>
                                                    {eventRevenueTimelineData.dates.map((date, dateIndex) => (
                                                        <td 
                                                            key={date} 
                                                            className={`p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-gray-300 whitespace-nowrap ${
                                                                metric.isEstimatedRevenue ? 'cursor-help hover:bg-yellow-50 dark:hover:bg-yellow-900/20' : ''
                                                            }`}
                                                            onMouseEnter={metric.isEstimatedRevenue ? (e) => {
                                                                const tooltipData = metric.getTooltipData(date, dateIndex);
                                                                setTooltipData(tooltipData);
                                                                setTooltipPosition({ x: e.clientX, y: e.clientY });
                                                            } : undefined}
                                                            onMouseLeave={metric.isEstimatedRevenue ? () => {
                                                                setTooltipData(null);
                                                            } : undefined}
                                                        >
                                                            {metric.getValue(date, dateIndex)}
                                                        </td>
                                                    ))}
                                                </tr>
                                            </React.Fragment>
                                        );
                                    });
                                })()}
                            </tbody>
                        </table>
                    </div>
                </div>
            ) : isLoadingEventRevenueTimeline ? (
                <div className="flex items-center justify-center p-8 bg-white dark:bg-gray-700 rounded-lg shadow-md my-4">
                    <Loader className="animate-spin mr-2 text-indigo-600 dark:text-indigo-400" />
                    <p className="text-gray-700 dark:text-white">Loading event revenue timeline...</p>
                </div>
            ) : eventRevenueTimelineError ? (
                <div className="my-4 p-4 bg-yellow-100 dark:bg-yellow-800 text-yellow-700 dark:text-white rounded-md shadow-md border border-yellow-300 dark:border-yellow-600">
                    <div className="flex items-center">
                        <AlertTriangle className="mr-2 flex-shrink-0" />
                        <div>
                            <span className="font-medium block">Event Revenue Timeline Error:</span>
                            <span className="block mt-1">{eventRevenueTimelineError}</span>
                        </div>
                    </div>
                </div>
            ) : null}

            {/* User Timeline Table */}
            {userTimelineData ? (
                <div className="mb-8">
                    <div className="flex justify-between items-center mb-3">
                        <h3 className="text-xl font-semibold text-gray-700 dark:text-white">User Timeline</h3>
                        <div className="flex gap-2">
                            <span className="text-sm font-medium px-3 py-1 bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100 rounded-full">
                                {userTimelineData.unique_users_count || userTimelineData.total_users} Unique Users
                            </span>
                            <span className="text-sm font-medium px-3 py-1 bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100 rounded-full">
                                {userTimelineData.total_user_product_combinations || userTimelineData.users?.length} User-Product Combinations
                            </span>
                            <span className="text-sm font-medium px-3 py-1 bg-purple-100 text-purple-800 dark:bg-purple-800 dark:text-purple-100 rounded-full">
                                {userTimelineData.valid_product_ids?.length || 0} Unique Products
                            </span>
                        </div>
                    </div>
                    
                    {/* Add filter stats display for timeline too */}
                    {userTimelineData.filter_stats && (
                        <FilterStatsDisplay filterStats={userTimelineData.filter_stats} />
                    )}
                    
                    {/* Legend */}
                    <div className="mb-4">
                        <div className="flex flex-wrap gap-3 mb-2">
                            <div className={`px-3 py-2 rounded ${timelineColors.default.bg} ${timelineColors.default.text}`}>No Event</div>
                            <div className={`px-3 py-2 rounded ${timelineColors.trial_started.bg} ${timelineColors.trial_started.text}`}>Trial Started</div>
                            <div className={`px-3 py-2 rounded ${timelineColors.trial_converted.bg} ${timelineColors.trial_converted.text}`}>Trial Converted</div>
                            <div className={`px-3 py-2 rounded ${timelineColors.trial_cancelled.bg} ${timelineColors.trial_cancelled.text}`}>Trial Cancelled</div>
                            <div className={`px-3 py-2 rounded ${timelineColors.conversion_cancelled.bg} ${timelineColors.conversion_cancelled.text}`}>Conversion Cancelled</div>
                            <div className={`px-3 py-2 rounded ${timelineColors.refunded.bg} ${timelineColors.refunded.text}`}>Refunded</div>
                        </div>
                        <div className="relative flex items-center bg-gray-100 dark:bg-gray-700 p-2 rounded text-sm">
                            <span className="mr-2">Multiple events on the same day:</span>
                            <div className="flex items-center">
                                <div className="relative mr-8 px-4 py-2 rounded bg-green-100 dark:bg-green-900/50" style={{minWidth: "180px"}}>
                                    <span className="text-green-800 dark:text-green-200">Trial Converted</span>
                                    <div className="absolute top-0 right-0 -mt-1 -mr-1 bg-blue-500 text-white text-[8px] rounded-full w-4 h-4 flex items-center justify-center">
                                        3
                                    </div>
                                    <div className="mt-1 flex flex-col gap-1 w-full">
                                        <div className="px-1 py-0.5 text-[9px] w-full text-center rounded bg-yellow-100 dark:bg-yellow-900/50 text-yellow-800 dark:text-yellow-200">
                                            Trial started
                                        </div>
                                        <div className="px-1 py-0.5 text-[9px] w-full text-center rounded bg-red-50 dark:bg-red-900/30 text-red-600 dark:text-red-300">
                                            Refunded
                                        </div>
                                    </div>
                                </div>
                                <span className="text-gray-600 dark:text-gray-300">The cell is colored by the first event, with a counter and full-text badges for each additional event</span>
                            </div>
                        </div>
                    </div>
                    
                    <div className="overflow-x-auto">
                        <table className="min-w-full text-sm border dark:border-gray-600">
                            <thead className="bg-gray-50 dark:bg-gray-700">
                                <tr>
                                    <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white sticky left-0 bg-gray-50 dark:bg-gray-700 z-10">User ID</th>
                                    <th className="p-2 border-b dark:border-gray-600 text-left text-gray-700 dark:text-white sticky left-[150px] bg-gray-50 dark:bg-gray-700 z-10">Product ID</th>
                                    {userTimelineData.dates.map(date => (
                                        <th key={date} className="p-2 border-b dark:border-gray-600 text-center text-gray-700 dark:text-white whitespace-nowrap">{date}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="bg-white dark:bg-gray-800">
                                {userTimelineData.users.map((user, index) => {
                                    // Get background colors for alternating users (not user-product rows)
                                    const isNewUser = index === 0 || userTimelineData.users[index-1].id !== user.id;
                                    const userBgClass = isNewUser ? (index % 4 < 2 ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-700') : ''; 
                                    
                                    return (
                                    <tr key={user.user_product_key} className={userBgClass}>
                                        <td className="p-2 border-b dark:border-gray-600 font-medium text-gray-700 dark:text-gray-300 sticky left-0 z-10" style={{backgroundColor: 'inherit'}}>
                                            {isNewUser ? user.id : ''}
                                        </td>
                                        <td className="p-2 border-b dark:border-gray-600 font-medium text-green-700 dark:text-green-300 sticky left-[150px] z-10" style={{backgroundColor: 'inherit'}}>
                                            {user.product_id}
                                        </td>
                                        {userTimelineData.dates.map(date => {
                                            const cellState = getCellState(user.user_product_key, date, userTimelineData.events);
                                            const events = getEventsForDate(user.user_product_key, date, userTimelineData.events);
                                            const hasEvents = events.length > 0;
                                            
                                            return (
                                                <td key={date} 
                                                    className={`p-2 border dark:border-gray-600 text-xs relative ${timelineColors[cellState].bg}`}>
                                                    {hasEvents ? (
                                                        <div className="flex flex-col items-center">
                                                            {/* Main event - full display */}
                                                            <div className={`text-center w-full ${timelineColors[cellState].text} font-medium`}>
                                                                {events[0].type.replace('RC ', '')}
                                                            </div>
                                                            
                                                            {/* Show badges for multiple events with full names */}
                                                            {events.length > 1 && (
                                                                <div className="mt-1 flex flex-col items-center w-full gap-1">
                                                                    {/* Skip the first event (already shown above) and show badges for additional events */}
                                                                    {events.slice(1).map((additionalEvent, idx) => {
                                                                        const eventType = additionalEvent.type.replace('RC ', '');
                                                                        let badgeState = 'default';
                                                                        
                                                                        // Determine appropriate color for this event
                                                                        if (additionalEvent.type === 'RC Trial started') badgeState = 'trial_started';
                                                                        else if (additionalEvent.type === 'RC Trial converted') badgeState = 'trial_converted';
                                                                        else if (additionalEvent.type === 'RC Trial cancelled') badgeState = 'trial_cancelled';
                                                                        else if (additionalEvent.type === 'RC Cancellation') {
                                                                            badgeState = additionalEvent.has_refund ? 'refunded' : 'conversion_cancelled';
                                                                        } else if (additionalEvent.type === 'RC Initial purchase') {
                                                                            badgeState = 'trial_converted';
                                                                        } else if (additionalEvent.type === 'RC Renewal') {
                                                                            badgeState = 'trial_converted';
                                                                        }
                                                                        
                                                                        return (
                                                                            <div 
                                                                                key={idx} 
                                                                                className={`px-1 py-0.5 text-[9px] w-full text-center rounded ${timelineColors[badgeState].bg} ${timelineColors[badgeState].text}`}
                                                                                title={`${eventType} at ${additionalEvent.time} for ${additionalEvent.product_id}`}
                                                                            >
                                                                                {eventType}
                                                                            </div>
                                                                        );
                                                                    })}
                                                                </div>
                                                            )}
                                                            
                                                            {/* Show a small indicator when there are multiple events */}
                                                            {events.length > 1 && (
                                                                <div className="absolute top-0 right-0 -mt-1 -mr-1 bg-blue-500 text-white text-[8px] rounded-full w-4 h-4 flex items-center justify-center">
                                                                    {events.length}
                                                                </div>
                                                            )}
                                                        </div>
                                                    ) : ''}
                                                </td>
                                            );
                                        })}
                                    </tr>
                                );})}
                            </tbody>
                        </table>
                    </div>
                </div>
            ) : isLoadingTimeline ? (
                <div className="flex items-center justify-center p-8 bg-white dark:bg-gray-700 rounded-lg shadow-md my-4">
                    <Loader className="animate-spin mr-2 text-indigo-600 dark:text-indigo-400" />
                    <p className="text-gray-700 dark:text-white">Loading user timeline data...</p>
                </div>
            ) : timelineError ? (
                <div className="my-4 p-4 bg-yellow-100 dark:bg-yellow-800 text-yellow-700 dark:text-white rounded-md shadow-md border border-yellow-300 dark:border-yellow-600">
                    <div className="flex items-center">
                        <AlertTriangle className="mr-2 flex-shrink-0" />
                        <div>
                            <span className="font-medium block">Timeline Error:</span>
                            <span className="block mt-1">{timelineError}</span>
                        </div>
                    </div>
                </div>
            ) : null}
            
            {/* Estimated Revenue Tooltip */}
            <EstimatedRevenueTooltip 
                data={tooltipData} 
                position={tooltipPosition} 
                onClose={() => setTooltipData(null)} 
            />
            <ChartModal />
        </div>
    );
};

export default CohortAnalyzerPage;