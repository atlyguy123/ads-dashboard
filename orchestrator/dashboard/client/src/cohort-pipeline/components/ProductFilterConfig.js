import React, { useState, useEffect } from 'react';
import Select from 'react-select';
import { PlusCircle, XCircle } from 'lucide-react';
import { api } from '../../services/api';

const ProductFilterConfig = ({ config, analysisParams, onConfigChange }) => {
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [availableEventProperties, setAvailableEventProperties] = useState([]);
    const [availableUserProperties, setAvailableUserProperties] = useState([]);
    const [propertyValueOptions, setPropertyValueOptions] = useState({});
    const [isLoadingProperties, setIsLoadingProperties] = useState(false);
    const [propertyDiscoveryMessage, setPropertyDiscoveryMessage] = useState('');

    // Load available properties on component mount
    useEffect(() => {
        initializeProperties();
    }, []);

    const initializeProperties = async () => {
        setIsLoadingProperties(true);
        try {
            const data = await api.getDiscoverableCohortProperties();
            if (data.event_properties) {
                setAvailableEventProperties(data.event_properties);
            }
            if (data.user_properties) {
                setAvailableUserProperties(data.user_properties);
            }
        } catch (error) {
            console.error('Failed to load properties:', error);
            setPropertyDiscoveryMessage('Failed to load available properties. Some features may be limited.');
        }
        setIsLoadingProperties(false);
    };

    const fetchPropertyValues = async (propertyName, propertySource, filterIdentifier) => {
        if (!propertyName || !propertySource) return;
        try {
            const data = await api.getDiscoverableCohortPropertyValues(propertyName, propertySource);
            setPropertyValueOptions(prev => ({ 
                ...prev, 
                [filterIdentifier + '_' + propertyName]: data.values || [] 
            }));
        } catch (err) {
            console.error(`Error fetching values for ${propertyName}:`, err);
            setPropertyValueOptions(prev => ({ 
                ...prev, 
                [filterIdentifier + '_' + propertyName]: [] 
            }));
        }
    };

    const triggerPropertyDiscovery = async () => {
        setIsLoadingProperties(true);
        setPropertyDiscoveryMessage('Discovering available properties...');
        try {
            await initializeProperties();
            setPropertyDiscoveryMessage('Property discovery completed successfully!');
            setTimeout(() => setPropertyDiscoveryMessage(''), 3000);
        } catch (error) {
            setPropertyDiscoveryMessage('Property discovery failed. Please try again.');
            setTimeout(() => setPropertyDiscoveryMessage(''), 5000);
        }
        setIsLoadingProperties(false);
    };

    const getSelectTheme = () => ({
        control: (provided, state) => ({
            ...provided,
            backgroundColor: 'var(--select-bg, white)',
            borderColor: state.isFocused ? '#3b82f6' : '#d1d5db',
            boxShadow: state.isFocused ? '0 0 0 1px #3b82f6' : 'none',
            '&:hover': {
                borderColor: '#3b82f6'
            }
        }),
        menu: (provided) => ({
            ...provided,
            backgroundColor: 'var(--select-menu-bg, white)',
            zIndex: 9999
        }),
        option: (provided, state) => ({
            ...provided,
            backgroundColor: state.isSelected 
                ? '#3b82f6' 
                : state.isFocused 
                    ? '#eff6ff' 
                    : 'var(--select-option-bg, white)',
            color: state.isSelected ? 'white' : 'var(--select-option-color, black)'
        }),
        multiValue: (provided) => ({
            ...provided,
            backgroundColor: '#eff6ff'
        }),
        multiValueLabel: (provided) => ({
            ...provided,
            color: '#1e40af'
        }),
        multiValueRemove: (provided) => ({
            ...provided,
            color: '#1e40af',
            '&:hover': {
                backgroundColor: '#dbeafe',
                color: '#1e40af'
            }
        })
    });

    const handleConfigUpdate = (newConfig) => {
        const updatedParams = {
            ...analysisParams,
            config: {
                ...analysisParams.config,
                ...newConfig
            }
        };
        onConfigChange(updatedParams);
    };

    const handleProductFilterChange = (field, value) => {
        const newProductFilter = {
            ...config.product_filter,
            [field]: value
        };
        
        handleConfigUpdate({
            product_filter: newProductFilter
        });
    };

    const handleLifecycleConfigChange = (field, value) => {
        const newLifecycleConfig = {
            ...config.lifecycle,
            [field]: value
        };
        
        handleConfigUpdate({
            lifecycle: newLifecycleConfig
        });
    };

    const handleTimelineConfigChange = (field, value) => {
        const newTimelineConfig = {
            ...config.timeline,
            [field]: value
        };
        
        handleConfigUpdate({
            timeline: newTimelineConfig
        });
    };

    const handleDetailedEventsConfigChange = (field, value) => {
        const newDetailedEventsConfig = {
            ...config.detailed_events,
            [field]: value
        };
        
        handleConfigUpdate({
            detailed_events: newDetailedEventsConfig
        });
    };

    const handleDateChange = (field, value) => {
        const updatedParams = {
            ...analysisParams,
            [field]: value
        };
        onConfigChange(updatedParams);
    };

    // Enhanced filter handler with property value fetching
    const handleFilterChange = (field, value) => {
        const updatedParams = {
            ...analysisParams,
            [field]: value
        };
        onConfigChange(updatedParams);
    };

    // Optional filters management (replaces both primary and secondary filters)
    const addOptionalFilter = () => {
        const updatedParams = {
            ...analysisParams,
            optional_filters: [
                ...(analysisParams.optional_filters || []),
                {
                    id: Date.now().toString(),
                    property_name: '',
                    property_values: [],
                    property_source: 'event'
                }
            ]
        };
        onConfigChange(updatedParams);
    };

    const removeOptionalFilter = (idToRemove) => {
        const updatedParams = {
            ...analysisParams,
            optional_filters: (analysisParams.optional_filters || []).filter(f => f.id !== idToRemove)
        };
        onConfigChange(updatedParams);
    };

    const handleOptionalFilterChange = (type, value, index) => {
        const newOptionalFilters = [...(analysisParams.optional_filters || [])];
        const filter = newOptionalFilters[index];
        
        if (type === 'property_source') {
            filter.property_source = value;
            filter.property_values = []; // Reset values when source changes
            // Re-fetch values if property name exists
            if (filter.property_name) {
                fetchPropertyValues(filter.property_name, value, `optional_filter_${index}`);
            }
        } else if (type === 'property_name') {
            filter.property_name = value;
            filter.property_values = []; // Reset values when property changes
            // Fetch property values for the new property
            if (value && filter.property_source) {
                fetchPropertyValues(value, filter.property_source, `optional_filter_${index}`);
            }
        } else if (type === 'property_values') {
            filter.property_values = value;
        }
        
        const updatedParams = {
            ...analysisParams,
            optional_filters: newOptionalFilters
        };
        onConfigChange(updatedParams);
    };

    const addToList = (list, newItem) => {
        if (newItem && !list.includes(newItem)) {
            return [...list, newItem];
        }
        return list;
    };

    const removeFromList = (list, item) => {
        return list.filter(i => i !== item);
    };

    return (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <div className="flex items-center justify-between mb-6">
                <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                    Analysis Configuration
                </h2>
                <div className="flex items-center space-x-2">
                    <button
                        onClick={triggerPropertyDiscovery}
                        disabled={isLoadingProperties}
                        className="px-3 py-1 text-sm bg-teal-200 hover:bg-teal-300 dark:bg-teal-700 dark:hover:bg-teal-600 text-teal-700 dark:text-teal-300 rounded-lg transition-colors disabled:opacity-50"
                        title="Refresh available properties"
                    >
                        {isLoadingProperties ? 'Loading...' : 'Discover Properties'}
                    </button>
                    <button
                        onClick={() => setShowAdvanced(!showAdvanced)}
                        className="px-3 py-1 text-sm bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded-lg transition-colors"
                    >
                        {showAdvanced ? 'Hide Advanced' : 'Show Advanced'}
                    </button>
                </div>
            </div>

            {propertyDiscoveryMessage && (
                <div className="mb-4 p-3 bg-teal-50 dark:bg-teal-900/40 border border-teal-200 dark:border-teal-600 rounded-md">
                    <p className="text-sm text-teal-700 dark:text-teal-300">{propertyDiscoveryMessage}</p>
                </div>
            )}

            <div className="space-y-6">
                {/* Basic Configuration */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* Date Range */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                            Date From
                        </label>
                        <input
                            type="date"
                            value={analysisParams.date_from}
                            onChange={(e) => handleDateChange('date_from', e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                        />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                            Date To
                        </label>
                        <input
                            type="date"
                            value={analysisParams.date_to}
                            onChange={(e) => handleDateChange('date_to', e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                        />
                    </div>
                </div>

                {/* User & Event Property Filters Section */}
                <div className="border-t pt-6">
                    <div className="flex justify-between items-center mb-4">
                        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                            User & Event Property Filters
                        </h3>
                        <button 
                            type="button" 
                            onClick={addOptionalFilter}
                            className="p-1.5 bg-teal-100 dark:bg-teal-600 text-teal-700 dark:text-white rounded-full hover:bg-teal-200 dark:hover:bg-teal-500 transition-colors"
                            title="Add optional filter"
                        >
                            <PlusCircle size={18} />
                        </button>
                    </div>
                    
                    <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                        <strong>Optional:</strong> Add filters to narrow down your cohort. User property filters will only include users with the specified property values. Event property filters will only include users who have events with the specified property values.
                    </p>
                    
                    {(analysisParams.optional_filters?.length === 0 || !analysisParams.optional_filters) && (
                        <p className="text-sm text-gray-600 dark:text-gray-400 italic mb-4">
                            No filters applied. All users with trial/purchase events in the time frame will be included. Click the + button to add a filter.
                        </p>
                    )}
                    
                    {(analysisParams.optional_filters || []).map((filter, index) => (
                        <div key={filter.id} className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end mb-4 p-4 border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-gray-700 rounded-md">
                            <div>
                                <label className="block text-xs font-medium text-gray-600 dark:text-gray-300 mb-1">
                                    Source Type
                                </label>
                                <select 
                                    value={filter.property_source}
                                    onChange={e => handleOptionalFilterChange('property_source', e.target.value, index)}
                                    className="block w-full p-2 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-600 text-gray-900 dark:text-white"
                                >
                                    <option value="event">Event Property</option>
                                    <option value="user">User Property</option>
                                </select>
                            </div>
                            <div>
                                <label className="block text-xs font-medium text-gray-600 dark:text-gray-300 mb-1">
                                    Property Name
                                </label>
                                <select 
                                    value={filter.property_name}
                                    onChange={e => handleOptionalFilterChange('property_name', e.target.value, index)}
                                    className="block w-full p-2 border border-gray-300 dark:border-gray-500 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-600 text-gray-900 dark:text-white"
                                >
                                    <option value="">-- Select Property --</option>
                                    {(filter.property_source === 'event' ? availableEventProperties : availableUserProperties).map(p => (
                                        <option key={p.key} value={p.key}>
                                            {p.display_name || p.key}
                                        </option>
                                    ))}
                                </select>
                            </div>
                            <div>
                                <label className="block text-xs font-medium text-gray-600 dark:text-gray-300 mb-1">
                                    Property Value(s)
                                </label>
                                <Select
                                    isMulti
                                    isDisabled={!filter.property_name || !filter.property_source}
                                    options={(propertyValueOptions[`optional_filter_${index}_${filter.property_name}`] || []).map(val => ({ value: val, label: val }))}
                                    value={filter.property_values.map(val => ({ value: val, label: val }))}
                                    onChange={(selected) => handleOptionalFilterChange('property_values', selected ? selected.map(option => option.value) : [], index)}
                                    styles={getSelectTheme()}
                                    placeholder="Select values..."
                                />
                            </div>
                            <div className="flex justify-end">
                                <button 
                                    type="button" 
                                    onClick={() => removeOptionalFilter(filter.id)}
                                    className="p-1.5 bg-red-100 dark:bg-red-700 text-red-600 dark:text-white rounded-full hover:bg-red-200 dark:hover:bg-red-600 transition-colors"
                                    title="Remove this filter"
                                >
                                    <XCircle size={18} />
                                </button>
                            </div>
                        </div>
                    ))}
                </div>

                {/* Product Filter Configuration */}
                <div className="border-t pt-6">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                        Product Filtering
                    </h3>
                    
                    <div className="space-y-4">
                        {/* Include Patterns */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                                Include Products Containing (comma-separated)
                            </label>
                            <input
                                type="text"
                                value={config.product_filter?.include_patterns?.join(', ') || ''}
                                onChange={(e) => handleProductFilterChange('include_patterns', 
                                    e.target.value.split(',').map(v => v.trim()).filter(v => v)
                                )}
                                placeholder="e.g., gluten, premium, pro"
                                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Products containing any of these patterns will be included
                            </p>
                        </div>

                        {/* Specific Product IDs */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                                Specific Product IDs (comma-separated)
                            </label>
                            <input
                                type="text"
                                value={config.product_filter?.specific_product_ids?.join(', ') || ''}
                                onChange={(e) => handleProductFilterChange('specific_product_ids', 
                                    e.target.value.split(',').map(v => v.trim()).filter(v => v)
                                )}
                                placeholder="e.g., prod_R7oYuL3bSUecnr, prod_ABC123"
                                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                These exact product IDs will always be included
                            </p>
                        </div>

                        {/* Exclude Patterns */}
                        <div>
                            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                                Exclude Products Containing (comma-separated)
                            </label>
                            <input
                                type="text"
                                value={config.product_filter?.exclude_patterns?.join(', ') || ''}
                                onChange={(e) => handleProductFilterChange('exclude_patterns', 
                                    e.target.value.split(',').map(v => v.trim()).filter(v => v)
                                )}
                                placeholder="e.g., test, demo, staging"
                                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                            />
                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                Products containing any of these patterns will be excluded
                            </p>
                        </div>
                    </div>
                </div>

                {/* Advanced Configuration */}
                {showAdvanced && (
                    <div className="border-t pt-6 space-y-6">
                        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                            Advanced Configuration
                        </h3>

                        {/* Lifecycle Configuration */}
                        <div>
                            <h4 className="text-md font-medium text-gray-900 dark:text-white mb-3">
                                Lifecycle Rate Settings
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                <div>
                                    <label className="block text-sm text-gray-700 dark:text-gray-300 mb-1">
                                        Trial Window (days)
                                    </label>
                                    <input
                                        type="number"
                                        value={config.lifecycle?.trial_window_days || 7}
                                        onChange={(e) => handleLifecycleConfigChange('trial_window_days', parseInt(e.target.value))}
                                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-700 dark:text-gray-300 mb-1">
                                        Cancellation Window (days)
                                    </label>
                                    <input
                                        type="number"
                                        value={config.lifecycle?.cancellation_window_days || 30}
                                        onChange={(e) => handleLifecycleConfigChange('cancellation_window_days', parseInt(e.target.value))}
                                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                                    />
                                </div>
                                <div className="flex items-center">
                                    <label className="flex items-center space-x-2">
                                        <input
                                            type="checkbox"
                                            checked={config.lifecycle?.smoothing_enabled !== false}
                                            onChange={(e) => handleLifecycleConfigChange('smoothing_enabled', e.target.checked)}
                                            className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                                        />
                                        <span className="text-sm text-gray-700 dark:text-gray-300">
                                            Smoothing Enabled
                                        </span>
                                    </label>
                                </div>
                            </div>
                        </div>

                        {/* Timeline Configuration */}
                        <div>
                            <h4 className="text-md font-medium text-gray-900 dark:text-white mb-3">
                                Timeline Settings
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="flex items-center">
                                    <label className="flex items-center space-x-2">
                                        <input
                                            type="checkbox"
                                            checked={config.timeline?.include_estimates !== false}
                                            onChange={(e) => handleTimelineConfigChange('include_estimates', e.target.checked)}
                                            className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                                        />
                                        <span className="text-sm text-gray-700 dark:text-gray-300">
                                            Include Revenue Estimates
                                        </span>
                                    </label>
                                </div>
                                <div className="flex items-center">
                                    <label className="flex items-center space-x-2">
                                        <input
                                            type="checkbox"
                                            checked={config.timeline?.include_confidence_intervals !== false}
                                            onChange={(e) => handleTimelineConfigChange('include_confidence_intervals', e.target.checked)}
                                            className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                                        />
                                        <span className="text-sm text-gray-700 dark:text-gray-300">
                                            Include Confidence Intervals
                                        </span>
                                    </label>
                                </div>
                            </div>
                        </div>

                        {/* Detailed Events Configuration */}
                        <div>
                            <h4 className="text-md font-medium text-gray-900 dark:text-white mb-3">
                                Detailed Events Analysis
                            </h4>
                            <div>
                                <label className="block text-sm text-gray-700 dark:text-gray-300 mb-1">
                                    Specific User ID (for detailed event analysis)
                                </label>
                                <input
                                    type="text"
                                    value={config.detailed_events?.user_id || ''}
                                    onChange={(e) => handleDetailedEventsConfigChange('user_id', e.target.value)}
                                    placeholder="e.g., user_12345 or distinct_id"
                                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                                />
                                <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                    Leave empty for cohort-wide analysis, or enter a specific user ID to see detailed events for that user
                                </p>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default ProductFilterConfig; 