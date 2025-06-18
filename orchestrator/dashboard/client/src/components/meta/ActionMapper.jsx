import React, { useState, useEffect } from 'react';
import { api } from '../../services/api';
import { 
  META_ACTION_TYPES, 
  ALL_META_ACTION_TYPES, 
  SUGGESTED_BUSINESS_CONCEPTS,
  getActionTypeCategory,
  getBusinessConceptSuggestions 
} from './utils/metaActionTypes';

const ActionMapper = () => {
  const [mappings, setMappings] = useState({});
  const [availableActions, setAvailableActions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [newMapping, setNewMapping] = useState({
    conceptName: '',
    actionTypes: [],
    aggregationType: 'sum' // 'sum', 'count', 'average'
  });
  const [showAddForm, setShowAddForm] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);

  // Default business concepts from comprehensive analysis
  const businessConcepts = Object.keys(SUGGESTED_BUSINESS_CONCEPTS);

  useEffect(() => {
    loadMappings();
    loadKnownActionTypes();
  }, []);

  const loadKnownActionTypes = () => {
    // Start with our comprehensive list of 49 known action types
    setAvailableActions(ALL_META_ACTION_TYPES);
    
    // Also try to discover additional ones from historical data
    discoverAvailableActions();
  };

  const loadMappings = async () => {
    try {
      // Try to load from backend first
      const backendMappings = await api.getActionMappings();
      if (backendMappings.mappings && Object.keys(backendMappings.mappings).length > 0) {
        setMappings(backendMappings.mappings);
        // Also sync to localStorage for offline access
        localStorage.setItem('meta_action_mappings', JSON.stringify(backendMappings.mappings));
        return;
      }
      
      // Fallback to localStorage
      const savedMappings = localStorage.getItem('meta_action_mappings');
      if (savedMappings) {
        const localMappings = JSON.parse(savedMappings);
        setMappings(localMappings);
        // Sync localStorage mappings to backend
        if (Object.keys(localMappings).length > 0) {
          await api.saveActionMappings(localMappings);
        }
      }
    } catch (error) {
      console.error('Error loading mappings:', error);
      // Fallback to localStorage if backend fails
      try {
        const savedMappings = localStorage.getItem('meta_action_mappings');
        if (savedMappings) {
          setMappings(JSON.parse(savedMappings));
        }
      } catch (localError) {
        console.error('Error loading from localStorage:', localError);
      }
    }
  };

  const saveMappings = async (newMappings) => {
    try {
      // Save to backend
      await api.saveActionMappings(newMappings);
      
      // Update local state
      setMappings(newMappings);
      
      // Also save to localStorage as backup
      localStorage.setItem('meta_action_mappings', JSON.stringify(newMappings));
      
      console.log('Action mappings saved successfully');
    } catch (error) {
      console.error('Error saving mappings to backend:', error);
      
      // Fallback to localStorage only
      setMappings(newMappings);
      localStorage.setItem('meta_action_mappings', JSON.stringify(newMappings));
      
      alert('Mappings saved locally, but failed to sync to server. They will be synced next time the page loads.');
    }
  };

  const discoverAvailableActions = async () => {
    setLoading(true);
    try {
      // Get recent historical data to discover action types
      const configs = await api.getHistoricalConfigurations();
      const discoveredActionTypes = new Set();

      for (const config of configs.slice(0, 3)) { // Check recent configs
        if (config.day_count > 0) {
          try {
            const data = await api.exportHistoricalData({
              start_date: config.latest_date,
              end_date: config.latest_date,
              fields: config.fields,
              breakdowns: config.breakdowns
            });

            // Extract action types from the data
            data.data.forEach(dayData => {
              let records = [];
              
              // More defensive extraction of records
              if (dayData.data && dayData.data.data && Array.isArray(dayData.data.data)) {
                records = dayData.data.data;
              } else if (dayData.data && Array.isArray(dayData.data)) {
                records = dayData.data;
              } else {
                console.warn('Unexpected data structure in ActionMapper:', dayData);
                return; // Skip this dayData
              }
              
              if (!Array.isArray(records)) {
                console.warn('Records is not an array in ActionMapper:', typeof records, records);
                return;
              }
              
              records.forEach(record => {
                if (!record || typeof record !== 'object') {
                  console.warn('Invalid record in ActionMapper:', record);
                  return;
                }
                
                // Extract from actions
                if (record.actions && Array.isArray(record.actions)) {
                  record.actions.forEach(action => {
                    if (action && action.action_type) {
                      discoveredActionTypes.add(action.action_type);
                    }
                  });
                }
                // Extract from action_values
                if (record.action_values && Array.isArray(record.action_values)) {
                  record.action_values.forEach(action => {
                    if (action && action.action_type) {
                      discoveredActionTypes.add(action.action_type);
                    }
                  });
                }
                // Extract from conversions
                if (record.conversions && Array.isArray(record.conversions)) {
                  record.conversions.forEach(action => {
                    if (action && action.action_type) {
                      discoveredActionTypes.add(action.action_type);
                    }
                  });
                }
                // Extract from conversion_values
                if (record.conversion_values && Array.isArray(record.conversion_values)) {
                  record.conversion_values.forEach(action => {
                    if (action && action.action_type) {
                      discoveredActionTypes.add(action.action_type);
                    }
                  });
                }
              });
            });
          } catch (error) {
            console.error(`Error loading data for config ${config.config_hash}:`, error);
          }
        }
      }

      // Merge discovered actions with our comprehensive list
      const allActions = new Set([...ALL_META_ACTION_TYPES, ...Array.from(discoveredActionTypes)]);
      setAvailableActions(Array.from(allActions).sort());
    } catch (error) {
      console.error('Error discovering actions:', error);
      // Fallback to our comprehensive list
      setAvailableActions(ALL_META_ACTION_TYPES);
    } finally {
      setLoading(false);
    }
  };

  const addMapping = () => {
    if (!newMapping.conceptName || newMapping.actionTypes.length === 0) {
      alert('Please enter a concept name and select at least one action type');
      return;
    }

    const newMappings = {
      ...mappings,
      [newMapping.conceptName]: {
        actionTypes: newMapping.actionTypes,
        aggregationType: newMapping.aggregationType,
        createdAt: new Date().toISOString()
      }
    };

    saveMappings(newMappings);
    setNewMapping({ conceptName: '', actionTypes: [], aggregationType: 'sum' });
    setShowAddForm(false);
  };

  const deleteMapping = (conceptName) => {
    if (window.confirm(`Are you sure you want to delete the mapping for "${conceptName}"?`)) {
      const newMappings = { ...mappings };
      delete newMappings[conceptName];
      saveMappings(newMappings);
    }
  };

  const toggleActionType = (actionType) => {
    const currentTypes = newMapping.actionTypes;
    const newTypes = currentTypes.includes(actionType)
      ? currentTypes.filter(t => t !== actionType)
      : [...currentTypes, actionType];
    
    setNewMapping({ ...newMapping, actionTypes: newTypes });
  };

  const categorizeActionTypes = (actionTypes) => {
    const categories = {};
    
    // Initialize categories based on our comprehensive categorization
    Object.keys(META_ACTION_TYPES).forEach(category => {
      const categoryName = category.replace(/_/g, ' ').toLowerCase()
        .replace(/\b\w/g, l => l.toUpperCase());
      categories[categoryName] = [];
    });
    
    // Add 'Other' category for unknown action types
    categories['Other'] = [];

    actionTypes.forEach(actionType => {
      const category = getActionTypeCategory(actionType);
      if (category === 'OTHER') {
        categories['Other'].push(actionType);
      } else {
        const categoryName = category.replace(/_/g, ' ').toLowerCase()
          .replace(/\b\w/g, l => l.toUpperCase());
        if (categories[categoryName]) {
          categories[categoryName].push(actionType);
        } else {
          categories['Other'].push(actionType);
        }
      }
    });

    return categories;
  };

  const getSuggestions = () => {
    if (newMapping.actionTypes.length === 0) return [];
    return getBusinessConceptSuggestions(newMapping.actionTypes);
  };

  const applySuggestion = (suggestion) => {
    setNewMapping({
      ...newMapping,
      conceptName: suggestion.concept,
      actionTypes: [...new Set([...newMapping.actionTypes, ...suggestion.matchingActions])]
    });
  };

  const getMappingStats = () => {
    const totalMappings = Object.keys(mappings).length;
    const totalActionsMapped = Object.values(mappings).reduce((sum, mapping) => sum + mapping.actionTypes.length, 0);
    const unmappedActions = availableActions.filter(action => 
      !Object.values(mappings).some(mapping => mapping.actionTypes.includes(action))
    );

    return { totalMappings, totalActionsMapped, unmappedActions };
  };

  const stats = getMappingStats();

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Meta Action Mapping</h2>
          <button
            onClick={() => setShowAddForm(!showAddForm)}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            {showAddForm ? 'Cancel' : 'Add New Mapping'}
          </button>
        </div>

        <div className="text-sm text-gray-600 dark:text-gray-400 mb-4">
          Map Meta action types to business concepts for better reporting and analysis. 
          This helps aggregate related actions into meaningful metrics.
        </div>

        {/* Stats */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div className="bg-blue-50 dark:bg-blue-900 p-3 rounded">
            <div className="text-sm text-blue-600 dark:text-blue-300">Total Mappings</div>
            <div className="text-xl font-bold text-blue-800 dark:text-blue-100">{stats.totalMappings}</div>
          </div>
          <div className="bg-green-50 dark:bg-green-900 p-3 rounded">
            <div className="text-sm text-green-600 dark:text-green-300">Actions Mapped</div>
            <div className="text-xl font-bold text-green-800 dark:text-green-100">{stats.totalActionsMapped}</div>
          </div>
          <div className="bg-orange-50 dark:bg-orange-900 p-3 rounded">
            <div className="text-sm text-orange-600 dark:text-orange-300">Unmapped Actions</div>
            <div className="text-xl font-bold text-orange-800 dark:text-orange-100">{stats.unmappedActions.length}</div>
          </div>
        </div>

        {/* Discovery */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-gray-500">
            {availableActions.length} unique action types available (including {ALL_META_ACTION_TYPES.length} known from comprehensive analysis)
          </span>
          <button
            onClick={discoverAvailableActions}
            disabled={loading}
            className="text-sm text-blue-600 hover:text-blue-800 disabled:text-gray-400"
          >
            {loading ? 'Discovering...' : 'Refresh Action Types'}
          </button>
        </div>
      </div>

      {/* Add New Mapping Form */}
      {showAddForm && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-lg font-medium mb-4">Create New Mapping</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <div>
              <label className="block text-sm font-medium mb-1">
                Concept Name
              </label>
              <input
                type="text"
                value={newMapping.conceptName}
                onChange={(e) => setNewMapping({ ...newMapping, conceptName: e.target.value })}
                placeholder="e.g., conversions, leads, purchases"
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
              />
              <div className="mt-1 text-xs text-gray-500">
                Suggested: {businessConcepts.join(', ')}
              </div>
            </div>
            
            <div>
              <label className="block text-sm font-medium mb-1">
                Aggregation Type
              </label>
              <select
                value={newMapping.aggregationType}
                onChange={(e) => setNewMapping({ ...newMapping, aggregationType: e.target.value })}
                className="w-full p-2 border rounded dark:bg-gray-700 dark:border-gray-600"
              >
                <option value="sum">Sum (total count/value)</option>
                <option value="count">Count (number of action types)</option>
                <option value="average">Average (mean value)</option>
              </select>
            </div>
          </div>

          {/* Intelligent Suggestions */}
          {newMapping.actionTypes.length > 0 && (
            <div className="mb-4 p-4 bg-blue-50 dark:bg-blue-900 rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-sm font-medium text-blue-800 dark:text-blue-200">💡 Smart Suggestions</h4>
                <button
                  onClick={() => setShowSuggestions(!showSuggestions)}
                  className="text-xs text-blue-600 hover:text-blue-800"
                >
                  {showSuggestions ? 'Hide' : 'Show'} Suggestions
                </button>
              </div>
              
              {showSuggestions && (
                <div className="space-y-2">
                  {getSuggestions().map((suggestion, index) => (
                    <div key={index} className="flex items-center justify-between p-2 bg-white dark:bg-gray-700 rounded text-xs">
                      <div>
                        <span className="font-medium text-blue-600">{suggestion.concept}</span>
                        <span className="text-gray-500 ml-2">({Math.round(suggestion.confidence * 100)}% match)</span>
                        <div className="text-gray-400 mt-1">{suggestion.description}</div>
                      </div>
                      <button
                        onClick={() => applySuggestion(suggestion)}
                        className="px-2 py-1 bg-blue-500 text-white rounded hover:bg-blue-600"
                      >
                        Apply
                      </button>
                    </div>
                  ))}
                  {getSuggestions().length === 0 && (
                    <div className="text-xs text-gray-500 italic">
                      No business concept suggestions for your current selection.
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          <div className="mb-4">
            <label className="block text-sm font-medium mb-2">
              Select Action Types ({newMapping.actionTypes.length} selected)
            </label>
            
            {Object.entries(categorizeActionTypes(availableActions)).map(([category, actions]) => (
              actions.length > 0 && (
                <div key={category} className="mb-4">
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">{category}</h4>
                  <div className="flex flex-wrap gap-2">
                    {actions.map(actionType => (
                      <button
                        key={actionType}
                        onClick={() => toggleActionType(actionType)}
                        className={`px-3 py-1 rounded-full text-xs ${
                          newMapping.actionTypes.includes(actionType)
                            ? 'bg-blue-500 text-white'
                            : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300'
                        }`}
                      >
                        {actionType}
                      </button>
                    ))}
                  </div>
                </div>
              )
            ))}
          </div>

          <div className="flex space-x-2">
            <button
              onClick={addMapping}
              className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700"
            >
              Create Mapping
            </button>
            <button
              onClick={() => setShowAddForm(false)}
              className="px-4 py-2 bg-gray-300 text-gray-700 rounded hover:bg-gray-400"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Existing Mappings */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-medium mb-4">Current Mappings ({Object.keys(mappings).length})</h3>
        
        {Object.keys(mappings).length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <p>No mappings created yet.</p>
            <p className="text-sm mt-2">Create your first mapping to start organizing action types into business concepts.</p>
          </div>
        ) : (
          <div className="space-y-4">
            {Object.entries(mappings).map(([conceptName, mapping]) => (
              <div key={conceptName} className="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
                <div className="flex justify-between items-start mb-2">
                  <div>
                    <h4 className="text-lg font-medium text-blue-800 dark:text-blue-200">{conceptName}</h4>
                    <div className="text-sm text-gray-500">
                      Aggregation: {mapping.aggregationType} • {mapping.actionTypes.length} action types
                    </div>
                  </div>
                  <button
                    onClick={() => deleteMapping(conceptName)}
                    className="text-red-500 hover:text-red-700 text-sm"
                  >
                    Delete
                  </button>
                </div>
                
                <div className="flex flex-wrap gap-2">
                  {mapping.actionTypes.map(actionType => (
                    <span
                      key={actionType}
                      className="px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded text-xs"
                    >
                      {actionType}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Unmapped Actions */}
      {stats.unmappedActions.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-lg font-medium mb-4">Unmapped Action Types ({stats.unmappedActions.length})</h3>
          <div className="flex flex-wrap gap-2">
            {stats.unmappedActions.map(actionType => (
              <span
                key={actionType}
                className="px-2 py-1 bg-orange-100 dark:bg-orange-900 text-orange-800 dark:text-orange-200 rounded text-xs"
              >
                {actionType}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default ActionMapper; 