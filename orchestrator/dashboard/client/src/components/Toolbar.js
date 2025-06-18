import React, { useState, useEffect } from 'react';

// Placeholder for API service
const api = {
  getGeoFilterOptions: async () => {
    console.log('Fetching geo filter options');
    await new Promise(resolve => setTimeout(resolve, 300));
    return {
      NA: { name: "North America", countries: { US: { name: "United States" }, CA: { name: "Canada" } } },
      EU: { name: "Europe", countries: { GB: { name: "United Kingdom" }, DE: { name: "Germany" } } }
    };
  },
  getAdAccountOptions: async () => {
    console.log('Fetching ad account options');
    await new Promise(resolve => setTimeout(resolve, 300));
    return [
      { id: 'acc1', name: 'Ad Account 1' },
      { id: 'acc2', name: 'Ad Account 2' },
    ];
  }
};

export const Toolbar = ({ filters = {}, onFiltersChange = () => {} }) => {
  const [geoOptions, setGeoOptions] = useState({});
  const [adAccountOptions, setAdAccountOptions] = useState([]);
  const [isLoadingOptions, setIsLoadingOptions] = useState(false);
  
  useEffect(() => {
    const loadFilterOptions = async () => {
      setIsLoadingOptions(true);
      try {
        const geoData = await api.getGeoFilterOptions();
        setGeoOptions(geoData || {});
        
        const accountData = await api.getAdAccountOptions();
        setAdAccountOptions(accountData || []);
      } catch (error) {
        console.error('Error loading filter options:', error);
      } finally {
        setIsLoadingOptions(false);
      }
    };
    
    loadFilterOptions();
  }, []);

  const handleDateChange = (e) => {
    const { name, value } = e.target;
    let newFilters = { ...filters };
    if (name === "datePreset") {
      let from_date = '', to_date = new Date().toISOString().split('T')[0];
      const today = new Date();
      if (value === "all") { from_date = ''; to_date = ''; }
      else if (value === "ytd") { from_date = new Date(today.getFullYear(), 0, 1).toISOString().split('T')[0]; }
      else if (parseInt(value) > 0) {
        const pastDate = new Date();
        pastDate.setDate(today.getDate() - parseInt(value));
        from_date = pastDate.toISOString().split('T')[0];
      }
      newFilters = { ...newFilters, dateRange: { from_date, to_date }, currentPreset: value };
    } else {
      newFilters = { ...newFilters, dateRange: { ...(newFilters.dateRange || {}), [name]: value } };
    }
    onFiltersChange(newFilters);
  };

  const handleFilterChange = (type, value) => {
    onFiltersChange({ ...filters, [type]: value });
  };

  return (
    <div className="p-4 bg-white dark:bg-gray-800 shadow rounded-lg space-y-4 md:space-y-0 md:flex md:flex-wrap md:items-center md:space-x-4">
      <div className="flex items-center space-x-2">
        <label htmlFor="datePreset" className="text-sm font-medium dark:text-gray-300">Date Range:</label>
        <select
          id="datePreset"
          name="datePreset"
          value={filters.currentPreset || '30'}
          onChange={handleDateChange}
          className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
        >
          <option value="1">Last 1 Day</option>
          <option value="7">Last 7 Days</option>
          <option value="30">Last 30 Days</option>
          <option value="90">Last 90 Days</option>
          <option value="ytd">Year to Date</option>
          <option value="all">All Time</option>
          <option value="custom">Custom</option>
        </select>
        { (filters.currentPreset === 'custom') && (
          <>
            <input
              type="date"
              name="from_date"
              value={filters.dateRange?.from_date || ''}
              onChange={handleDateChange}
              className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
            />
            <span className="text-sm dark:text-gray-300">to</span>
            <input
              type="date"
              name="to_date"
              value={filters.dateRange?.to_date || ''}
              onChange={handleDateChange}
              className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
            />
          </>
        )}
      </div>

      <div className="flex items-center space-x-2">
        <label htmlFor="geoFilter" className="text-sm font-medium dark:text-gray-300">Geography:</label>
        <select
          id="geoFilter"
          value={filters.geo || 'all'}
          onChange={(e) => handleFilterChange('geo', e.target.value)}
          className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
          disabled={isLoadingOptions || Object.keys(geoOptions).length === 0}
        >
          <option value="all">All Geographies</option>
          {Object.entries(geoOptions).map(([continentCode, continent]) => (
            <optgroup key={continentCode} label={continent.name || continentCode}>
              {Object.entries(continent.countries || {}).map(([countryCode, country]) => (
                <option key={`country-${countryCode}`} value={`country-${countryCode}`}>
                  {country.name || countryCode}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
      </div>

      <div className="flex items-center space-x-2">
        <label htmlFor="deviceFilter" className="text-sm font-medium dark:text-gray-300">Device:</label>
        <select
          id="deviceFilter"
          value={filters.device || 'all'}
          onChange={(e) => handleFilterChange('device', e.target.value)}
          className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
        >
          <option value="all">All Devices</option>
          <option value="desktop">Desktop</option>
          <option value="mobile">Mobile</option>
        </select>
      </div>

      <div className="flex items-center space-x-2">
        <label htmlFor="hierarchyFilter" className="text-sm font-medium dark:text-gray-300">Hierarchy:</label>
        <select
          id="hierarchyFilter"
          value={filters.hierarchy || 'campaign'}
          onChange={(e) => handleFilterChange('hierarchy', e.target.value)}
          className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
        >
          <option value="campaign">Campaign</option>
          <option value="adset">Ad Set</option>
          <option value="ad">Ad</option>
        </select>
      </div>
      
      {adAccountOptions.length > 0 && (
        <div className="flex items-center space-x-2">
          <label htmlFor="adAccountFilter" className="text-sm font-medium dark:text-gray-300">Ad Account:</label>
          <select
            id="adAccountFilter"
            value={filters.adAccountId || 'all'}
            onChange={(e) => handleFilterChange('adAccountId', e.target.value)}
            className="p-2 border rounded-md bg-white dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300"
          >
            <option value="all">All Ad Accounts</option>
            {adAccountOptions.map(account => (
              <option key={account.id} value={account.id}>{account.name}</option>
            ))}
          </select>
        </div>
      )}
    </div>
  );
}; 