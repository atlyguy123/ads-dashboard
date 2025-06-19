// Debug API Services
// Centralized API calls for all debug tools

const BASE_URL = '/api/debug';

// Generic API call helper
const apiCall = async (endpoint, data = {}) => {
  try {
    const response = await fetch(`${BASE_URL}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result = await response.json();
    return result;
  } catch (error) {
    console.error('API call failed:', error);
    throw error;
  }
};

// Conversion Rates Debug API
export const conversionRatesDebugApi = {
  loadOverview: () => apiCall('/conversion-rates/overview'),
  loadCohortTree: () => apiCall('/conversion-rates/cohort-tree'),
  validateData: () => apiCall('/conversion-rates/validate'),
  searchCohorts: (filters) => apiCall('/conversion-rates/search', filters),
};

// Price Bucket Debug API
export const priceBucketDebugApi = {
  loadOverview: () => apiCall('/price-bucket/overview'),
  searchData: (filters) => apiCall('/price-bucket/search', filters),
};

// Value Estimation Debug API
export const valueEstimationDebugApi = {
  loadOverview: () => apiCall('/value-estimation/overview'),
  loadExamples: () => apiCall('/value-estimation/examples'),
  refreshStatus: (statusType, statusValue) => apiCall('/value-estimation/examples/refresh', { status_type: statusType, status_value: statusValue }),
  validateCalculations: () => apiCall('/value-estimation/validate'),
};

// Generic debug API for module-based calls
export const debugApi = {
  // Call any debug module action
  callModuleAction: (moduleName, action, data = {}) => {
    return apiCall(`/modules/${moduleName}/actions/${action}`, data);
  },
  
  // Get available debug modules
  getModules: () => apiCall('/modules'),
  
  // Get module information
  getModuleInfo: (moduleName) => apiCall(`/modules/${moduleName}/info`),
};

export default {
  conversionRatesDebugApi,
  priceBucketDebugApi,
  valueEstimationDebugApi,
  debugApi,
}; 