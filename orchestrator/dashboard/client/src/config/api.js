/**
 * Centralized API Configuration
 * Single source of truth for all API calls
 */

// Get API base URL from environment variable
// For development: http://localhost:5001
// For production: empty string (relative URLs)
const API_BASE_URL = process.env.REACT_APP_API_URL || '';

// Remove trailing slash if present
const normalizedBaseUrl = API_BASE_URL.endsWith('/') 
  ? API_BASE_URL.slice(0, -1) 
  : API_BASE_URL;

/**
 * Build full API URL from endpoint path
 * @param {string} endpoint - API endpoint path (e.g., '/api/run/master_pipeline')
 * @returns {string} - Full URL
 */
export const buildApiUrl = (endpoint) => {
  // Ensure endpoint starts with /
  const normalizedEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  return `${normalizedBaseUrl}${normalizedEndpoint}`;
};

/**
 * Standard fetch wrapper with consistent error handling
 * @param {string} endpoint - API endpoint path
 * @param {Object} options - Fetch options
 * @returns {Promise} - Response promise
 */
export const apiRequest = async (endpoint, options = {}) => {
  const url = buildApiUrl(endpoint);
  
  const defaultOptions = {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  };

  if (process.env.NODE_ENV === 'development') {
    console.log(`🔄 API Request: ${options.method || 'GET'} ${url}`);
  }

  try {
    const response = await fetch(url, defaultOptions);
    
    if (process.env.NODE_ENV === 'development') {
      console.log(`✅ API Response: ${response.status} for ${options.method || 'GET'} ${endpoint}`);
    }
    
    return response;
  } catch (error) {
    if (process.env.NODE_ENV === 'development') {
      console.error(`❌ API Error: ${error.message} for ${options.method || 'GET'} ${endpoint}`);
    }
    throw error;
  }
};

/**
 * Timezone utility functions for consistent time handling
 */
export const formatDateForDisplay = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
};

export const getCurrentETTime = () => {
  return new Date().toLocaleString('en-US', {
    timeZone: 'America/New_York'
  });
};

export const getCurrentETDate = () => {
  return new Date().toLocaleDateString('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).split('/').reverse().join('-'); // Returns YYYY-MM-DD format
};

export const formatDateForAPI = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).split('/').reverse().join('-'); // Returns YYYY-MM-DD format
};

/**
 * Configuration object for easy access
 */
export const apiConfig = {
  baseUrl: normalizedBaseUrl,
  buildUrl: buildApiUrl,
  request: apiRequest,
  timezone: {
    formatDateForDisplay,
    getCurrentETTime,
    getCurrentETDate,
    formatDateForAPI
  }
};

export default apiConfig; 