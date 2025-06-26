// Analytics Pipeline API Service
// 
// Handles all API calls related to the analytics pipeline functionality

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

class AnalyticsPipelineApiService {
  constructor() {
    this.baseUrl = `${API_BASE_URL}/api/analytics-pipeline`;
  }

  async makeRequest(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    };

    try {
      const response = await fetch(url, config);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }
      
      return data;
    } catch (error) {
      console.error(`Analytics Pipeline API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  /**
   * Start the analytics pipeline
   * @param {Object} options - Pipeline options
   * @param {string} options.date - Date to run pipeline for (YYYY-MM-DD format)
   * @returns {Promise<Object>} - Pipeline start response with thread_id
   */
  async startAnalyticsPipeline(options = {}) {
    return this.makeRequest('/start', {
      method: 'POST',
      body: JSON.stringify(options),
    });
  }

  /**
   * Get current analytics pipeline status and progress
   * @returns {Promise<Object>} - Current pipeline status including progress, stage, errors
   */
  async getAnalyticsPipelineStatus() {
    return this.makeRequest('/status');
  }

  /**
   * Cancel the currently running analytics pipeline
   * @returns {Promise<Object>} - Cancellation confirmation
   */
  async cancelAnalyticsPipeline() {
    return this.makeRequest('/cancel', {
      method: 'POST',
    });
  }

  /**
   * Health check for the analytics pipeline API
   * @returns {Promise<Object>} - API health status with available endpoints
   */
  async healthCheck() {
    return this.makeRequest('/health');
  }
}

// Create and export a singleton instance
const analyticsPipelineApi = new AnalyticsPipelineApiService();

export { analyticsPipelineApi }; 