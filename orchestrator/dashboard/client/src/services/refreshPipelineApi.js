// Refresh Pipeline API Service
// 
// Handles all API calls related to the complete refresh pipeline functionality

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

class RefreshPipelineApiService {
  constructor() {
    this.baseUrl = `${API_BASE_URL}/api/refresh-pipeline`;
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
      console.error(`Refresh Pipeline API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  /**
   * Start the complete refresh pipeline
   * @param {Object} options - Pipeline options
   * @param {boolean} options.debug_mode - Whether to use debug mode (5 days instead of 30)
   * @param {number|null} options.debug_days_override - Override for number of days in debug mode
   * @returns {Promise<Object>} - Pipeline start response with pipeline_id
   */
  async startRefreshPipeline(options = {}) {
    return this.makeRequest('/start', {
      method: 'POST',
      body: JSON.stringify(options),
    });
  }

  /**
   * Get current refresh pipeline status and progress
   * @returns {Promise<Object>} - Current pipeline status including progress, stages, errors
   */
  async getRefreshPipelineStatus() {
    return this.makeRequest('/status');
  }

  /**
   * Cancel the currently running refresh pipeline
   * @returns {Promise<Object>} - Cancellation confirmation
   */
  async cancelRefreshPipeline() {
    return this.makeRequest('/cancel', {
      method: 'POST',
    });
  }

  /**
   * Get information about the last refresh pipeline run
   * @returns {Promise<Object>} - Last refresh data including timestamp and status
   */
  async getLastRefreshTime() {
    return this.makeRequest('/last-refresh');
  }

  /**
   * Health check for the refresh pipeline API
   * @returns {Promise<Object>} - API health status
   */
  async healthCheck() {
    return this.makeRequest('/status');
  }

  // Resume an interrupted pipeline from a specific stage
  async resumeRefreshPipeline(stageIndex, options = {}) {
    const response = await fetch(`${this.baseUrl}/resume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        stage_index: stageIndex,
        ...options
      }),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    return await response.json();
  }

  // Dismiss an interrupted pipeline notification
  async dismissInterruptedPipeline(pipelineId) {
    const response = await fetch(`${this.baseUrl}/dismiss-interrupted`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        pipeline_id: pipelineId
      }),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    return await response.json();
  }

  /**
   * Get cached dashboard parameters from the last pipeline refresh
   * @returns {Promise<Object>} - Cached dashboard parameters including date range and config
   */
  async getCachedDashboardParams() {
    return this.makeRequest('/dashboard-params');
  }
}

// Create and export a singleton instance
const refreshPipelineApi = new RefreshPipelineApiService();

export { refreshPipelineApi }; 