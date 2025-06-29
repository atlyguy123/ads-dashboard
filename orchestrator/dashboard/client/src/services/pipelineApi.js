// Pipeline API Service
// 
// Handles all API calls related to master pipeline functionality

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

class PipelineApiService {
  constructor() {
    this.baseUrl = API_BASE_URL;
  }

  /**
   * Make a request to the API
   * @param {string} endpoint - API endpoint (relative path)
   * @param {Object} options - Fetch options
   * @returns {Promise<Object>} - API response
   */
  async makeRequest(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const defaultOptions = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    };

    try {
      const response = await fetch(url, { ...defaultOptions, ...options });
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.message || data.error || `HTTP ${response.status}`);
      }
      
      return data;
    } catch (error) {
      console.error(`Pipeline API request failed (${endpoint}):`, error);
      throw error;
    }
  }

  /**
   * Get all pipelines with their current status
   * @returns {Promise<Array>} - List of pipelines
   */
  async getPipelines() {
    return this.makeRequest('/api/pipelines');
  }

  /**
   * Run a specific pipeline
   * @param {string} pipelineName - Name of the pipeline to run
   * @returns {Promise<Object>} - Run result
   */
  async runPipeline(pipelineName) {
    return this.makeRequest(`/api/run/${pipelineName}`, {
      method: 'POST'
    });
  }

  /**
   * Cancel a running pipeline
   * @param {string} pipelineName - Name of the pipeline to cancel
   * @returns {Promise<Object>} - Cancel result
   */
  async cancelPipeline(pipelineName) {
    return this.makeRequest(`/api/cancel/${pipelineName}`, {
      method: 'POST'
    });
  }

  /**
   * Reset all steps in a pipeline
   * @param {string} pipelineName - Name of the pipeline to reset
   * @returns {Promise<Object>} - Reset result
   */
  async resetAllSteps(pipelineName) {
    return this.makeRequest(`/api/reset-all/${pipelineName}`, {
      method: 'POST'
    });
  }

  /**
   * Run a specific step in a pipeline
   * @param {string} pipelineName - Name of the pipeline
   * @param {string} stepId - ID of the step to run
   * @returns {Promise<Object>} - Run result
   */
  async runStep(pipelineName, stepId) {
    return this.makeRequest(`/api/run/${pipelineName}/${stepId}`, {
      method: 'POST'
    });
  }

  /**
   * Cancel a specific step in a pipeline
   * @param {string} pipelineName - Name of the pipeline
   * @param {string} stepId - ID of the step to cancel
   * @returns {Promise<Object>} - Cancel result
   */
  async cancelStep(pipelineName, stepId) {
    return this.makeRequest(`/api/cancel/${pipelineName}/${stepId}`, {
      method: 'POST'
    });
  }

  /**
   * Reset a specific step in a pipeline
   * @param {string} pipelineName - Name of the pipeline
   * @param {string} stepId - ID of the step to reset
   * @returns {Promise<Object>} - Reset result
   */
  async resetStep(pipelineName, stepId) {
    return this.makeRequest(`/api/reset/${pipelineName}/${stepId}`, {
      method: 'POST'
    });
  }

  /**
   * Get running processes information
   * @returns {Promise<Object>} - Running processes info
   */
  async getRunningProcesses() {
    return this.makeRequest('/api/running');
  }

  /**
   * Refresh pipelines configuration
   * @returns {Promise<Object>} - Refresh result
   */
  async refreshPipelines() {
    return this.makeRequest('/api/refresh', {
      method: 'POST'
    });
  }
}

// Create and export a singleton instance
const pipelineApi = new PipelineApiService();

export { pipelineApi }; 