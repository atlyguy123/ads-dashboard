import axios from 'axios';

// Base URL for API requests - updated to use localhost:5001 consistently
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5001';

// Create an axios instance
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  // Increase timeout for large responses
  timeout: 600000, // 10 minutes
  // Set max content length to 500MB
  maxContentLength: 500 * 1024 * 1024,
  maxBodyLength: 500 * 1024 * 1024,
});

// Add response interceptor to handle large responses
apiClient.interceptors.response.use(
  (response) => {
    // Log response size for debugging
    if (response.data) {
      const responseSize = JSON.stringify(response.data).length;
      if (responseSize > 10 * 1024 * 1024) { // 10MB+
        console.warn(`Large API response: ${(responseSize / (1024 * 1024)).toFixed(2)} MB`);
      }
    }
    return response;
  },
  (error) => {
    // Enhanced error handling for memory issues
    if (error.code === 'ERR_INSUFFICIENT_RESOURCES' || 
        error.message?.includes('out of memory') ||
        error.message?.includes('Maximum call stack')) {
      console.error('Response too large for browser to handle:', error);
      error.message = 'Response is too large for your browser to handle. Please use debug mode or reduce the date range.';
    }
    return Promise.reject(error);
  }
);

export const api = {
  // Mixpanel Debug Endpoints
  
  /**
   * Get raw Mixpanel data with filters
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} - Response data
   */
  getRawMixpanelData: async (params = {}) => {
    try {
      const response = await apiClient.get('/api/mixpanel/data', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel data:', error);
      throw error;
    }
  },
  
  /**
   * Get the last Mixpanel data load timestamp
   * @returns {Promise<Object>} - Response with timestamp
   */
  getMixpanelDebugSyncTS: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/sync-ts');
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel sync timestamp:', error);
      throw error;
    }
  },
  
  /**
   * Reset the last Mixpanel data load timestamp
   * @returns {Promise<Object>} - Response with success message
   */
  resetMixpanelDebugSyncTS: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/sync-ts/reset');
      return response.data;
    } catch (error) {
      console.error('Error resetting Mixpanel sync timestamp:', error);
      throw error;
    }
  },
  
  /**
   * Get the latest processed date to continue from
   * @returns {Promise<Object>} - Response with latest processed date info
   */
  getLatestProcessedDate: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/latest-processed-date');
      return response.data;
    } catch (error) {
      console.error('Error fetching latest processed date:', error);
      throw error;
    }
  },
  
  /**
   * Reset all Mixpanel data in the database
   * @returns {Promise<Object>} - Response with success message
   */
  resetMixpanelDatabase: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/database/reset');
      return response.data;
    } catch (error) {
      console.error('Error resetting Mixpanel database:', error);
      throw error;
    }
  },
  
  /**
   * Refresh Mixpanel data by clearing data directories
   * @returns {Promise<Object>} - Response with success message
   */
  refreshMixpanelData: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/debug/data/refresh');
      return response.data;
    } catch (error) {
      console.error('Error refreshing Mixpanel data:', error);
      throw error;
    }
  },
  
  /**
   * Trigger Mixpanel data ingestion
   * @param {string} startDate - Optional start date (YYYY-MM-DD)
   * @returns {Promise<Object>} - Response with success message
   */
  triggerMixpanelIngest: async (startDate) => {
    try {
      const response = await apiClient.post('/api/mixpanel/ingest', null, {
        params: startDate ? { start_date: startDate } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error triggering data ingest:', error);
      throw error;
    }
  },
  
  /**
   * Start the Mixpanel data processing pipeline
   * @param {Object} options - Processing options
   * @param {string} options.start_date - Start date (YYYY-MM-DD)
   * @param {boolean} options.wipe_folder - Whether to wipe the folder before processing
   * @returns {Promise<Object>} - Response with success status
   */
  startMixpanelProcessing: async (options) => {
    try {
      const response = await apiClient.post('/api/mixpanel/process/start', options);
      return response.data;
    } catch (error) {
      console.error('Error starting Mixpanel processing:', error);
      throw error;
    }
  },
  
  /**
   * Get the current status of Mixpanel data processing
   * @returns {Promise<Object>} - Response with processing status
   */
  getMixpanelProcessStatus: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/process/status');
      return response.data;
    } catch (error) {
      console.error('Error fetching process status:', error);
      throw error;
    }
  },
  
  /**
   * Cancel the current Mixpanel data processing job
   * @returns {Promise<Object>} - Response with success status
   */
  cancelMixpanelProcessing: async () => {
    try {
      const response = await apiClient.post('/api/mixpanel/process/cancel');
      return response.data;
    } catch (error) {
      console.error('Error canceling processing:', error);
      throw error;
    }
  },
  
  /**
   * Get test DB user data
   * @param {string} distinctId - Optional user distinct ID
   * @returns {Promise<Object>} - Response with user data
   */
  getTestDbUser: async (distinctId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/test-db-user', {
        params: distinctId ? { distinct_id: distinctId } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching test DB user:', error);
      throw error;
    }
  },
  
  /**
   * Get test DB events data
   * @param {string} distinctId - Optional user distinct ID
   * @returns {Promise<Object>} - Response with events data
   */
  getTestDbEvents: async (distinctId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/test-db-events', {
        params: distinctId ? { distinct_id: distinctId } : {}
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching test DB events:', error);
      throw error;
    }
  },
  
  /**
   * Fetch Meta API data
   * @param {Object} params - Request parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {number} params.time_increment - Time increment in days
   * @param {string} [params.fields] - Comma-separated list of fields to retrieve
   * @returns {Promise<Object>} - Response with Meta API data
   */
  fetchMetaData: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/fetch', params);
      return response.data;
    } catch (error) {
      console.error('Error fetching Meta API data:', error);
      throw error;
    }
  },

  /**
   * Check the status of an async Meta API job
   * @param {string} reportRunId - The report run ID
   * @returns {Promise<Object>} - Job status information
   */
  checkMetaJobStatus: async (reportRunId) => {
    try {
      const response = await apiClient.get(`/api/meta/job/${reportRunId}/status`);
      return response.data;
    } catch (error) {
      console.error('Error checking Meta job status:', error);
      throw error;
    }
  },

  /**
   * Get results from a completed async Meta API job
   * @param {string} reportRunId - The report run ID
   * @param {boolean} useFileUrl - Whether to use file URL download
   * @returns {Promise<Object>} - Job results
   */
  getMetaJobResults: async (reportRunId, useFileUrl = false) => {
    try {
      const response = await apiClient.get(`/api/meta/job/${reportRunId}/results`, {
        params: { use_file_url: useFileUrl }
      });
      return response.data;
    } catch (error) {
      console.error('Error getting Meta job results:', error);
      throw error;
    }
  },

  // --- Cohort Analyzer API Methods ---
  analyzeCohortData: async (filters) => {
    try {
      const response = await apiClient.post('/api/cohort-analysis', filters);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Cohort Analyzer V3 API Methods ---
  analyzeCohortDataV3: async (filters) => {
    try {
      console.log('[V3] Sending cohort analysis request to V3 API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze', filters);
      console.log('[V3] Received response from V3 API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  analyzeCohortDataV3Enhanced: async (filters) => {
    try {
      console.log('[V3] Sending enhanced cohort analysis request to V3 API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze-enhanced', filters);
      console.log('[V3] Received enhanced response from V3 API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 Enhanced API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Cohort Analyzer V3 Refactored API Methods ---
  analyzeCohortDataV3Refactored: async (filters) => {
    try {
      console.log('[V3-Refactored] Sending cohort analysis request to V3 Refactored API:', filters);
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', filters);
      console.log('[V3-Refactored] Received response from V3 Refactored API:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error analyzing cohort data with V3 Refactored API:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3RefactoredHealth: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/refactored-health');
      return response.data;
    } catch (error) {
      console.error('Error checking V3 Refactored health:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3RefactoredVersion: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/refactored-version');
      return response.data;
    } catch (error) {
      console.error('Error getting V3 Refactored version:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // V3 Refactored Debug Methods
  runV3RefactoredStageAnalysis: async (filters, stage) => {
    try {
      console.log(`🚀🚀🚀 [V3-REFACTORED API] CALLING runV3RefactoredStageAnalysis for stage: ${stage}`);
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Will POST to: /api/v3/cohort/analyze-refactored`);
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Filters:`, filters);
      
      const debugFilters = {
        ...filters,
        debug_mode: true,
        debug_stage: stage,
        pipeline_version: '3.0.0_refactored'
      };
      
      console.log(`🚀🚀🚀 [V3-REFACTORED API] Final payload:`, debugFilters);
      
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', debugFilters);
      
      console.log(`🚀🚀🚀 [V3-REFACTORED API] SUCCESS! Received response:`, response.data);
      return response.data;
    } catch (error) {
      console.error(`🚨🚨🚨 [V3-REFACTORED API] ERROR in stage ${stage} analysis:`, error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3Health: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/health');
      return response.data;
    } catch (error) {
      console.error('Error checking V3 health:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getV3Version: async () => {
    try {
      const response = await apiClient.get('/api/v3/cohort/version');
      return response.data;
    } catch (error) {
      console.error('Error getting V3 version:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getCohortUserTimeline: async (filters) => {
    try {
      console.log('[DEBUG] getCohortUserTimeline called with filters:', filters);
      // Convert optional_filters to legacy format for the unified pipeline
      const optional_filters = filters.optional_filters || [];
      let primary_user_filter = {};
      let secondary_filters = [];
      
      if (optional_filters.length > 0) {
        // Convert optional_filters to legacy format
        const user_filters = optional_filters.filter(f => f.property_source === 'user');
        const event_filters = optional_filters.filter(f => f.property_source === 'event');
        
        // Use the first user filter as primary_user_filter
        primary_user_filter = user_filters.length > 0 ? user_filters[0] : {};
        
        // Only event filters go to secondary_filters
        secondary_filters = event_filters;
      } else {
        // Fall back to legacy format if provided
        primary_user_filter = filters.primary_user_filter || {};
        secondary_filters = filters.secondary_filters || [];
      }
      
      const payload = {
        date_from: filters.date_from_str,
        date_to: filters.date_to_str,
        primary_user_filter: primary_user_filter,
        secondary_filters: secondary_filters,
        config: {}
      };
      
      console.log('[DEBUG] getCohortUserTimeline sending payload:', payload);
      const response = await apiClient.post('/api/cohort-pipeline/timeline', payload);
      console.log('[DEBUG] getCohortUserTimeline received response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error fetching cohort user timeline:', error.response?.data || error.message);
      console.error('[DEBUG] getCohortUserTimeline full error:', error);
      throw error.response?.data || error;
    }
  },

  getUserEventRevenueTimeline: async (filters, distinctId = null, productId = null) => {
    try {
      console.log('[DEBUG] getUserEventRevenueTimeline called with filters:', filters, 'distinctId:', distinctId, 'productId:', productId);
      
      // CRITICAL FIX: Use V3 API instead of legacy API
      // The legacy API has the business logic bug where trial conversions incorrectly set initial_purchase = 1
      // The V3 API uses the correct EventStateTracker that properly handles business rules
      
      // Convert filters to V3 format
      const v3Payload = {
        date_from_str: filters.date_from_str,
        date_to_str: filters.date_to_str,
        // Convert optional_filters to V3 user_filters format
        user_filters: []
      };
      
      // Handle filter conversion
      if (filters.optional_filters && filters.optional_filters.length > 0) {
        // Use optional_filters (new format)
        v3Payload.user_filters = filters.optional_filters.filter(f => f.property_source === 'user');
      } else if (filters.primary_user_filter && filters.primary_user_filter.property_name) {
        // Convert legacy primary_user_filter format
        v3Payload.user_filters = [{
          property_name: filters.primary_user_filter.property_name,
          property_values: filters.primary_user_filter.property_values,
          property_source: 'user'
        }];
      }
      
      // Add user/product filtering if specified
      if (distinctId) v3Payload.distinct_id = distinctId;
      if (productId) v3Payload.product_id = productId;
      
      console.log('[DEBUG] getUserEventRevenueTimeline sending V3 payload:', v3Payload);
      
      // Use V3 API endpoint that has correct business logic
      const response = await apiClient.post('/api/v3/cohort/analyze-refactored', v3Payload);
      console.log('[DEBUG] getUserEventRevenueTimeline received V3 response:', response.data);
      
      // Convert V3 response to legacy format for compatibility with existing frontend code
      const v3Data = response.data;
      const stage3Data = v3Data?.stage_results?.stage3;
      
      if (!stage3Data || !stage3Data.timeline_results) {
        throw new Error('Invalid V3 response format');
      }
      
      const timelineResults = stage3Data.timeline_results;
      
      // Convert V3 timeline data to legacy format
      const legacyFormat = {
        dates: timelineResults.dates || [],
        event_rows: {},
        estimate_rows: {},
        arpc_per_product: timelineResults.arpc_per_product || {},
        available_users: Object.keys(timelineResults.user_daily_metrics || {}),
        available_products: timelineResults.available_products || []
      };
      
      // Convert daily metrics to legacy event_rows format
      if (timelineResults.timeline_data) {
        legacyFormat.dates.forEach(date => {
          const dayData = timelineResults.timeline_data[date] || {};
          
          // Map V3 field names to legacy field names
          if (!legacyFormat.event_rows.trial_started) legacyFormat.event_rows.trial_started = {};
          if (!legacyFormat.event_rows.trial_pending) legacyFormat.event_rows.trial_pending = {};
          if (!legacyFormat.event_rows.trial_ended) legacyFormat.event_rows.trial_ended = {};
          if (!legacyFormat.event_rows.trial_converted) legacyFormat.event_rows.trial_converted = {};
          if (!legacyFormat.event_rows.trial_canceled) legacyFormat.event_rows.trial_canceled = {};
          if (!legacyFormat.event_rows.initial_purchase) legacyFormat.event_rows.initial_purchase = {};
          if (!legacyFormat.event_rows.subscription_active) legacyFormat.event_rows.subscription_active = {};
          if (!legacyFormat.event_rows.subscription_cancelled) legacyFormat.event_rows.subscription_cancelled = {};
          if (!legacyFormat.event_rows.refund) legacyFormat.event_rows.refund = {};
          
          legacyFormat.event_rows.trial_started[date] = dayData.trial_started || 0;
          legacyFormat.event_rows.trial_pending[date] = dayData.trial_pending || 0;
          legacyFormat.event_rows.trial_ended[date] = dayData.trial_ended || 0;
          legacyFormat.event_rows.trial_converted[date] = dayData.trial_converted || 0;
          legacyFormat.event_rows.trial_canceled[date] = dayData.trial_cancelled || 0; // Note: cancelled vs canceled
          legacyFormat.event_rows.initial_purchase[date] = dayData.initial_purchase || 0; // CRITICAL: This will now be 0 for trial conversions
          legacyFormat.event_rows.subscription_active[date] = dayData.subscription_active || 0;
          legacyFormat.event_rows.subscription_cancelled[date] = dayData.subscription_cancelled || 0;
          legacyFormat.event_rows.refund[date] = dayData.refund_count || 0;
          
          // Revenue data
          if (!legacyFormat.estimate_rows.current_revenue) legacyFormat.estimate_rows.current_revenue = {};
          if (!legacyFormat.estimate_rows.estimated_revenue) legacyFormat.estimate_rows.estimated_revenue = {};
          if (!legacyFormat.estimate_rows.estimated_net_revenue) legacyFormat.estimate_rows.estimated_net_revenue = {};
          
          legacyFormat.estimate_rows.current_revenue[date] = dayData.revenue || 0;
          legacyFormat.estimate_rows.estimated_revenue[date] = dayData.estimated_revenue || 0;
          legacyFormat.estimate_rows.estimated_net_revenue[date] = dayData.estimated_revenue || 0;
        });
      }
      
      // Add cumulative data for legacy compatibility
      legacyFormat.event_rows.cumulative_initial_purchase = {};
      let cumulativeInitialPurchase = 0;
      legacyFormat.dates.forEach(date => {
        cumulativeInitialPurchase += legacyFormat.event_rows.initial_purchase[date] || 0;
        legacyFormat.event_rows.cumulative_initial_purchase[date] = cumulativeInitialPurchase;
      });
      
      console.log('[DEBUG] getUserEventRevenueTimeline converted to legacy format:', legacyFormat);
      return legacyFormat;
      
    } catch (err) {
      console.error('getUserEventRevenueTimeline error:', err);
      throw err;
    }
  },

  getDiscoverableCohortProperties: async () => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/discoverable_properties');
      return response.data;
    } catch (error) {
      console.error('Error fetching discoverable cohort properties:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  getDiscoverableCohortPropertyValues: async (propertyKey, propertySource) => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/property_values', {
        params: { property_key: propertyKey, property_source: propertySource }
      });
      return response.data;
    } catch (error) {
      console.error(`Error fetching values for ${propertyKey} (${propertySource}):`, error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  triggerCohortPropertyDiscovery: async () => {
    try {
      const response = await apiClient.post('/api/cohort_analyzer/trigger_discovery');
      return response.data;
    } catch (error) {
      console.error('Error triggering cohort property discovery:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },
  
  enableCohortProperties: async () => {
    try {
      const response = await apiClient.post('/api/cohort_analyzer/enable_properties');
      return response.data;
    } catch (error) {
      console.error('Error enabling cohort properties:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },
  
  getPropertyDiscoveryStatus: async () => {
    try {
      const response = await apiClient.get('/api/cohort_analyzer/discovery_status');
      return response.data;
    } catch (error) {
      console.error('Error checking property discovery status:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  // --- Mixpanel Debug API Methods ---
  getMixpanelDatabaseStats: async () => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/database-stats');
      return response.data;
    } catch (error) {
      console.error('Error fetching Mixpanel database statistics:', error);
      throw error;
    }
  },
  
  /**
   * Get events for a specific user
   * @param {string} userId - User ID or distinct ID
   * @returns {Promise<Array>} - Response with user events
   */
  getUserEvents: async (userId) => {
    try {
      const response = await apiClient.get('/api/mixpanel/debug/user-events', {
        params: { user_id: userId }
      });
      return response.data;
    } catch (error) {
      console.error('Error fetching user events:', error);
      throw error;
    }
  },

  // --- Meta Historical Data API Methods ---
  
  /**
   * Start a historical data collection job
   * @param {Object} params - Collection parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Response with job ID
   */
  startHistoricalCollection: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/historical/start', params);
      return response.data;
    } catch (error) {
      console.error('Error starting historical collection:', error);
      throw error;
    }
  },

  /**
   * Get status of a historical collection job
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} - Job status information
   */
  getHistoricalJobStatus: async (jobId) => {
    try {
      const response = await apiClient.get(`/api/meta/historical/jobs/${jobId}/status`);
      return response.data;
    } catch (error) {
      console.error('Error getting job status:', error);
      throw error;
    }
  },

  /**
   * Cancel a historical collection job
   * @param {string} jobId - Job ID
   * @returns {Promise<Object>} - Cancellation confirmation
   */
  cancelHistoricalJob: async (jobId) => {
    try {
      const response = await apiClient.post(`/api/meta/historical/jobs/${jobId}/cancel`);
      return response.data;
    } catch (error) {
      console.error('Error cancelling job:', error);
      throw error;
    }
  },

  /**
   * Get data coverage summary for a configuration
   * @param {Object} params - Configuration parameters
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @param {string} [params.start_date] - Optional start date filter
   * @param {string} [params.end_date] - Optional end date filter
   * @returns {Promise<Object>} - Coverage summary
   */
  getDataCoverage: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/coverage', { params });
      return response.data;
    } catch (error) {
      console.error('Error getting data coverage:', error);
      throw error;
    }
  },

  /**
   * Get list of missing dates for a configuration
   * @param {Object} params - Configuration parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Missing dates information
   */
  getMissingDates: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/missing-dates', { params });
      return response.data;
    } catch (error) {
      console.error('Error getting missing dates:', error);
      throw error;
    }
  },

  /**
   * Delete all data for a specific historical configuration
   * @param {string} configHash - Configuration hash to delete
   * @returns {Promise<Object>} - Deletion confirmation
   */
  deleteHistoricalConfiguration: async (configHash) => {
    try {
      const response = await apiClient.delete(`/api/meta/historical/configurations/${configHash}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting historical configuration:', error);
      throw error;
    }
  },

  /**
   * Get all stored request configurations
   * @returns {Promise<Array>} - List of configurations
   */
  getHistoricalConfigurations: async () => {
    try {
      const response = await apiClient.get('/api/meta/historical/configurations');
      return response.data;
    } catch (error) {
      console.error('Error getting configurations:', error);
      throw error;
    }
  },

  /**
   * Export stored data for a configuration and date range
   * @param {Object} params - Export parameters
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @param {string} [params.format] - Export format (default: 'json')
   * @returns {Promise<Object>} - Exported data
   */
  exportHistoricalData: async (params) => {
    try {
      const response = await apiClient.get('/api/meta/historical/export', { params });
      return response.data;
    } catch (error) {
      console.error('Error exporting data:', error);
      throw error;
    }
  },

  /**
   * Get stored data for a specific day and configuration
   * @param {Object} params - Day data parameters
   * @param {string} params.date - Date (YYYY-MM-DD)
   * @param {string} params.fields - Comma-separated list of fields
   * @param {string} [params.breakdowns] - Comma-separated list of breakdowns
   * @param {Object} [params.filtering] - Filtering configuration
   * @returns {Promise<Object>} - Day data
   */
  getHistoricalDayData: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/historical/get-day-data', params);
      return response.data;
    } catch (error) {
      console.error('Error getting day data:', error);
      throw error;
    }
  },

  /**
   * Get current action mappings
   * @returns {Promise<Object>} - Action mappings
   */
  getActionMappings: async () => {
    try {
      const response = await apiClient.get('/api/meta/action-mappings');
      return response.data;
    } catch (error) {
      console.error('Error getting action mappings:', error);
      throw error;
    }
  },

  /**
   * Save action mappings
   * @param {Object} mappings - Action mappings to save
   * @returns {Promise<Object>} - Save confirmation
   */
  saveActionMappings: async (mappings) => {
    try {
      const response = await apiClient.post('/api/meta/action-mappings', { mappings });
      return response.data;
    } catch (error) {
      console.error('Error saving action mappings:', error);
      throw error;
    }
  },

  /**
   * Get overview of all historical data tables
   * @returns {Promise<Object>} - Tables overview with row counts and structure
   */
  getTablesOverview: async () => {
    try {
      const response = await apiClient.get('/api/meta/historical/tables/overview');
      return response.data;
    } catch (error) {
      console.error('Error getting tables overview:', error);
      throw error;
    }
  },

  /**
   * Get data from a specific historical table
   * @param {string} tableName - Name of the table
   * @param {number} [limit=100] - Number of records to fetch
   * @param {number} [offset=0] - Offset for pagination
   * @returns {Promise<Object>} - Table data with pagination info
   */
  getTableData: async (tableName, limit = 100, offset = 0) => {
    try {
      const response = await apiClient.get(`/api/meta/historical/tables/${tableName}`, {
        params: { limit, offset }
      });
      return response.data;
    } catch (error) {
      console.error(`Error getting table data for ${tableName}:`, error);
      throw error;
    }
  },

  /**
   * Get aggregated daily metrics from a performance table
   * @param {string} tableName - Name of the table
   * @returns {Promise<Object>} - Aggregated daily metrics for all available dates
   */
  getTableAggregatedData: async (tableName) => {
    try {
      const response = await apiClient.get(`/api/meta/historical/tables/${tableName}/aggregated`);
      return response.data;
    } catch (error) {
      console.error(`Error getting aggregated data for ${tableName}:`, error);
      throw error;
    }
  },

  /**
   * Update Meta table with data for a specific date range
   * @param {Object} params - Update parameters
   * @param {string} params.table_name - Name of the table to update
   * @param {string} params.breakdown_type - Breakdown type (null, 'country', 'region', 'device')
   * @param {string} params.start_date - Start date (YYYY-MM-DD)
   * @param {string} params.end_date - End date (YYYY-MM-DD)
   * @param {boolean} params.skip_existing - Whether to skip existing dates
   * @returns {Promise<Object>} - Response with update status
   */
  updateMetaTable: async (params) => {
    try {
      const response = await apiClient.post('/api/meta/update-table', params);
      return response.data;
    } catch (error) {
      console.error('Error updating Meta table:', error);
      throw error;
    }
  },

  /**
   * Delete all data for a specific date from a table
   * @param {string} tableName - Name of the table
   * @param {string} date - Date to delete (YYYY-MM-DD)
   * @returns {Promise<Object>} - Response with deletion status
   */
  deleteTableDate: async (tableName, date) => {
    try {
      const response = await apiClient.delete(`/api/meta/historical/tables/${tableName}/date/${date}`);
      return response.data;
    } catch (error) {
      console.error(`Error deleting date ${date} from table ${tableName}:`, error);
      throw error;
    }
  },
}; 