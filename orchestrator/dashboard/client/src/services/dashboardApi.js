// Dashboard API Service
// 
// Handles all API calls related to dashboard functionality

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

class DashboardApiService {
  constructor() {
    this.baseUrl = API_BASE_URL;
  }

  async makeRequest(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    console.log('🔥🔥🔥 DASHBOARD API - makeRequest called:', url, options);
    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    };

    try {
      console.log('🔥🔥🔥 DASHBOARD API - Fetching:', url, config);
      const response = await fetch(url, config);
      const data = await response.json();
      console.log('🔥🔥🔥 DASHBOARD API - Response:', data);
      
      if (!response.ok) {
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
      }
      
      return data;
    } catch (error) {
      console.error(`API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  /**
   * Get available data configurations
   */
  async getConfigurations() {
    return this.makeRequest('/configurations');
  }

  /**
   * Get dashboard data for specified parameters
   */
  async getDashboardData(params) {
    return this.makeRequest('/data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Trigger manual data collection
   */
  async triggerCollection(params) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/collection/trigger`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(params)
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to trigger collection');
      }
      
      return data;
    } catch (error) {
      console.error('Error triggering collection:', error);
      throw error;
    }
  }

  /**
   * Get collection job status
   */
  async getCollectionStatus(jobId) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/collection/status/${jobId}`);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch collection status');
      }
      
      return data;
    } catch (error) {
      console.error('Error fetching collection status:', error);
      throw error;
    }
  }

  /**
   * Get data coverage summary for a configuration
   */
  async getCoverageSummary(configKey) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard/coverage/${configKey}`);
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch coverage summary');
      }
      
      return data;
    } catch (error) {
      console.error('Error fetching coverage summary:', error);
      throw error;
    }
  }

  /**
   * Health check for the dashboard API
   */
  async healthCheck() {
    return this.makeRequest('/health');
  }

  async getChartData(params) {
    return this.makeRequest('/chart-data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Get analytics data from the analytics pipeline - NEW ANALYTICS API
   */
  async getAnalyticsData(params) {
    console.log('🔍 ANALYTICS API - getAnalyticsData called with params:', params);
    const result = await this.makeRequest('/analytics/data', {
      method: 'POST',
      body: JSON.stringify(params),
    });
    
    // 🔍 CRITICAL DEBUG - Check if estimated_revenue_adjusted is in the response
    if (result && result.data && result.data.length > 0) {
      const firstRecord = result.data[0];
      console.log('🔍 ANALYTICS API - CRITICAL FIELD CHECK:', {
        'total_records': result.data.length,
        'first_record_keys': Object.keys(firstRecord),
        'has_estimated_revenue_adjusted': 'estimated_revenue_adjusted' in firstRecord,
        'estimated_revenue_adjusted_value': firstRecord.estimated_revenue_adjusted,
        'estimated_revenue_usd_value': firstRecord.estimated_revenue_usd,
        'spend_value': firstRecord.spend,
        'campaign_name': firstRecord.campaign_name
      });
    } else {
      console.log('🔍 ANALYTICS API - No data returned or empty result:', result);
    }
    
    return result;
  }

  /**
   * Get chart data for analytics sparklines and detailed views
   */
  async getAnalyticsChartData(params) {
    console.log('🔥🔥🔥 DASHBOARD API - getAnalyticsChartData called with:', params);
    const queryParams = new URLSearchParams(params).toString();
    console.log('🔥🔥🔥 DASHBOARD API - Query params:', queryParams);
    console.log('🔥🔥🔥 DASHBOARD API - Full URL:', `/analytics/chart-data?${queryParams}`);
    return this.makeRequest(`/analytics/chart-data?${queryParams}`);
  }

  /**
   * Run pipeline analysis for a specific campaign, adset, or ad
   */
  async runPipeline(params, dashboardParams = null) {
    try {
      // Use dashboard's actual date range if available, otherwise default to last 30 days
      let dateFrom, dateTo;
      
      if (dashboardParams && dashboardParams.start_date && dashboardParams.end_date) {
        // Dashboard controls use start_date and end_date
        dateFrom = dashboardParams.start_date;
        dateTo = dashboardParams.end_date;
      } else if (dashboardParams && dashboardParams.date_from && dashboardParams.date_to) {
        // Fallback for other formats that might use date_from and date_to
        dateFrom = dashboardParams.date_from;
        dateTo = dashboardParams.date_to;
      } else {
        // Fallback to last 30 days
        dateFrom = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        dateTo = new Date().toISOString().split('T')[0];
      }

      // Prepare the pipeline parameters matching CohortAnalyzerV3RefactoredPage format
      const pipelineParams = {
        date_from: dateFrom,
        date_to: dateTo,
        timeline_end_date: new Date(Date.now() + 180 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 6 months from now
        pipeline_version: '3.0.0_refactored',
        use_conversion_probabilities: true,
        optional_filters: [],
        secondary_filters: [],
        config: {
          product_filter: {
            include_patterns: [".*"], // Include all products by default
            exclude_patterns: [],
            specific_product_ids: []
          },
          lifecycle: {
            trial_window_days: 7,
            cancellation_window_days: 30,
            smoothing_enabled: true
          },
          timeline: {
            include_estimates: true,
            include_confidence_intervals: false
          }
        }
      };

      // Set the primary_user_filter based on the ID type (matching cohort page format)
      if (params.ad_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_ad_id',
          property_values: [params.ad_id],
          property_source: 'user'
        };
      } else if (params.adset_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_ad_set_id', 
          property_values: [params.adset_id],
          property_source: 'user'
        };
      } else if (params.campaign_id) {
        pipelineParams.primary_user_filter = {
          property_name: 'abi_campaign_id',
          property_values: [params.campaign_id],
          property_source: 'user'
        };
      } else {
        // No specific filter - this will get all users in the date range
        pipelineParams.primary_user_filter = {
          property_name: '',
          property_values: [],
          property_source: 'user'
        };
      }

      console.log('🔍 Pipeline Debug: Sending V3 refactored parameters:', pipelineParams);

      const response = await fetch(`${API_BASE_URL}/api/v3/cohort/analyze-refactored`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(pipelineParams)
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Failed to run pipeline');
      }
      
      return data;
    } catch (error) {
      console.error('Error running pipeline:', error);
      throw error;
    }
  }

  /**
   * Get available date range from meta analytics database
   */
  async getAvailableDateRange() {
    try {
      const response = await this.makeRequest('/api/dashboard/analytics/date-range');
      return response;
    } catch (error) {
      console.error('Error fetching available date range:', error);
      // Return fallback data if API fails
      return {
        success: true,
        data: {
          earliest_date: '2025-01-01',
          latest_date: new Date().toISOString().split('T')[0]
        }
      };
    }
  }
}

// Create and export a singleton instance
const dashboardApi = new DashboardApiService();
export { dashboardApi }; 