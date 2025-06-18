/**
 * Cohort Pipeline API Service
 * 
 * This service handles all API calls to the new cohort pipeline backend.
 * It provides a clean interface for the frontend components to interact
 * with the modular pipeline architecture.
 */

const API_BASE_URL = '/api/v2/cohort';

/**
 * Generic API request handler with error handling
 */
const apiRequest = async (endpoint, options = {}) => {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers,
            },
            ...options,
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || `HTTP error! status: ${response.status}`);
        }

        return data;
    } catch (error) {
        console.error(`API request failed for ${endpoint}:`, error);
        throw error;
    }
};

/**
 * Run complete cohort analysis using the new pipeline
 */
export const runFullAnalysis = async (analysisParams) => {
    return apiRequest('/analyze', {
        method: 'POST',
        body: JSON.stringify(analysisParams),
    });
};

/**
 * Run ARPU-only analysis
 */
export const runARPUAnalysis = async (analysisParams) => {
    return apiRequest('/arpu-only', {
        method: 'POST',
        body: JSON.stringify(analysisParams),
    });
};

/**
 * Run lifecycle rates-only analysis
 */
export const runLifecycleAnalysis = async (analysisParams) => {
    return apiRequest('/lifecycle-only', {
        method: 'POST',
        body: JSON.stringify(analysisParams),
    });
};

/**
 * Generate timeline data
 */
export const generateTimeline = async (analysisParams) => {
    return apiRequest('/timeline', {
        method: 'POST',
        body: JSON.stringify(analysisParams),
    });
};

/**
 * Validate analysis inputs
 */
export const validateInputs = async (analysisParams) => {
    return apiRequest('/validate', {
        method: 'POST',
        body: JSON.stringify(analysisParams),
    });
};

/**
 * Get performance report
 */
export const getPerformanceReport = async () => {
    return apiRequest('/performance', {
        method: 'GET',
    });
};

/**
 * Optimize database
 */
export const optimizeDatabase = async () => {
    return apiRequest('/optimize-db', {
        method: 'POST',
    });
};

/**
 * Health check
 */
export const healthCheck = async () => {
    return apiRequest('/health', {
        method: 'GET',
    });
};

/**
 * Debug-specific API calls for stage-by-stage analysis
 */
export const debugStageAnalysis = {
    /**
     * Run analysis up to a specific stage
     */
    runToStage: async (analysisParams, stage) => {
        return apiRequest('/analyze', {
            method: 'POST',
            body: JSON.stringify({
                ...analysisParams,
                debug_mode: true,
                debug_stage: stage,
            }),
        });
    },

    /**
     * Get intermediate results for a specific stage
     */
    getStageResults: async (analysisParams, stage) => {
        // For debug mode, always use the full analysis endpoint
        // This ensures we get complete pipeline results including filter_stats
        // The backend will handle stopping at the appropriate stage based on debug_stage parameter
        const debugPayload = {
            ...analysisParams,
            debug_mode: true,
            debug_stage: stage,
        };
        
        console.log('[API DEBUG] Calling /api/v2/cohort/analyze with debug payload:', debugPayload);
        console.log('[API DEBUG] debug_mode:', debugPayload.debug_mode);
        console.log('[API DEBUG] debug_stage:', debugPayload.debug_stage);
        
        return apiRequest('/analyze', {
            method: 'POST',
            body: JSON.stringify(debugPayload),
        });
    },
};

/**
 * Utility functions for data transformation
 */
export const dataTransforms = {
    /**
     * Transform pipeline data for chart visualization
     */
    transformForCharts: (pipelineData) => {
        if (!pipelineData?.data?.timeline_data) return null;

        const timelineData = pipelineData.data.timeline_data;
        const dates = timelineData.dates || [];
        const dailyMetrics = timelineData.daily_metrics || {};

        return dates.map(date => ({
            date,
            ...dailyMetrics[date],
        }));
    },

    /**
     * Transform ARPU data for display
     */
    transformARPUData: (pipelineData) => {
        if (!pipelineData?.data?.arpu_data) return null;

        const arpuData = pipelineData.data.arpu_data;
        return {
            cohortWide: arpuData.cohort_wide || {},
            perProduct: arpuData.per_product || {},
        };
    },

    /**
     * Transform lifecycle rates for table display
     */
    transformLifecycleRates: (pipelineData) => {
        if (!pipelineData?.data?.lifecycle_rates) return null;

        const lifecycleRates = pipelineData.data.lifecycle_rates;
        return {
            aggregate: lifecycleRates.aggregate || {},
            perProduct: lifecycleRates.per_product || {},
        };
    },

    /**
     * Extract summary statistics
     */
    extractSummaryStats: (pipelineData) => {
        if (!pipelineData?.data?.cohort_summary) return null;

        return pipelineData.data.cohort_summary;
    },
};

/**
 * Error handling utilities
 */
export const errorHandlers = {
    /**
     * Check if error is a validation error
     */
    isValidationError: (error) => {
        return error.message && error.message.includes('validation');
    },

    /**
     * Check if error is a database error
     */
    isDatabaseError: (error) => {
        return error.message && error.message.includes('database');
    },

    /**
     * Get user-friendly error message
     */
    getUserFriendlyMessage: (error) => {
        if (errorHandlers.isValidationError(error)) {
            return 'Please check your input parameters and try again.';
        }
        if (errorHandlers.isDatabaseError(error)) {
            return 'Database connection issue. Please try again later.';
        }
        return error.message || 'An unexpected error occurred.';
    },
};

export default {
    runFullAnalysis,
    runARPUAnalysis,
    runLifecycleAnalysis,
    generateTimeline,
    validateInputs,
    getPerformanceReport,
    optimizeDatabase,
    healthCheck,
    debugStageAnalysis,
    dataTransforms,
    errorHandlers,
}; 