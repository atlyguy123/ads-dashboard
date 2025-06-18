import { useState, useCallback } from 'react';
import pipelineApi, { errorHandlers } from '../api/pipelineApi';

/**
 * Custom hook for managing cohort pipeline data and API calls
 */
export const usePipelineData = () => {
    const [pipelineData, setPipelineData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [lastAnalysisParams, setLastAnalysisParams] = useState(null);

    /**
     * Clear error state
     */
    const clearError = useCallback(() => {
        setError(null);
    }, []);

    /**
     * Run full cohort analysis
     */
    const runAnalysis = useCallback(async (analysisParams) => {
        setLoading(true);
        setError(null);
        
        try {
            console.log('Running full analysis with params:', analysisParams);
            const result = await pipelineApi.runFullAnalysis(analysisParams);
            
            console.log('Analysis result:', result);
            setPipelineData(result);
            setLastAnalysisParams(analysisParams);
            
            return result;
        } catch (err) {
            console.error('Analysis failed:', err);
            const friendlyMessage = errorHandlers.getUserFriendlyMessage(err);
            setError(friendlyMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Run ARPU-only analysis
     */
    const runARPUAnalysis = useCallback(async (analysisParams) => {
        setLoading(true);
        setError(null);
        
        try {
            console.log('Running ARPU analysis with params:', analysisParams);
            const result = await pipelineApi.runARPUAnalysis(analysisParams);
            
            // Update only the ARPU portion of the data
            setPipelineData(prevData => ({
                ...prevData,
                data: {
                    ...prevData?.data,
                    arpu_data: result.data?.arpu_data,
                }
            }));
            
            return result;
        } catch (err) {
            console.error('ARPU analysis failed:', err);
            const friendlyMessage = errorHandlers.getUserFriendlyMessage(err);
            setError(friendlyMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Run lifecycle rates-only analysis
     */
    const runLifecycleAnalysis = useCallback(async (analysisParams) => {
        setLoading(true);
        setError(null);
        
        try {
            console.log('Running lifecycle analysis with params:', analysisParams);
            const result = await pipelineApi.runLifecycleAnalysis(analysisParams);
            
            // Update only the lifecycle portion of the data
            setPipelineData(prevData => ({
                ...prevData,
                data: {
                    ...prevData?.data,
                    lifecycle_rates: result.data?.lifecycle_rates,
                }
            }));
            
            return result;
        } catch (err) {
            console.error('Lifecycle analysis failed:', err);
            const friendlyMessage = errorHandlers.getUserFriendlyMessage(err);
            setError(friendlyMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Generate timeline data
     */
    const generateTimeline = useCallback(async (analysisParams) => {
        setLoading(true);
        setError(null);
        
        try {
            console.log('Generating timeline with params:', analysisParams);
            const result = await pipelineApi.generateTimeline(analysisParams);
            
            // Update only the timeline portion of the data
            setPipelineData(prevData => ({
                ...prevData,
                data: {
                    ...prevData?.data,
                    timeline_data: result.data?.timeline_data,
                }
            }));
            
            return result;
        } catch (err) {
            console.error('Timeline generation failed:', err);
            const friendlyMessage = errorHandlers.getUserFriendlyMessage(err);
            setError(friendlyMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Run stage-specific analysis for debug mode
     */
    const runStageAnalysis = useCallback(async (analysisParams, stage) => {
        setLoading(true);
        setError(null);
        
        try {
            console.log(`Running stage analysis for ${stage} with params:`, analysisParams);
            const result = await pipelineApi.debugStageAnalysis.getStageResults(analysisParams, stage);
            
            console.log(`Stage ${stage} result:`, result);
            setPipelineData(result);
            setLastAnalysisParams({ ...analysisParams, debug_stage: stage });
            
            return result;
        } catch (err) {
            console.error(`Stage ${stage} analysis failed:`, err);
            const friendlyMessage = errorHandlers.getUserFriendlyMessage(err);
            setError(friendlyMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Validate analysis inputs
     */
    const validateInputs = useCallback(async (analysisParams) => {
        try {
            console.log('Validating inputs:', analysisParams);
            const result = await pipelineApi.validateInputs(analysisParams);
            console.log('Validation result:', result);
            return result;
        } catch (err) {
            console.error('Input validation failed:', err);
            throw err;
        }
    }, []);

    /**
     * Get performance report
     */
    const getPerformanceReport = useCallback(async () => {
        try {
            const result = await pipelineApi.getPerformanceReport();
            console.log('Performance report:', result);
            return result;
        } catch (err) {
            console.error('Failed to get performance report:', err);
            throw err;
        }
    }, []);

    /**
     * Health check
     */
    const healthCheck = useCallback(async () => {
        try {
            const result = await pipelineApi.healthCheck();
            console.log('Health check result:', result);
            return result;
        } catch (err) {
            console.error('Health check failed:', err);
            throw err;
        }
    }, []);

    /**
     * Reset all data
     */
    const resetData = useCallback(() => {
        setPipelineData(null);
        setError(null);
        setLastAnalysisParams(null);
    }, []);

    /**
     * Get transformed data for specific components
     */
    const getTransformedData = useCallback(() => {
        if (!pipelineData) return null;

        return {
            charts: pipelineApi.dataTransforms.transformForCharts(pipelineData),
            arpu: pipelineApi.dataTransforms.transformARPUData(pipelineData),
            lifecycle: pipelineApi.dataTransforms.transformLifecycleRates(pipelineData),
            summary: pipelineApi.dataTransforms.extractSummaryStats(pipelineData),
        };
    }, [pipelineData]);

    return {
        // Data state
        pipelineData,
        loading,
        error,
        lastAnalysisParams,

        // Actions
        runAnalysis,
        runARPUAnalysis,
        runLifecycleAnalysis,
        generateTimeline,
        runStageAnalysis,
        validateInputs,
        getPerformanceReport,
        healthCheck,
        resetData,
        clearError,

        // Computed data
        transformedData: getTransformedData(),

        // Utility functions
        isValidationError: error ? errorHandlers.isValidationError({ message: error }) : false,
        isDatabaseError: error ? errorHandlers.isDatabaseError({ message: error }) : false,
    };
}; 