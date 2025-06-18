import { useState, useEffect } from 'react';
import { loadSavedState, getDateRangeFromTimeframe } from '../components/conversion_probability/utils/conversionUtils';

export const useConversionProbability = () => {
  // Initialize state with saved values
  const savedState = loadSavedState();
  
  // Analysis configuration state
  const [config, setConfig] = useState({
    ...savedState.config,
    min_price_samples: savedState.config.min_price_samples || 100
  });

  // Analysis state
  const [currentAnalysis, setCurrentAnalysis] = useState(null);
  const [analysisProgress, setAnalysisProgress] = useState(null);
  const [analysisResults, setAnalysisResults] = useState(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  // NEW: New hierarchical analysis state
  const [isRunningNewAnalysis, setIsRunningNewAnalysis] = useState(false);

  // Property analysis state
  const [propertyAnalysis, setPropertyAnalysis] = useState(null);
  const [isAnalyzingProperties, setIsAnalyzingProperties] = useState(false);

  // UI state
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [expandedSections, setExpandedSections] = useState({
    ...savedState.expandedSections,
    analysisHierarchy: true // Default to expanded when analysis is complete
  });

  // Available analyses
  const [availableAnalyses, setAvailableAnalyses] = useState([]);

  // Save state to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('conversionProbability_config', JSON.stringify(config));
  }, [config]);

  useEffect(() => {
    localStorage.setItem('conversionProbability_expandedSections', JSON.stringify(expandedSections));
  }, [expandedSections]);

  // Load analysis results function
  const loadAnalysisResults = async (analysisId) => {
    try {
      const response = await fetch(`/api/conversion-probability/results/${analysisId}`);
      const data = await response.json();
      
      if (data.success) {
        setAnalysisResults(data.data);
        // Auto-expand hierarchy section when results are loaded
        setExpandedSections(prev => ({ ...prev, analysisHierarchy: true }));
        loadAvailableAnalyses();
      } else {
        setError(data.error || 'Failed to load analysis results');
      }
    } catch (err) {
      setError('Error loading analysis results: ' + err.message);
    }
  };

  const loadAvailableAnalyses = async () => {
    try {
      const response = await fetch('/api/conversion-probability/analyses');
      const data = await response.json();
      
      if (data.success) {
        setAvailableAnalyses(data.data.files);
      }
    } catch (err) {
      console.error('Error loading available analyses:', err);
    }
  };

  // Polling for progress updates
  useEffect(() => {
    let interval;
    if (currentAnalysis && isAnalyzing) {
      interval = setInterval(async () => {
        try {
          const response = await fetch(`/api/conversion-probability/progress/${currentAnalysis}`);
          const data = await response.json();
          
          if (data.success) {
            setAnalysisProgress(data.data.progress);
            
            if (data.data.status === 'completed' || data.data.status === 'failed') {
              setIsAnalyzing(false);
              if (data.data.status === 'completed') {
                setSuccess('Analysis completed successfully!');
                loadAnalysisResults(currentAnalysis);
              } else {
                setError('Analysis failed. Please try again.');
              }
            }
          }
        } catch (err) {
          console.error('Error polling progress:', err);
        }
      }, 2000);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [currentAnalysis, isAnalyzing]);

  // Load available analyses on component mount
  useEffect(() => {
    loadAvailableAnalyses();
  }, []);

  const analyzeProperties = async () => {
    setIsAnalyzingProperties(true);
    setError(null);
    
    try {
      const dateRange = getDateRangeFromTimeframe(config.timeframe);
      
      const response = await fetch('/api/conversion-probability/analyze-properties', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          timeframe_start: dateRange.start,
          timeframe_end: dateRange.end,
          sample_limit: 10000,
          min_price_samples: config.min_price_samples,
          min_cohort_size: config.min_cohort_size
        }),
      });

      const data = await response.json();
      
      if (data.success) {
        setPropertyAnalysis(data.data);
        setSuccess('Property analysis completed successfully!');
        setExpandedSections(prev => ({ ...prev, propertyAnalysis: false }));
      } else {
        setError(data.error || 'Property analysis failed');
      }
    } catch (err) {
      setError('Error analyzing properties: ' + err.message);
    } finally {
      setIsAnalyzingProperties(false);
    }
  };

  const startAnalysis = async () => {
    setIsAnalyzing(true);
    setError(null);
    setAnalysisProgress(null);
    
    try {
      const dateRange = getDateRangeFromTimeframe(config.timeframe);
      
      const requestBody = {
        timeframe_start: dateRange.start,
        timeframe_end: dateRange.end,
        min_cohort_size: config.min_cohort_size,
        force_recalculate: config.force_recalculate
      };
      
      if (propertyAnalysis) {
        requestBody.existing_property_analysis = propertyAnalysis;
      }
      
      const response = await fetch('/api/conversion-probability/start-analysis', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

      const data = await response.json();
      
      if (data.success) {
        setCurrentAnalysis(data.data.analysis_id);
        
        if (data.data.cached) {
          setIsAnalyzing(false);
          setSuccess('Using cached analysis results!');
          loadAnalysisResults(data.data.analysis_id);
        } else {
          setSuccess(`Analysis started! Estimated duration: ${data.data.estimated_duration_minutes} minutes`);
        }
      } else {
        setError(data.error || 'Failed to start analysis');
        setIsAnalyzing(false);
      }
    } catch (err) {
      setError('Error starting analysis: ' + err.message);
      setIsAnalyzing(false);
    }
  };

  // NEW: Run new hierarchical analysis
  const runNewHierarchicalAnalysis = async () => {
    setIsRunningNewAnalysis(true);
    setError(null);
    setAnalysisResults(null); // Clear previous results
    
    try {
      // NEW: Don't send any timeframe - let the backend use the default comprehensive range (2024-01-01 to today)
      const requestBody = {}; // Empty - will use 2024-01-01 to today automatically
      
      const response = await fetch('/api/conversion-probability/run-new-hierarchical-analysis', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

      const data = await response.json();
      
      if (data.success) {
        // Results are returned immediately since the new pipeline runs synchronously
        setSuccess(`✨ New hierarchical analysis completed! ${data.data.timeframe_description} - Processed ${data.data.total_combinations} segments with ${data.data.rollup_percentage.toFixed(1)}% rollup rate.`);
        
        // Load the results from the file (they were saved to latest_analysis.json)
        // We can either use the analysis_id or just load the latest results
        await loadLatestAnalysisResults();
        
        // Auto-expand hierarchy section
        setExpandedSections(prev => ({ ...prev, analysisHierarchy: true }));
        
      } else {
        setError(data.error || 'New hierarchical analysis failed');
      }
    } catch (err) {
      setError('Error running new hierarchical analysis: ' + err.message);
    } finally {
      setIsRunningNewAnalysis(false);
    }
  };

  // Helper function to load latest analysis results (for new pipeline)
  const loadLatestAnalysisResults = async () => {
    try {
      // Since the new pipeline saves to latest_analysis.json, we can try to load it directly
      // or use a generic endpoint that loads the latest
      const response = await fetch(`/api/conversion-probability/results/latest`);
      let data = await response.json();
      
      if (!data.success) {
        // Fallback: get available analyses and load the most recent one
        const analysesResponse = await fetch('/api/conversion-probability/analyses');
        const analysesData = await analysesResponse.json();
        
        if (analysesData.success && analysesData.data.files.length > 0) {
          // Get the most recent analysis
          const mostRecentAnalysis = analysesData.data.files[0]; // Assumes they're sorted by date
          data = await (await fetch(`/api/conversion-probability/results/${mostRecentAnalysis.analysis_id}`)).json();
        }
      }
      
      if (data.success) {
        setAnalysisResults(data.data);
        loadAvailableAnalyses();
      }
    } catch (err) {
      console.error('Error loading latest analysis results:', err);
      // Try alternative approach - reload the page to pick up the new file
      setTimeout(() => {
        window.location.reload();
      }, 1000);
    }
  };

  const toggleSection = (section) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  return {
    // State
    config,
    currentAnalysis,
    analysisProgress,
    analysisResults,
    isAnalyzing,
    isRunningNewAnalysis, // NEW
    propertyAnalysis,
    isAnalyzingProperties,
    error,
    success,
    expandedSections,
    availableAnalyses,

    // Actions
    setConfig,
    setError,
    setSuccess,
    analyzeProperties,
    startAnalysis,
    runNewHierarchicalAnalysis, // NEW
    toggleSection
  };
}; 