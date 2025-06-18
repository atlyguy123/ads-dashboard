import { useState, useCallback, useEffect } from 'react';

/**
 * Custom hook for managing debug mode functionality
 */
export const useDebugMode = () => {
    const DEBUG_STORAGE_KEY = 'cohort_pipeline_v3_debug_mode';
    
    // Function to load saved debug state from localStorage
    const loadSavedDebugState = () => {
        try {
            const saved = localStorage.getItem(DEBUG_STORAGE_KEY);
            if (saved) {
                return JSON.parse(saved);
            }
        } catch (error) {
            console.warn('Failed to load saved debug state:', error);
        }
        return {
            debugMode: false,
            debugStage: null,
            stageHistory: []
        };
    };

    // Function to save debug state to localStorage
    const saveDebugState = (debugMode, debugStage, stageHistory) => {
        try {
            const stateToSave = {
                debugMode,
                debugStage,
                stageHistory
            };
            localStorage.setItem(DEBUG_STORAGE_KEY, JSON.stringify(stateToSave));
        } catch (error) {
            console.warn('Failed to save debug state:', error);
        }
    };

    // Initialize state with saved values
    const savedState = loadSavedDebugState();
    const [debugMode, setDebugMode] = useState(savedState.debugMode);
    const [debugStage, setDebugStage] = useState(savedState.debugStage);
    const [stageHistory, setStageHistory] = useState(savedState.stageHistory);

    // Save state whenever it changes
    useEffect(() => {
        saveDebugState(debugMode, debugStage, stageHistory);
    }, [debugMode, debugStage, stageHistory]);

    /**
     * Available pipeline stages for debug mode
     */
    const availableStages = [
        {
            id: 'stage1',
            name: 'Cohort Identification',
            description: 'Take ad set/campaign ID and return corresponding user cohort with properties',
            order: 1,
        },
        {
            id: 'stage2',
            name: 'User-to-Segment Matching',
            description: 'Connect users to conversion probabilities using pre-calculated segment data',
            order: 2,
        },
        {
            id: 'stage3',
            name: 'Revenue Timeline Generation',
            description: 'Generate revenue timelines using conversion rates and refund rates',
            order: 3,
        },
    ];

    /**
     * Toggle debug mode on/off
     */
    const toggleDebugMode = useCallback(() => {
        setDebugMode(prev => {
            const newMode = !prev;
            if (!newMode) {
                // Reset debug stage when turning off debug mode
                setDebugStage(null);
                setStageHistory([]);
            }
            return newMode;
        });
    }, []);

    /**
     * Set debug mode explicitly
     */
    const setDebugModeExplicit = useCallback((enabled) => {
        setDebugMode(enabled);
        if (!enabled) {
            setDebugStage(null);
            setStageHistory([]);
        }
    }, []);

    /**
     * Set the current debug stage
     */
    const setDebugStageExplicit = useCallback((stage) => {
        setDebugStage(stage);
        
        // Add to stage history if not already present
        if (stage && !stageHistory.includes(stage)) {
            setStageHistory(prev => [...prev, stage]);
        }
    }, [stageHistory]);

    /**
     * Get stage information by ID
     */
    const getStageInfo = useCallback((stageId) => {
        return availableStages.find(stage => stage.id === stageId);
    }, []);

    /**
     * Get the next stage in the pipeline
     */
    const getNextStage = useCallback((currentStageId) => {
        const currentStage = getStageInfo(currentStageId);
        if (!currentStage) return null;
        
        return availableStages.find(stage => stage.order === currentStage.order + 1);
    }, [getStageInfo]);

    /**
     * Get the previous stage in the pipeline
     */
    const getPreviousStage = useCallback((currentStageId) => {
        const currentStage = getStageInfo(currentStageId);
        if (!currentStage) return null;
        
        return availableStages.find(stage => stage.order === currentStage.order - 1);
    }, [getStageInfo]);

    /**
     * Navigate to next stage
     */
    const goToNextStage = useCallback(() => {
        const nextStage = getNextStage(debugStage);
        if (nextStage) {
            setDebugStageExplicit(nextStage.id);
        }
    }, [debugStage, getNextStage, setDebugStageExplicit]);

    /**
     * Navigate to previous stage
     */
    const goToPreviousStage = useCallback(() => {
        const previousStage = getPreviousStage(debugStage);
        if (previousStage) {
            setDebugStageExplicit(previousStage.id);
        }
    }, [debugStage, getPreviousStage, setDebugStageExplicit]);

    /**
     * Reset debug state
     */
    const resetDebugState = useCallback(() => {
        setDebugMode(false);
        setDebugStage(null);
        setStageHistory([]);
    }, []);

    /**
     * Check if a stage has been executed
     */
    const isStageExecuted = useCallback((stageId) => {
        return stageHistory.includes(stageId);
    }, [stageHistory]);

    /**
     * Get stages that can be executed (current stage and earlier)
     */
    const getExecutableStages = useCallback(() => {
        if (!debugStage) return availableStages;
        
        const currentStage = getStageInfo(debugStage);
        if (!currentStage) return availableStages;
        
        return availableStages.filter(stage => stage.order <= currentStage.order);
    }, [debugStage, getStageInfo]);

    /**
     * Get debug mode status summary
     */
    const getDebugStatus = useCallback(() => {
        return {
            enabled: debugMode,
            currentStage: debugStage,
            currentStageInfo: debugStage ? getStageInfo(debugStage) : null,
            stagesExecuted: stageHistory.length,
            totalStages: availableStages.length,
            canGoNext: !!getNextStage(debugStage),
            canGoPrevious: !!getPreviousStage(debugStage),
            progress: debugStage ? 
                (getStageInfo(debugStage)?.order || 0) / availableStages.length * 100 : 0,
        };
    }, [debugMode, debugStage, stageHistory, getStageInfo, getNextStage, getPreviousStage]);

    return {
        // State
        debugMode,
        debugStage,
        stageHistory,
        availableStages,

        // Actions
        setDebugMode: setDebugModeExplicit,
        setDebugStage: setDebugStageExplicit,
        toggleDebugMode,
        goToNextStage,
        goToPreviousStage,
        resetDebugState,

        // Computed values
        currentStageInfo: debugStage ? getStageInfo(debugStage) : null,
        nextStage: getNextStage(debugStage),
        previousStage: getPreviousStage(debugStage),
        executableStages: getExecutableStages(),
        debugStatus: getDebugStatus(),

        // Utility functions
        getStageInfo,
        isStageExecuted,
    };
}; 