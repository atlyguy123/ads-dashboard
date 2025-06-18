import React, { useState, useEffect } from 'react';
import { ChevronDown, ChevronRight, Search, RefreshCw, AlertCircle, CheckCircle, Info, X, Calendar, Users, AlertTriangle, BarChart3, Globe, Package, TrendingUp, Target } from 'lucide-react';

const ConversionProbabilityPage = () => {
  // Timeframe options
  const timeframeOptions = [
    { value: 'all', label: 'All Time', description: 'Analyze all historical data' },
    { value: '1month', label: 'Last 1 Month', description: 'Last 30 days' },
    { value: '3months', label: 'Last 3 Months', description: 'Last 90 days' },
    { value: '6months', label: 'Last 6 Months', description: 'Last 180 days' },
    { value: '1year', label: 'Last 1 Year', description: 'Last 365 days' }
  ];

  // Helper function to calculate date range from timeframe
  const getDateRangeFromTimeframe = (timeframe) => {
    if (timeframe === 'all') {
      return { start: null, end: null };
    }
    
    const end = new Date();
    const start = new Date();
    
    switch (timeframe) {
      case '1month':
        start.setDate(start.getDate() - 30);
        break;
      case '3months':
        start.setDate(start.getDate() - 90);
        break;
      case '6months':
        start.setDate(start.getDate() - 180);
        break;
      case '1year':
        start.setDate(start.getDate() - 365);
        break;
      default:
        return { start: null, end: null };
    }
    
    return {
      start: start.toISOString().split('T')[0],
      end: end.toISOString().split('T')[0]
    };
  };

  // Load saved state from localStorage
  const loadSavedState = () => {
    try {
      const savedConfig = localStorage.getItem('conversionProbability_config');
      const savedSearchFilters = localStorage.getItem('conversionProbability_searchFilters');
      const savedExpandedSections = localStorage.getItem('conversionProbability_expandedSections');
      
      return {
        config: savedConfig ? JSON.parse(savedConfig) : {
          timeframe: 'all',
          min_cohort_size: 50,
          force_recalculate: false,
          min_price_samples: 100
        },
        searchFilters: savedSearchFilters ? JSON.parse(savedSearchFilters) : {
          product_id: '',
          region: '',
          country: '',
          app_store: '',
          min_cohort_size: '',
          sort_by: 'trial_conversion_rate',
          sort_order: 'desc',
          limit: 100
        },
        expandedSections: savedExpandedSections ? JSON.parse(savedExpandedSections) : {
          propertyAnalysis: false,
          searchResults: true
        }
      };
    } catch (err) {
      console.error('Error loading saved state:', err);
      return {
        config: {
          timeframe: 'all',
          min_cohort_size: 50,
          force_recalculate: false,
          min_price_samples: 100
        },
        searchFilters: {
          product_id: '',
          region: '',
          country: '',
          app_store: '',
          min_cohort_size: '',
          sort_by: 'trial_conversion_rate',
          sort_order: 'desc',
          limit: 100
        },
        expandedSections: {
          propertyAnalysis: false,
          searchResults: true
        }
      };
    }
  };

  // Initialize state with saved values
  const savedState = loadSavedState();
  
  // Analysis configuration state
  const [config, setConfig] = useState({
    ...savedState.config,
    min_price_samples: savedState.config.min_price_samples || 100  // Default to 100 samples
  });

  // Analysis state
  const [currentAnalysis, setCurrentAnalysis] = useState(null);
  const [analysisProgress, setAnalysisProgress] = useState(null);
  const [analysisResults, setAnalysisResults] = useState(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  // Property analysis state
  const [propertyAnalysis, setPropertyAnalysis] = useState(null);
  const [isAnalyzingProperties, setIsAnalyzingProperties] = useState(false);

  // Search state
  const [searchFilters, setSearchFilters] = useState(savedState.searchFilters);
  const [searchResults, setSearchResults] = useState(null);
  const [isSearching, setIsSearching] = useState(false);

  // UI state
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [showMethodologyDialog, setShowMethodologyDialog] = useState(false);
  const [expandedSections, setExpandedSections] = useState(savedState.expandedSections);
  
  // NEW: Rollup modal state
  const [showRollupModal, setShowRollupModal] = useState(false);
  const [rollupModalData, setRollupModalData] = useState(null);

  // Available analyses
  const [availableAnalyses, setAvailableAnalyses] = useState([]);

  // Save state to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('conversionProbability_config', JSON.stringify(config));
  }, [config]);

  useEffect(() => {
    localStorage.setItem('conversionProbability_searchFilters', JSON.stringify(searchFilters));
  }, [searchFilters]);

  useEffect(() => {
    localStorage.setItem('conversionProbability_expandedSections', JSON.stringify(expandedSections));
  }, [expandedSections]);

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
        // Auto-minimize Step 2 after completion
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
      
      // Prepare the request body with existing property analysis results
      const requestBody = {
        timeframe_start: dateRange.start,
        timeframe_end: dateRange.end,
        min_cohort_size: config.min_cohort_size,
        force_recalculate: config.force_recalculate
      };
      
      // If we have property analysis results, include them to avoid re-running property analysis
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

  const loadAnalysisResults = async (analysisId) => {
    try {
      const response = await fetch(`/api/conversion-probability/results/${analysisId}`);
      const data = await response.json();
      
      if (data.success) {
        setAnalysisResults(data.data);
        loadAvailableAnalyses(); // Refresh the list
      } else {
        setError(data.error || 'Failed to load analysis results');
      }
    } catch (err) {
      setError('Error loading analysis results: ' + err.message);
    }
  };

  const searchConversionProbabilities = async () => {
    setIsSearching(true);
    setError(null);
    
    try {
      const filters = Object.fromEntries(
        Object.entries(searchFilters).filter(([key, value]) => value !== '' && value !== null)
      );

      const response = await fetch('/api/conversion-probability/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(filters),
      });

      const data = await response.json();
      
      if (data.success) {
        setSearchResults(data.data);
        setExpandedSections(prev => ({ ...prev, searchResults: true }));
      } else {
        setError(data.error || 'Search failed');
      }
    } catch (err) {
      setError('Error searching: ' + err.message);
    } finally {
      setIsSearching(false);
    }
  };

  const formatPercentage = (value) => {
    return `${(value * 100).toFixed(2)}%`;
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString();
  };

  const formatAppStore = (store) => {
    const storeMap = {
      'APP_STORE': 'App Store',
      'PLAY_STORE': 'Play Store', 
      'STRIPE': 'Stripe'
    };
    return storeMap[store] || store;
  };

  const toggleSection = (section) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };
  
  // NEW: Function to open rollup modal with data
  const openRollupModal = (rollupData) => {
    setRollupModalData(rollupData);
    setShowRollupModal(true);
  };
  
  // NEW: Helper to get rollup information from search results
  const getRollupInformation = (searchResults) => {
    console.log('getRollupInformation called with search results:', searchResults);
    
    if (!searchResults || !searchResults.rollup_information) {
      console.log('No search results or rollup_information found');
      return null;
    }
    
    const rollupInfo = searchResults.rollup_information;
    console.log('Rollup information found:', rollupInfo);
    
    return {
      totalSegments: rollupInfo.total_segments || 0,
      rolledUpSegments: rollupInfo.rolled_up_segments?.length || 0,
      calculatedSegments: rollupInfo.calculated_segments?.length || 0,
      rollupTargets: rollupInfo.rollup_targets || {},
      segmentDetails: rollupInfo.segment_details || []
    };
  };

  // Get current timeframe option for display
  const currentTimeframeOption = timeframeOptions.find(option => option.value === config.timeframe) || timeframeOptions[0];

  // Helper function to generate status indicators with hover tooltips
  const getStatusIndicator = (passesThreshold, rollupInfo, level = 'segment', rollupTarget = null) => {
    if (passesThreshold) {
      return (
        <span className="px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
          ✅ PASSES
        </span>
      );
    }
    
    // FIXED: Show the FINAL rollup target consistently with better fallback handling
    let finalTarget = rollupTarget;
    
    // If no rollup target provided, try to extract from rollup info
    if (!finalTarget && rollupInfo) {
      if (rollupInfo.rollupReasons && rollupInfo.rollupReasons.length > 0) {
        finalTarget = rollupInfo.rollupReasons[0];
      } else if (rollupInfo.rollupTarget) {
        finalTarget = rollupInfo.rollupTarget;
      }
    }
    
    // FIXED: Always provide a fallback if no target is found
    if (!finalTarget || finalTarget.trim() === '') {
      return (
        <span className="px-2 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">
          ❌ INSUFFICIENT DATA
        </span>
      );
    }
    
    // FIXED: Normalize the final target to show simple, consistent names
    let displayTarget = finalTarget;
    
    // Normalize to simple names
    if (finalTarget.includes('Country-wide')) {
      displayTarget = finalTarget; // Keep country-wide as is
    } else if (finalTarget.includes('Tier 1') || finalTarget.includes('tier1')) {
      displayTarget = 'Tier 1';
    } else if (finalTarget.includes('Tier 2') || finalTarget.includes('tier2')) {
      displayTarget = 'Tier 2';
    } else if (finalTarget.includes('Tier 3') || finalTarget.includes('tier3')) {
      displayTarget = 'Tier 3';
    } else if (finalTarget.includes('Tier 4') || finalTarget.includes('tier4')) {
      displayTarget = 'Tier 4';
    } else if (finalTarget.includes('$') && finalTarget.includes('-')) {
      // Price bucket format like "$95-$105"
      displayTarget = finalTarget;
    } else {
      // Default to showing the target as-is, but ensure it's not empty
      displayTarget = finalTarget || 'Unknown Target';
    }
    
    // Show FINAL rollup target
    const tooltipContent = rollupInfo ? (
      <div className="text-xs">
        <div className="font-medium mb-1">Rollup Details:</div>
        <div>• Final destination: {finalTarget}</div>
        {rollupInfo.finalUserCount && (
          <div>• Final user count: {rollupInfo.finalUserCount.toLocaleString()}</div>
        )}
        {rollupInfo.propertiesDropped && rollupInfo.propertiesDropped.length > 0 && (
          <div>• Properties dropped: {rollupInfo.propertiesDropped.join(', ')}</div>
        )}
        {rollupInfo.rollupReasons && rollupInfo.rollupReasons.length > 0 && (
          <div>• Reason: {rollupInfo.rollupReasons[0]}</div>
        )}
      </div>
    ) : null;
    
    return (
      <div className="relative group">
        <span className="px-2 py-1 rounded-full text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200 cursor-help">
          → {displayTarget}
        </span>
        
        {/* Hover tooltip */}
        {tooltipContent && (
          <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900 text-xs rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-10 whitespace-nowrap max-w-xs">
            {tooltipContent}
            <div className="absolute top-full left-1/2 transform -translate-x-1/2 border-4 border-transparent border-t-gray-900 dark:border-t-gray-100"></div>
          </div>
        )}
      </div>
    );
  };

  const buildHierarchicalStructure = (segments, minCohortSize) => {
    // Economic tier mappings (updated to match backend with all 203 countries)
    const economicTiers = {
      // Tier 1: High-income developed countries (GDP per capita > $40,000)
      'tier1_high_income': [
        // North America & Oceania
        'US', 'CA', 'AU', 'NZ',
        // Western Europe
        'GB', 'IE', 'DE', 'FR', 'NL', 'BE', 'AT', 'CH', 'LU', 
        'DK', 'SE', 'NO', 'FI', 'IS',
        // Other high-income
        'IL', 'SG', 'HK', 'QA', 'AE', 'KW', 'BH', 'OM', 'SA',
        // Small wealthy territories & microstates
        'MC', 'LI', 'AD', 'MT', 'CY', 'IM', 'JE', 'GG', 'GI', 'BM', 'SM',
        'KY', 'VI', 'AW', 'TC', 'VG', 'BQ', 'SX', 'MF', 'PM', 'AX', 'CW',
        // Overseas territories of wealthy countries
        'MQ', 'GP', 'RE', 'FO', 'GL'
      ],
      
      // Tier 2: Upper-middle income & developed countries ($15,000-$40,000)
      'tier2_upper_middle': [
        // East Asia
        'JP', 'KR', 'TW', 'MO',
        // Southern & Eastern Europe  
        'IT', 'ES', 'PT', 'GR', 'SI', 'CZ', 'SK', 'EE', 'LV', 'LT',
        'HR', 'HU', 'PL', 'RO', 'BG', 'MT',
        // Latin America (upper-middle)
        'UY', 'CR', 'PA', 'PR',
        // Other upper-middle
        'BN', 'MV', 'SC', 'MU', 'BB', 'BS', 'TT'
      ],
      
      // Tier 3: Emerging economies & lower-middle income ($4,000-$15,000)
      'tier3_emerging': [
        // Latin America
        'BR', 'MX', 'AR', 'CL', 'CO', 'PE', 'EC', 'DO', 'JM', 'PY', 'GT',
        'SV', 'HN', 'NI', 'BZ', 'VE', 'BO', 'GY', 'SR',
        // Asia-Pacific
        'CN', 'TH', 'MY', 'RU', 'TR', 'KZ', 'AZ', 'GE', 'AM', 'BY',
        'UZ', 'KG', 'TJ', 'MN', 'FJ', 'TO', 'VU', 'PG', 'TL',
        // Africa (emerging)
        'ZA', 'NA', 'BW', 'MU', 'SC',
        // Europe (emerging)
        'RS', 'ME', 'MK', 'AL', 'BA', 'MD', 'UA', 'XK',
        // Caribbean
        'LC', 'GD', 'KN', 'AG', 'DM'
      ],
      
      // Tier 4: Developing countries (GDP per capita < $4,000)
      'tier4_developing': [
        // South Asia
        'IN', 'PK', 'BD', 'LK', 'NP', 'BT', 'AF', 'MV',
        // Southeast Asia
        'ID', 'PH', 'VN', 'MM', 'KH', 'LA',
        // Middle East & North Africa
        'EG', 'MA', 'TN', 'DZ', 'LY', 'SD', 'LB', 'JO', 'PS', 'SY', 'IQ', 'IR', 'YE',
        // Sub-Saharan Africa
        'NG', 'KE', 'GH', 'TZ', 'UG', 'ET', 'RW', 'ZM', 'ZW', 'MW', 'MZ', 'AO',
        'CM', 'CI', 'SN', 'ML', 'BF', 'NE', 'TD', 'CF', 'CD', 'CG', 'GA', 'GQ',
        'ST', 'CV', 'GM', 'GW', 'SL', 'LR', 'BJ', 'TG', 'GH', 'BI', 'DJ', 'SO',
        'ER', 'SS', 'MR', 'MG', 'KM', 'MU', 'SZ', 'LS',
        // Caribbean & Pacific (developing)
        'HT', 'CU', 'NC', 'PF', 'AS', 'GU', 'FM', 'MH', 'PW', 'NR', 'TV', 'KI',
        'SB', 'WS', 'ST', 'CV'
      ]
    };

    const getEconomicTier = (country) => {
      for (const [tier, countries] of Object.entries(economicTiers)) {
        if (countries.includes(country)) {
          return `Tier ${tier.split('_')[0].replace('tier', '')}`;
        }
      }
      return 'Tier 4';
    };

    const hierarchy = {};

    // Group by Product + Store + Price (Level 1)
    segments.forEach(segment => {
      const productId = segment.product_id;
      const appStore = segment.app_store || 'Unknown Store';
      const priceRange = segment.price_bucket;
      const country = segment.country || 'Unknown';
      const region = segment.region;
      const users = segment.user_count;
      
      // Use the backend's rollup information directly
      const hasSufficientData = segment.has_sufficient_data;
      const propertiesDropped = segment.properties_dropped || [];
      const rollupDescription = segment.rollup_description || '';
      const rollupTarget = segment.rollup_target || null; // FINAL rollup target from backend
      const finalUserCount = segment.final_user_count || users;
      const accuracyScore = segment.accuracy_score || 'very_high';
      
      const economicTier = getEconomicTier(country);

      // Initialize Product+Store+Price level (Level 1)
      const level1Key = `${productId}|${appStore}|${priceRange}`;
      if (!hierarchy[level1Key]) {
        hierarchy[level1Key] = {
          productId: productId,
          appStore: appStore,
          priceRange: priceRange,
          totalUsers: 0,
          passesThreshold: false,
          rollupInfo: {
            hasRollups: false,
            rolledUpSegments: 0,
            totalSegments: 0,
            rollupReasons: new Set(),
            propertiesDropped: new Set(),
            finalUserCount: 0,
            rollupTarget: null
          },
          economicTiers: {}
        };
      }
      hierarchy[level1Key].totalUsers += users;
      hierarchy[level1Key].rollupInfo.totalSegments += 1;
      hierarchy[level1Key].rollupInfo.finalUserCount += finalUserCount;
      
      if (!hasSufficientData || propertiesDropped.length > 0) {
        hierarchy[level1Key].rollupInfo.hasRollups = true;
        hierarchy[level1Key].rollupInfo.rolledUpSegments += 1;
        if (rollupDescription) hierarchy[level1Key].rollupInfo.rollupReasons.add(rollupDescription);
        if (rollupTarget) hierarchy[level1Key].rollupInfo.rollupTarget = rollupTarget;
        propertiesDropped.forEach(prop => hierarchy[level1Key].rollupInfo.propertiesDropped.add(prop));
      }

      // Initialize economic tier level (Level 2)
      if (!hierarchy[level1Key].economicTiers[economicTier]) {
        hierarchy[level1Key].economicTiers[economicTier] = {
          totalUsers: 0,
          passesThreshold: false,
          rollupInfo: {
            hasRollups: false,
            rolledUpSegments: 0,
            totalSegments: 0,
            rollupReasons: new Set(),
            propertiesDropped: new Set(),
            finalUserCount: 0,
            rollupTarget: null
          },
          countries: {}
        };
      }
      hierarchy[level1Key].economicTiers[economicTier].totalUsers += users;
      hierarchy[level1Key].economicTiers[economicTier].rollupInfo.totalSegments += 1;
      hierarchy[level1Key].economicTiers[economicTier].rollupInfo.finalUserCount += finalUserCount;
      
      if (!hasSufficientData || propertiesDropped.length > 0) {
        hierarchy[level1Key].economicTiers[economicTier].rollupInfo.hasRollups = true;
        hierarchy[level1Key].economicTiers[economicTier].rollupInfo.rolledUpSegments += 1;
        if (rollupDescription) hierarchy[level1Key].economicTiers[economicTier].rollupInfo.rollupReasons.add(rollupDescription);
        if (rollupTarget) hierarchy[level1Key].economicTiers[economicTier].rollupInfo.rollupTarget = rollupTarget;
        propertiesDropped.forEach(prop => hierarchy[level1Key].economicTiers[economicTier].rollupInfo.propertiesDropped.add(prop));
      }

      // Initialize country level (Level 3)
      if (!hierarchy[level1Key].economicTiers[economicTier].countries[country]) {
        hierarchy[level1Key].economicTiers[economicTier].countries[country] = {
          totalUsers: 0,
          passesThreshold: false,
          rollupInfo: {
            hasRollups: false,
            rolledUpSegments: 0,
            totalSegments: 0,
            rollupReasons: new Set(),
            propertiesDropped: new Set(),
            finalUserCount: 0,
            rollupTarget: null
          },
          regions: []
        };
      }
      hierarchy[level1Key].economicTiers[economicTier].countries[country].totalUsers += users;
      hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.totalSegments += 1;
      hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.finalUserCount += finalUserCount;
      
      if (!hasSufficientData || propertiesDropped.length > 0) {
        hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.hasRollups = true;
        hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.rolledUpSegments += 1;
        if (rollupDescription) hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.rollupReasons.add(rollupDescription);
        if (rollupTarget) hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.rollupTarget = rollupTarget;
        propertiesDropped.forEach(prop => hierarchy[level1Key].economicTiers[economicTier].countries[country].rollupInfo.propertiesDropped.add(prop));
      }

      // Add region if it exists (Level 4)
      if (region) {
        hierarchy[level1Key].economicTiers[economicTier].countries[country].regions.push({
          name: region,
          users: users,
          passes: hasSufficientData && propertiesDropped.length === 0,
          rollupTarget: rollupTarget,
          rollupReason: rollupDescription,
          accuracyScore: accuracyScore,
          finalUserCount: finalUserCount,
          originalUserCount: users,
          propertiesDropped: propertiesDropped
        });
      }
    });

    // Calculate pass/fail status for each level based on aggregated data
    Object.keys(hierarchy).forEach(level1Key => {
      const level1Data = hierarchy[level1Key];
      level1Data.passesThreshold = level1Data.totalUsers >= minCohortSize;
      
      // Convert Sets to Arrays for easier use in UI
      level1Data.rollupInfo.rollupReasons = Array.from(level1Data.rollupInfo.rollupReasons);
      level1Data.rollupInfo.propertiesDropped = Array.from(level1Data.rollupInfo.propertiesDropped);

      Object.keys(level1Data.economicTiers).forEach(tierName => {
        const tierData = level1Data.economicTiers[tierName];
        tierData.passesThreshold = tierData.totalUsers >= minCohortSize;
        
        // Convert Sets to Arrays for easier use in UI
        tierData.rollupInfo.rollupReasons = Array.from(tierData.rollupInfo.rollupReasons);
        tierData.rollupInfo.propertiesDropped = Array.from(tierData.rollupInfo.propertiesDropped);

        Object.keys(tierData.countries).forEach(country => {
          const countryData = tierData.countries[country];
          countryData.passesThreshold = countryData.totalUsers >= minCohortSize;
          
          // Convert Sets to Arrays for easier use in UI
          countryData.rollupInfo.rollupReasons = Array.from(countryData.rollupInfo.rollupReasons);
          countryData.rollupInfo.propertiesDropped = Array.from(countryData.rollupInfo.propertiesDropped);
        });
      });
    });

    return hierarchy;
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Conversion Probability Analysis
        </h1>
        <p className="text-gray-600 dark:text-gray-400 mb-4">
          Analyze conversion probabilities for different user cohorts based on product, price, region, country, and app store properties.
        </p>
        
        {/* Workflow Guide */}
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
            <Info className="h-4 w-4 mr-2" />
            How This Analysis Works
          </h3>
          <div className="text-sm text-blue-800 dark:text-blue-300 space-y-1">
            <p><strong>Step 1:</strong> Click "Discover Data Structure" to analyze your data and understand what products, price ranges, and user segments exist</p>
            <p><strong>Step 2:</strong> Review the discovered data structure to understand how many combinations will be analyzed</p>
            <p><strong>Step 3:</strong> Click "Run Conversion Analysis" to calculate conversion rates for each user segment</p>
            <p><strong>Step 4:</strong> Search and explore the results to find conversion patterns and insights</p>
          </div>
        </div>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="mb-6 bg-red-50 dark:bg-red-900/50 border border-red-200 dark:border-red-800 rounded-lg p-4">
          <div className="flex items-center">
            <AlertCircle className="h-5 w-5 text-red-400 mr-3" />
            <div className="flex-1">
              <p className="text-red-800 dark:text-red-200">{error}</p>
            </div>
            <button
              onClick={() => setError(null)}
              className="text-red-400 hover:text-red-600"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>
      )}

      {/* Success Alert */}
      {success && (
        <div className="mb-6 bg-green-50 dark:bg-green-900/50 border border-green-200 dark:border-green-800 rounded-lg p-4">
          <div className="flex items-center">
            <CheckCircle className="h-5 w-5 text-green-400 mr-3" />
            <div className="flex-1">
              <p className="text-green-800 dark:text-green-200">{success}</p>
            </div>
            <button
              onClick={() => setSuccess(null)}
              className="text-green-400 hover:text-green-600"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>
      )}

      {/* Getting Started Guide - shown when no analysis has been run */}
      {!propertyAnalysis && !analysisResults && (
        <div className="bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-900/20 dark:to-blue-900/20 border border-indigo-200 dark:border-indigo-800 rounded-lg p-6 mb-6">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <div className="flex items-center justify-center h-12 w-12 rounded-md bg-indigo-500 text-white">
                <BarChart3 className="h-6 w-6" />
              </div>
            </div>
            <div className="ml-4 flex-1">
              <h3 className="text-lg font-semibold text-indigo-900 dark:text-indigo-200 mb-2">
                Welcome to Conversion Probability Analysis
              </h3>
              <p className="text-indigo-800 dark:text-indigo-300 mb-4">
                This powerful tool analyzes your user data to identify conversion patterns across different user segments. 
                You'll discover how conversion rates vary by product, price range, geographic location, and app store.
              </p>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                <div className="p-3 bg-white dark:bg-indigo-900/30 rounded-lg border border-indigo-200 dark:border-indigo-700">
                  <h4 className="font-medium text-indigo-900 dark:text-indigo-200 mb-2">What You'll Discover:</h4>
                  <ul className="text-sm text-indigo-800 dark:text-indigo-300 space-y-1">
                    <li>• Trial-to-paid conversion rates by segment</li>
                    <li>• Refund patterns across different user groups</li>
                    <li>• Geographic and pricing insights</li>
                    <li>• Statistical confidence for all metrics</li>
                  </ul>
                </div>
                
                <div className="p-3 bg-white dark:bg-indigo-900/30 rounded-lg border border-indigo-200 dark:border-indigo-700">
                  <h4 className="font-medium text-indigo-900 dark:text-indigo-200 mb-2">How It Works:</h4>
                  <ul className="text-sm text-indigo-800 dark:text-indigo-300 space-y-1">
                    <li>• Automatically groups products by price ranges</li>
                    <li>• Creates user segments by location and store</li>
                    <li>• Calculates reliable conversion statistics</li>
                    <li>• Provides searchable, actionable insights</li>
                  </ul>
                </div>
              </div>
              
              <div className="flex items-center p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
                <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400 mr-2 flex-shrink-0" />
                <div className="text-sm text-yellow-800 dark:text-yellow-200">
                  <strong>Ready to start?</strong> Configure your analysis parameters below, then click "Discover Data Structure" to begin.
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Analysis Configuration */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
        <div className="p-6">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-2">
            Step 1: Configure Analysis Parameters
          </h2>
          <p className="text-gray-600 dark:text-gray-400 mb-6">
            Set the time period and minimum group size for your analysis. These settings determine which data to analyze and how to group users.
          </p>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                <Calendar className="inline h-4 w-4 mr-1" />
                Time Period
              </label>
              <select
                value={config.timeframe}
                onChange={(e) => setConfig(prev => ({ ...prev, timeframe: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
              >
                {timeframeOptions.map(option => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                {currentTimeframeOption.description}
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                <Users className="inline h-4 w-4 mr-1" />
                Minimum Group Size
              </label>
              <input
                type="number"
                min="10"
                max="1000"
                value={config.min_cohort_size}
                onChange={(e) => setConfig(prev => ({ ...prev, min_cohort_size: parseInt(e.target.value) || 50 }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Minimum number of users required in each group for reliable statistics
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                <BarChart3 className="inline h-4 w-4 mr-1" />
                Minimum Price Samples
              </label>
              <input
                type="number"
                min="10"
                max="1000"
                value={config.min_price_samples}
                onChange={(e) => setConfig(prev => ({ ...prev, min_price_samples: parseInt(e.target.value) || 100 }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Minimum price samples required per product for reliable price bucketing
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-1 gap-6 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                <RefreshCw className="inline h-4 w-4 mr-1" />
                Force Recalculation
              </label>
              <div className="flex items-center mt-2">
                <input
                  type="checkbox"
                  checked={config.force_recalculate}
                  onChange={(e) => setConfig(prev => ({ ...prev, force_recalculate: e.target.checked }))}
                  className="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
                />
                <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                  Recalculate even if cached results exist
                </span>
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Check this to ignore previously calculated results and run fresh analysis
              </p>
            </div>
          </div>

          <div className="flex gap-3">
            <button
              onClick={analyzeProperties}
              disabled={isAnalyzingProperties}
              className="inline-flex items-center px-6 py-3 border border-indigo-300 dark:border-indigo-600 rounded-md shadow-sm text-sm font-medium text-indigo-700 dark:text-indigo-300 bg-indigo-50 dark:bg-indigo-900/30 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isAnalyzingProperties ? (
                <RefreshCw className="animate-spin h-5 w-5 mr-2" />
              ) : (
                <Search className="h-5 w-5 mr-2" />
              )}
              {isAnalyzingProperties ? 'Discovering...' : 'Discover Data Structure'}
            </button>
            
            <button
              onClick={startAnalysis}
              disabled={isAnalyzing || isAnalyzingProperties || !propertyAnalysis}
              className="inline-flex items-center px-6 py-3 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isAnalyzing ? (
                <RefreshCw className="animate-spin h-5 w-5 mr-2" />
              ) : (
                <BarChart3 className="h-5 w-5 mr-2" />
              )}
              {isAnalyzing ? 'Analyzing...' : 'Run Conversion Analysis'}
            </button>
          </div>
          
          {!propertyAnalysis && (
            <div className="mt-4 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-md">
              <div className="flex items-center">
                <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400 mr-2" />
                <p className="text-sm text-yellow-800 dark:text-yellow-200">
                  <strong>Next Step:</strong> Click "Discover Data Structure" first to analyze your data and understand what will be included in the conversion analysis.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Property Analysis Results */}
      {propertyAnalysis && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
          <div className="p-4 border-b border-gray-200 dark:border-gray-700">
            <button
              onClick={() => toggleSection('propertyAnalysis')}
              className="flex items-center justify-between w-full text-left"
            >
              <div className="flex items-center">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  Step 2: Data Structure Discovery Results
                </h3>
                <span className="ml-3 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                  ✓ Complete
                </span>
              </div>
              {expandedSections.propertyAnalysis ? (
                <ChevronDown className="h-5 w-5 text-gray-500" />
              ) : (
                <ChevronRight className="h-5 w-5 text-gray-500" />
              )}
            </button>
          </div>
          
          {expandedSections.propertyAnalysis && (
            <div className="p-6">
              <div className="mb-6 p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
                <h4 className="text-sm font-semibold text-green-900 dark:text-green-200 mb-2 flex items-center">
                  <CheckCircle className="h-4 w-4 mr-2" />
                  What This Analysis Discovered
                </h4>
                <p className="text-sm text-green-800 dark:text-green-300 mb-3">
                  We analyzed your actual users and grouped them into meaningful segments. Here's what we found:
                </p>
                <ul className="text-sm text-green-800 dark:text-green-300 space-y-1 list-disc list-inside">
                  <li><strong>{propertyAnalysis.products_analyzed} products</strong> with sufficient data for analysis (at least {propertyAnalysis.min_price_samples_used || 100} price samples each)</li>
                  <li><strong>{propertyAnalysis.total_combinations.toLocaleString()} viable user segments</strong> based on actual user data</li>
                  <li><strong>{propertyAnalysis.user_analysis?.total_users_analyzed?.toLocaleString() || 0} total users</strong> analyzed across all segments</li>
                  <li><strong>Geographic coverage:</strong> {propertyAnalysis.user_analysis?.total_countries_found || 0} countries and {propertyAnalysis.user_analysis?.total_regions_found || 0} regions with active users</li>
                  <li><strong>App stores:</strong> {propertyAnalysis.user_analysis?.app_stores?.length || 0} different stores</li>
                </ul>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
                <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                  <div className="text-3xl font-bold text-blue-600 dark:text-blue-400">
                    {propertyAnalysis.products_analyzed}
                  </div>
                  <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Products Found</div>
                  <div className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                    Products with enough data for reliable analysis
                  </div>
                </div>
                
                <div className="text-center p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                  <div className="text-3xl font-bold text-purple-600 dark:text-purple-400">
                    {Object.values(propertyAnalysis.price_buckets || {}).reduce((total, buckets) => total + buckets.length, 0)}
                  </div>
                  <div className="text-sm text-purple-800 dark:text-purple-300 font-medium">Price Groups</div>
                  <div className="text-xs text-purple-600 dark:text-purple-400 mt-1">
                    Intelligent price ranges created for analysis
                  </div>
                </div>
                
                <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                  <div className="text-3xl font-bold text-green-600 dark:text-green-400">
                    {propertyAnalysis.user_analysis?.total_countries_found || 0}
                  </div>
                  <div className="text-sm text-green-800 dark:text-green-300 font-medium">Countries</div>
                  <div className="text-xs text-green-600 dark:text-green-400 mt-1">
                    Countries with active users
                  </div>
                </div>
                
                <div className="text-center p-4 bg-orange-50 dark:bg-orange-900/20 rounded-lg">
                  <div className="text-3xl font-bold text-orange-600 dark:text-orange-400">
                    {propertyAnalysis.total_combinations.toLocaleString()}
                  </div>
                  <div className="text-sm text-orange-800 dark:text-orange-300 font-medium">User Segments</div>
                  <div className="text-xs text-orange-600 dark:text-orange-400 mt-1">
                    Viable segments found in your data
                  </div>
                </div>
              </div>
              
              {/* Geographic Market Overview */}
              {propertyAnalysis.user_analysis?.geographic_structure && (
                <div className="mb-6">
                  <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <Globe className="h-5 w-5 mr-2" />
                    Global Market Overview
                  </h4>
                  
                  {(() => {
                    const geoData = propertyAnalysis.user_analysis.geographic_structure;
                    const countries = Object.entries(geoData).map(([country, data]) => ({
                      name: country,
                      users: data.total_users,
                      regions: Object.keys(data.regions).length,
                      viableRegions: Object.values(data.regions).filter(r => r.user_count >= 50).length
                    })).sort((a, b) => b.users - a.users);
                    
                    const topCountries = countries.slice(0, 10);
                    const totalUsers = countries.reduce((sum, c) => sum + c.users, 0);
                    const totalRegions = countries.reduce((sum, c) => sum + c.regions, 0);
                    const viableRegions = countries.reduce((sum, c) => sum + c.viableRegions, 0);
                    
                    return (
                      <div className="space-y-4">
                        {/* Summary Stats */}
                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 p-4 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                          <div className="text-center">
                            <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{countries.length}</div>
                            <div className="text-sm text-blue-800 dark:text-blue-300">Total Countries</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{totalRegions}</div>
                            <div className="text-sm text-indigo-800 dark:text-indigo-300">Total Regions</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-green-600 dark:text-green-400">{viableRegions}</div>
                            <div className="text-sm text-green-800 dark:text-green-300">Viable Regions</div>
                            <div className="text-xs text-green-600 dark:text-green-400">≥50 users each</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">{Math.round((viableRegions / totalRegions) * 100)}%</div>
                            <div className="text-sm text-purple-800 dark:text-purple-300">Data Coverage</div>
                            <div className="text-xs text-purple-600 dark:text-purple-400">Regions with sufficient data</div>
                          </div>
                        </div>
                        
                        {/* Top Countries Table */}
                        <div className="bg-white dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
                          <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-600">
                            <h5 className="text-sm font-semibold text-gray-900 dark:text-white">Top Markets by User Count</h5>
                          </div>
                          <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                              <thead className="bg-gray-50 dark:bg-gray-800">
                                <tr>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Country</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Users</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Market Share</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Regions</th>
                                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Data Quality</th>
                                </tr>
                              </thead>
                              <tbody className="bg-white dark:bg-gray-700 divide-y divide-gray-200 dark:divide-gray-600">
                                {topCountries.map((country, idx) => (
                                  <tr key={country.name} className={idx % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                                    <td className="px-4 py-3 whitespace-nowrap">
                                      <div className="flex items-center">
                                        <span className="text-sm font-medium text-gray-900 dark:text-white">{country.name}</span>
                                        {idx < 3 && (
                                          <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                                            Top {idx + 1}
                                          </span>
                                        )}
                                      </div>
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-white font-medium">
                                      {country.users.toLocaleString()}
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap">
                                      <div className="flex items-center">
                                        <div className="w-16 bg-gray-200 dark:bg-gray-600 rounded-full h-2 mr-2">
                                          <div 
                                            className="bg-blue-600 h-2 rounded-full" 
                                            style={{ width: `${(country.users / totalUsers) * 100}%` }}
                                          ></div>
                                        </div>
                                        <span className="text-sm text-gray-600 dark:text-gray-400">
                                          {Math.round((country.users / totalUsers) * 100)}%
                                        </span>
                                      </div>
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                                      {country.regions} total
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap">
                                      <div className="flex items-center">
                                        <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                                          country.viableRegions / country.regions >= 0.5
                                            ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                                            : country.viableRegions / country.regions >= 0.25
                                            ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
                                            : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                                        }`}>
                                          {country.viableRegions}/{country.regions} viable
                                        </span>
                                      </div>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          {countries.length > 10 && (
                            <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-600 text-sm text-gray-500 dark:text-gray-400">
                              Showing top 10 of {countries.length} countries. {countries.length - 10} additional countries with smaller user bases.
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })()}
                </div>
              )}
              
              {/* Product Segment Analysis */}
              {propertyAnalysis.user_segments && (
                <div className="mb-6">
                  <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <Package className="h-5 w-5 mr-2" />
                    Product Segment Analysis - Hierarchical View
                  </h4>
                  
                  {(() => {
                    const productSegments = Object.entries(propertyAnalysis.user_segments);
                    const totalSegments = productSegments.reduce((sum, [_, segments]) => sum + segments.length, 0);
                    const viableSegments = productSegments.reduce((sum, [_, segments]) => 
                      sum + segments.filter(s => s.has_sufficient_data).length, 0);
                    
                    return (
                      <div className="space-y-6">
                        {/* Rollup Efficiency Summary */}
                        {propertyAnalysis.rollup_stats && (
                          <div className="p-4 bg-gradient-to-r from-green-50 to-emerald-50 dark:from-green-900/20 dark:to-emerald-900/20 rounded-lg border border-green-200 dark:border-green-800">
                            <h5 className="text-lg font-semibold text-green-900 dark:text-green-200 mb-3 flex items-center">
                              <TrendingUp className="h-5 w-5 mr-2" />
                              Intelligent Rollup Efficiency
                            </h5>
                            <div className="mb-3 text-sm text-green-800 dark:text-green-300">
                              <strong>How it works:</strong> When user segments have too few people for reliable statistics, 
                              we intelligently combine them with similar segments (same price range, similar economic regions) 
                              to reach the minimum group size of {config.min_cohort_size} users. This ensures all analysis 
                              results are statistically meaningful.
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                              <div className="text-center">
                                <div className="text-2xl font-bold text-gray-600 dark:text-gray-400">{propertyAnalysis.rollup_stats.total_segments}</div>
                                <div className="text-sm text-gray-700 dark:text-gray-300">Original Segments</div>
                              </div>
                              <div className="text-center">
                                <div className="text-2xl font-bold text-green-600 dark:text-green-400">{propertyAnalysis.rollup_stats.viable_segments}</div>
                                <div className="text-sm text-green-700 dark:text-green-300">Viable Segments</div>
                              </div>
                              <div className="text-center">
                                <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{propertyAnalysis.rollup_stats.rolled_up_segments}</div>
                                <div className="text-sm text-blue-700 dark:text-blue-300">Rolled Up</div>
                              </div>
                              <div className="text-center">
                                <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                                  {propertyAnalysis.rollup_stats.rolled_up_segments > 0 ? 
                                    `${((propertyAnalysis.rollup_stats.rolled_up_segments / propertyAnalysis.rollup_stats.total_segments) * 100).toFixed(1)}%` :
                                    '0%'
                                  }
                                </div>
                                <div className="text-sm text-purple-700 dark:text-purple-300">Segments Optimized</div>
                                <div className="text-xs text-purple-600 dark:text-purple-400 mt-1">
                                  {propertyAnalysis.rollup_stats.rolled_up_segments > 0 ? 
                                    `${propertyAnalysis.rollup_stats.rolled_up_segments} segments combined for efficiency` :
                                    'All segments had sufficient data'
                                  }
                                </div>
                              </div>
                            </div>
                            
                            {/* Accuracy Distribution */}
                            {propertyAnalysis.rollup_stats.accuracy_distribution && (
                              <div className="mt-4 pt-4 border-t border-green-200 dark:border-green-700">
                                <h6 className="text-sm font-medium text-green-800 dark:text-green-200 mb-2">Accuracy Score Distribution:</h6>
                                <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
                                  {/* Order accuracy scores from Very High to Very Low */}
                                  {['very_high', 'high', 'medium', 'low', 'very_low'].map(score => {
                                    const count = propertyAnalysis.rollup_stats.accuracy_distribution[score] || 0;
                                    return (
                                      <div key={score} className="text-center p-2 bg-white dark:bg-gray-800 rounded border">
                                        <div className="font-semibold text-gray-900 dark:text-white">{count}</div>
                                        <div className="text-gray-600 dark:text-gray-400 capitalize">{score.replace('_', ' ')}</div>
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                        
                        {/* Summary */}
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4 bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-900/20 dark:to-pink-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
                          <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">{productSegments.length}</div>
                            <div className="text-sm text-purple-800 dark:text-purple-300">Products Analyzed</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-pink-600 dark:text-pink-400">{totalSegments}</div>
                            <div className="text-sm text-pink-800 dark:text-pink-300">Total Segments</div>
                          </div>
                          <div className="text-center">
                            <div className="text-2xl font-bold text-green-600 dark:text-green-400">{viableSegments}</div>
                            <div className="text-sm text-green-800 dark:text-green-300">Ready for Analysis</div>
                            <div className="text-xs text-green-600 dark:text-green-400">≥{config.min_cohort_size} users each</div>
                          </div>
                        </div>
                        
                        {/* Hierarchical Tree View for each Product */}
                        <div className="space-y-8">
                          {/* Legend */}
                          <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg border border-gray-200 dark:border-gray-600">
                            <h5 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">Understanding the Hierarchy View</h5>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                              <div>
                                <h6 className="font-medium text-gray-800 dark:text-gray-200 mb-2">Status Indicators:</h6>
                                <div className="space-y-1">
                                  <div className="flex items-center">
                                    <span className="px-1 py-0.5 rounded text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 mr-2">✅ PASSES</span>
                                    <span className="text-gray-600 dark:text-gray-400">Has enough users (≥{config.min_cohort_size}) for reliable analysis</span>
                                  </div>
                                  <div className="flex items-center">
                                    <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 mr-2">⚠️ HAS ROLLUPS</span>
                                    <span className="text-gray-600 dark:text-gray-400">Contains segments that roll up to broader categories (hover for details)</span>
                                  </div>
                                  <div className="flex items-center">
                                    <span className="px-1 py-0.5 rounded text-xs bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300 mr-2">❌ COMBINES</span>
                                    <span className="text-gray-600 dark:text-gray-400">Entire level needs to combine with others (hover for details)</span>
                                  </div>
                                  <div className="flex items-center">
                                    <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 mr-2">→ Target</span>
                                    <span className="text-gray-600 dark:text-gray-400">Rollup target destination (hover for details)</span>
                                  </div>
                                </div>
                              </div>
                              <div>
                                <h6 className="font-medium text-gray-800 dark:text-gray-200 mb-2">4-Level Hierarchy & Rollup Strategy:</h6>
                                <div className="space-y-1 text-gray-600 dark:text-gray-400">
                                  <div>1. <strong>🏢📱💰 Product+Store+Price</strong> (Level 1)</div>
                                  <div>2. <strong>🌍 Economic Tier</strong> (+ Economic grouping)</div>
                                  <div>3. <strong>🏁 Country</strong> (+ Specific country)</div>
                                  <div>4. <strong>📍 Region</strong> (+ Regional detail)</div>
                                  <div className="mt-2 pt-2 border-t border-gray-300 dark:border-gray-600">
                                    <strong>Rollup ceiling:</strong> Cannot roll up beyond Product+Store+Price level. Even if Level 1 fails, everything rolls up to that specific Product+Store+Price combination.
                                  </div>
                                  <div className="mt-2 text-xs italic">💡 Hover over any status indicator for detailed rollup information</div>
                                </div>
                              </div>
                            </div>
                          </div>
                          
                          {productSegments.map(([productId, segments]) => {
                            // Build hierarchical structure
                            const hierarchy = buildHierarchicalStructure(segments, config.min_cohort_size);
                            const priceBuckets = propertyAnalysis.price_buckets?.[productId] || [];
                            
                            return (
                              <div key={productId} className="bg-white dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
                                {/* Product Header */}
                                <div className="px-6 py-4 bg-gradient-to-r from-indigo-50 to-blue-50 dark:from-indigo-900/20 dark:to-blue-900/20 border-b border-gray-200 dark:border-gray-600">
                                  <div className="flex items-center justify-between">
                                    <div className="flex items-center">
                                      <Package className="h-6 w-6 text-indigo-600 dark:text-indigo-400 mr-3" />
                                      <div>
                                        <h5 className="text-lg font-semibold text-gray-900 dark:text-white">{productId}</h5>
                                        <p className="text-sm text-gray-600 dark:text-gray-400">
                                          Complete rollup hierarchy showing what passes and what combines
                                        </p>
                                      </div>
                                    </div>
                                    <div className="text-right">
                                      <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">
                                        {segments.reduce((sum, s) => sum + s.user_count, 0).toLocaleString()}
                                      </div>
                                      <div className="text-sm text-gray-600 dark:text-gray-400">Total Users</div>
                                    </div>
                                  </div>
                                </div>
                                
                                {/* Hierarchical Tree */}
                                <div className="p-6">
                                  {Object.entries(hierarchy).map(([level1Key, level1Data]) => (
                                    <div key={level1Key} className="mb-8 last:mb-0">
                                      {/* Product + Store + Price Level (Level 1) */}
                                      <div className="flex items-center mb-4">
                                        <div className="flex items-center">
                                          <div className="w-4 h-4 bg-purple-500 rounded mr-3"></div>
                                          <span className="text-lg font-semibold text-gray-900 dark:text-white">
                                            🏢 {level1Data.productId} | 📱 {formatAppStore(level1Data.appStore)} | 💰 {level1Data.priceRange}
                                          </span>
                                        </div>
                                        <div className="ml-auto flex items-center space-x-4">
                                          <span className="text-xs text-gray-500 dark:text-gray-400">
                                            {level1Data.totalUsers.toLocaleString()} users
                                          </span>
                                          {getStatusIndicator(level1Data.passesThreshold, level1Data.rollupInfo, 'product_store_price', level1Data.rollupInfo.rollupTarget)}
                                        </div>
                                      </div>
                                      
                                      {/* Economic Tiers (Level 2) */}
                                      <div className="ml-6 space-y-4">
                                        {Object.entries(level1Data.economicTiers).map(([tierName, tierData]) => (
                                          <div key={tierName}>
                                            {/* Economic Tier Level */}
                                            <div className="flex items-center mb-3">
                                              <div className="flex items-center">
                                                <div className="w-3 h-3 bg-indigo-500 rounded mr-3"></div>
                                                <span className="text-md font-medium text-gray-700 dark:text-gray-300">
                                                  🌍 {tierName}
                                                </span>
                                              </div>
                                              <div className="ml-auto flex items-center space-x-3">
                                                <span className="text-xs text-gray-500 dark:text-gray-400">
                                                  {tierData.totalUsers.toLocaleString()} users
                                                </span>
                                                {getStatusIndicator(tierData.passesThreshold, tierData.rollupInfo, 'economic_tier', tierData.rollupInfo.rollupTarget)}
                                              </div>
                                            </div>
                                            
                                            {/* Countries (Level 3) */}
                                            <div className="ml-6 space-y-3">
                                              {Object.entries(tierData.countries).map(([country, countryData]) => (
                                                <div key={country}>
                                                  {/* Country Level */}
                                                  <div className="flex items-center mb-2">
                                                    <div className="flex items-center">
                                                      <div className="w-2.5 h-2.5 bg-blue-500 rounded mr-3"></div>
                                                      <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                                                        🏁 {country}
                                                      </span>
                                                    </div>
                                                    <div className="ml-auto flex items-center space-x-3">
                                                      <span className="text-xs text-gray-500 dark:text-gray-400">
                                                        {countryData.totalUsers.toLocaleString()} users
                                                      </span>
                                                      {getStatusIndicator(countryData.passesThreshold, countryData.rollupInfo, 'country', countryData.rollupInfo.rollupTarget)}
                                                    </div>
                                                  </div>
                                                  
                                                  {/* Regions (Level 4) */}
                                                  {countryData.regions.length > 0 && (
                                                    <div className="ml-6 space-y-1">
                                                      {countryData.regions.map((region, idx) => (
                                                        <div key={idx} className="flex items-center justify-between py-1">
                                                          <div className="flex items-center">
                                                            <div className="w-2 h-2 bg-green-500 rounded mr-2"></div>
                                                            <span className="text-xs text-gray-600 dark:text-gray-400">
                                                              📍 {region.name}
                                                            </span>
                                                          </div>
                                                          <div className="flex items-center space-x-2">
                                                            <span className="text-xs text-gray-500 dark:text-gray-500">
                                                              {region.users.toLocaleString()} users
                                                            </span>
                                                            {region.passes ? (
                                                              <span className="px-1 py-0.5 rounded text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300">
                                                                ✅ PASSES
                                                              </span>
                                                            ) : (
                                                              <div className="relative group">
                                                                <span className="px-1 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 cursor-help">
                                                                  → {region.rollupTarget}
                                                                </span>
                                                                
                                                                {/* Hover tooltip for region rollup details */}
                                                                <div className="absolute bottom-full right-0 mb-2 px-3 py-2 bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900 text-xs rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-10 whitespace-nowrap max-w-xs">
                                                                  <div className="text-xs">
                                                                    <div className="font-medium mb-1">Rollup Details:</div>
                                                                    <div>• Target: {region.rollupTarget}</div>
                                                                    {region.rollupReason && (
                                                                      <div>• Reason: {region.rollupReason}</div>
                                                                    )}
                                                                    {region.propertiesDropped && region.propertiesDropped.length > 0 && (
                                                                      <div>• Properties dropped: {region.propertiesDropped.join(', ')}</div>
                                                                    )}
                                                                    {region.finalUserCount && region.finalUserCount !== region.originalUserCount && (
                                                                      <div>• Final user count: {region.finalUserCount.toLocaleString()}</div>
                                                                    )}
                                                                  </div>
                                                                  <div className="absolute top-full right-4 transform border-4 border-transparent border-t-gray-900 dark:border-t-gray-100"></div>
                                                                </div>
                                                              </div>
                                                            )}
                                                          </div>
                                                        </div>
                                                      ))}
                                                    </div>
                                                  )}
                                                </div>
                                              ))}
                                            </div>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })()}
                </div>
              )}
              
              {/* Helpful Information Box */}
              <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
                  <Info className="h-4 w-4 mr-2" />
                  Understanding Your User Segments
                </h4>
                <div className="text-sm text-blue-800 dark:text-blue-300 space-y-2">
                  <div>
                    <strong>User-Centric Analysis:</strong> Instead of generating theoretical combinations, we analyze your actual users 
                    and group them based on their real properties (product purchases, geographic location, app store). This ensures 
                    every segment represents real user behavior.
                  </div>
                  <div>
                    <strong>4-Level Hierarchy:</strong> Users are organized in a hierarchical structure: 
                    Product+Store+Price → Economic Tier → Country → Region. 
                    When segments have insufficient users, we use an intelligent tree-based rollup strategy that tests each level 
                    from top to bottom. When a level fails the minimum user threshold, all segments below it roll up to the level 
                    ABOVE the failing level (which had to pass for the failing level to be tested).
                  </div>
                  <div>
                    <strong>Rollup Ceiling:</strong> Rollups cannot go beyond the Product+Store+Price level. This level represents 
                    the fundamental business unit (specific product, sold through specific store, at specific price range). 
                    Even if this level fails the minimum threshold, segments still roll up to this level rather than combining 
                    across different products or stores.
                    <span className="ml-2 text-xs">
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 mr-1">Country-wide</span>
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 mr-1">Economic Tier</span>
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">Product+Store+Price</span>
                    </span>
                  </div>
                  <div>
                    <strong>Price Bucketing:</strong> Products are automatically grouped into price ranges using statistical analysis. 
                    Only products with at least {config.min_price_samples} price samples are included to ensure meaningful price segments.
                  </div>
                  <div>
                    <strong>Segment Viability:</strong> Green indicators show segments with enough users for reliable analysis. 
                    Yellow/orange indicators show segments that roll up to broader categories to reach the minimum group size.
                  </div>
                </div>
              </div>
              
              <div className="text-sm text-gray-500 dark:text-gray-400 mb-6">
                <strong>About Minimum Group Size:</strong> This setting affects the conversion analysis (Step 3), not the data discovery. 
                It determines how many users must be in each segment to calculate reliable conversion rates. Segments with fewer users 
                will be automatically broadened (by including more locations or stores) to reach the minimum size.
              </div>
              
              <div className="flex gap-3">
                <button
                  onClick={() => setShowMethodologyDialog(true)}
                  className="inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600"
                >
                  <Info className="h-4 w-4 mr-2" />
                  View Technical Details
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Analysis Progress */}
      {isAnalyzing && analysisProgress && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
          <div className="p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
              Step 3: Running Conversion Analysis
            </h3>
            <p className="text-gray-600 dark:text-gray-400 mb-4">
              Calculating conversion rates for each user segment. This process analyzes {analysisProgress.total_combinations.toLocaleString()} different combinations of user properties.
            </p>
            
            {/* Main Progress Bar */}
            <div className="mb-6">
              <div className="flex justify-between text-sm text-gray-600 dark:text-gray-400 mb-2">
                <span>Overall Progress</span>
                <span>{analysisProgress.percentage_complete.toFixed(1)}% Complete</span>
              </div>
              <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3">
                <div 
                  className="bg-indigo-600 h-3 rounded-full transition-all duration-300 relative"
                  style={{ width: `${analysisProgress.percentage_complete}%` }}
                >
                  <div className="absolute inset-0 bg-indigo-400 rounded-full animate-pulse opacity-50"></div>
                </div>
              </div>
            </div>
            
            {/* Progress Statistics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
              <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Segments Processed</div>
                <div className="text-xl font-bold text-blue-600 dark:text-blue-400">
                  {analysisProgress.completed_combinations.toLocaleString()}
                </div>
                <div className="text-xs text-blue-600 dark:text-blue-400">
                  of {analysisProgress.total_combinations.toLocaleString()} total
                </div>
              </div>
              
              <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
                <div className="text-sm text-green-800 dark:text-green-300 font-medium">Valid Groups Found</div>
                <div className="text-xl font-bold text-green-600 dark:text-green-400">
                  {analysisProgress.valid_cohorts_found.toLocaleString()}
                </div>
                <div className="text-xs text-green-600 dark:text-green-400">
                  Groups with enough users for analysis
                </div>
              </div>
              
              <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg">
                <div className="text-sm text-red-800 dark:text-red-300 font-medium">Skipped Groups</div>
                <div className="text-xl font-bold text-red-600 dark:text-red-400">
                  {analysisProgress.invalid_cohorts_found.toLocaleString()}
                </div>
                <div className="text-xs text-red-600 dark:text-red-400">
                  Groups with insufficient data
                </div>
              </div>
              
              <div className="p-3 bg-orange-50 dark:bg-orange-900/20 rounded-lg">
                <div className="text-sm text-orange-800 dark:text-orange-300 font-medium">Time Remaining</div>
                <div className="text-xl font-bold text-orange-600 dark:text-orange-400">
                  {analysisProgress.estimated_time_remaining_minutes}
                </div>
                <div className="text-xs text-orange-600 dark:text-orange-400">
                  minutes estimated
                </div>
              </div>
            </div>
            
            {/* Warning and Error Tracking */}
            {(analysisProgress.warning_counts && Object.keys(analysisProgress.warning_counts).length > 0) && (
              <div className="mb-6 p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
                <h4 className="text-sm font-semibold text-yellow-900 dark:text-yellow-200 mb-3 flex items-center">
                  <AlertTriangle className="h-4 w-4 mr-2" />
                  Analysis Warnings & Issues
                </h4>
                
                {/* User Count Mismatch Explanation */}
                {analysisProgress.user_count_mismatches > 0 && (
                  <div className="mb-4 p-3 bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800 rounded-lg">
                    <h5 className="text-sm font-semibold text-orange-900 dark:text-orange-200 mb-2 flex items-center">
                      <Info className="h-4 w-4 mr-2" />
                      What User Count Mismatches Mean
                    </h5>
                    <div className="text-sm text-orange-800 dark:text-orange-300 space-y-2">
                      <p>
                        <strong>What happens:</strong> During the discovery phase, we calculated that a segment would have X users. 
                        But when we actually query the database during analysis, we find Y users instead.
                      </p>
                      <p>
                        <strong>Why this occurs:</strong> The discovery phase uses sampling and approximation for speed, while the analysis phase 
                        uses exact database queries. Small differences are normal, but large differences indicate data inconsistencies.
                      </p>
                      <p>
                        <strong>Impact on results:</strong> The analysis automatically uses the actual user count found during analysis, 
                        so your conversion rates are still accurate. However, segments that were expected to have enough users might 
                        end up being too small and get rolled up to broader categories.
                      </p>
                      <p>
                        <strong>What we do:</strong> When mismatches occur, we log them for debugging but continue the analysis using 
                        the correct user count. All final results use the actual user counts, not the estimated ones.
                      </p>
                    </div>
                  </div>
                )}
                
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                  <div className="text-center p-3 bg-white dark:bg-yellow-900/30 rounded border">
                    <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                      {analysisProgress.user_count_mismatches || 0}
                    </div>
                    <div className="text-sm text-red-800 dark:text-red-300">User Count Mismatches</div>
                    <div className="text-xs text-red-600 dark:text-red-400">
                      Segments where expected vs actual user counts differ
                    </div>
                  </div>
                  
                  <div className="text-center p-3 bg-white dark:bg-yellow-900/30 rounded border">
                    <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                      {analysisProgress.segments_with_warnings || 0}
                    </div>
                    <div className="text-sm text-orange-800 dark:text-orange-300">Segments with Issues</div>
                    <div className="text-xs text-orange-600 dark:text-orange-400">
                      Total segments that encountered warnings
                    </div>
                  </div>
                  
                  <div className="text-center p-3 bg-white dark:bg-yellow-900/30 rounded border">
                    <div className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
                      {Object.values(analysisProgress.warning_counts).reduce((sum, count) => sum + count, 0)}
                    </div>
                    <div className="text-sm text-yellow-800 dark:text-yellow-300">Total Warnings</div>
                    <div className="text-xs text-yellow-600 dark:text-yellow-400">
                      All warning events during analysis
                    </div>
                  </div>
                </div>
                
                {/* Warning Types Breakdown */}
                <div className="space-y-2">
                  <div className="text-sm font-medium text-yellow-900 dark:text-yellow-200">Warning Types:</div>
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(analysisProgress.warning_counts).map(([type, count]) => (
                      <span 
                        key={type}
                        className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200"
                      >
                        {type.replace('_', ' ')}: {count}
                      </span>
                    ))}
                  </div>
                </div>
                
                {/* Recent Warnings */}
                {analysisProgress.warnings && analysisProgress.warnings.length > 0 && (
                  <div className="mt-4">
                    <div className="text-sm font-medium text-yellow-900 dark:text-yellow-200 mb-2">
                      Recent Warnings (last {Math.min(5, analysisProgress.warnings.length)}):
                    </div>
                    <div className="space-y-2 max-h-48 overflow-y-auto">
                      {analysisProgress.warnings.slice(-5).map((warning, idx) => (
                        <div key={idx} className="text-xs p-2 bg-white dark:bg-yellow-900/40 rounded border border-yellow-200 dark:border-yellow-700">
                          <div className="font-medium text-yellow-900 dark:text-yellow-200">
                            {warning.type.replace('_', ' ')}
                          </div>
                          <div className="text-yellow-800 dark:text-yellow-300 mt-1">
                            {warning.message}
                          </div>
                          {warning.segment && (
                            <div className="text-yellow-600 dark:text-yellow-400 mt-1 font-mono text-xs">
                              {warning.segment.combination?.product_id} | {warning.segment.combination?.price_bucket}
                              {warning.segment.expected_count && warning.segment.actual_count && (
                                <span> | Expected: {warning.segment.expected_count}, Got: {warning.segment.actual_count}</span>
                              )}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
            
            {/* Processing Rate and Current Step */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
              <div className="p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                <div className="text-sm text-purple-800 dark:text-purple-300 font-medium">Processing Rate</div>
                <div className="text-lg font-bold text-purple-600 dark:text-purple-400">
                  {analysisProgress.combinations_per_minute ? analysisProgress.combinations_per_minute.toFixed(1) : '0'} per minute
                </div>
                <div className="text-xs text-purple-600 dark:text-purple-400">
                  Current analysis speed
                </div>
              </div>
              
              <div className="p-3 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg">
                <div className="text-sm text-indigo-800 dark:text-indigo-300 font-medium">Current Step</div>
                <div className="text-sm font-medium text-indigo-600 dark:text-indigo-400">
                  {analysisProgress.current_step || 'Initializing...'}
                </div>
                <div className="text-xs text-indigo-600 dark:text-indigo-400">
                  What's happening now
                </div>
              </div>
            </div>
            
            {/* Current Combination Being Processed */}
            {analysisProgress.current_combination && (
              <div className="p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                <div className="text-sm text-gray-600 dark:text-gray-400 mb-1">Currently Processing Combination:</div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">
                  Product: {analysisProgress.current_combination.product_id} | 
                  Price: {analysisProgress.current_combination.price_bucket} | 
                  Region: {analysisProgress.current_combination.region || 'Any'} | 
                  Country: {analysisProgress.current_combination.country || 'Any'} | 
                  Store: {analysisProgress.current_combination.app_store || 'Any'}
                </div>
              </div>
            )}
            
            {/* Analysis Health Status */}
            <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  <div className={`w-3 h-3 rounded-full mr-2 ${
                    analysisProgress.user_count_mismatches > 0 ? 'bg-yellow-500' : 'bg-green-500'
                  }`}></div>
                  <span className="text-sm font-medium text-blue-900 dark:text-blue-200">
                    Analysis Health: {analysisProgress.user_count_mismatches > 0 ? 'Minor Issues Detected' : 'Running Smoothly'}
                  </span>
                </div>
                <div className="text-xs text-blue-600 dark:text-blue-400">
                  {analysisProgress.user_count_mismatches > 0 ? 
                    'Some user count mismatches detected - analysis will continue with corrections' :
                    'All segments processing as expected'
                  }
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Analysis Results Summary */}
      {analysisResults && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
          <div className="p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
              Step 3: Conversion Analysis Complete
            </h3>
            <p className="text-gray-600 dark:text-gray-400 mb-4">
              Analysis finished! We found conversion patterns across {analysisResults.summary.valid_cohorts.toLocaleString()} user segments. Use the search below to explore specific segments and their conversion rates.
            </p>
            
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
              <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
                <div className="text-sm text-green-800 dark:text-green-300 font-medium">Successful Analyses</div>
                <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                  {analysisResults.summary.valid_cohorts.toLocaleString()}
                </div>
                <div className="text-xs text-green-600 dark:text-green-400">
                  User segments with reliable conversion data
                </div>
              </div>
              
              <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg">
                <div className="text-sm text-red-800 dark:text-red-300 font-medium">Insufficient Data</div>
                <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                  {analysisResults.summary.invalid_cohorts.toLocaleString()}
                </div>
                <div className="text-xs text-red-600 dark:text-red-400">
                  Segments with too few users
                </div>
              </div>
              
              <div className="p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
                <div className="text-sm text-yellow-800 dark:text-yellow-300 font-medium">Expanded Segments</div>
                <div className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
                  {analysisResults.summary.cohorts_with_dropped_properties.toLocaleString()}
                </div>
                <div className="text-xs text-yellow-600 dark:text-yellow-400">
                  Segments broadened to include more users
                </div>
              </div>
              
              <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <div className="text-sm text-blue-800 dark:text-blue-300 font-medium">Analysis Date</div>
                <div className="text-lg font-semibold text-blue-600 dark:text-blue-400">
                  {formatDate(analysisResults.last_updated)}
                </div>
                <div className="text-xs text-blue-600 dark:text-blue-400">
                  When this analysis was completed
                </div>
              </div>
            </div>
            
            {/* Analysis Warnings Summary (if any warnings occurred during analysis) */}
            {analysisResults.analysis_warnings && (
              <div className="mb-4 p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
                <h4 className="text-sm font-semibold text-yellow-900 dark:text-yellow-200 mb-2 flex items-center">
                  <AlertTriangle className="h-4 w-4 mr-2" />
                  Analysis Completed with Warnings
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-3">
                  <div className="text-center p-2 bg-white dark:bg-yellow-900/30 rounded">
                    <div className="text-lg font-bold text-red-600 dark:text-red-400">
                      {analysisResults.analysis_warnings.user_count_mismatches || 0}
                    </div>
                    <div className="text-xs text-red-800 dark:text-red-300">User Count Mismatches</div>
                  </div>
                  <div className="text-center p-2 bg-white dark:bg-yellow-900/30 rounded">
                    <div className="text-lg font-bold text-orange-600 dark:text-orange-400">
                      {analysisResults.analysis_warnings.segments_with_warnings || 0}
                    </div>
                    <div className="text-xs text-orange-800 dark:text-orange-300">Segments with Issues</div>
                  </div>
                  <div className="text-center p-2 bg-white dark:bg-yellow-900/30 rounded">
                    <div className="text-lg font-bold text-yellow-600 dark:text-yellow-400">
                      {analysisResults.analysis_warnings.total_warnings || 0}
                    </div>
                    <div className="text-xs text-yellow-800 dark:text-yellow-300">Total Warnings</div>
                  </div>
                </div>
                <div className="text-sm text-yellow-800 dark:text-yellow-300">
                  <strong>Impact:</strong> The analysis completed successfully, but some segments had user count discrepancies. 
                  All results have been corrected using actual user counts found during analysis. 
                  This may indicate minor inconsistencies in the data discovery vs analysis phases.
                </div>
              </div>
            )}
            
            <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
              <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
                <CheckCircle className="h-4 w-4 mr-2" />
                What These Results Show
              </h4>
              <ul className="text-sm text-blue-800 dark:text-blue-300 space-y-1 list-disc list-inside">
                <li><strong>Trial Conversion Rates:</strong> How many users who start trials actually convert to paid subscriptions</li>
                <li><strong>Refund Rates:</strong> Percentage of users who request refunds after different types of purchases</li>
                <li><strong>Segment Comparisons:</strong> How conversion rates vary by product, price, location, and app store</li>
                <li><strong>Statistical Confidence:</strong> All rates include confidence intervals to show reliability</li>
              </ul>
            </div>
          </div>
        </div>
      )}

      {/* Search Interface */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 mb-6">
        <div className="p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
            Step 4: Explore Conversion Results
          </h3>
          <p className="text-gray-600 dark:text-gray-400 mb-4">
            Search and filter the conversion analysis results to find insights about specific user segments, products, or regions.
          </p>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Product ID
              </label>
              <input
                type="text"
                placeholder="e.g., product_123"
                value={searchFilters.product_id}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, product_id: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Filter by specific product identifier
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Region
              </label>
              <input
                type="text"
                placeholder="e.g., North America"
                value={searchFilters.region}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, region: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Filter by geographic region
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Country
              </label>
              <input
                type="text"
                placeholder="e.g., US"
                value={searchFilters.country}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, country: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Filter by specific country code
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                App Store
              </label>
              <input
                type="text"
                placeholder="e.g., APP_STORE"
                value={searchFilters.app_store}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, app_store: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Filter by app store platform
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Minimum Users in Segment
              </label>
              <input
                type="number"
                placeholder="50"
                value={searchFilters.min_cohort_size}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, min_cohort_size: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Only show segments with at least this many users
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Maximum Results
              </label>
              <input
                type="number"
                placeholder="100"
                value={searchFilters.limit}
                onChange={(e) => setSearchFilters(prev => ({ ...prev, limit: parseInt(e.target.value) || 100 }))}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:text-white text-sm"
              />
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Limit number of results returned
              </p>
            </div>
          </div>

          <div className="flex items-center gap-4 mb-4">
            <button
              onClick={searchConversionProbabilities}
              disabled={isSearching}
              className="inline-flex items-center px-6 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSearching ? (
                <RefreshCw className="animate-spin h-4 w-4 mr-2" />
              ) : (
                <Search className="h-4 w-4 mr-2" />
              )}
              {isSearching ? 'Searching...' : 'Search Results'}
            </button>
            
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Leave filters empty to see all results
            </div>
          </div>
        </div>
      </div>

      {/* Search Results */}
      {searchResults && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
          <div className="p-4 border-b border-gray-200 dark:border-gray-700">
            <button
              onClick={() => toggleSection('searchResults')}
              className="flex items-center justify-between w-full text-left"
            >
              <div className="flex items-center">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  Conversion Analysis Results
                </h3>
                <span className="ml-3 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                  {searchResults.total_matches} segments found
                </span>
                
                {/* NEW: Rollup Details Button */}
                {(() => {
                  const rollupCount = searchResults?.rollup_information?.rolled_up_segments?.length || 0;
                  if (rollupCount > 0) {
                    return (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          openRollupModal(searchResults.rollup_information);
                        }}
                        className="ml-3 inline-flex items-center px-3 py-1 border border-blue-300 dark:border-blue-600 rounded-md text-xs font-medium text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors"
                      >
                        <Target className="h-3 w-3 mr-1" />
                        View Rollup Details ({rollupCount})
                      </button>
                    );
                  }
                  return null;
                })()}
              </div>
              {expandedSections.searchResults ? (
                <ChevronDown className="h-5 w-5 text-gray-500" />
              ) : (
                <ChevronRight className="h-5 w-5 text-gray-500" />
              )}
            </button>
          </div>
          
          {expandedSections.searchResults && (
            <div>
              {/* Results Summary */}
              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-200 mb-2 flex items-center">
                  <BarChart3 className="h-4 w-4 mr-2" />
                  Understanding Your Results
                </h4>
                <div className="text-sm text-blue-800 dark:text-blue-300 space-y-1">
                  <p><strong>Trial Conversion Rate:</strong> Percentage of users who start a trial and then purchase a subscription</p>
                  <p><strong>Refund Rates:</strong> Percentage of users who request refunds after different purchase types</p>
                  <p><strong>Cohort Size:</strong> Number of users in each segment (larger = more reliable statistics)</p>
                  <p><strong>Properties Dropped:</strong> When a segment had too few users, we broadened it by including more locations or stores</p>
                </div>
              </div>
              
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                  <thead className="bg-gray-50 dark:bg-gray-700">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Product & Price
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Location & Store
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Users in Segment
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Trial → Paid
                        <div className="text-xs font-normal text-gray-400 normal-case">Conversion Rate</div>
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Trial → Refund
                        <div className="text-xs font-normal text-gray-400 normal-case">Rate</div>
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Purchase → Refund
                        <div className="text-xs font-normal text-gray-400 normal-case">Rate</div>
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Renewal → Refund
                        <div className="text-xs font-normal text-gray-400 normal-case">Rate</div>
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                        Segment Notes
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                    {searchResults.matches.map((result, index) => (
                      <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-gray-900 dark:text-white">
                            {result.cohort.product_id}
                          </div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">
                            {result.cohort.price_bucket}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-900 dark:text-white">
                            {result.cohort.region || 'Any Region'}
                          </div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">
                            {result.cohort.country || 'Any Country'} • {result.cohort.app_store || 'Any Store'}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-gray-900 dark:text-white">
                            {result.cohort.cohort_size.toLocaleString()}
                          </div>
                          <div className="text-xs text-gray-500 dark:text-gray-400">
                            {result.cohort.cohort_size >= 1000 ? 'High confidence' : 
                             result.cohort.cohort_size >= 100 ? 'Good confidence' : 'Lower confidence'}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-lg font-bold text-gray-900 dark:text-white">
                            {formatPercentage(result.metrics.trial_conversion_rate)}
                          </div>
                          {result.metrics.confidence_intervals?.trial_conversion && (
                            <div className="text-xs text-gray-500 dark:text-gray-400">
                              ±{((result.metrics.confidence_intervals.trial_conversion[1] - result.metrics.confidence_intervals.trial_conversion[0]) / 2 * 100).toFixed(1)}%
                            </div>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-gray-900 dark:text-white">
                            {formatPercentage(result.metrics.trial_converted_to_refund_rate)}
                          </div>
                          {result.metrics.confidence_intervals?.trial_refund && (
                            <div className="text-xs text-gray-500 dark:text-gray-400">
                              ±{((result.metrics.confidence_intervals.trial_refund[1] - result.metrics.confidence_intervals.trial_refund[0]) / 2 * 100).toFixed(1)}%
                            </div>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-gray-900 dark:text-white">
                            {formatPercentage(result.metrics.initial_purchase_to_refund_rate)}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-gray-900 dark:text-white">
                            {formatPercentage(result.metrics.renewal_to_refund_rate)}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {result.cohort.properties_dropped && result.cohort.properties_dropped.length > 0 ? (
                            <div>
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                                <AlertTriangle className="h-3 w-3 mr-1" />
                                Expanded Segment
                              </span>
                              <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                {result.cohort.rollup_target && result.cohort.rollup_reason ? (
                                  <div>
                                    <div className="font-medium">→ Rolled up to {result.cohort.rollup_target}</div>
                                    <div>Reason: {result.cohort.rollup_reason}</div>
                                    <div className="text-gray-400">Original size: {result.cohort.original_cohort_size} → Final: {result.cohort.cohort_size} users</div>
                                  </div>
                                ) : (
                                  <div>
                                    <div>Broadened to include more {result.cohort.properties_dropped.join(', ')}</div>
                                    <div className="text-gray-400">Original size: {result.cohort.original_cohort_size} → Final: {result.cohort.cohort_size} users</div>
                                  </div>
                                )}
                              </div>
                            </div>
                          ) : (
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                              <CheckCircle className="h-3 w-3 mr-1" />
                              Exact Match
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              {/* Results Footer */}
              <div className="p-4 bg-gray-50 dark:bg-gray-700 border-t border-gray-200 dark:border-gray-600">
                <div className="flex items-center justify-between text-sm text-gray-600 dark:text-gray-400">
                  <div className="flex items-center space-x-4">
                    <span>Showing {searchResults.matches.length} of {searchResults.total_matches} segments</span>
                    <span>•</span>
                    <span>Last updated: {formatDate(searchResults.file_last_updated)}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <span className="text-xs">Confidence intervals show statistical uncertainty</span>
                    <Info className="h-4 w-4" />
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Methodology Dialog */}
      {showMethodologyDialog && propertyAnalysis && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-20 mx-auto p-5 border w-11/12 md:w-3/4 lg:w-2/3 shadow-lg rounded-md bg-white dark:bg-gray-800">
            <div className="mt-3">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                  Technical Details: Data Analysis Methodology
                </h3>
                <button
                  onClick={() => setShowMethodologyDialog(false)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <X className="h-6 w-6" />
                </button>
              </div>
              
              <div className="space-y-6">
                {/* Overview */}
                <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                  <h4 className="text-md font-semibold text-blue-900 dark:text-blue-200 mb-2">
                    How We Analyzed Your Data
                  </h4>
                  <p className="text-sm text-blue-800 dark:text-blue-300">
                    We used statistical analysis to automatically group your products by price ranges and identify meaningful user segments. 
                    This ensures we have enough data in each group to calculate reliable conversion rates.
                  </p>
                </div>
                
                {/* Price Bucketing */}
                <div>
                  <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3">
                    Price Range Grouping Strategy
                  </h4>
                  <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg mb-3">
                    <div className="flex items-center mb-2">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200 mr-2">
                        {propertyAnalysis.bucketing_methodology.method}
                      </span>
                      <span className="text-sm font-medium text-gray-900 dark:text-white">
                        Primary Method Used
                      </span>
                    </div>
                    <p className="text-sm text-gray-700 dark:text-gray-300 mb-2">
                      <strong>Strategy:</strong> {propertyAnalysis.bucketing_methodology.description}
                    </p>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      <strong>Technical Details:</strong> {propertyAnalysis.bucketing_methodology.details}
                    </p>
                  </div>
                  
                  {propertyAnalysis.bucketing_methodology.product_breakdown && (
                    <div>
                      <h5 className="text-sm font-medium text-gray-900 dark:text-white mb-2">
                        Methods Used by Product:
                      </h5>
                      <div className="flex flex-wrap gap-2">
                        {Object.entries(propertyAnalysis.bucketing_methodology.product_breakdown).map(([method, count]) => (
                          <span 
                            key={method} 
                            className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-gray-100 text-gray-800 dark:bg-gray-600 dark:text-gray-200"
                          >
                            {method.replace('_', ' ')}: {count} product{count !== 1 ? 's' : ''}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                
                {/* Data Quality */}
                <div>
                  <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3">
                    Data Quality & Coverage
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
                      <div className="text-sm font-medium text-green-900 dark:text-green-200">Time Period Analyzed</div>
                      <div className="text-lg font-semibold text-green-700 dark:text-green-300">
                        {propertyAnalysis.timeframe_analyzed?.start ? 
                          `${formatDate(propertyAnalysis.timeframe_analyzed.start)} to ${formatDate(propertyAnalysis.timeframe_analyzed.end)}` :
                          'All available data'
                        }
                      </div>
                    </div>
                    <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                      <div className="text-sm font-medium text-blue-900 dark:text-blue-200">Sample Limit</div>
                      <div className="text-lg font-semibold text-blue-700 dark:text-blue-300">
                        {propertyAnalysis.sample_limit_used?.toLocaleString() || 'No limit'} samples per product
                      </div>
                    </div>
                  </div>
                </div>
                
                {/* Statistical Methods */}
                <div>
                  <h4 className="text-md font-semibold text-gray-900 dark:text-white mb-3">
                    Statistical Methods
                  </h4>
                  <div className="space-y-3">
                    <div className="p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                      <div className="text-sm font-medium text-gray-900 dark:text-white mb-1">Outlier Removal</div>
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        Used Interquartile Range (IQR) method to remove price outliers that could skew the analysis
                      </div>
                    </div>
                    <div className="p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                      <div className="text-sm font-medium text-gray-900 dark:text-white mb-1">Minimum Sample Size</div>
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        Required minimum 100 samples per price bucket to ensure statistical reliability
                      </div>
                    </div>
                    <div className="p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                      <div className="text-sm font-medium text-gray-900 dark:text-white mb-1">Confidence Intervals</div>
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        All conversion rates include 95% confidence intervals to show statistical uncertainty
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="mt-6 flex justify-end">
                <button
                  onClick={() => setShowMethodologyDialog(false)}
                  className="px-6 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {/* NEW: Rollup Modal */}
      {showRollupModal && rollupModalData && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-10 mx-auto p-5 border w-11/12 md:w-5/6 lg:w-4/5 xl:w-3/4 shadow-lg rounded-md bg-white dark:bg-gray-800 max-h-[90vh] overflow-y-auto">
            <div className="mt-3">
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                  Segment Rollup Details
                </h3>
                <button
                  onClick={() => setShowRollupModal(false)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <X className="h-6 w-6" />
                </button>
              </div>
              
              {/* Summary Section */}
              <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                <h4 className="text-lg font-semibold text-blue-900 dark:text-blue-200 mb-3 flex items-center">
                  <Target className="h-5 w-5 mr-2" />
                  Rollup Summary
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div className="text-center p-3 bg-white dark:bg-blue-900/30 rounded">
                    <div className="text-2xl font-bold text-gray-600 dark:text-gray-300">{rollupModalData.totalSegments}</div>
                    <div className="text-sm text-gray-700 dark:text-gray-400">Total Segments</div>
                  </div>
                  <div className="text-center p-3 bg-white dark:bg-blue-900/30 rounded">
                    <div className="text-2xl font-bold text-green-600 dark:text-green-400">{rollupModalData.totalSegments - rollupModalData.rolledUpSegments}</div>
                    <div className="text-sm text-green-700 dark:text-green-400">Direct Analysis</div>
                  </div>
                  <div className="text-center p-3 bg-white dark:bg-blue-900/30 rounded">
                    <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">{rollupModalData.rolledUpSegments}</div>
                    <div className="text-sm text-orange-700 dark:text-orange-400">Rolled Up</div>
                  </div>
                  <div className="text-center p-3 bg-white dark:bg-blue-900/30 rounded">
                    <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                      {Math.round((rollupModalData.rolledUpSegments / rollupModalData.totalSegments) * 100)}%
                    </div>
                    <div className="text-sm text-purple-700 dark:text-purple-400">Rollup Rate</div>
                  </div>
                </div>
              </div>
              
              {/* Rollup Targets Section */}
              {Object.keys(rollupModalData.rollupTargets).length > 0 && (
                <div className="mb-6">
                  <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                    Rollup Targets & Mappings
                  </h4>
                  <div className="space-y-4">
                    {Object.entries(rollupModalData.rollupTargets).map(([target, segments]) => (
                      <div key={target} className="border border-gray-200 dark:border-gray-600 rounded-lg overflow-hidden">
                        <div className="px-4 py-3 bg-gray-50 dark:bg-gray-700 border-b border-gray-200 dark:border-gray-600">
                          <h5 className="text-md font-medium text-gray-900 dark:text-white flex items-center">
                            <Target className="h-4 w-4 mr-2 text-orange-500" />
                            {target}
                            <span className="ml-2 inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">
                              {segments.length} segment{segments.length !== 1 ? 's' : ''}
                            </span>
                          </h5>
                        </div>
                        <div className="p-4">
                          <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                              <thead className="bg-gray-50 dark:bg-gray-800">
                                <tr>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Product</th>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Price Bucket</th>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Location</th>
                                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Original Users</th>
                                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Final Users</th>
                                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Properties Dropped</th>
                                </tr>
                              </thead>
                              <tbody className="bg-white dark:bg-gray-700 divide-y divide-gray-200 dark:divide-gray-600">
                                {segments.map((segment, idx) => (
                                  <tr key={idx} className={idx % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                                      {segment.productId}
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                                      {segment.priceBucket}
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                                      <div>
                                        {segment.region || 'Any Region'}
                                        {segment.country && <div className="text-xs">{segment.country}</div>}
                                        {segment.appStore && <div className="text-xs">{segment.appStore}</div>}
                                      </div>
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-right text-gray-900 dark:text-white">
                                      {segment.originalUsers.toLocaleString()}
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-right font-medium text-gray-900 dark:text-white">
                                      {segment.finalUsers.toLocaleString()}
                                      {segment.finalUsers > segment.originalUsers && (
                                        <div className="text-xs text-green-600 dark:text-green-400">
                                          +{(segment.finalUsers - segment.originalUsers).toLocaleString()}
                                        </div>
                                      )}
                                    </td>
                                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                                      {segment.propertiesDropped && segment.propertiesDropped.length > 0 ? (
                                        <div className="space-y-1">
                                          {segment.propertiesDropped.map(prop => (
                                            <span key={prop} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200 mr-1">
                                              {prop}
                                            </span>
                                          ))}
                                        </div>
                                      ) : (
                                        <span className="text-gray-400">None</span>
                                      )}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              
              {/* All Segments Detail View */}
              <div className="mb-6">
                <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                  Complete Segment Analysis
                </h4>
                <div className="bg-white dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
                  <div className="overflow-x-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-600">
                      <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Product</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Price Bucket</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Location</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Users</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Status</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Rollup Details</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white dark:bg-gray-700 divide-y divide-gray-200 dark:divide-gray-600">
                        {rollupModalData.segmentDetails.map((segment, idx) => (
                          <tr key={idx} className={idx % 2 === 0 ? 'bg-white dark:bg-gray-700' : 'bg-gray-50 dark:bg-gray-800'}>
                            <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                              {segment.productId}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                              {segment.priceBucket}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                              <div>
                                {segment.region || 'Any Region'}
                                {segment.country && <div className="text-xs">{segment.country}</div>}
                                {segment.appStore && <div className="text-xs">{segment.appStore}</div>}
                              </div>
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                              <div className="text-gray-900 dark:text-white font-medium">
                                {segment.finalUsers.toLocaleString()}
                              </div>
                              {segment.finalUsers !== segment.originalUsers && (
                                <div className="text-xs text-gray-500 dark:text-gray-400">
                                  (was {segment.originalUsers.toLocaleString()})
                                </div>
                              )}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap">
                              {segment.hasRollup ? (
                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">
                                  Rolled Up
                                </span>
                              ) : (
                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                                  Direct
                                </span>
                              )}
                            </td>
                            <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                              {segment.hasRollup ? (
                                <div>
                                  <div className="font-medium text-gray-900 dark:text-white">
                                    → {segment.rollupTarget}
                                  </div>
                                  {segment.rollupDescription && (
                                    <div className="text-xs mt-1">{segment.rollupDescription}</div>
                                  )}
                                  {segment.propertiesDropped && segment.propertiesDropped.length > 0 && (
                                    <div className="flex flex-wrap gap-1 mt-1">
                                      {segment.propertiesDropped.map(prop => (
                                        <span key={prop} className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                                          {prop}
                                        </span>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              ) : (
                                <span className="text-gray-400">No rollup needed</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
              
              <div className="flex justify-end">
                <button
                  onClick={() => setShowRollupModal(false)}
                  className="px-6 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ConversionProbabilityPage; 