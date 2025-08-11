import React from 'react';

// Timeframe options configuration
export const timeframeOptions = [
  { value: 'all', label: 'All Time', description: 'Analyze all historical data' },
  { value: '1month', label: 'Last 1 Month', description: 'Last 30 days' },
  { value: '3months', label: 'Last 3 Months', description: 'Last 90 days' },
  { value: '6months', label: 'Last 6 Months', description: 'Last 180 days' },
  { value: '1year', label: 'Last 1 Year', description: 'Last 365 days' }
];

// Helper function to calculate date range from timeframe
export const getDateRangeFromTimeframe = (timeframe) => {
  if (timeframe === 'all') {
    return { start: null, end: null };
  }
  
  const end = new Date();
  // Dynamic start date: 1 year ago to ensure comprehensive coverage
  const start = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
  
  // Note: We now always start from 1 year ago and go to today to ensure all products are included
  // This prevents newer products from being excluded due to narrow date windows
  
  return {
    start: start.toISOString().split('T')[0],
    end: end.toISOString().split('T')[0]
  };
};

// Formatting functions
export const formatPercentage = (value) => {
  return `${(value * 100).toFixed(2)}%`;
};

export const formatDate = (dateString) => {
  return new Date(dateString).toLocaleDateString();
};

export const formatAppStore = (store) => {
  const storeMap = {
    'APP_STORE': 'App Store',
    'PLAY_STORE': 'Play Store', 
    'STRIPE': 'Stripe'
  };
  return storeMap[store] || store;
};

// Format segment ID to show first4...last4 format
export const formatSegmentId = (segmentId) => {
  if (!segmentId) return 'No ID';
  if (segmentId.length <= 8) return segmentId;
  return `${segmentId.slice(0, 4)}...${segmentId.slice(-4)}`;
};

// Helper function to generate consistent color from UUID
const getColorFromUUID = (uuid) => {
  if (!uuid) return { bg: 'bg-gray-100', text: 'text-gray-800', border: 'border-gray-200', darkBg: 'dark:bg-gray-900', darkText: 'dark:text-gray-200', darkBorder: 'dark:border-gray-700' };
  
  // Create a simple hash from the UUID
  let hash = 0;
  for (let i = 0; i < uuid.length; i++) {
    const char = uuid.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  
  // Use the hash to select from a set of colors that work well together
  const colors = [
    { bg: 'bg-blue-100', text: 'text-blue-800', border: 'border-blue-200', darkBg: 'dark:bg-blue-900', darkText: 'dark:text-blue-200', darkBorder: 'dark:border-blue-700' },
    { bg: 'bg-green-100', text: 'text-green-800', border: 'border-green-200', darkBg: 'dark:bg-green-900', darkText: 'dark:text-green-200', darkBorder: 'dark:border-green-700' },
    { bg: 'bg-purple-100', text: 'text-purple-800', border: 'border-purple-200', darkBg: 'dark:bg-purple-900', darkText: 'dark:text-purple-200', darkBorder: 'dark:border-purple-700' },
    { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-200', darkBg: 'dark:bg-red-900', darkText: 'dark:text-red-200', darkBorder: 'dark:border-red-700' },
    { bg: 'bg-yellow-100', text: 'text-yellow-800', border: 'border-yellow-200', darkBg: 'dark:bg-yellow-900', darkText: 'dark:text-yellow-200', darkBorder: 'dark:border-yellow-700' },
    { bg: 'bg-indigo-100', text: 'text-indigo-800', border: 'border-indigo-200', darkBg: 'dark:bg-indigo-900', darkText: 'dark:text-indigo-200', darkBorder: 'dark:border-indigo-700' },
    { bg: 'bg-pink-100', text: 'text-pink-800', border: 'border-pink-200', darkBg: 'dark:bg-pink-900', darkText: 'dark:text-pink-200', darkBorder: 'dark:border-pink-700' },
    { bg: 'bg-cyan-100', text: 'text-cyan-800', border: 'border-cyan-200', darkBg: 'dark:bg-cyan-900', darkText: 'dark:text-cyan-200', darkBorder: 'dark:border-cyan-700' },
    { bg: 'bg-emerald-100', text: 'text-emerald-800', border: 'border-emerald-200', darkBg: 'dark:bg-emerald-900', darkText: 'dark:text-emerald-200', darkBorder: 'dark:border-emerald-700' },
    { bg: 'bg-orange-100', text: 'text-orange-800', border: 'border-orange-200', darkBg: 'dark:bg-orange-900', darkText: 'dark:text-orange-200', darkBorder: 'dark:border-orange-700' }
  ];
  
  const colorIndex = Math.abs(hash) % colors.length;
  return colors[colorIndex];
};

// Create segment ID badge component
export const getSegmentIdBadge = (segmentId, label = 'Segment ID') => {
  const colors = getColorFromUUID(segmentId);
  
  return (
    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${colors.bg} ${colors.text} ${colors.darkBg} ${colors.darkText} ${colors.border} ${colors.darkBorder} border`}>
      <span className="mr-1">{label}:</span>
      <span className="font-mono">{formatSegmentId(segmentId)}</span>
    </span>
  );
};

// Create rollup target ID badge component
export const getRollupTargetIdBadge = (targetSegmentId, label = 'Target') => {
  const colors = getColorFromUUID(targetSegmentId);
  
  return (
    <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${colors.bg} ${colors.text} ${colors.darkBg} ${colors.darkText} ${colors.border} ${colors.darkBorder} border`}>
      <span className="mr-1">→ {label}:</span>
      <span className="font-mono">{formatSegmentId(targetSegmentId)}</span>
    </span>
  );
};

// Load saved state from localStorage
export const loadSavedState = () => {
  try {
    const savedConfig = localStorage.getItem('conversionProbability_config');
    const savedExpandedSections = localStorage.getItem('conversionProbability_expandedSections');
    
    return {
      config: savedConfig ? JSON.parse(savedConfig) : {
        timeframe: 'all',
        min_cohort_size: 50,
        force_recalculate: false,
        min_price_samples: 100
      },
      expandedSections: savedExpandedSections ? JSON.parse(savedExpandedSections) : {
        propertyAnalysis: false,
        analysisHierarchy: true
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
      expandedSections: {
        propertyAnalysis: false,
        analysisHierarchy: true
      }
    };
  }
};

// Function to create segment-specific rollup data for individual segments
export const createSegmentRollupData = (segment) => {
  // Check for any signs of rollup having occurred
  const hasPropertiesDropped = segment.cohort.properties_dropped && segment.cohort.properties_dropped.length > 0;
  const hasRollupTarget = segment.cohort.rollup_target;
  const hasRollupReason = segment.cohort.rollup_reason;
  const hasOriginalSize = segment.cohort.original_cohort_size && segment.cohort.original_cohort_size !== segment.cohort.cohort_size;
  
  const hasAnyRollup = hasPropertiesDropped || hasRollupTarget || hasRollupReason || hasOriginalSize;
  
  if (!hasAnyRollup) {
    // Even when no rollup, return a valid structure showing this is a direct segment
    return {
      totalSegments: 1,
      rolledUpSegments: 0,
      calculatedSegments: 1,
      rollupTargets: {},
      segmentDetails: [{
        segmentId: segment.cohort.segment_id || 'unknown',
        productId: segment.cohort.product_id,
        priceBucket: segment.cohort.price_bucket,
        region: segment.cohort.region,
        country: segment.cohort.country,
        appStore: segment.cohort.app_store,
        originalUsers: segment.cohort.cohort_size,
        finalUsers: segment.cohort.cohort_size,
        hasRollup: false,
        rollupTarget: null,
        rollupDescription: 'No rollup required - segment had sufficient data',
        propertiesDropped: []
      }]
    };
  }
  
  // Build rollup information
  const rollupTargetName = segment.cohort.rollup_target || 'Expanded Segment';
  
  return {
    totalSegments: 1, // This represents 1 final segment
    rolledUpSegments: 1, // This segment had rollup applied
    calculatedSegments: 0,
    rollupTargets: {
      [rollupTargetName]: [{
        segmentId: segment.cohort.segment_id || 'unknown',
        productId: segment.cohort.product_id,
        priceBucket: segment.cohort.price_bucket,
        region: segment.cohort.region,
        country: segment.cohort.country,
        appStore: segment.cohort.app_store,
        originalUsers: segment.cohort.original_cohort_size || segment.cohort.cohort_size,
        finalUsers: segment.cohort.cohort_size,
        propertiesDropped: segment.cohort.properties_dropped || [],
        rollupReason: segment.cohort.rollup_reason || 'Segment expanded for better statistics',
        rollupTarget: segment.cohort.rollup_target_segment_id || segment.cohort.rollup_target
      }]
    },
    segmentDetails: [{
      segmentId: segment.cohort.segment_id || 'unknown',
      productId: segment.cohort.product_id,
      priceBucket: segment.cohort.price_bucket,
      region: segment.cohort.region,
      country: segment.cohort.country,
      appStore: segment.cohort.app_store,
      originalUsers: segment.cohort.original_cohort_size || segment.cohort.cohort_size,
      finalUsers: segment.cohort.cohort_size,
      hasRollup: true,
      rollupTarget: segment.cohort.rollup_target_segment_id || rollupTargetName,
      rollupDescription: segment.cohort.rollup_reason || 'Segment was expanded to include more data',
      propertiesDropped: segment.cohort.properties_dropped || []
    }]
  };
};

// Helper function to generate status indicators with hover tooltips
export const getStatusIndicator = (passesThreshold, rollupInfo, level = 'segment', rollupTarget = null) => {
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

export const buildHierarchicalStructure = (segments, minCohortSize) => {
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

  // Helper function to generate deterministic segment ID based on segment properties
  const generateSegmentId = (properties) => {
    // Create a deterministic hash based on the segment properties
    const propertyString = Object.keys(properties)
      .sort()
      .map(key => `${key}:${properties[key] || 'null'}`)
      .join('|');
    
    // Simple hash function for demo - in production you might want a more robust one
    let hash = 0;
    for (let i = 0; i < propertyString.length; i++) {
      const char = propertyString.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    
    // Convert to positive number and create a UUID-like string
    const positiveHash = Math.abs(hash);
    const segmentId = positiveHash.toString(16).padStart(8, '0');
    return `${segmentId.slice(0, 4)}-${segmentId.slice(4, 8)}-seg-${Date.now().toString().slice(-4)}`;
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
    
    // Get segment ID from backend or generate one
    const originalSegmentId = segment.segment_id || generateSegmentId({
      product_id: productId,
      app_store: appStore,
      price_bucket: priceRange,
      country: country,
      region: region
    });
    
    const economicTier = getEconomicTier(country);

    // Initialize Product+Store+Price level (Level 1)
    const level1Key = `${productId}|${appStore}|${priceRange}`;
    if (!hierarchy[level1Key]) {
      // Generate segment ID for Level 1 (Product+Store+Price)
      const level1SegmentId = generateSegmentId({
        product_id: productId,
        app_store: appStore,
        price_bucket: priceRange
      });
      
      hierarchy[level1Key] = {
        productId: productId,
        appStore: appStore,
        priceRange: priceRange,
        segmentId: level1SegmentId,
        totalUsers: 0,
        passesThreshold: false,
        rollupInfo: {
          hasRollups: false,
          rolledUpSegments: 0,
          totalSegments: 0,
          rollupReasons: new Set(),
          propertiesDropped: new Set(),
          finalUserCount: 0,
          rollupTarget: null,
          rollupTargetSegmentId: null
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
    const level2Key = `${level1Key}|${economicTier}`;
    if (!hierarchy[level1Key].economicTiers[economicTier]) {
      // Generate segment ID for Level 2 (Product+Store+Price+EconomicTier)
      const level2SegmentId = generateSegmentId({
        product_id: productId,
        app_store: appStore,
        price_bucket: priceRange,
        economic_tier: economicTier
      });
      
      hierarchy[level1Key].economicTiers[economicTier] = {
        tierName: economicTier,
        segmentId: level2SegmentId,
        totalUsers: 0,
        passesThreshold: false,
        rollupInfo: {
          hasRollups: false,
          rolledUpSegments: 0,
          totalSegments: 0,
          rollupReasons: new Set(),
          propertiesDropped: new Set(),
          finalUserCount: 0,
          rollupTarget: null,
          rollupTargetSegmentId: null
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
      // Generate segment ID for Level 3 (Product+Store+Price+EconomicTier+Country)
      const level3SegmentId = generateSegmentId({
        product_id: productId,
        app_store: appStore,
        price_bucket: priceRange,
        economic_tier: economicTier,
        country: country
      });
      
      hierarchy[level1Key].economicTiers[economicTier].countries[country] = {
        countryName: country,
        segmentId: level3SegmentId,
        totalUsers: 0,
        passesThreshold: false,
        rollupInfo: {
          hasRollups: false,
          rolledUpSegments: 0,
          totalSegments: 0,
          rollupReasons: new Set(),
          propertiesDropped: new Set(),
          finalUserCount: 0,
          rollupTarget: null,
          rollupTargetSegmentId: null
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
      // Use original segment ID from backend or generate Level 4 segment ID
      const level4SegmentId = originalSegmentId;
      
      // Determine rollup target segment ID based on rollup logic
      let rollupTargetSegmentId = null;
      if (!hasSufficientData || propertiesDropped.length > 0) {
        // If this segment rolls up, determine which level it rolls up to
        if (rollupTarget && rollupTarget.includes('Country-wide')) {
          // Rolls up to country level (Level 3)
          rollupTargetSegmentId = hierarchy[level1Key].economicTiers[economicTier].countries[country].segmentId;
        } else if (rollupTarget && (rollupTarget.includes('Tier') || rollupTarget.includes('tier'))) {
          // Rolls up to economic tier level (Level 2)
          rollupTargetSegmentId = hierarchy[level1Key].economicTiers[economicTier].segmentId;
        } else {
          // Rolls up to product+store+price level (Level 1)
          rollupTargetSegmentId = hierarchy[level1Key].segmentId;
        }
      }
      
      hierarchy[level1Key].economicTiers[economicTier].countries[country].regions.push({
        name: region,
        segmentId: level4SegmentId,
        rollupTargetSegmentId: rollupTargetSegmentId,
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

  // Calculate pass/fail status and assign rollup target segment IDs for each level
  Object.keys(hierarchy).forEach(level1Key => {
    const level1Data = hierarchy[level1Key];
    level1Data.passesThreshold = level1Data.totalUsers >= minCohortSize;
    
    // Convert Sets to Arrays for easier use in UI
    level1Data.rollupInfo.rollupReasons = Array.from(level1Data.rollupInfo.rollupReasons);
    level1Data.rollupInfo.propertiesDropped = Array.from(level1Data.rollupInfo.propertiesDropped);
    
    // If Level 1 doesn't pass, it can't roll up anywhere (it's the ceiling)
    if (!level1Data.passesThreshold) {
      level1Data.rollupInfo.rollupTargetSegmentId = null; // Cannot roll up beyond this level
    }

    Object.keys(level1Data.economicTiers).forEach(tierName => {
      const tierData = level1Data.economicTiers[tierName];
      tierData.passesThreshold = tierData.totalUsers >= minCohortSize;
      
      // Convert Sets to Arrays for easier use in UI
      tierData.rollupInfo.rollupReasons = Array.from(tierData.rollupInfo.rollupReasons);
      tierData.rollupInfo.propertiesDropped = Array.from(tierData.rollupInfo.propertiesDropped);
      
      // If tier doesn't pass, it rolls up to Level 1 (Product+Store+Price)
      if (!tierData.passesThreshold) {
        tierData.rollupInfo.rollupTargetSegmentId = level1Data.segmentId;
      }

      Object.keys(tierData.countries).forEach(country => {
        const countryData = tierData.countries[country];
        countryData.passesThreshold = countryData.totalUsers >= minCohortSize;
        
        // Convert Sets to Arrays for easier use in UI
        countryData.rollupInfo.rollupReasons = Array.from(countryData.rollupInfo.rollupReasons);
        countryData.rollupInfo.propertiesDropped = Array.from(countryData.rollupInfo.propertiesDropped);
        
        // If country doesn't pass, it rolls up to Level 2 (Economic Tier)
        if (!countryData.passesThreshold) {
          countryData.rollupInfo.rollupTargetSegmentId = tierData.segmentId;
        }
      });
    });
  });

  return hierarchy;
}; 