import React, { useState, useEffect } from 'react';

const PricingManagementPage = () => {
  const [rules, setRules] = useState([]);
  const [missingProducts, setMissingProducts] = useState([]);
  const [schemaVersion, setSchemaVersion] = useState('1.0.0');
  const [productGroups, setProductGroups] = useState({});
  const [productsStructure, setProductsStructure] = useState({}); // New products structure from API
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedProducts, setExpandedProducts] = useState(new Set());
  const [expandedCountries, setExpandedCountries] = useState(new Set());
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [addingCountryFor, setAddingCountryFor] = useState(null);
  const [products, setProducts] = useState([]);
  const [countries, setCountries] = useState([]);
  const [showDataProvenance, setShowDataProvenance] = useState(null); // For showing underlying data
  
  // Rule history state
  const [showRuleHistory, setShowRuleHistory] = useState(null);
  const [ruleHistory, setRuleHistory] = useState({});
  
  // Edit rule state
  const [showEditModal, setShowEditModal] = useState(false);
  const [editFormData, setEditFormData] = useState({
    rule_id: '',
    product_id: '',
    price_usd: '',
    start_date: '',
    countries: []
  });
  
  // Form state for creating rules
  const [formData, setFormData] = useState({
    product_id: '',
    price_usd: '',
    start_date: '',
    countries: []
  });
  
  // Smart defaults and loading states
  const [suggestedPrice, setSuggestedPrice] = useState(null);
  const [loadingPrice, setLoadingPrice] = useState(false);
  const [countrySelectAll, setCountrySelectAll] = useState(false);

  // Trial activity data from backend
  const [trialActivityData, setTrialActivityData] = useState({});
  
  // Auto-repair notification state
  const [autoRepairNotification, setAutoRepairNotification] = useState(null);

  // Load pricing rules and dropdown data
  useEffect(() => {
    loadPricingRules();
    loadDropdownData();
  }, []);

  // Group rules by product and country when rules change
  useEffect(() => {
    if (rules.length > 0 || missingProducts.length > 0) {
      groupRulesByProductAndCountry();
    }
  }, [rules, missingProducts]);

  // Auto-expand logic when data loads
  useEffect(() => {
    if (Object.keys(productGroups).length > 0) {
      // Keep products collapsed by default - only expand if search matches
      if (searchTerm) {
        const matchingProducts = Object.keys(productGroups).filter(productId =>
          productId.toLowerCase().includes(searchTerm.toLowerCase())
        );
        setExpandedProducts(new Set(matchingProducts));
      } else {
        setExpandedProducts(new Set()); // Collapsed by default
      }
      
      // Countries don't need expand/collapse state anymore - they're always shown when product is expanded
      setExpandedCountries(new Set());
    }
  }, [productGroups, searchTerm]);

  // Keep country select all state in sync
  useEffect(() => {
    if (countries.length > 0) {
      setCountrySelectAll(formData.countries.length === countries.length && formData.countries.length > 0);
    }
  }, [formData.countries, countries]);

  const loadPricingRules = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/pricing/rules');
      const data = await response.json();
      
      if (data.success) {
        setRules(data.data.rules || []);
        // Handle both old and new schema formats
        if (data.data.missing_products) {
          setMissingProducts(data.data.missing_products);
        }
        if (data.data.schema_version) {
          setSchemaVersion(data.data.schema_version);
        }
        // Store the new products structure from API
        if (data.data.products) {
          setProductsStructure(data.data.products);
        }
        // Store trial activity data from backend
        if (data.data.trial_activity_data) {
          setTrialActivityData(data.data.trial_activity_data);
        }
        
        // Handle auto-repair notifications
        if (data.data.auto_repair_count && data.data.auto_repair_count > 0) {
          setAutoRepairNotification({
            type: 'success',
            message: `✅ Auto-repaired ${data.data.auto_repair_count} pricing rules with missing end dates`,
            timestamp: Date.now()
          });
          
          // Auto-hide notification after 8 seconds
          setTimeout(() => {
            setAutoRepairNotification(null);
          }, 8000);
        }
      } else {
        setError(data.error || 'Failed to load pricing rules');
      }
    } catch (err) {
      setError('Network error loading pricing rules');
      console.error('Error loading pricing rules:', err);
    } finally {
      setLoading(false);
    }
  };

  const forceRepairRules = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/pricing/repair-rules', {
        method: 'POST'
      });
      const data = await response.json();
      
      if (data.success) {
        setAutoRepairNotification({
          type: 'success',
          message: `🔧 ${data.message}`,
          timestamp: Date.now()
        });
        
        // Reload data after repair
        setTimeout(() => {
          loadPricingRules();
        }, 1000);
      } else {
        setError(data.error || 'Failed to repair rules');
      }
    } catch (err) {
      setError('Network error during rule repair');
      console.error('Error repairing rules:', err);
    } finally {
      setLoading(false);
    }
  };

  const loadDropdownData = async () => {
    try {
      const [productsRes, countriesRes] = await Promise.all([
        fetch('/api/pricing/products'),
        fetch('/api/pricing/countries')
      ]);
      
      const [productsData, countriesData] = await Promise.all([
        productsRes.json(),
        countriesRes.json()
      ]);

      if (productsData.success && productsData.data && productsData.data.products) {
        const productList = productsData.data.products.map(p => p.product_id);
        setProducts(productList);
      }
      
      if (countriesData.success && countriesData.data && countriesData.data.countries) {
        const countryList = countriesData.data.countries.map(c => c.country_code);
        setCountries(countryList);
      }
    } catch (err) {
      console.error('Error loading dropdown data:', err);
      setProducts([]);
      setCountries([]);
    }
  };

  const loadDataProvenance = async (ruleId) => {
    try {
      const response = await fetch(`/api/pricing/rules/${ruleId}/provenance`);
      const data = await response.json();
      
      if (data.success) {
        setShowDataProvenance({
          ruleId,
          data: data.data
        });
      } else {
        console.error('Failed to load data provenance:', data.error);
      }
    } catch (err) {
      console.error('Error loading data provenance:', err);
    }
  };

  const loadRuleHistory = async (productId) => {
    try {
      const response = await fetch(`/api/pricing/rules/${productId}/history`);
      const data = await response.json();
      
      if (data.success) {
        setShowRuleHistory({
          productId,
          data: data.data
        });
      } else {
        console.error('Failed to load rule history:', data.error);
      }
    } catch (err) {
      console.error('Error loading rule history:', err);
    }
  };

  const openEditModal = (rule) => {
    setEditFormData({
      rule_id: rule.rule_id,
      product_id: rule.product_id,
      price_usd: rule.price_usd.toString(),
      start_date: rule.start_date,
      countries: [...rule.countries]
    });
    setEditingRule(rule);
    setShowEditModal(true);
  };

  const updateRule = async (e) => {
    e.preventDefault();
    
    try {
      const response = await fetch(`/api/pricing/rules/${editFormData.rule_id}?simple_edit=true`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          product_id: editFormData.product_id,
          price_usd: parseFloat(editFormData.price_usd),
          start_date: editFormData.start_date,
          countries: editFormData.countries
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setShowEditModal(false);
        setEditingRule(null);
        setEditFormData({
          rule_id: '',
          product_id: '',
          price_usd: '',
          start_date: '',
          countries: []
        });
        // Reload all data to ensure consistency
        loadPricingRules();
        setError(null);
      } else {
        setError(data.error || 'Failed to update rule');
      }
    } catch (err) {
      setError('Network error updating rule');
      console.error('Error updating rule:', err);
    }
  };

  const groupRulesByProductAndCountry = () => {
    const groups = {};
    
    // Group existing rules by product and country
    rules.forEach(rule => {
      const productId = rule.product_id;
      
      if (!groups[productId]) {
        groups[productId] = {
          totalRules: 0,
          countries: {},
          allCountries: [],
          mostRecentPricingUpdate: null,  // When pricing was last changed (start_date)
          hasMissingRules: false
        };
      }
      
      groups[productId].totalRules++;
      
      // Track most recent pricing update (start_date) - when pricing was actually changed
      const pricingStartDate = new Date(rule.start_date);
      if (!groups[productId].mostRecentPricingUpdate || pricingStartDate > groups[productId].mostRecentPricingUpdate) {
        groups[productId].mostRecentPricingUpdate = pricingStartDate;
      }
      
      rule.countries.forEach(country => {
        if (!groups[productId].countries[country]) {
          groups[productId].countries[country] = {
            rules: [],
            priceRange: { min: Infinity, max: -Infinity },
            hasActive: false,
            hasFuture: false,
            hasHistorical: false
          };
          groups[productId].allCountries.push(country);
        }
        
        const countryData = groups[productId].countries[country];
        countryData.rules.push(rule);
        
        // Update price range
        countryData.priceRange.min = Math.min(countryData.priceRange.min, rule.price_usd);
        countryData.priceRange.max = Math.max(countryData.priceRange.max, rule.price_usd);
        
        // Determine rule status for country
        const now = new Date();
        const startDate = new Date(rule.start_date);
        const endDate = rule.end_date ? new Date(rule.end_date) : null;
        
        if (endDate && now > endDate) {
          countryData.hasHistorical = true;
        } else if (now >= startDate && (!endDate || now <= endDate)) {
          countryData.hasActive = true;
        } else {
          countryData.hasFuture = true;
        }
      });
    });
    
    // Add missing products and update trial activity for all products
    missingProducts.forEach(missingProduct => {
      const productId = missingProduct.product_id;
      const trialActivityDate = new Date(missingProduct.last_trial || missingProduct.last_conversion);
      
      if (!groups[productId]) {
        groups[productId] = {
          totalRules: 0,
          countries: {},
          allCountries: [],
          mostRecentPricingUpdate: trialActivityDate,
          mostRecentTrialActivity: trialActivityDate,
          hasMissingRules: true,
          missingProductInfo: missingProduct
        };
      } else {
        // Product exists with rules - update trial activity and mark as having missing rules if needed
        groups[productId].hasMissingRules = true;
        groups[productId].missingProductInfo = missingProduct;
        
        // Update with trial activity date - this is key for proper sorting
        if (!groups[productId].mostRecentTrialActivity || trialActivityDate > groups[productId].mostRecentTrialActivity) {
          groups[productId].mostRecentTrialActivity = trialActivityDate;
        }
        
        // Also update pricing update if trial activity is more recent
        if (!groups[productId].mostRecentPricingUpdate || trialActivityDate > groups[productId].mostRecentPricingUpdate) {
          groups[productId].mostRecentPricingUpdate = trialActivityDate;
        }
      }
    });
    
    // Update all products with accurate trial activity data from backend
    Object.keys(groups).forEach(productId => {
      if (trialActivityData[productId]) {
        const activityInfo = trialActivityData[productId];
        const activityDate = new Date(activityInfo.most_recent_activity);
        
        // Update with the accurate trial activity date
        groups[productId].mostRecentTrialActivity = activityDate;
        groups[productId].totalTrialEvents = activityInfo.total_events;
        
        // If this product doesn't have pricing rules, also update the pricing update date
        if (groups[productId].totalRules === 0) {
          groups[productId].mostRecentPricingUpdate = activityDate;
        }
      }
    });
    
    // For products with rules but no trial activity data, use pricing date as trial activity
    Object.values(groups).forEach(product => {
      if (!product.mostRecentTrialActivity && product.mostRecentPricingUpdate) {
        product.mostRecentTrialActivity = product.mostRecentPricingUpdate;
      }
    });
    
    // Fix any infinite values in price ranges
    Object.values(groups).forEach(product => {
      Object.values(product.countries).forEach(countryData => {
        if (countryData.priceRange.min === Infinity) {
          countryData.priceRange.min = 0;
        }
        if (countryData.priceRange.max === -Infinity) {
          countryData.priceRange.max = 0;
        }
      });
    });
    
    setProductGroups(groups);
  };

  const toggleProductExpansion = (productId) => {
    const newExpanded = new Set(expandedProducts);
    if (newExpanded.has(productId)) {
      newExpanded.delete(productId);
    } else {
      newExpanded.add(productId);
    }
    setExpandedProducts(newExpanded);
  };

  const deleteRule = async (ruleId) => {
    if (!window.confirm('Are you sure you want to delete this pricing rule?')) {
      return;
    }

    try {
      const response = await fetch(`/api/pricing/rules/${ruleId}?hard_delete=true`, {
        method: 'DELETE'
      });
      
      const data = await response.json();
      
      if (data.success) {
        // Remove the rule from frontend state immediately
        setRules(rules.filter(rule => rule.rule_id !== ruleId));
        // Also reload all data to ensure consistency across all affected areas
        loadPricingRules();
      } else {
        setError(data.error || 'Failed to delete rule');
      }
    } catch (err) {
      setError('Network error deleting rule');
      console.error('Error deleting rule:', err);
    }
  };

  const createRule = async (e) => {
    e.preventDefault();
    
    try {
      const response = await fetch('/api/pricing/rules', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          product_id: formData.product_id,
          price_usd: parseFloat(formData.price_usd),
          start_date: formData.start_date,
          countries: formData.countries
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setShowCreateModal(false);
        setFormData({
          product_id: '',
          price_usd: '',
          start_date: '',
          countries: []
        });
        loadPricingRules();
      } else {
        setError(data.error || 'Failed to create rule');
      }
    } catch (err) {
      setError('Network error creating rule');
      console.error('Error creating rule:', err);
    }
  };

  const roundToNearestPricePoint = (price) => {
    // Round to nearest $5 increment, then subtract $0.01 to make it .99
    const nearestFive = Math.round(price / 5) * 5;
    const finalPrice = nearestFive - 0.01;
    
    // Ensure minimum price of $4.99
    return Math.max(4.99, finalPrice);
  };

  const fetchSuggestedPrice = async (productId) => {
    if (!productId) return;
    
    setLoadingPrice(true);
    try {
      const response = await fetch(`/api/pricing/products/${productId}/most-recent-conversion-price`);
      const data = await response.json();
      
      if (data.success) {
        // Round the suggested price to nearest $5 increment minus $0.01
        const roundedPrice = roundToNearestPricePoint(data.data.suggested_price);
        
        setSuggestedPrice({
          ...data.data,
          suggested_price: roundedPrice,
          original_price: data.data.suggested_price,
          note: `Rounded from $${data.data.suggested_price} to $${roundedPrice} (${data.data.note})`
        });
        setFormData(prev => ({
          ...prev,
          price_usd: roundedPrice.toString()
        }));
      } else {
        setSuggestedPrice(null);
      }
    } catch (err) {
      console.error('Error fetching suggested price:', err);
      setSuggestedPrice(null);
    } finally {
      setLoadingPrice(false);
    }
  };

  const handleCountrySelectAll = () => {
    if (countrySelectAll) {
      // Deselect all
      setFormData({...formData, countries: []});
      setCountrySelectAll(false);
    } else {
      // Select all
      setFormData({...formData, countries: [...countries]});
      setCountrySelectAll(true);
    }
  };

  const getDefaultStartDate = (productId) => {
    // Check if this product has any existing rules
    const existingRules = rules.filter(rule => rule.product_id === productId);
    
    if (existingRules.length === 0) {
      // First rule for this product - default to Jan 1 2024
      return '2024-01-01';
    } else {
      // Subsequent rule - leave empty
      return '';
    }
  };

  const openCreateModal = (productId = '') => {
    const defaultStartDate = productId ? getDefaultStartDate(productId) : '';
    
    setFormData({
      product_id: productId,
      price_usd: '',
      start_date: defaultStartDate,
      countries: [...countries] // Auto-select all countries by default
    });
    
    // Reset smart defaults
    setSuggestedPrice(null);
    setCountrySelectAll(true); // Set to true since we're selecting all countries
    
    // If we have a specific product, fetch suggested price
    if (productId) {
      fetchSuggestedPrice(productId);
    }
    
    setShowCreateModal(true);
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'Ongoing';
    return new Date(dateStr).toLocaleDateString();
  };

  const getRuleStatusColor = (rule, countryRules = null) => {
    const now = new Date();
    const startDate = new Date(rule.start_date);
    let endDate = rule.end_date ? new Date(rule.end_date) : null;

    // If no explicit end date and we have country rules, calculate effective end date
    if (!endDate && countryRules && Array.isArray(countryRules)) {
      // Find the next rule that starts after this rule
      const sortedRules = countryRules
        .filter(r => r.rule_id !== rule.rule_id)  // Exclude current rule
        .sort((a, b) => new Date(a.start_date) - new Date(b.start_date));
      
      const nextRule = sortedRules.find(r => new Date(r.start_date) > startDate);
      if (nextRule) {
        // This rule effectively ends the day before the next rule starts
        const nextStartDate = new Date(nextRule.start_date);
        endDate = new Date(nextStartDate.getTime() - 24 * 60 * 60 * 1000); // Previous day
      }
    }

    if (endDate && now > endDate) {
      return 'bg-gray-100 border-gray-300 text-gray-600'; // Historical
    } else if (now >= startDate && (!endDate || now <= endDate)) {
      return 'bg-green-100 border-green-300 text-green-800'; // Active
    } else {
      return 'bg-blue-100 border-blue-300 text-blue-800'; // Future
    }
  };

  const getRuleStatusLabel = (rule, countryRules = null) => {
    const now = new Date();
    const startDate = new Date(rule.start_date);
    let endDate = rule.end_date ? new Date(rule.end_date) : null;

    // If no explicit end date and we have country rules, calculate effective end date
    if (!endDate && countryRules && Array.isArray(countryRules)) {
      // Find the next rule that starts after this rule
      const sortedRules = countryRules
        .filter(r => r.rule_id !== rule.rule_id)  // Exclude current rule
        .sort((a, b) => new Date(a.start_date) - new Date(b.start_date));
      
      const nextRule = sortedRules.find(r => new Date(r.start_date) > startDate);
      if (nextRule) {
        // This rule effectively ends the day before the next rule starts
        const nextStartDate = new Date(nextRule.start_date);
        endDate = new Date(nextStartDate.getTime() - 24 * 60 * 60 * 1000); // Previous day
      }
    }

    if (endDate && now > endDate) {
      return 'Historical';
    } else if (now >= startDate && (!endDate || now <= endDate)) {
      return 'Active';
    } else {
      return 'Future';
    }
  };

  const formatPriceRange = (countryData) => {
    if (countryData.priceRange.min === countryData.priceRange.max) {
      return `$${countryData.priceRange.min}`;
    }
    return `$${countryData.priceRange.min} - $${countryData.priceRange.max}`;
  };

  const getPriceProgression = (product) => {
    if (!product || !product.countries || Object.keys(product.countries).length === 0) {
      return [];
    }
    
    // Get all rules across all countries for this product
    const allRules = [];
    Object.values(product.countries).forEach(countryData => {
      allRules.push(...countryData.rules);
    });
    
    // Remove duplicates (same rule might appear in multiple countries)
    const uniqueRules = allRules.filter((rule, index, self) => 
      index === self.findIndex(r => r.rule_id === rule.rule_id)
    );
    
    // Sort by start date to show progression
    const sortedRules = uniqueRules.sort((a, b) => 
      new Date(a.start_date) - new Date(b.start_date)
    );
    
    // Create price progression - show unique price points in chronological order
    const priceProgression = [];
    let lastPrice = null;
    
    sortedRules.forEach(rule => {
      if (rule.price_usd !== lastPrice) {
        priceProgression.push({
          price: rule.price_usd,
          date: rule.start_date,
          ruleId: rule.rule_id
        });
        lastPrice = rule.price_usd;
      }
    });
    
    return priceProgression;
  };

  const sortCountriesByRecentActivity = (countries, productCountries) => {
    /**
     * Sort countries by most recent rule update, then alphabetically
     * @param {Array} countries - Array of country codes
     * @param {Object} productCountries - Product countries data with rules
     * @returns {Array} Sorted country codes
     */
    
    // More detailed debugging
    console.log('🔍 sortCountriesByRecentActivity called with:', {
      countriesCount: countries.length,
      firstFewCountries: countries.slice(0, 5),
      hasProductCountries: !!productCountries,
      productCountriesKeys: Object.keys(productCountries || {}).slice(0, 5)
    });
    
    const sortedCountries = countries.sort((a, b) => {
      const countryA = productCountries[a];
      const countryB = productCountries[b];
      
      // Get the most recent rule start date for each country
      const getMostRecentRuleDate = (countryData) => {
        if (!countryData || !countryData.rules || countryData.rules.length === 0) {
          return null;
        }
        // Rules are already sorted with newest first, so take the first one
        return new Date(countryData.rules[0].start_date);
      };
      
      const dateA = getMostRecentRuleDate(countryA);
      const dateB = getMostRecentRuleDate(countryB);
      
      // If both have dates, sort by most recent first
      if (dateA && dateB) {
        const dateDiff = dateB - dateA; // Most recent first
        if (dateDiff !== 0) return dateDiff;
      }
      
      // If only one has a date, prioritize it
      if (dateA && !dateB) return -1;
      if (!dateA && dateB) return 1;
      
      // If both have same date or no dates, sort alphabetically
      return a.localeCompare(b);
    });
    
    // Show the results
    if (countries.includes('US')) {
      console.log('🎯 Final sorted countries (first 10):', sortedCountries.slice(0, 10));
      const usIndex = sortedCountries.indexOf('US');
      const adIndex = sortedCountries.indexOf('AD');
      const aeIndex = sortedCountries.indexOf('AE');
      console.log(`📍 Positions - US: ${usIndex + 1}, AD: ${adIndex + 1}, AE: ${aeIndex + 1}`);
      
      // Show the most recent rule for US, AD, AE
      if (productCountries['US'] && productCountries['US'].rules && productCountries['US'].rules[0]) {
        console.log(`🇺🇸 US most recent rule:`, productCountries['US'].rules[0].start_date, `$${productCountries['US'].rules[0].price_usd}`);
      }
      if (productCountries['AD'] && productCountries['AD'].rules && productCountries['AD'].rules[0]) {
        console.log(`🇦🇩 AD most recent rule:`, productCountries['AD'].rules[0].start_date, `$${productCountries['AD'].rules[0].price_usd}`);
      }
    }
    
    return sortedCountries;
  };

  // Filter and sort products based on search and most recent trial activity
  const filteredProducts = Object.keys(productGroups)
    .filter(productId =>
      productId.toLowerCase().includes(searchTerm.toLowerCase())
    )
    .sort((a, b) => {
      const productA = productGroups[a];
      const productB = productGroups[b];
      
      // First, separate products with rules vs missing rules
      const aMissingRules = productA?.hasMissingRules || false;
      const bMissingRules = productB?.hasMissingRules || false;
      
      // Products with rules come first, missing rules go to bottom
      if (aMissingRules && !bMissingRules) return 1;  // A has missing rules, goes to bottom
      if (!aMissingRules && bMissingRules) return -1; // B has missing rules, goes to bottom
      
      // Within each group, sort by most recent trial activity (more urgent trials first)
      const dateA = productA?.mostRecentTrialActivity;
      const dateB = productB?.mostRecentTrialActivity;
      
      if (!dateA && !dateB) return a.localeCompare(b); // Fallback to alphabetical
      if (!dateA) return 1; // Products without trial dates go to the end of their group
      if (!dateB) return -1;
      
      return dateB - dateA; // Most recent trial activity first within each group
    });

  if (loading) {
    return (
      <div className="container mx-auto p-6">
        <div className="flex justify-center items-center h-64">
          <div className="text-lg">Loading pricing rules...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">Pricing Management</h1>
            <p className="text-gray-600 dark:text-gray-400">
              Manage product pricing across different countries and time periods
            </p>
          </div>
          <div className="text-right">
            <div className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${
              schemaVersion === '2.0.0' 
                ? 'bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-200'
                : 'bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-200'
            }`}>
              Schema v{schemaVersion}
            </div>
            {schemaVersion === '2.0.0' && missingProducts.length > 0 && (
              <div className="mt-1 text-xs text-red-600 dark:text-red-400">
                Enhanced with missing product detection
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Auto-Repair Notification */}
      {autoRepairNotification && (
        <div className="bg-emerald-50 border border-emerald-200 dark:border-emerald-800 text-emerald-800 dark:text-emerald-200 px-4 py-3 rounded-xl mb-4 shadow-sm">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-full bg-emerald-500 flex items-center justify-center">
                <svg className="w-4 h-4 text-white" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
              </div>
              <div>
                <p className="font-semibold">{autoRepairNotification.message}</p>
                <p className="text-sm opacity-80">End dates have been automatically calculated based on newer rules</p>
              </div>
            </div>
            <button 
              onClick={() => setAutoRepairNotification(null)}
              className="text-emerald-600 hover:text-emerald-800 dark:text-emerald-400 dark:hover:text-emerald-300 transition-colors"
            >
              <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </button>
          </div>
        </div>
      )}

      {/* Error Display */}
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
          <button 
            onClick={() => setError(null)}
            className="float-right text-red-700 hover:text-red-900"
          >
            ×
          </button>
        </div>
      )}

      {/* Controls Row */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4 mb-6">
        <div className="flex flex-wrap gap-4 items-center justify-between">
          {/* Search */}
          <div className="flex-1 min-w-64">
            <input
              type="text"
              placeholder="Search by product ID..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700"
            />
          </div>

          {/* Actions */}
          <div className="flex gap-2">
            <button
              onClick={() => openCreateModal()}
              className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md"
            >
              Create Rule
            </button>
            <button
              onClick={forceRepairRules}
              className="bg-orange-600 hover:bg-orange-700 text-white px-4 py-2 rounded-md"
              disabled={loading}
            >
              🔧 Fix "Ongoing" Rules
            </button>
            <button
              onClick={loadPricingRules}
              className="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-md"
            >
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-6 gap-4 mb-6">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className="text-2xl font-bold text-blue-600">{filteredProducts.length}</div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Products</div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className="text-2xl font-bold text-green-600">
            {filteredProducts.reduce((sum, productId) => 
              sum + (productGroups[productId]?.totalRules || 0), 0
            )}
          </div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Total Rules</div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className={`text-2xl font-bold ${
            filteredProducts.filter(productId => productGroups[productId]?.hasMissingRules).length > 0 
              ? 'text-red-600' 
              : 'text-gray-400'
          }`}>
            {filteredProducts.filter(productId => productGroups[productId]?.hasMissingRules).length}
          </div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Missing Rules</div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className="text-2xl font-bold text-purple-600">
            {[...new Set(filteredProducts.flatMap(productId => 
              productGroups[productId]?.allCountries || []
            ))].length}
          </div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Countries</div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className="text-2xl font-bold text-orange-600">
            {(() => {
              const thirtyDaysAgo = new Date();
              thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
              return filteredProducts.filter(productId => {
                const lastUpdate = productGroups[productId]?.mostRecentPricingUpdate;
                return lastUpdate && lastUpdate > thirtyDaysAgo;
              }).length;
            })()}
          </div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Recent Updates</div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-4">
          <div className="text-2xl font-bold text-indigo-600">
            {filteredProducts.filter(productId => expandedProducts.has(productId)).length}
          </div>
          <div className="text-sm text-gray-600 dark:text-gray-400">Expanded</div>
        </div>
      </div>

      {/* Products List */}
      <div className="space-y-4">
        {filteredProducts.length === 0 ? (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-8 text-center text-gray-500 dark:text-gray-400">
            {Object.keys(productGroups).length === 0 ? 'No pricing rules found' : 'No products match your search criteria'}
          </div>
        ) : (
          filteredProducts.map(productId => {
            const product = productGroups[productId];
            const isExpanded = expandedProducts.has(productId);
            
            return (
              <div key={productId} className={`group relative overflow-hidden transition-all duration-300 ease-out ${
                product.hasMissingRules 
                  ? 'bg-gradient-to-br from-rose-50 to-red-50 dark:from-red-950/30 dark:to-red-900/20 border-2 border-rose-200 dark:border-red-800/50 shadow-lg shadow-red-100/50 dark:shadow-red-900/20' 
                  : 'bg-white dark:bg-slate-900 border border-slate-200/60 dark:border-slate-700/50 shadow-sm hover:shadow-xl hover:shadow-slate-200/30 dark:hover:shadow-slate-900/30'
              } rounded-2xl backdrop-blur-sm`}>
                {/* Product Header */}
                <div 
                  className={`relative p-6 border-b cursor-pointer transition-all duration-200 ${
                    product.hasMissingRules
                      ? 'border-rose-200/70 dark:border-red-800/40 hover:bg-gradient-to-br hover:from-rose-100 hover:to-red-100 dark:hover:from-red-950/40 dark:hover:to-red-900/30'
                      : 'border-slate-200/60 dark:border-slate-700/50 hover:bg-slate-50/70 dark:hover:bg-slate-800/50'
                  }`}
                  onClick={() => toggleProductExpansion(productId)}
                >
                  {/* Critical Alert Banner for Missing Rules */}
                  {product.hasMissingRules && (
                    <div className="absolute top-0 left-0 right-0 bg-gradient-to-r from-rose-500 to-red-500 text-white text-xs font-semibold px-4 py-2 flex items-center gap-2">
                      <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                      </svg>
                      <span>MISSING PRICING RULES — URGENT ACTION REQUIRED</span>
                    </div>
                  )}
                  
                  <div className={`${product.hasMissingRules ? 'mt-8' : ''} flex items-center justify-between`}>
                    <div className="flex-1 min-w-0">
                      {/* Product ID with sophisticated typography */}
                      <div className="flex items-center gap-4 mb-3">
                        <h3 className={`text-2xl font-bold tracking-tight ${
                          product.hasMissingRules 
                            ? 'text-red-900 dark:text-red-100' 
                            : 'text-slate-900 dark:text-slate-100'
                        }`}>
                          {productId}
                        </h3>
                        
                        {/* Enhanced status indicators */}
                        <div className="flex items-center gap-2">
                          <div className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold transition-all ${
                            product.hasMissingRules
                              ? 'bg-red-500 text-white shadow-md'
                              : 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300'
                          }`}>
                            {product.hasMissingRules ? (
                              <>
                                <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                                  <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                                </svg>
                                Missing Rules
                              </>
                            ) : (
                              <>
                                <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                                </svg>
                                Active Rules
                              </>
                            )}
                          </div>
                        </div>
                      </div>
                      
                      {/* Missing rules detailed alert */}
                      {product.hasMissingRules && product.missingProductInfo ? (
                        <div className="mb-4 p-4 bg-white/90 dark:bg-slate-800/90 backdrop-blur-sm border border-red-200/50 dark:border-red-800/50 rounded-xl shadow-sm">
                          <div className="space-y-2">
                            <div className="flex items-start gap-3">
                              <svg className="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                                <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                              </svg>
                              <div>
                                <p className="text-sm font-semibold text-red-900 dark:text-red-100 mb-1">
                                  Active trials without pricing rules detected
                                </p>
                                <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-red-800 dark:text-red-200">
                                  <div><span className="font-medium">{product.missingProductInfo.trial_count}</span> trial events</div>
                                  <div>First: <span className="font-medium">{formatDate(product.missingProductInfo.first_trial)}</span></div>
                                  <div>Latest: <span className="font-medium">{formatDate(product.missingProductInfo.last_trial)}</span></div>
                                  <div className="col-span-2 mt-1 text-red-700 dark:text-red-300 font-medium">
                                    ⚠️ Revenue analysis incomplete without pricing rules
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      ) : (
                        /* Normal product metrics */
                        <div className="flex items-center gap-6 mb-4">
                          <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                            <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                              <span className="font-bold text-slate-900 dark:text-slate-100">{product.totalRules}</span> rules
                            </span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-purple-500"></div>
                            <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                              <span className="font-bold text-slate-900 dark:text-slate-100">{product.allCountries.length}</span> countries
                            </span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
                            <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                              Last activity: <span className="font-bold text-slate-900 dark:text-slate-100">{formatDate(product.mostRecentTrialActivity?.toISOString().split('T')[0])}</span>
                            </span>
                          </div>
                        </div>
                      )}
                      
                      {/* Enhanced country tags - only show if not missing rules */}
                      {!product.hasMissingRules && product.allCountries.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 mb-4">
                          {sortCountriesByRecentActivity([...product.allCountries], product.countries).slice(0, 12).map(country => (
                            <span 
                              key={country}
                              className="inline-flex items-center px-2.5 py-1 rounded-lg text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200/50 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-800/50 transition-colors hover:bg-blue-100 dark:hover:bg-blue-900/50"
                            >
                              {country}
                            </span>
                          ))}
                          {product.allCountries.length > 12 && (
                            <span className="inline-flex items-center px-2.5 py-1 rounded-lg text-xs font-medium bg-slate-100 text-slate-600 border border-slate-200/50 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-700">
                              +{product.allCountries.length - 12} more
                            </span>
                          )}
                        </div>
                      )}

                      {/* Beautiful Price Progression - only show if not missing rules */}
                      {!product.hasMissingRules && (() => {
                        const priceProgression = getPriceProgression(product);
                        return priceProgression.length > 0 && (
                          <div className="p-4 bg-gradient-to-br from-slate-50 to-slate-100/50 dark:from-slate-800/50 dark:to-slate-900/50 rounded-xl border border-slate-200/50 dark:border-slate-700/50 backdrop-blur-sm">
                            <div className="flex items-center gap-2 mb-3">
                              <div className="flex items-center gap-2">
                                <div className="w-6 h-6 rounded-lg bg-gradient-to-br from-emerald-500 to-green-600 flex items-center justify-center">
                                  <svg className="w-3.5 h-3.5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                                  </svg>
                                </div>
                                <h4 className="text-sm font-bold text-slate-800 dark:text-slate-200">Price Evolution</h4>
                              </div>
                              <div className="flex-1"></div>
                              <span className="text-xs font-medium text-slate-500 dark:text-slate-400 bg-white/70 dark:bg-slate-800/70 px-2 py-1 rounded-md">
                                {priceProgression.length} changes
                              </span>
                            </div>
                            <div className="flex items-center gap-3 overflow-x-auto pb-2" style={{scrollbarWidth: 'thin'}}>
                              {priceProgression.map((point, index) => (
                                <div key={point.ruleId} className="flex items-center gap-3 flex-shrink-0">
                                  <div className="text-center group">
                                    <div className="relative">
                                      <div className="text-xl font-black text-emerald-700 dark:text-emerald-400 mb-0.5 group-hover:scale-105 transition-transform">
                                        ${point.price}
                                      </div>
                                      <div className="text-xs font-medium text-slate-500 dark:text-slate-400 bg-white/80 dark:bg-slate-800/80 px-2 py-0.5 rounded-md shadow-sm">
                                        {formatDate(point.date)}
                                      </div>
                                    </div>
                                  </div>
                                  {index < priceProgression.length - 1 && (
                                    <div className="flex items-center">
                                      <div className="w-8 h-0.5 bg-gradient-to-r from-slate-300 to-slate-400 dark:from-slate-600 dark:to-slate-500 rounded-full"></div>
                                      <svg className="w-3 h-3 text-slate-400 dark:text-slate-500 -ml-1" fill="currentColor" viewBox="0 0 20 20">
                                        <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd" />
                                      </svg>
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      })()}
                    </div>
                    
                    {/* Enhanced action buttons */}
                    <div className="flex items-center gap-3 ml-6">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          openCreateModal(productId);
                        }}
                        className={`group relative overflow-hidden px-5 py-2.5 rounded-xl text-sm font-semibold transition-all duration-200 transform hover:scale-105 active:scale-95 shadow-lg ${
                          product.hasMissingRules
                            ? 'bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white shadow-red-200 dark:shadow-red-900/30'
                            : 'bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700 text-white shadow-blue-200 dark:shadow-blue-900/30'
                        }`}
                      >
                        <div className="relative z-10 flex items-center gap-2">
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
                          </svg>
                          {product.hasMissingRules ? 'Create First Rule' : 'Add Rule'}
                        </div>
                      </button>
                      
                      {/* Show Rules History Button */}
                      {!product.hasMissingRules && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            loadRuleHistory(productId);
                          }}
                          className="group relative overflow-hidden px-5 py-2.5 rounded-xl text-sm font-semibold transition-all duration-200 transform hover:scale-105 active:scale-95 bg-gradient-to-r from-slate-600 to-slate-700 hover:from-slate-700 hover:to-slate-800 text-white shadow-lg shadow-slate-200 dark:shadow-slate-900/30"
                        >
                          <div className="relative z-10 flex items-center gap-2">
                            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                            </svg>
                            History
                          </div>
                        </button>
                      )}
                      
                      {/* Elegant expand/collapse indicator */}
                      <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-slate-100 dark:bg-slate-800 group-hover:bg-slate-200 dark:group-hover:bg-slate-700 transition-colors">
                        <svg 
                          className={`w-5 h-5 text-slate-600 dark:text-slate-400 transform transition-transform duration-200 ${isExpanded ? 'rotate-180' : ''}`}
                          fill="none" 
                          stroke="currentColor" 
                          viewBox="0 0 24 24"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                        </svg>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Country Breakdown */}
                {isExpanded && (
                  <div className="p-6 bg-gradient-to-br from-slate-50/50 to-white dark:from-slate-800/50 dark:to-slate-900/50">
                    <div className="space-y-6">
                      {sortCountriesByRecentActivity([...product.allCountries], product.countries).map(country => {
                        const countryData = product.countries[country];
                        
                        return (
                          <div key={country} className="group relative overflow-hidden bg-white dark:bg-slate-800/90 border border-slate-200/60 dark:border-slate-700/50 rounded-2xl shadow-sm hover:shadow-lg transition-all duration-300 backdrop-blur-sm">
                            {/* Country Header */}
                            <div className="relative p-6 bg-gradient-to-r from-slate-50 via-white to-slate-50 dark:from-slate-800/80 dark:via-slate-800/60 dark:to-slate-800/80 border-b border-slate-200/60 dark:border-slate-700/50">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-6">
                                  <div className="flex items-center gap-3">
                                    <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-blue-200/50 dark:shadow-blue-900/30">
                                      <span className="text-white font-bold text-lg">{country.slice(0, 2).toUpperCase()}</span>
                                    </div>
                                    <div>
                                      <h4 className="text-2xl font-bold text-slate-900 dark:text-slate-100">{country}</h4>
                                      <p className="text-sm text-slate-600 dark:text-slate-400">Pricing timeline</p>
                                    </div>
                                  </div>
                                  
                                  <div className="flex items-center gap-4">
                                    <div className="flex items-center gap-3 px-4 py-2 bg-white/70 dark:bg-slate-800/70 backdrop-blur-sm rounded-xl border border-slate-200/50 dark:border-slate-700/50 shadow-sm">
                                      <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                                      <span className="text-sm font-semibold text-slate-700 dark:text-slate-300">
                                        {countryData.rules.length} rules
                                      </span>
                                    </div>
                                    
                                    <div className="flex items-center gap-3 px-4 py-2 bg-gradient-to-r from-emerald-50 to-green-50 dark:from-emerald-900/30 dark:to-green-900/30 backdrop-blur-sm rounded-xl border border-emerald-200/50 dark:border-emerald-800/50 shadow-sm">
                                      <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
                                      <span className="text-lg font-black text-emerald-700 dark:text-emerald-400">
                                        {formatPriceRange(countryData)}
                                      </span>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </div>

                            {/* Enhanced Price Timeline */}
                            <div className="p-6">
                              <div className="flex items-center gap-3 mb-6">
                                <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-slate-600 to-slate-700 flex items-center justify-center shadow-md">
                                  <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                                  </svg>
                                </div>
                                <h5 className="text-lg font-bold text-slate-800 dark:text-slate-200">
                                  Chronological Pricing History
                                </h5>
                                <div className="flex-1 h-px bg-gradient-to-r from-slate-200 to-transparent dark:from-slate-700"></div>
                              </div>
                              
                              {/* Horizontal Timeline Cards */}
                              <div className="relative">
                                <div className="flex gap-6 overflow-x-auto pb-4 scrollbar-hide" style={{scrollbarWidth: 'none'}}>
                                  {countryData.rules.map((rule, index) => {
                                    const status = getRuleStatusLabel(rule, countryData.rules);
                                    const isActive = status === 'Active';
                                    const isFuture = status === 'Future';
                                    const isHistorical = status === 'Historical';
                                    
                                    return (
                                      <div 
                                        key={rule.rule_id}
                                        className={`relative flex-shrink-0 w-80 group/card transition-all duration-300 hover:scale-105`}
                                      >
                                        {/* Timeline Connector */}
                                        {index < countryData.rules.length - 1 && (
                                          <div className="absolute top-1/2 -right-3 z-10 w-6 h-px bg-gradient-to-r from-slate-300 via-slate-400 to-slate-300 dark:from-slate-600 dark:via-slate-500 dark:to-slate-600"></div>
                                        )}
                                        
                                        <div className={`relative overflow-hidden rounded-2xl shadow-lg transition-all duration-300 group-hover/card:shadow-xl ${
                                          isActive 
                                            ? 'bg-gradient-to-br from-emerald-50 via-green-50 to-emerald-100 dark:from-emerald-950/50 dark:via-emerald-900/30 dark:to-green-900/40 border-2 border-emerald-300/60 dark:border-emerald-700/50 shadow-emerald-200/50 dark:shadow-emerald-900/30' 
                                            : isFuture
                                            ? 'bg-gradient-to-br from-blue-50 via-sky-50 to-blue-100 dark:from-blue-950/50 dark:via-blue-900/30 dark:to-sky-900/40 border-2 border-blue-300/60 dark:border-blue-700/50 shadow-blue-200/50 dark:shadow-blue-900/30'
                                            : 'bg-gradient-to-br from-slate-50 via-gray-50 to-slate-100 dark:from-slate-800/50 dark:via-slate-900/30 dark:to-slate-800/40 border-2 border-slate-300/60 dark:border-slate-700/50 shadow-slate-200/50 dark:shadow-slate-900/30'
                                        }`}>
                                          {/* Status Indicator */}
                                          <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-transparent via-current to-transparent opacity-60"></div>
                                          
                                          <div className="p-6">
                                            {/* Rule Header */}
                                            <div className="flex items-center justify-between mb-4">
                                              <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-xl text-xs font-bold shadow-sm ${
                                                isActive 
                                                  ? 'bg-emerald-500 text-white'
                                                  : isFuture
                                                  ? 'bg-blue-500 text-white'
                                                  : 'bg-slate-500 text-white'
                                              }`}>
                                                {isActive && <div className="w-2 h-2 bg-white rounded-full animate-pulse"></div>}
                                                {isFuture && <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z" clipRule="evenodd" /></svg>}
                                                {isHistorical && <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clipRule="evenodd" /></svg>}
                                                {status.toUpperCase()}
                                              </div>
                                              
                                              <div className="text-xs font-mono text-slate-500 dark:text-slate-400 bg-white/80 dark:bg-slate-800/80 px-2 py-1 rounded-lg">
                                                {rule.rule_id.slice(-8)}
                                              </div>
                                            </div>
                                            
                                            {/* Price Display */}
                                            <div className="mb-6">
                                              <div className="text-4xl font-black text-slate-900 dark:text-slate-100 mb-2 tracking-tight">
                                                ${rule.price_usd}
                                              </div>
                                              <div className={`text-sm font-semibold ${
                                                isActive 
                                                  ? 'text-emerald-700 dark:text-emerald-300' 
                                                  : isFuture
                                                  ? 'text-blue-700 dark:text-blue-300'
                                                  : 'text-slate-600 dark:text-slate-400'
                                              }`}>
                                                {formatDate(rule.start_date)} 
                                                <span className="mx-2 opacity-60">→</span> 
                                                {formatDate(rule.end_date) || (
                                                  <span className={`inline-flex items-center gap-1 ${isActive ? 'text-emerald-600' : 'text-slate-500'}`}>
                                                    {isActive && <div className="w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse"></div>}
                                                    Ongoing
                                                  </span>
                                                )}
                                              </div>
                                            </div>
                                            
                                            {/* Action Button */}
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                loadDataProvenance(rule.rule_id);
                                              }}
                                              className={`w-full group/btn relative overflow-hidden px-4 py-3 rounded-xl text-sm font-semibold transition-all duration-200 transform hover:scale-105 active:scale-95 shadow-md hover:shadow-lg ${
                                                isActive
                                                  ? 'bg-emerald-600 hover:bg-emerald-700 text-white shadow-emerald-200 dark:shadow-emerald-900/50'
                                                  : isFuture
                                                  ? 'bg-blue-600 hover:bg-blue-700 text-white shadow-blue-200 dark:shadow-blue-900/50'
                                                  : 'bg-slate-600 hover:bg-slate-700 text-white shadow-slate-200 dark:shadow-slate-900/50'
                                              }`}
                                            >
                                              <div className="relative z-10 flex items-center justify-center gap-2">
                                                <svg className="w-4 h-4 transition-transform group-hover/btn:scale-110" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                                                </svg>
                                                View Data Source
                                              </div>
                                            </button>
                                          </div>
                                        </div>
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* Footer Info */}
      <div className="mt-6 text-sm text-gray-500 dark:text-gray-400 text-center">
        Pricing rules are organized by product and country for easy market management.
        <br />
        <span className="inline-flex items-center gap-4 mt-2">
          <span className="flex items-center gap-1">
            <div className="w-3 h-3 bg-green-100 border border-green-300 rounded"></div>
            Active
          </span>
          <span className="flex items-center gap-1">
            <div className="w-3 h-3 bg-blue-100 border border-blue-300 rounded"></div>
            Future
          </span>
          <span className="flex items-center gap-1">
            <div className="w-3 h-3 bg-gray-100 border border-gray-300 rounded"></div>
            Historical
          </span>
        </span>
      </div>

      {/* Create Rule Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-2xl w-full mx-4 max-h-[90vh] overflow-hidden">
            {/* Modal Header */}
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-bold text-gray-900 dark:text-gray-100">
                  ➕ Create New Pricing Rule
                </h3>
                <button
                  onClick={() => setShowCreateModal(false)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-2">
                Create a new pricing rule for a product and countries
              </p>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto max-h-[70vh]">
              <form onSubmit={createRule} className="space-y-6">
                {/* Product Selection */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Product ID *
                  </label>
                  <select
                    value={formData.product_id}
                    onChange={(e) => {
                      const newProductId = e.target.value;
                      setFormData({...formData, product_id: newProductId, start_date: getDefaultStartDate(newProductId)});
                      if (newProductId) {
                        fetchSuggestedPrice(newProductId);
                      } else {
                        setSuggestedPrice(null);
                      }
                    }}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    required
                  >
                    <option value="">Select a product...</option>
                    {products.map(product => (
                      <option key={product} value={product}>{product}</option>
                    ))}
                  </select>
                  
                  {/* Product hint for missing rules */}
                  {formData.product_id && productGroups[formData.product_id]?.hasMissingRules && (
                    <div className="mt-2 p-3 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg">
                      <div className="flex items-center gap-2 text-amber-800 dark:text-amber-200 text-sm">
                        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                          <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                        </svg>
                        <span className="font-medium">First pricing rule for this product!</span>
                      </div>
                      {productGroups[formData.product_id]?.missingProductInfo && (
                        <div className="mt-1 text-xs text-amber-700 dark:text-amber-300">
                          This product has {productGroups[formData.product_id].missingProductInfo.trial_count} trial events but no pricing rules yet.
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Price with Smart Suggestions */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Price (USD) *
                  </label>
                  <div className="relative">
                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                      <span className="text-gray-500 dark:text-gray-400">$</span>
                    </div>
                    <input
                      type="number"
                      step="0.01"
                      min="0"
                      value={formData.price_usd}
                      onChange={(e) => setFormData({...formData, price_usd: e.target.value})}
                      className="w-full pl-8 pr-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                      placeholder="29.99"
                      required
                    />
                    {loadingPrice && (
                      <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
                        <svg className="animate-spin h-4 w-4 text-gray-400" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                      </div>
                    )}
                  </div>
                  
                  {/* Smart Price Suggestion */}
                  {suggestedPrice && (
                    <div className="mt-2 p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2 text-green-800 dark:text-green-200 text-sm">
                          <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                          </svg>
                          <span className="font-medium">
                            Suggested: ${suggestedPrice.suggested_price} from most recent conversion
                          </span>
                        </div>
                        <button
                          type="button"
                          onClick={() => setFormData({...formData, price_usd: suggestedPrice.suggested_price.toString()})}
                          className="text-xs bg-green-600 hover:bg-green-700 text-white px-2 py-1 rounded"
                        >
                          Use This
                        </button>
                      </div>
                      <div className="mt-1 text-xs text-green-700 dark:text-green-300">
                        {suggestedPrice.note}
                      </div>
                    </div>
                  )}
                </div>

                {/* Start Date with Smart Default */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Start Date *
                  </label>
                  <input
                    type="date"
                    value={formData.start_date}
                    onChange={(e) => setFormData({...formData, start_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    required
                  />
                  
                  {/* Date context */}
                  {formData.product_id && (
                    <div className="mt-2 text-xs text-gray-600 dark:text-gray-400">
                      {rules.filter(rule => rule.product_id === formData.product_id).length === 0 
                        ? "💡 First rule for this product - defaulted to Jan 1, 2024"
                        : "📅 Additional rule - choose when this pricing takes effect"
                      }
                    </div>
                  )}
                </div>

                {/* Countries with Select All */}
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
                      Countries *
                    </label>
                    <button
                      type="button"
                      onClick={handleCountrySelectAll}
                      className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 font-medium"
                    >
                      {countrySelectAll ? 'Deselect All' : 'Select All'} ({countries.length})
                    </button>
                  </div>
                  
                  <div className="max-h-40 overflow-y-auto border border-gray-300 dark:border-gray-600 rounded-md p-3 bg-white dark:bg-gray-700 space-y-2">
                    {countries.map(country => (
                      <label key={country} className="flex items-center space-x-3 text-sm hover:bg-gray-50 dark:hover:bg-gray-600 p-1 rounded cursor-pointer">
                        <input
                          type="checkbox"
                          checked={formData.countries.includes(country)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              const newCountries = [...formData.countries, country];
                              setFormData({...formData, countries: newCountries});
                              setCountrySelectAll(newCountries.length === countries.length);
                            } else {
                              const newCountries = formData.countries.filter(c => c !== country);
                              setFormData({...formData, countries: newCountries});
                              setCountrySelectAll(false);
                            }
                          }}
                          className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500 dark:focus:ring-blue-600 dark:ring-offset-gray-700 dark:focus:ring-offset-gray-700 focus:ring-2 dark:bg-gray-600 dark:border-gray-500"
                        />
                        <span className="text-gray-900 dark:text-gray-100 font-medium">{country}</span>
                      </label>
                    ))}
                  </div>
                  
                  <div className="mt-2 flex items-center justify-between text-xs">
                    <span className="text-gray-500 dark:text-gray-400">
                      {formData.countries.length} of {countries.length} countries selected
                    </span>
                    {formData.countries.length > 0 && (
                      <span className="text-blue-600 dark:text-blue-400 font-medium">
                        ✓ Ready to create rule
                      </span>
                    )}
                  </div>
                </div>

                {/* Form Actions */}
                <div className="flex gap-3 pt-6 border-t border-gray-200 dark:border-gray-700">
                  <button
                    type="submit"
                    disabled={!formData.product_id || !formData.price_usd || !formData.start_date || formData.countries.length === 0}
                    className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-white px-6 py-3 rounded-lg font-medium transition-colors shadow-lg hover:shadow-xl"
                  >
                    Create Pricing Rule
                  </button>
                  <button
                    type="button"
                    onClick={() => setShowCreateModal(false)}
                    className="flex-1 bg-gray-500 hover:bg-gray-600 text-white px-6 py-3 rounded-lg font-medium transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* Edit Rule Modal */}
      {showEditModal && editingRule && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-2xl w-full mx-4 max-h-[90vh] overflow-hidden">
            {/* Modal Header */}
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-bold text-gray-900 dark:text-gray-100">
                  ✏️ Edit Pricing Rule
                </h3>
                <button
                  onClick={() => {
                    setShowEditModal(false);
                    setEditingRule(null);
                  }}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-2">
                Modify pricing rule for {editFormData.product_id} • Rule ID: {editFormData.rule_id}
              </p>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto max-h-[70vh]">
              <form onSubmit={updateRule} className="space-y-6">
                {/* Rule ID Display */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Rule ID (Read-only)
                  </label>
                  <input
                    type="text"
                    value={editFormData.rule_id}
                    disabled
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-gray-100 dark:bg-gray-600 text-gray-500 dark:text-gray-400"
                  />
                </div>

                {/* Product ID Display (non-editable for safety) */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Product ID (Read-only)
                  </label>
                  <input
                    type="text"
                    value={editFormData.product_id}
                    disabled
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-gray-100 dark:bg-gray-600 text-gray-500 dark:text-gray-400"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Product ID cannot be changed to maintain data integrity
                  </p>
                </div>

                {/* Price */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Price (USD) *
                  </label>
                  <div className="relative">
                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                      <span className="text-gray-500 dark:text-gray-400">$</span>
                    </div>
                    <input
                      type="number"
                      step="0.01"
                      min="0"
                      value={editFormData.price_usd}
                      onChange={(e) => setEditFormData({...editFormData, price_usd: e.target.value})}
                      className="w-full pl-8 pr-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                      required
                    />
                  </div>
                  {editingRule && editFormData.price_usd !== editingRule.price_usd.toString() && (
                    <div className="mt-2 text-sm text-amber-600 dark:text-amber-400">
                      ⚠️ Changing from ${editingRule.price_usd} to ${editFormData.price_usd}
                    </div>
                  )}
                </div>

                {/* Start Date */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Start Date *
                  </label>
                  <input
                    type="date"
                    value={editFormData.start_date}
                    onChange={(e) => setEditFormData({...editFormData, start_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    required
                  />
                  {editingRule && editFormData.start_date !== editingRule.start_date && (
                    <div className="mt-2 text-sm text-amber-600 dark:text-amber-400">
                      ⚠️ Changing from {formatDate(editingRule.start_date)} to {formatDate(editFormData.start_date)}
                    </div>
                  )}
                </div>

                {/* Countries */}
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
                      Countries *
                    </label>
                    <button
                      type="button"
                      onClick={() => {
                        if (editFormData.countries.length === countries.length) {
                          setEditFormData({...editFormData, countries: []});
                        } else {
                          setEditFormData({...editFormData, countries: [...countries]});
                        }
                      }}
                      className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 font-medium"
                    >
                      {editFormData.countries.length === countries.length ? 'Deselect All' : 'Select All'} ({countries.length})
                    </button>
                  </div>
                  
                  <div className="max-h-40 overflow-y-auto border border-gray-300 dark:border-gray-600 rounded-md p-3 bg-white dark:bg-gray-700 space-y-2">
                    {countries.map(country => (
                      <label key={country} className="flex items-center space-x-3 text-sm hover:bg-gray-50 dark:hover:bg-gray-600 p-1 rounded cursor-pointer">
                        <input
                          type="checkbox"
                          checked={editFormData.countries.includes(country)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setEditFormData({...editFormData, countries: [...editFormData.countries, country]});
                            } else {
                              setEditFormData({...editFormData, countries: editFormData.countries.filter(c => c !== country)});
                            }
                          }}
                          className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                        />
                        <span className="text-gray-900 dark:text-gray-100 font-medium">{country}</span>
                      </label>
                    ))}
                  </div>
                  
                  <div className="mt-2 flex items-center justify-between text-xs">
                    <span className="text-gray-500 dark:text-gray-400">
                      {editFormData.countries.length} of {countries.length} countries selected
                    </span>
                    {editingRule && JSON.stringify(editFormData.countries.sort()) !== JSON.stringify(editingRule.countries.sort()) && (
                      <span className="text-amber-600 dark:text-amber-400 font-medium">
                        ⚠️ Countries modified
                      </span>
                    )}
                  </div>
                </div>

                {/* Change Summary */}
                {editingRule && (
                  editFormData.price_usd !== editingRule.price_usd.toString() ||
                  editFormData.start_date !== editingRule.start_date ||
                  JSON.stringify(editFormData.countries.sort()) !== JSON.stringify(editingRule.countries.sort())
                ) && (
                  <div className="p-4 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg">
                    <h4 className="font-medium text-amber-800 dark:text-amber-200 mb-2">
                      📝 Changes Summary
                    </h4>
                    <div className="text-sm text-amber-700 dark:text-amber-300 space-y-1">
                      {editFormData.price_usd !== editingRule.price_usd.toString() && (
                        <div>• Price: ${editingRule.price_usd} → ${editFormData.price_usd}</div>
                      )}
                      {editFormData.start_date !== editingRule.start_date && (
                        <div>• Start Date: {formatDate(editingRule.start_date)} → {formatDate(editFormData.start_date)}</div>
                      )}
                      {JSON.stringify(editFormData.countries.sort()) !== JSON.stringify(editingRule.countries.sort()) && (
                        <div>• Countries: {editingRule.countries.length} → {editFormData.countries.length} selected</div>
                      )}
                    </div>
                  </div>
                )}

                {/* Form Actions */}
                <div className="flex gap-3 pt-6 border-t border-gray-200 dark:border-gray-700">
                  <button
                    type="submit"
                    disabled={!editFormData.price_usd || !editFormData.start_date || editFormData.countries.length === 0}
                    className="flex-1 bg-green-600 hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-white px-6 py-3 rounded-lg font-medium transition-colors shadow-lg hover:shadow-xl"
                  >
                    💾 Save Changes
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setShowEditModal(false);
                      setEditingRule(null);
                    }}
                    className="flex-1 bg-gray-500 hover:bg-gray-600 text-white px-6 py-3 rounded-lg font-medium transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* Data Provenance Modal */}
      {showDataProvenance && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-4xl max-h-[80vh] overflow-hidden">
            {/* Modal Header */}
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-bold text-gray-900 dark:text-gray-100">
                  📊 Data Provenance: Rule {showDataProvenance.ruleId}
                </h3>
                <button
                  onClick={() => setShowDataProvenance(null)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-2">
                This shows the underlying data that led to the creation of this pricing rule
              </p>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto max-h-[60vh]">
              {showDataProvenance.data ? (
                <div className="space-y-6">
                  {/* Rule Summary */}
                  <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
                    <h4 className="font-semibold text-blue-900 dark:text-blue-200 mb-2">Rule Summary</h4>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="font-medium">Product:</span> {showDataProvenance.data.product_id}
                      </div>
                      <div>
                        <span className="font-medium">Countries:</span> {showDataProvenance.data.countries?.join(', ')}
                      </div>
                      <div>
                        <span className="font-medium">USD Price:</span> ${showDataProvenance.data.price_usd}
                      </div>
                      <div>
                        <span className="font-medium">Date Range:</span> {showDataProvenance.data.start_date} → {showDataProvenance.data.end_date || 'ongoing'}
                      </div>
                    </div>
                  </div>

                  {/* Underlying Events */}
                  <div>
                    <h4 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">
                      🔍 Underlying Events ({showDataProvenance.data.events_analyzed || 0} events)
                    </h4>
                    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
                      <div className="text-sm text-gray-600 dark:text-gray-400 mb-3">
                        These are the actual pricing events from your database that led to this rule:
                      </div>
                      
                      {/* Events Table */}
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="bg-gray-200 dark:bg-gray-700">
                              <th className="p-2 text-left">Trial Start</th>
                              <th className="p-2 text-left">Conversion</th>
                              <th className="p-2 text-left">Actual Price</th>
                              <th className="p-2 text-left">Country</th>
                              <th className="p-2 text-left">User ID</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(showDataProvenance.data.sample_events || []).map((event, index) => (
                              <tr key={index} className="border-b border-gray-200 dark:border-gray-600">
                                <td className="p-2">{event.trial_start_date}</td>
                                <td className="p-2">{event.conversion_date}</td>
                                <td className="p-2 font-mono">${event.actual_price}</td>
                                <td className="p-2">{event.country}</td>
                                <td className="p-2 font-mono text-xs">{event.user_id?.substring(0, 8)}...</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>

                  {/* Price Analysis */}
                  <div>
                    <h4 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">
                      💰 Price Analysis
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      {/* Actual Prices Seen */}
                      <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
                        <h5 className="font-medium text-green-900 dark:text-green-200 mb-2">Actual Prices Charged</h5>
                        <div className="text-sm space-y-1">
                          {(showDataProvenance.data.actual_prices || []).map((price, index) => (
                            <div key={index} className="flex justify-between">
                              <span className="font-mono">${price}</span>
                              <span className="text-xs text-gray-500">
                                {showDataProvenance.data.price_counts?.[price] || 0} events
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>

                      {/* Clustering Logic */}
                      <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-lg p-4">
                        <h5 className="font-medium text-yellow-900 dark:text-yellow-200 mb-2">USD Clustering</h5>
                        <div className="text-sm space-y-2">
                          <div>
                            <span className="font-medium">Target USD:</span> ${showDataProvenance.data.price_usd}
                          </div>
                          <div>
                            <span className="font-medium">Price Range:</span> ${showDataProvenance.data.price_range?.min || 0} - ${showDataProvenance.data.price_range?.max || 0}
                          </div>
                          <div>
                            <span className="font-medium">Variance:</span> {showDataProvenance.data.price_variance || 0}%
                          </div>
                          <div className="text-xs text-gray-600 dark:text-gray-400">
                            Prices were clustered within 15% tolerance to account for FX fluctuations and rounding
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Detection Logic */}
                  <div>
                    <h4 className="font-semibold text-gray-900 dark:text-gray-100 mb-3">
                      🔬 Detection Logic
                    </h4>
                    <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
                      <div className="text-sm space-y-2">
                        <div>
                          <span className="font-medium">Method:</span> {showDataProvenance.data.detection_method || 'USD price clustering'}
                        </div>
                        <div>
                          <span className="font-medium">Confidence:</span> {showDataProvenance.data.confidence_score || 'High'}
                        </div>
                        <div>
                          <span className="font-medium">Notes:</span> {showDataProvenance.data.notes || 'Auto-generated from conversion events'}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8">
                  <div className="text-gray-500 dark:text-gray-400">
                    Loading data provenance information...
                  </div>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="p-6 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900">
              <div className="flex justify-end">
                <button
                  onClick={() => setShowDataProvenance(null)}
                  className="px-4 py-2 bg-gray-600 hover:bg-gray-700 text-white rounded-md"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Rule History Modal */}
      {showRuleHistory && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-4xl w-full mx-4 max-h-[80vh] overflow-hidden">
            {/* Modal Header */}
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-bold text-gray-900 dark:text-gray-100">
                  📜 Rule History: {showRuleHistory.productId}
                </h3>
                <button
                  onClick={() => setShowRuleHistory(null)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-2">
                Complete chronological history of pricing rule changes (newest first)
              </p>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto max-h-[60vh]">
              {showRuleHistory.data?.history && showRuleHistory.data.history.length > 0 ? (
                <div className="space-y-6">
                  {/* Summary Stats */}
                  <div className="grid grid-cols-3 gap-4 mb-6">
                    <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                        {showRuleHistory.data.history.filter(e => e.action === 'created').length}
                      </div>
                      <div className="text-sm text-green-800 dark:text-green-300">Rules Created</div>
                    </div>
                    <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                        {showRuleHistory.data.history.filter(e => e.action === 'updated').length}
                      </div>
                      <div className="text-sm text-blue-800 dark:text-blue-300">Rules Updated</div>
                    </div>
                    <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                        {showRuleHistory.data.history.filter(e => e.action === 'deleted').length}
                      </div>
                      <div className="text-sm text-red-800 dark:text-red-300">Rules Deleted</div>
                    </div>
                  </div>

                  {/* Rule History Timeline */}
                  <div className="space-y-4">
                    <h4 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
                      📅 Chronological History (Newest First)
                    </h4>
                    
                    {showRuleHistory.data.history.map((entry, index) => {
                      // Check if this rule still exists and can be edited
                      const currentRule = rules.find(r => r.rule_id === entry.rule_data.rule_id);
                      const canEdit = entry.action !== 'deleted' && currentRule;
                      
                      return (
                        <div 
                          key={entry.entry_id}
                          className={`relative p-6 rounded-xl border-l-4 shadow-lg ${
                            entry.action === 'created' ? 'bg-green-50 border-green-400 dark:bg-green-900/20' :
                            entry.action === 'updated' ? 'bg-blue-50 border-blue-400 dark:bg-blue-900/20' :
                            'bg-red-50 border-red-400 dark:bg-red-900/20'
                          }`}
                        >
                          {/* Timeline connector */}
                          {index < showRuleHistory.data.history.length - 1 && (
                            <div className="absolute left-0 bottom-0 w-1 h-6 bg-gray-300 transform translate-y-full"></div>
                          )}
                          
                          <div className="flex items-start justify-between">
                            <div className="flex-1">
                              {/* Action Badge and Main Info */}
                              <div className="flex items-center gap-4 mb-3">
                                <span className={`px-3 py-1 text-sm font-bold rounded-full ${
                                  entry.action === 'created' ? 'bg-green-100 text-green-800' :
                                  entry.action === 'updated' ? 'bg-blue-100 text-blue-800' :
                                  'bg-red-100 text-red-800'
                                }`}>
                                  {entry.action === 'created' ? '✅ CREATED' :
                                   entry.action === 'updated' ? '📝 UPDATED' : '🗑️ DELETED'}
                                </span>
                                
                                <div className="text-xl font-bold text-gray-900 dark:text-gray-100">
                                  ${entry.rule_data.price_usd}
                                </div>
                                
                                <div className="text-sm font-medium text-gray-600 dark:text-gray-400 bg-gray-100 dark:bg-gray-700 px-3 py-1 rounded-full">
                                  {entry.rule_data.countries?.length} countries
                                </div>
                                
                                <div className="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded font-mono">
                                  {entry.rule_data.rule_id}
                                </div>
                              </div>
                              
                              {/* Rule Details Grid */}
                              <div className="grid grid-cols-2 gap-6 mb-4">
                                <div>
                                  <div className="text-sm font-medium text-gray-700 dark:text-gray-300">Date Range</div>
                                  <div className="text-base text-gray-900 dark:text-gray-100">
                                    {formatDate(entry.rule_data.start_date)} → {formatDate(entry.rule_data.end_date) || 'Ongoing'}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-sm font-medium text-gray-700 dark:text-gray-300">Countries Applied</div>
                                  <div className="text-sm text-gray-900 dark:text-gray-100">
                                    {entry.rule_data.countries?.slice(0, 3).join(', ')}
                                    {entry.rule_data.countries?.length > 3 && ` +${entry.rule_data.countries.length - 3} more`}
                                  </div>
                                </div>
                              </div>
                              
                              {/* User Notes */}
                              {entry.user_notes && (
                                <div className="bg-white dark:bg-gray-800 p-3 rounded-lg border border-gray-200 dark:border-gray-600 mb-4">
                                  <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Notes</div>
                                  <div className="text-sm text-gray-600 dark:text-gray-400">{entry.user_notes}</div>
                                </div>
                              )}
                              
                              {/* Action Buttons */}
                              <div className="flex gap-3">
                                {canEdit && (
                                  <button
                                    onClick={() => {
                                      setShowRuleHistory(null); // Close history modal
                                      openEditModal(currentRule); // Open edit modal
                                    }}
                                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
                                  >
                                    ✏️ Edit This Rule
                                  </button>
                                )}
                                
                                <button
                                  onClick={() => {
                                    setShowRuleHistory(null);
                                    loadDataProvenance(entry.rule_data.rule_id);
                                  }}
                                  className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
                                >
                                  📊 View Data Source
                                </button>
                                
                                {canEdit && (
                                  <button
                                    onClick={() => {
                                      if (window.confirm(`Are you sure you want to delete rule ${entry.rule_data.rule_id}?`)) {
                                        deleteRule(entry.rule_data.rule_id);
                                        setShowRuleHistory(null);
                                      }
                                    }}
                                    className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
                                  >
                                    🗑️ Delete Rule
                                  </button>
                                )}
                              </div>
                            </div>
                            
                            {/* Timestamp */}
                            <div className="text-right text-sm text-gray-500 dark:text-gray-400 ml-6">
                              <div className="font-medium">{new Date(entry.timestamp).toLocaleDateString()}</div>
                              <div>{new Date(entry.timestamp).toLocaleTimeString()}</div>
                              {!canEdit && entry.action !== 'deleted' && (
                                <div className="text-xs text-red-500 mt-1">Rule no longer exists</div>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : (
                <div className="text-center py-12">
                  <div className="text-6xl mb-4">📜</div>
                  <div className="text-xl font-medium text-gray-600 dark:text-gray-400 mb-2">
                    No Rule History Yet
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-500">
                    Create your first pricing rule to start tracking changes
                  </div>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="p-6 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900">
              <div className="flex justify-between items-center">
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  {showRuleHistory.data?.total_count || 0} total changes tracked
                </div>
                <button
                  onClick={() => setShowRuleHistory(null)}
                  className="px-4 py-2 bg-gray-600 hover:bg-gray-700 text-white rounded-lg font-medium"
                >
                  Close History
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PricingManagementPage; 