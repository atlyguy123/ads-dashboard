/**
 * Comprehensive Meta Action Types extracted from 25,686-line sample data
 * Total: 49 unique action types found in production data
 */

export const META_ACTION_TYPES = {
  // PURCHASE & REVENUE (14 action types)
  PURCHASE: [
    'purchase', // Standard Meta purchase event
    'onsite_web_purchase', // Purchase on website
    'onsite_web_app_purchase', // Purchase via web app
    'omni_purchase', // Cross-platform purchase tracking
    'web_in_store_purchase', // Web-to-store purchase
    'web_app_in_store_purchase', // Web app to store purchase
    'offsite_conversion.fb_pixel_purchase', // Facebook Pixel purchase conversion
    'add_payment_info', // Payment information added
    'offsite_conversion.fb_pixel_add_payment_info', // FB Pixel payment info
    'initiate_checkout', // Checkout process started
    'onsite_web_initiate_checkout', // Website checkout initiation
    'omni_initiated_checkout', // Cross-platform checkout
    'offsite_conversion.fb_pixel_initiate_checkout', // FB Pixel checkout
    'offsite_conversion.fb_pixel_custom.server_purchase' // Server-side purchase tracking
  ],

  // LEADS & CONVERSIONS (8 action types)
  LEADS_CONVERSIONS: [
    'lead', // Standard lead generation
    'onsite_web_lead', // Website lead form
    'offsite_conversion.fb_pixel_lead', // Facebook Pixel lead
    'complete_registration', // Registration completed
    'offsite_conversion.fb_pixel_complete_registration', // FB Pixel registration
    'omni_complete_registration', // Cross-platform registration
    'offsite_conversion.custom.532547799297472', // Custom conversion event
    'offsite_conversion.fb_pixel_custom' // Generic FB Pixel custom event
  ],

  // TRIAL & SUBSCRIPTION (3 action types)
  TRIAL_SUBSCRIPTION: [
    'start_trial_mobile_app', // Mobile app trial started
    'start_trial_total', // Total trial starts (all platforms)
    'offsite_conversion.fb_pixel_custom.TrialStartedRoasEvent' // Custom trial event for ROAS
  ],

  // ENGAGEMENT (8 action types)
  ENGAGEMENT: [
    'page_engagement', // Page interaction events
    'post_engagement', // Post interaction events
    'video_view', // Video viewing events
    'link_click', // Link clicking events
    'post_reaction', // Reactions to posts (likes, etc.)
    'like', // Like actions
    'comment', // Comment actions
    'post' // Post creation/sharing
  ],

  // CONTENT VIEWING (8 action types)
  CONTENT_VIEWING: [
    'view_content', // Standard content view
    'onsite_web_view_content', // Website content view
    'onsite_web_app_view_content', // Web app content view
    'omni_view_content', // Cross-platform content view
    'offsite_conversion.fb_pixel_view_content', // FB Pixel content view
    'landing_page_view', // Landing page visits
    'omni_landing_page_view', // Cross-platform landing page view
    'onsite_conversion.post_save' // Content saving action
  ],

  // APP EVENTS (5 action types)
  APP_EVENTS: [
    'mobile_app_install', // Mobile app installation
    'omni_app_install', // Cross-platform app install
    'omni_activate_app', // App activation
    'app_custom_event.fb_mobile_activate_app', // FB Mobile app activation
    'offsite_conversion.fb_pixel_custom.web2app-checkout-stripe-success' // Web-to-app checkout
  ],

  // MESSAGING & COMMUNICATION (3 action types)
  MESSAGING: [
    'onsite_conversion.messaging_first_reply', // First message reply
    'onsite_conversion.messaging_conversation_started_7d', // Conversation started
    'onsite_conversion.messaging_block' // Messaging blocked
  ]
};

// Flattened list of all action types for easy searching
export const ALL_META_ACTION_TYPES = Object.values(META_ACTION_TYPES).flat();

// Action type frequency data (from sample file analysis)
export const ACTION_TYPE_FREQUENCY = {
  'post_engagement': 342,
  'page_engagement': 342,
  'video_view': 321,
  'link_click': 229,
  'post_reaction': 212,
  'offsite_conversion.fb_pixel_custom.TrialStartedRoasEvent': 135,
  'web_in_store_purchase': 92,
  'web_app_in_store_purchase': 92,
  'purchase': 92,
  'onsite_web_purchase': 92,
  'onsite_web_app_purchase': 92,
  'omni_purchase': 92,
  'omni_landing_page_view': 92,
  'offsite_conversion.fb_pixel_purchase': 92,
  'landing_page_view': 92,
  'omni_activate_app': 71,
  'app_custom_event.fb_mobile_activate_app': 71,
  'offsite_conversion.fb_pixel_custom': 68,
  'omni_app_install': 63,
  'mobile_app_install': 63
  // ... and 29 other action types with lower frequency
};

// Business concept suggestions based on action types
export const SUGGESTED_BUSINESS_CONCEPTS = {
  'conversions': {
    description: 'All purchase and conversion-related actions',
    suggestedActions: [
      'purchase', 'onsite_web_purchase', 'omni_purchase', 
      'offsite_conversion.fb_pixel_purchase', 'lead', 'complete_registration'
    ]
  },
  'revenue_actions': {
    description: 'Purchase actions that generate revenue',
    suggestedActions: [
      'purchase', 'onsite_web_purchase', 'onsite_web_app_purchase',
      'omni_purchase', 'web_in_store_purchase', 'offsite_conversion.fb_pixel_purchase'
    ]
  },
  'trial_starts': {
    description: 'Trial initiation events',
    suggestedActions: [
      'start_trial_mobile_app', 'start_trial_total',
      'offsite_conversion.fb_pixel_custom.TrialStartedRoasEvent'
    ]
  },
  'lead_generation': {
    description: 'Lead capture and registration events',
    suggestedActions: [
      'lead', 'onsite_web_lead', 'offsite_conversion.fb_pixel_lead',
      'complete_registration', 'offsite_conversion.fb_pixel_complete_registration'
    ]
  },
  'engagement': {
    description: 'User engagement and interaction events',
    suggestedActions: [
      'page_engagement', 'post_engagement', 'video_view',
      'link_click', 'post_reaction', 'like', 'comment'
    ]
  },
  'app_installs': {
    description: 'Mobile app installation events',
    suggestedActions: [
      'mobile_app_install', 'omni_app_install', 'omni_activate_app'
    ]
  },
  'content_views': {
    description: 'Content viewing and page visit events',
    suggestedActions: [
      'view_content', 'onsite_web_view_content', 'omni_view_content',
      'landing_page_view', 'offsite_conversion.fb_pixel_view_content'
    ]
  },
  'checkout_funnel': {
    description: 'Checkout process events',
    suggestedActions: [
      'initiate_checkout', 'onsite_web_initiate_checkout', 'omni_initiated_checkout',
      'add_payment_info', 'offsite_conversion.fb_pixel_add_payment_info'
    ]
  }
};

// Helper function to get category for an action type
export const getActionTypeCategory = (actionType) => {
  for (const [category, actions] of Object.entries(META_ACTION_TYPES)) {
    if (actions.includes(actionType)) {
      return category;
    }
  }
  return 'OTHER';
};

// Helper function to get business concept suggestions for action types
export const getBusinessConceptSuggestions = (selectedActionTypes) => {
  const suggestions = [];
  
  Object.entries(SUGGESTED_BUSINESS_CONCEPTS).forEach(([concept, config]) => {
    const matchingActions = selectedActionTypes.filter(action => 
      config.suggestedActions.includes(action)
    );
    
    if (matchingActions.length > 0) {
      suggestions.push({
        concept,
        description: config.description,
        matchingActions,
        confidence: matchingActions.length / config.suggestedActions.length
      });
    }
  });
  
  return suggestions.sort((a, b) => b.confidence - a.confidence);
}; 