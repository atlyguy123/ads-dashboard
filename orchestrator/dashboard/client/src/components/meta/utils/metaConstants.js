// Default fields if none provided
export const DEFAULT_FIELDS = 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend';

// Virtual abstractions → translate to real impression_device + filter list
export const VIRTUAL_OS_MAP = {
  os_ios:  { label: 'iOS',     filterValues: ['iphone', 'ipad'] },
  os_and:  { label: 'Android', filterValues: ['android_smartphone', 'android_tablet'] },
};

// Field categories based on the research document
export const FIELD_CATEGORIES = [
  {
    name: "Basic Ad Data",
    fields: [
      { id: "ad_id", label: "Ad ID" },
      { id: "ad_name", label: "Ad Name" },
      { id: "adset_id", label: "Adset ID" },
      { id: "adset_name", label: "Adset Name" },
      { id: "campaign_id", label: "Campaign ID" },
      { id: "campaign_name", label: "Campaign Name" },
      { id: "impressions", label: "Impressions" },
      { id: "clicks", label: "Clicks" },
      { id: "spend", label: "Spend" },
    ]
  },
  {
    name: "Reach & Frequency",
    fields: [
      { id: "reach", label: "Reach" },
      { id: "frequency", label: "Frequency" },
      { id: "unique_clicks", label: "Unique Clicks" },
      { id: "unique_ctr", label: "Unique CTR" },
    ]
  },
  {
    name: "Cost Efficiency",
    fields: [
      { id: "cpc", label: "CPC" },
      { id: "cpm", label: "CPM" },
      { id: "cpp", label: "Cost per 1 000 People Reached" },
      { id: "cost_per_conversion", label: "Cost Per Conversion", isActionMetric: true },
      { id: "purchase_roas", label: "Purchase ROAS", isActionMetric: true },
      { id: "mobile_app_purchase_roas", label: "Mobile App Purchase ROAS", isActionMetric: true },
    ]
  },
  {
    name: "Conversion Detail",
    fields: [
      { id: "actions", label: "Actions", isActionMetric: true },
      { id: "action_values", label: "Action Values", isActionMetric: true },
      { id: "conversions", label: "Conversions", isActionMetric: true },
      { id: "conversion_values", label: "Conversion Values", isActionMetric: true },
    ]
  },
  {
    name: "Video Engagement",
    fields: [
      { id: "video_30_sec_watched_actions", label: "Video 30s Watched" },
      { id: "video_avg_percent_watched_actions", label: "Video Avg % Watched" },
      { id: "video_thruplay_watched_actions", label: "Video Thruplay Watched" },
    ]
  },
  {
    name: "Click Nuance",
    fields: [
      { id: "inline_link_clicks", label: "Inline Link Clicks" },
      { id: "outbound_clicks", label: "Outbound Clicks" },
      { id: "website_ctr", label: "Website CTR" },
    ]
  },
  {
    name: "Ad Recall & Quality",
    fields: [
      { id: "estimated_ad_recallers", label: "Est. Ad Recallers" },
      { id: "estimated_ad_recall_rate", label: "Est. Ad Recall Rate" },
      { id: "quality_ranking", label: "Quality Ranking" },
      { id: "engagement_rate_ranking", label: "Engagement Rate Ranking" },
      { id: "conversion_rate_ranking", label: "Conversion Rate Ranking" },
    ]
  },
  {
    name: "Context & Meta",
    fields: [
      { id: "account_currency", label: "Account Currency" },
      { id: "buying_type", label: "Buying Type" },
      { id: "objective", label: "Objective" },
      { id: "labels", label: "Labels" },
    ]
  },
];

// Breakdown categories and validation groups
export const GEOGRAPHY_GROUP = ['country', 'region', 'dma'];
export const DEVICE_GROUP = ['device_platform', 'impression_device', 'os_ios', 'os_and'];
export const PLACEMENT_GROUP = ['publisher_platform', 'platform_position', 'placement'];
export const TIME_GROUP = ['hourly_stats_aggregated_by_advertiser_time_zone', 'hourly_stats_aggregated_by_audience_time_zone'];
export const ASSET_GROUP = ['image_asset', 'video_asset', 'title_asset', 'body_asset', 'call_to_action_asset', 'link_url_asset'];

// Meta action metrics trigger implicit action_type breakdown
export const ACTION_METRICS = FIELD_CATEGORIES
  .flatMap(c => c.fields)
  .filter(f => f.isActionMetric)
  .map(f => f.id);

// Fields that are delivery-only and can be used with multiple asset breakdowns
export const DELIVERY_ONLY_FIELDS = ['impressions', 'clicks', 'spend', 'reach'];

// Breakdowns that can be used with action_type (action metrics)
export const ACTION_TYPE_ALLOWED_BREAKDOWNS = [
  'conversion_destination',
  'country',
  'region'
];

// Known good combinations of breakdowns
export const VALID_PAIRS = [
  ['age', 'gender'],
  ['country', 'publisher_platform'],
  ['device_platform', 'publisher_platform'],
  ['publisher_platform', 'platform_position'],
  ['impression_device', 'country']
];

// Breakdown categories based on the research document
export const BREAKDOWN_CATEGORIES = [
  {
    name: "Geography",
    group: GEOGRAPHY_GROUP,
    breakdowns: [
      { id: "country", label: "Country" },
      { id: "region", label: "Region" },
      { id: "dma", label: "DMA" },
    ]
  },
  {
    name: "Demographics",
    breakdowns: [
      { id: "age", label: "Age" },
      { id: "gender", label: "Gender" },
    ]
  },
  {
    name: "Time-of-day",
    group: TIME_GROUP,
    breakdowns: [
      { id: "hourly_stats_aggregated_by_advertiser_time_zone", label: "Advertiser Timezone" },
      { id: "hourly_stats_aggregated_by_audience_time_zone", label: "Audience Timezone" },
    ]
  },
  {
    name: "Device / OS",
    group: DEVICE_GROUP,
    breakdowns: [
      { id: "impression_device", label: "Impression Device" },
      { id: "device_platform",   label: "Device Platform"   },
      { id: "os_ios",            label: "iOS"               },
      { id: "os_and",            label: "Android"           },
    ]
  },
  {
    name: "Placement",
    group: PLACEMENT_GROUP,
    breakdowns: [
      { id: "publisher_platform", label: "Publisher Platform" },
      { id: "platform_position", label: "Platform Position" },
      { id: "placement", label: "Placement" },
    ]
  },
  {
    name: "Creative Assets",
    group: ASSET_GROUP,
    breakdowns: [
      { id: "image_asset", label: "Image Asset" },
      { id: "video_asset", label: "Video Asset" },
      { id: "title_asset", label: "Title Asset" },
      { id: "body_asset", label: "Body Asset" },
      { id: "call_to_action_asset", label: "CTA Asset" },
      { id: "link_url_asset", label: "Link URL Asset" },
    ]
  },
  {
    name: "Commerce / SKAd",
    breakdowns: [
      { id: "product_id", label: "Product ID" },
      { id: "skan_conversion_id", label: "SKAN Conversion ID" },
    ]
  },
  {
    name: "Others",
    breakdowns: [
      { id: "landing_destination", label: "Landing Destination" },
      { id: "conversion_destination", label: "Conversion Destination", isActionBreakdown: true },
    ]
  },
]; 