# Dashboard Data Transformer
# 
# Transforms flat Meta API data from the historical database into the 
# hierarchical campaign -> adset -> ad structure required by the dashboard.

import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transforms Meta API data into dashboard hierarchy format"""
    
    # Field availability mapping for color coding
    FIELD_AVAILABILITY = {
        # GREEN - Available from Meta API
        'name': {'source': 'meta', 'color': 'green'},
        'campaign_name': {'source': 'meta', 'color': 'green'},
        'adset_name': {'source': 'meta', 'color': 'green'},
        'ad_name': {'source': 'meta', 'color': 'green'},
        'impressions': {'source': 'meta', 'color': 'green'},
        'clicks': {'source': 'meta', 'color': 'green'},
        'spend': {'source': 'meta', 'color': 'green'},
        
        # YELLOW - Requires Meta + Mixpanel integration
        'total_conversions': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'revenue_usd': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'roas': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'estimated_conversions': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'estimated_revenue_usd': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'estimated_roas': {'source': 'meta+mixpanel', 'color': 'yellow'},
        
        # GREEN - Available from Meta action mappings
        'meta_trials_started': {'source': 'meta', 'color': 'green'},
        'meta_purchases': {'source': 'meta', 'color': 'green'},
        
        # RED - Requires Mixpanel integration
        'total_trials_started': {'source': 'mixpanel', 'color': 'red'},
        'total_trials_ended': {'source': 'mixpanel', 'color': 'red'},
        'trials_in_progress': {'source': 'mixpanel', 'color': 'red'},
        'total_refunds_usd': {'source': 'mixpanel', 'color': 'red'},
        'conversions_net_of_refunds': {'source': 'mixpanel', 'color': 'red'},
        'click_to_trial_rate': {'source': 'mixpanel', 'color': 'red'},
        'trial_to_pay_rate': {'source': 'mixpanel', 'color': 'red'},
        'pay_to_refund_rate': {'source': 'mixpanel', 'color': 'red'},
        'total_converted_amount_mixpanel': {'source': 'mixpanel', 'color': 'red'},
        'cost_per_trial_mixpanel': {'source': 'mixpanel', 'color': 'red'},
        'cost_per_purchase_mixpanel': {'source': 'mixpanel', 'color': 'red'},
        'profit': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'trial_conversion_diff_pct': {'source': 'meta+mixpanel', 'color': 'yellow'},
        'average_accuracy': {'source': 'mixpanel', 'color': 'red'}
    }
    
    # Business metric mapping - maps business concept names to dashboard field names
    BUSINESS_METRIC_MAPPING = {
        'trial_starts': 'meta_trials_started',
        'trials': 'meta_trials_started', 
        'trial_started': 'meta_trials_started',
        'conversions': 'meta_purchases',
        'purchases': 'meta_purchases',
        'revenue_actions': 'meta_purchases'
    }
    
    @classmethod
    def transform_to_hierarchy(cls, raw_data: List[Dict], business_metrics: Optional[Dict] = None) -> List[Dict]:
        """
        Transform flat Meta API data into hierarchical campaign -> adset -> ad structure
        
        Args:
            raw_data: List of Meta API records
            business_metrics: Optional dict of business metrics by date
        
        Returns:
            List of campaign dictionaries with nested adsets and ads
        """
        if not raw_data:
            return []
        
        # Don't apply business metrics to individual records - we'll handle this at campaign level
        # to avoid double-counting daily totals across multiple ads
        
        # Group data by campaign -> adset -> ad
        campaigns = defaultdict(lambda: {
            'ads': defaultdict(list),
            'adsets': defaultdict(lambda: {'ads': defaultdict(list)})
        })
        
        # Process each record
        for record in raw_data:
            # Ensure record is a dictionary
            if not isinstance(record, dict):
                logger.warning(f"Record is not a dict, type: {type(record)}, skipping")
                continue
                
            campaign_id = record.get('campaign_id')
            adset_id = record.get('adset_id') 
            ad_id = record.get('ad_id')
            
            if not all([campaign_id, adset_id, ad_id]):
                logger.warning(f"Skipping record with missing IDs: {record}")
                continue
                
            # Add to hierarchy
            campaigns[campaign_id]['adsets'][adset_id]['ads'][ad_id].append(record)
        
        # Build hierarchy with aggregated data
        result = []
        for campaign_id, campaign_data in campaigns.items():
            campaign = cls._build_campaign(campaign_id, campaign_data, business_metrics, raw_data)
            if campaign:
                result.append(campaign)
        
        return result
    
    @classmethod
    def _apply_business_metrics_to_campaign(cls, campaign_id: str, entity_business_metrics: Dict, all_records: List[Dict]) -> Dict:
        """
        Apply entity-specific business metrics to a campaign
        
        Args:
            campaign_id: The campaign ID
            entity_business_metrics: Dict of entity-specific business metrics by entity key and date
            all_records: All records to find dates for this campaign
            
        Returns:
            Dict with business metric totals for this campaign
        """
        campaign_business_totals = {
            'meta_trials_started': 0,
            'meta_purchases': 0
        }
        
        if not entity_business_metrics:
            return campaign_business_totals
        
        # Get business metrics for this specific campaign
        campaign_key = f"campaign:{campaign_id}"
        if campaign_key in entity_business_metrics:
            campaign_metrics = entity_business_metrics[campaign_key]
            
            # Sum business metrics across all dates for this campaign
            for date, day_metrics in campaign_metrics.items():
                logger.debug(f"Applying business metrics for {date} to campaign {campaign_id}: {day_metrics}")
                
                # Map business concepts to dashboard fields
                for concept_name, concept_data in day_metrics.items():
                    dashboard_field = cls._get_dashboard_field_for_concept(concept_name)
                    
                    if dashboard_field and dashboard_field in campaign_business_totals:
                        # Add the daily total to campaign total
                        daily_value = concept_data.get('count', 0)
                        campaign_business_totals[dashboard_field] += daily_value
                        logger.debug(f"Added {daily_value} {concept_name} to campaign {campaign_id} -> {dashboard_field}")
        
        logger.info(f"Campaign {campaign_id} business totals: {campaign_business_totals}")
        return campaign_business_totals

    @classmethod
    def _apply_business_metrics_to_adset(cls, adset_id: str, entity_business_metrics: Dict) -> Dict:
        """
        Apply entity-specific business metrics to an adset
        
        Args:
            adset_id: The adset ID
            entity_business_metrics: Dict of entity-specific business metrics by entity key and date
            
        Returns:
            Dict with business metric totals for this adset
        """
        adset_business_totals = {
            'meta_trials_started': 0,
            'meta_purchases': 0
        }
        
        if not entity_business_metrics:
            return adset_business_totals
        
        # Get business metrics for this specific adset
        adset_key = f"adset:{adset_id}"
        if adset_key in entity_business_metrics:
            adset_metrics = entity_business_metrics[adset_key]
            
            # Sum business metrics across all dates for this adset
            for date, day_metrics in adset_metrics.items():
                logger.debug(f"Applying business metrics for {date} to adset {adset_id}: {day_metrics}")
                
                # Map business concepts to dashboard fields
                for concept_name, concept_data in day_metrics.items():
                    dashboard_field = cls._get_dashboard_field_for_concept(concept_name)
                    
                    if dashboard_field and dashboard_field in adset_business_totals:
                        # Add the daily total to adset total
                        daily_value = concept_data.get('count', 0)
                        adset_business_totals[dashboard_field] += daily_value
                        logger.debug(f"Added {daily_value} {concept_name} to adset {adset_id} -> {dashboard_field}")
        
        logger.debug(f"Adset {adset_id} business totals: {adset_business_totals}")
        return adset_business_totals

    @classmethod
    def _apply_business_metrics_to_ad(cls, ad_id: str, entity_business_metrics: Dict) -> Dict:
        """
        Apply entity-specific business metrics to an ad
        
        Args:
            ad_id: The ad ID
            entity_business_metrics: Dict of entity-specific business metrics by entity key and date
            
        Returns:
            Dict with business metric totals for this ad
        """
        ad_business_totals = {
            'meta_trials_started': 0,
            'meta_purchases': 0
        }
        
        if not entity_business_metrics:
            return ad_business_totals
        
        # Get business metrics for this specific ad
        ad_key = f"ad:{ad_id}"
        if ad_key in entity_business_metrics:
            ad_metrics = entity_business_metrics[ad_key]
            
            # Sum business metrics across all dates for this ad
            for date, day_metrics in ad_metrics.items():
                logger.debug(f"Applying business metrics for {date} to ad {ad_id}: {day_metrics}")
                
                # Map business concepts to dashboard fields
                for concept_name, concept_data in day_metrics.items():
                    dashboard_field = cls._get_dashboard_field_for_concept(concept_name)
                    
                    if dashboard_field and dashboard_field in ad_business_totals:
                        # Add the daily total to ad total
                        daily_value = concept_data.get('count', 0)
                        ad_business_totals[dashboard_field] += daily_value
                        logger.debug(f"Added {daily_value} {concept_name} to ad {ad_id} -> {dashboard_field}")
        
        logger.debug(f"Ad {ad_id} business totals: {ad_business_totals}")
        return ad_business_totals
    
    @classmethod
    def _get_dashboard_field_for_concept(cls, concept_name: str) -> Optional[str]:
        """
        Map a business concept name to the corresponding dashboard field
        
        Args:
            concept_name: Name of the business concept (e.g., 'trial_starts', 'conversions')
            
        Returns:
            Dashboard field name or None if no mapping exists
        """
        # Normalize the concept name (lowercase, remove spaces/underscores)
        normalized_name = concept_name.lower().replace(' ', '_').replace('-', '_')
        
        # Check direct mapping first
        if normalized_name in cls.BUSINESS_METRIC_MAPPING:
            return cls.BUSINESS_METRIC_MAPPING[normalized_name]
        
        # Check if it contains known keywords
        if any(keyword in normalized_name for keyword in ['trial', 'start']):
            return 'meta_trials_started'
        elif any(keyword in normalized_name for keyword in ['conversion', 'purchase', 'revenue']):
            return 'meta_purchases'
        
        logger.warning(f"No dashboard field mapping found for business concept: {concept_name}")
        return None

    @classmethod
    def _build_campaign(cls, campaign_id: str, campaign_data: Dict, entity_business_metrics: Optional[Dict] = None, all_records: List[Dict] = None) -> Optional[Dict]:
        """Build a campaign object with aggregated data and child adsets"""
        adsets = []
        campaign_totals = cls._init_totals()
        campaign_name = None
        
        # Process each adset
        for adset_id, adset_data in campaign_data['adsets'].items():
            adset = cls._build_adset(adset_id, adset_data, entity_business_metrics)
            if adset:
                adsets.append(adset)
                # Aggregate adset totals to campaign (excluding business metrics which are entity-specific)
                for field in ['impressions', 'clicks', 'spend']:
                    campaign_totals[field] = campaign_totals.get(field, 0) + adset.get(field, 0)
                # Get campaign name from first record
                if not campaign_name and adset.get('campaign_name'):
                    campaign_name = adset['campaign_name']
        
        if not adsets:
            return None
            
        # Calculate derived metrics for campaign
        cls._calculate_derived_metrics(campaign_totals)
        
        # Apply entity-specific business metrics to campaign
        if entity_business_metrics:
            campaign_business_totals = cls._apply_business_metrics_to_campaign(campaign_id, entity_business_metrics, all_records)
            campaign_totals.update(campaign_business_totals)
        
        return {
            'id': f'campaign_{campaign_id}',
            'type': 'campaign',
            'level': 0,
            'campaign_name': campaign_name or f'Campaign {campaign_id}',
            'name': campaign_name or f'Campaign {campaign_id}',
            'children': adsets,
            **campaign_totals,
            **cls._get_placeholder_fields()
        }
    
    @classmethod
    def _build_adset(cls, adset_id: str, adset_data: Dict, entity_business_metrics: Optional[Dict] = None) -> Optional[Dict]:
        """Build an adset object with aggregated data and child ads"""
        ads = []
        adset_totals = cls._init_totals()
        adset_name = None
        campaign_name = None
        
        # Process each ad
        for ad_id, ad_records in adset_data['ads'].items():
            ad = cls._build_ad(ad_id, ad_records, entity_business_metrics)
            if ad:
                ads.append(ad)
                # Aggregate ad totals to adset (excluding business metrics which are entity-specific)
                for field in ['impressions', 'clicks', 'spend']:
                    adset_totals[field] = adset_totals.get(field, 0) + ad.get(field, 0)
                # Get names from first record
                if not adset_name and ad.get('adset_name'):
                    adset_name = ad['adset_name']
                if not campaign_name and ad.get('campaign_name'):
                    campaign_name = ad['campaign_name']
        
        if not ads:
            return None
            
        # Calculate derived metrics for adset
        cls._calculate_derived_metrics(adset_totals)
        
        # Apply entity-specific business metrics to adset
        if entity_business_metrics:
            adset_business_totals = cls._apply_business_metrics_to_adset(adset_id, entity_business_metrics)
            adset_totals.update(adset_business_totals)
        
        return {
            'id': f'adset_{adset_id}',
            'type': 'adset', 
            'level': 1,
            'adset_name': adset_name or f'Adset {adset_id}',
            'campaign_name': campaign_name,
            'name': adset_name or f'Adset {adset_id}',
            'children': ads,
            **adset_totals,
            **cls._get_placeholder_fields()
        }
    
    @classmethod 
    def _build_ad(cls, ad_id: str, ad_records: List[Dict], entity_business_metrics: Optional[Dict] = None) -> Optional[Dict]:
        """Build an ad object by aggregating multiple records (if any)"""
        if not ad_records:
            return None
            
        # Sum numeric fields across all records for this ad
        ad_totals = cls._init_totals()
        ad_name = None
        adset_name = None
        campaign_name = None
        
        for record in ad_records:
            # Get names from first record
            if not ad_name:
                ad_name = record.get('ad_name')
            if not adset_name:
                adset_name = record.get('adset_name')
            if not campaign_name:
                campaign_name = record.get('campaign_name')
                
            # Aggregate numeric fields (excluding business metrics - those are handled per entity)
            for field in ['impressions', 'clicks', 'spend']:
                value = record.get(field, 0)
                if value:
                    ad_totals[field] = ad_totals.get(field, 0) + cls._safe_numeric(value)
        
        # Calculate derived metrics for ad
        cls._calculate_derived_metrics(ad_totals)
        
        # Apply entity-specific business metrics to ad
        if entity_business_metrics:
            ad_business_totals = cls._apply_business_metrics_to_ad(ad_id, entity_business_metrics)
            ad_totals.update(ad_business_totals)
        
        return {
            'id': f'ad_{ad_id}',
            'type': 'ad',
            'level': 2,
            'ad_name': ad_name or f'Ad {ad_id}',
            'adset_name': adset_name,
            'campaign_name': campaign_name,
            'name': ad_name or f'Ad {ad_id}',
            **ad_totals,
            **cls._get_placeholder_fields()
        }
    
    @classmethod
    def _init_totals(cls) -> Dict:
        """Initialize totals dictionary with zero values"""
        return {
            'impressions': 0,
            'clicks': 0,
            'spend': 0
            # Note: meta_trials_started and meta_purchases are handled at campaign level only
        }
    
    @classmethod
    def _calculate_derived_metrics(cls, data: Dict) -> None:
        """Calculate derived metrics like CTR, CPC, CPM"""
        impressions = data.get('impressions', 0)
        clicks = data.get('clicks', 0)
        spend = data.get('spend', 0)
        
        # Calculate CTR (Click Through Rate)
        if impressions > 0:
            data['ctr'] = clicks / impressions
        else:
            data['ctr'] = 0
            
        # Calculate CPC (Cost Per Click)
        if clicks > 0:
            data['cpc'] = spend / clicks
        else:
            data['cpc'] = 0
            
        # Calculate CPM (Cost Per Mille)
        if impressions > 0:
            data['cpm'] = (spend / impressions) * 1000
        else:
            data['cpm'] = 0
    
    @classmethod
    def _get_placeholder_fields(cls) -> Dict:
        """Get placeholder values for fields not available from Meta"""
        return {
            # Mixpanel-only fields (RED)
            'total_trials_started': None,
            'total_trials_ended': None,
            'trials_in_progress': None,
            'total_refunds_usd': None,
            'conversions_net_of_refunds': None,
            'click_to_trial_rate': None,
            'trial_to_pay_rate': None,
            'pay_to_refund_rate': None,
            'revenue_usd': None,
            'estimated_conversions': None,
            'estimated_revenue_usd': None,
            'roas': None,
            'estimated_roas': None,
            'total_converted_amount_mixpanel': None,
            'cost_per_trial_mixpanel': None,
            'cost_per_purchase_mixpanel': None,
            'profit': None,
            'trial_conversion_diff_pct': None,
            'average_accuracy': None
        }
    
    @classmethod
    def _safe_numeric(cls, value: Any) -> float:
        """Safely convert value to numeric"""
        try:
            if value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    @classmethod
    def get_field_availability(cls) -> Dict:
        """Get field availability mapping for UI color coding"""
        return cls.FIELD_AVAILABILITY 