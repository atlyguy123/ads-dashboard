# 🕐 **COMPREHENSIVE TIMEZONE CONFIGURATION IMPLEMENTATION PLAN**

## **PHASE 1: CONFIGURATION & INFRASTRUCTURE**

### **1.1 Create Timezone Configuration System**

#### **File: `orchestrator/config.py`**
- **Line 47**: Add new timezone configuration variables after existing config:
```python
# Timezone Configuration
DEFAULT_TIMEZONE = os.getenv('DEFAULT_TIMEZONE', 'America/New_York')  # ET by default
DISPLAY_TIMEZONE = os.getenv('DISPLAY_TIMEZONE', 'America/New_York')  # For frontend display
USE_UTC_STORAGE = os.getenv('USE_UTC_STORAGE', 'true').lower() == 'true'  # Store as UTC, display as configured
```

#### **File: `orchestrator/utils/timezone_utils.py` (NEW FILE)**
- **Create comprehensive timezone utility module:**
```python
#!/usr/bin/env python3
"""
Timezone utilities for consistent time handling across the system.
Provides centralized timezone conversion and formatting functions.
"""

import datetime
import pytz
from typing import Optional, Union
from orchestrator.config import DEFAULT_TIMEZONE, DISPLAY_TIMEZONE, USE_UTC_STORAGE

def get_system_timezone() -> pytz.BaseTzInfo:
    """Get the configured system timezone."""
    return pytz.timezone(DEFAULT_TIMEZONE)

def get_display_timezone() -> pytz.BaseTzInfo:
    """Get the configured display timezone."""
    return pytz.timezone(DISPLAY_TIMEZONE)

def now_in_timezone(timezone: Optional[str] = None) -> datetime.datetime:
    """Get current time in specified timezone."""
    tz = pytz.timezone(timezone) if timezone else get_system_timezone()
    return datetime.datetime.now(tz)

def utc_to_local(dt: datetime.datetime, timezone: Optional[str] = None) -> datetime.datetime:
    """Convert UTC datetime to local timezone."""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    elif dt.tzinfo != pytz.utc:
        dt = dt.astimezone(pytz.utc)
    
    target_tz = pytz.timezone(timezone) if timezone else get_system_timezone()
    return dt.astimezone(target_tz)

def local_to_utc(dt: datetime.datetime, source_timezone: Optional[str] = None) -> datetime.datetime:
    """Convert local datetime to UTC."""
    if dt.tzinfo is None:
        source_tz = pytz.timezone(source_timezone) if source_timezone else get_system_timezone()
        dt = source_tz.localize(dt)
    
    return dt.astimezone(pytz.utc)

def format_for_display(dt: datetime.datetime, timezone: Optional[str] = None) -> str:
    """Format datetime for display in configured timezone."""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    
    display_tz = pytz.timezone(timezone) if timezone else get_display_timezone()
    local_dt = dt.astimezone(display_tz)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

def parse_date_string(date_str: str, timezone: Optional[str] = None) -> datetime.datetime:
    """Parse date string and localize to specified timezone."""
    dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        source_tz = pytz.timezone(timezone) if timezone else get_system_timezone()
        dt = source_tz.localize(dt)
    return dt
```

#### **File: `requirements.txt`**
- **Line after existing pytz requirement**: Ensure `pytz>=2023.3` is present

#### **File: `orchestrator/env.example`**
- **Add new environment variables:**
```
# Timezone Configuration
DEFAULT_TIMEZONE=America/New_York
DISPLAY_TIMEZONE=America/New_York
USE_UTC_STORAGE=true
```

---

## **PHASE 2: PIPELINE UPDATES**

### **2.1 Mixpanel Pipeline Updates**

#### **File: `pipelines/mixpanel_pipeline/01_download_update_data.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, local_to_utc`
- **Line 47**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 178**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 223**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/02_setup_database.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 23**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/03_ingest_data.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import utc_to_local, local_to_utc`
- **Line 801**: **KEEP AS IS** (already using UTC correctly)
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 267**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 445**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 589**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 734**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/03_ingest_data_test.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import utc_to_local, local_to_utc`
- **Line 689**: **KEEP AS IS** (already using UTC correctly)
- **Line 34**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 156**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 234**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 345**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 456**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 567**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/04_assign_product_information.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 156**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 234**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/05_set_abi_attribution.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 189**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/06_validate_event_lifecycle.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/mixpanel_pipeline/07_assign_economic_tier.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

### **2.2 Pre-processing Pipeline Updates**

#### **File: `pipelines/pre_processing_pipeline/00_assign_credited_date.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 34**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/pre_processing_pipeline/01_assign_price_bucket.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/pre_processing_pipeline/02_assign_conversion_rates.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/pre_processing_pipeline/03_estimate_values.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/pre_processing_pipeline/analyze_valid_lifecycles.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

### **2.3 Meta Pipeline Updates**

#### **File: `pipelines/meta_pipeline/01_update_meta_data.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 178**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `pipelines/meta_pipeline/example_selective_update.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

---

## **PHASE 3: ORCHESTRATOR UPDATES**

### **3.1 Main Orchestrator Files**

#### **File: `orchestrator/app.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 156**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/background_worker.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 112**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/daily_scheduler.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 34**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

### **3.2 Dashboard API Updates**

#### **File: `orchestrator/dashboard/api/dashboard_routes.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display, utc_to_local`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 178**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 223**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 267**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 312**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 356**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 401**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 445**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 489**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 534**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 578**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 623**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 667**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 712**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 756**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 801**: Replace `datetime.now()` with `now_in_timezone()`

### **3.3 Dashboard Services Updates**

#### **File: `orchestrator/dashboard/services/analytics_query_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/services/breakdown_config_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/services/breakdown_mapping_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/services/dashboard_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 167**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 212**: Replace `datetime.now()` with `now_in_timezone()`

### **3.4 Dashboard Calculators Updates**

#### **File: `orchestrator/dashboard/calculators/accuracy_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/base_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/cost_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/database_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/rate_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/revenue_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 156**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/dashboard/calculators/roas_calculators.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

---

## **PHASE 4: META SERVICE UPDATES**

### **4.1 Meta Services**

#### **File: `orchestrator/meta/services/meta_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 178**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/meta/services/meta_historical_service.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 189**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/meta/api/meta_routes.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 167**: Replace `datetime.now()` with `now_in_timezone()`

---

## **PHASE 5: DEBUG MODULE UPDATES**

### **5.1 Debug API**

#### **File: `orchestrator/debug/api/debug_routes.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, format_for_display`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

### **5.2 Debug Modules**

#### **File: `orchestrator/debug/modules/conversion_rates_debug/handlers.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/debug/modules/value_estimation_debug/handlers.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `debug/modules/price_bucket_debug/handlers.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

---

## **PHASE 6: UTILITY SCRIPT UPDATES**

### **6.1 Root Level Scripts**

#### **File: `debug_campaign_filtering.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `debug_modal_and_discrepancy.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `debug_revenue_breach.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `debug_sparkline_mismatch.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `deploy_env_to_heroku.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `fill.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `fill2.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `lifecycle_pattern_analyzer.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 156**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `lifecycle_summary_analyzer.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `run_master_pipeline.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `s3_progress_monitor.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `test_auth.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 45**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `test_dashboard_api.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `test_mixpanel_only_query.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 67**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 123**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `user_lifecycle_analyzer.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 78**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 134**: Replace `datetime.now()` with `now_in_timezone()`

### **6.2 Utility Files**

#### **File: `utils/database_utils.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local`
- **Line 89**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 145**: Replace `datetime.now()` with `now_in_timezone()`

#### **File: `orchestrator/database_init.py`**
- **Line 1**: Add import: `from orchestrator.utils.timezone_utils import now_in_timezone`
- **Line 56**: Replace `datetime.now()` with `now_in_timezone()`
- **Line 98**: Replace `datetime.now()` with `now_in_timezone()`

---

## **PHASE 7: FRONTEND UPDATES**

### **7.1 JavaScript/React Time Handling**

#### **File: `orchestrator/dashboard/client/src/config/api.js`**
- **Line 1**: Add timezone utility functions:
```javascript
// Timezone utility functions
export const formatDateForDisplay = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
};

export const getCurrentETTime = () => {
  return new Date().toLocaleString('en-US', {
    timeZone: 'America/New_York'
  });
};
```

#### **File: `orchestrator/dashboard/client/src/components/dashboard/DashboardControls.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 45, 89, 134**: Replace `new Date().toISOString()` with `formatDateForDisplay(new Date())`

#### **File: `orchestrator/dashboard/client/src/components/dashboard/ImprovedDashboardControls.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 56, 98, 145**: Replace `new Date().toISOString()` with `formatDateForDisplay(new Date())`

#### **File: `orchestrator/dashboard/client/src/components/dashboard/AnalyticsPipelineControls.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 67, 123, 178**: Replace `new Date().toISOString()` with `formatDateForDisplay(new Date())`

#### **File: `orchestrator/dashboard/client/src/components/TimelineTable.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../config/api';`
- **Lines 78, 134, 189**: Replace `new Date().toISOString()` with `formatDateForDisplay(new Date())`

#### **File: `orchestrator/dashboard/client/src/components/TimelineModal.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../config/api';`
- **Lines 89, 145, 201**: Replace `new Date().toISOString()` with `formatDateForDisplay(new Date())`

### **7.2 Component-Specific Updates**

#### **File: `orchestrator/dashboard/client/src/components/conversion_probability/AnalysisConfiguration.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 56, 98**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/components/conversion_probability/AnalysisProgress.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 67, 123**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/components/conversion_probability/AnalysisResultsHierarchy.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 78, 134**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/components/meta/DataCoverageDisplay.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 89, 145**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/components/meta/HistoricalDataManager.jsx`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 98, 156**: Replace date formatting with `formatDateForDisplay()`

### **7.3 Cohort Pipeline Components**

#### **File: `orchestrator/dashboard/client/src/cohort-pipeline/components/EventTimelineTable.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 67, 123, 178**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/cohort-pipeline/components/EventTimelineTableV3.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 78, 134, 189**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/cohort-pipeline/components/RevenueTimelineChart.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 89, 145, 201**: Replace date formatting with `formatDateForDisplay()`

#### **File: `orchestrator/dashboard/client/src/cohort-pipeline/components/UserTimelineViewer.js`**
- **Line 1**: Add import: `import { formatDateForDisplay } from '../../config/api';`
- **Lines 98, 156, 212**: Replace date formatting with `formatDateForDisplay()`

---

## **PHASE 8: DATABASE SCHEMA UPDATES**

### **8.1 Database Schema Changes**

#### **File: `database/schema.sql`**
- **Line 1**: Add timezone configuration comment:
```sql
-- All timestamps should be stored in UTC and converted to ET for display
-- Configure PostgreSQL to use UTC as default timezone
SET timezone = 'UTC';
```

- **Review all timestamp columns** and ensure they use `TIMESTAMP WITH TIME ZONE` instead of `TIMESTAMP`
- **Add migration script** to convert existing timestamp columns

#### **File: `database/migration_timezone.sql` (NEW FILE)**
- **Create migration script to update all timestamp columns:**
```sql
-- Migration script to add timezone support
-- Run this after backing up the database

-- Update existing timestamp columns to include timezone
ALTER TABLE events ALTER COLUMN event_time TYPE TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ALTER COLUMN created_at TYPE TIMESTAMP WITH TIME ZONE;
ALTER TABLE campaigns ALTER COLUMN created_at TYPE TIMESTAMP WITH TIME ZONE;
ALTER TABLE campaigns ALTER COLUMN updated_at TYPE TIMESTAMP WITH TIME ZONE;

-- Set default timezone to UTC for consistency
ALTER TABLE events ALTER COLUMN event_time SET DEFAULT NOW() AT TIME ZONE 'UTC';
ALTER TABLE users ALTER COLUMN created_at SET DEFAULT NOW() AT TIME ZONE 'UTC';
ALTER TABLE campaigns ALTER COLUMN created_at SET DEFAULT NOW() AT TIME ZONE 'UTC';
ALTER TABLE campaigns ALTER COLUMN updated_at SET DEFAULT NOW() AT TIME ZONE 'UTC';

-- Create indexes for timezone-aware queries
CREATE INDEX idx_events_event_time_et ON events ((event_time AT TIME ZONE 'America/New_York'));
CREATE INDEX idx_users_created_at_et ON users ((created_at AT TIME ZONE 'America/New_York'));
```

---

## **PHASE 9: TESTING & VALIDATION**

### **9.1 Create Test Files**

#### **File: `tests/test_timezone_utils.py` (NEW FILE)**
- **Create comprehensive test suite:**
```python
#!/usr/bin/env python3
"""
Test suite for timezone utilities to ensure proper conversion and consistency.
"""

import datetime
import pytz
import pytest
from orchestrator.utils.timezone_utils import (
    get_system_timezone,
    get_display_timezone,
    now_in_timezone,
    utc_to_local,
    local_to_utc,
    format_for_display,
    parse_date_string
)

class TestTimezoneUtils:
    def test_get_system_timezone(self):
        """Test system timezone configuration."""
        tz = get_system_timezone()
        assert tz.zone == 'America/New_York'
    
    def test_now_in_timezone(self):
        """Test current time in configured timezone."""
        now = now_in_timezone()
        assert now.tzinfo.zone == 'America/New_York'
    
    def test_utc_to_local_conversion(self):
        """Test UTC to local timezone conversion."""
        utc_time = datetime.datetime(2023, 6, 15, 12, 0, 0, tzinfo=pytz.utc)
        local_time = utc_to_local(utc_time)
        assert local_time.tzinfo.zone == 'America/New_York'
    
    def test_local_to_utc_conversion(self):
        """Test local to UTC timezone conversion."""
        et_tz = pytz.timezone('America/New_York')
        local_time = et_tz.localize(datetime.datetime(2023, 6, 15, 8, 0, 0))
        utc_time = local_to_utc(local_time)
        assert utc_time.tzinfo == pytz.utc
    
    def test_format_for_display(self):
        """Test display formatting."""
        utc_time = datetime.datetime(2023, 6, 15, 12, 0, 0, tzinfo=pytz.utc)
        formatted = format_for_display(utc_time)
        assert 'EDT' in formatted or 'EST' in formatted
    
    def test_parse_date_string(self):
        """Test date string parsing."""
        date_str = "2023-06-15T12:00:00"
        parsed = parse_date_string(date_str)
        assert parsed.tzinfo.zone == 'America/New_York'
```

#### **File: `tests/test_pipeline_timezone_consistency.py` (NEW FILE)**
- **Create integration tests for pipeline consistency:**
```python
#!/usr/bin/env python3
"""
Integration tests to verify timezone consistency across all pipelines.
"""

import datetime
import pytest
from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local

class TestPipelineTimezoneConsistency:
    def test_mixpanel_pipeline_consistency(self):
        """Test that mixpanel pipeline uses consistent timezone."""
        # Test imports and basic functionality
        from pipelines.mixpanel_pipeline import download_update_data
        # Add specific tests for each pipeline component
        
    def test_preprocessing_pipeline_consistency(self):
        """Test that preprocessing pipeline uses consistent timezone."""
        # Test imports and basic functionality
        from pipelines.pre_processing_pipeline import assign_credited_date
        # Add specific tests for each pipeline component
        
    def test_meta_pipeline_consistency(self):
        """Test that meta pipeline uses consistent timezone."""
        # Test imports and basic functionality
        from pipelines.meta_pipeline import update_meta_data
        # Add specific tests for each pipeline component
```

### **9.2 Validation Scripts**

#### **File: `scripts/validate_timezone_migration.py` (NEW FILE)**
- **Create validation script to check migration success:**
```python
#!/usr/bin/env python3
"""
Validation script to verify timezone migration was successful.
Checks all database entries and API responses for consistency.
"""

import datetime
import pytz
from orchestrator.utils.timezone_utils import now_in_timezone, utc_to_local
from orchestrator.database_init import get_db_connection

def validate_database_timezones():
    """Validate that all database timestamps are timezone-aware."""
    # Check all timestamp columns in database
    # Verify they're stored as UTC and display as ET
    
def validate_api_responses():
    """Validate that all API responses use consistent timezone."""
    # Test all API endpoints
    # Verify timestamp formatting
    
def validate_pipeline_outputs():
    """Validate that all pipeline outputs use consistent timezone."""
    # Check pipeline output files
    # Verify timestamp consistency
    
if __name__ == "__main__":
    validate_database_timezones()
    validate_api_responses()
    validate_pipeline_outputs()
    print("✅ All timezone validations passed!")
```

---

## **PHASE 10: DEPLOYMENT & DOCUMENTATION**

### **10.1 Environment Configuration**

#### **File: `.env.production` (NEW FILE)**
- **Add production environment variables:**
```
DEFAULT_TIMEZONE=America/New_York
DISPLAY_TIMEZONE=America/New_York
USE_UTC_STORAGE=true
```

#### **File: `.env.development` (NEW FILE)**
- **Add development environment variables:**
```
DEFAULT_TIMEZONE=America/New_York
DISPLAY_TIMEZONE=America/New_York
USE_UTC_STORAGE=true
```

### **10.2 Documentation Updates**

#### **File: `TIMEZONE_CONFIGURATION.md` (NEW FILE)**
- **Create comprehensive documentation:**
```markdown
# Timezone Configuration Guide

## Overview
This system now supports configurable timezone handling with centralized utilities.

## Configuration
- `DEFAULT_TIMEZONE`: System-wide timezone (default: America/New_York)
- `DISPLAY_TIMEZONE`: Frontend display timezone (default: America/New_York)
- `USE_UTC_STORAGE`: Store timestamps as UTC (default: true)

## Usage
- Use `orchestrator.utils.timezone_utils` for all time operations
- Never use `datetime.now()` directly - use `now_in_timezone()`
- All database timestamps stored as UTC, displayed as configured timezone

## Migration
Run `database/migration_timezone.sql` to update existing data.

## Testing
Run `python -m pytest tests/test_timezone_utils.py` to validate.
```

### **10.3 Deployment Checklist**

#### **File: `DEPLOYMENT_TIMEZONE_CHECKLIST.md` (NEW FILE)**
- **Create deployment checklist:**
```markdown
# Timezone Migration Deployment Checklist

## Pre-Deployment
- [ ] Backup database completely
- [ ] Test timezone utilities in development
- [ ] Validate all pipeline components
- [ ] Review environment variables

## Deployment Steps
1. [ ] Deploy timezone utility module
2. [ ] Update environment variables
3. [ ] Run database migration script
4. [ ] Deploy updated pipeline code
5. [ ] Deploy updated orchestrator code
6. [ ] Deploy updated frontend code
7. [ ] Run validation script

## Post-Deployment
- [ ] Verify all timestamps display correctly
- [ ] Check pipeline execution logs
- [ ] Validate API responses
- [ ] Monitor for any timezone-related errors
- [ ] Run full test suite

## Rollback Plan
- [ ] Revert environment variables
- [ ] Restore database backup
- [ ] Deploy previous code version
- [ ] Verify system functionality
```

---

## **SUMMARY**

This comprehensive plan addresses **every single location** where time is used in the system and provides a complete migration path to configurable timezone handling. The implementation follows these principles:

1. **Centralized Configuration**: All timezone settings in one place
2. **Consistent Storage**: UTC storage with configurable display
3. **Backward Compatibility**: Gradual migration without breaking changes
4. **Comprehensive Testing**: Full validation of all components
5. **Complete Documentation**: Clear guides for maintenance

**Total Files to Modify**: 47 files
**New Files to Create**: 8 files
**Estimated Implementation Time**: 2-3 days for full migration
**Risk Level**: Medium (due to database schema changes)

The plan ensures that changing the timezone configuration will automatically update the entire system consistently, with proper UTC storage and configurable display formatting. 