# Meta API Rate Limiting & Throttling Guide

> 🚨 **CRITICAL for Performance Optimization & Parallelization**
> 
> This document contains essential information for scaling Meta API requests safely and efficiently.
> Read this BEFORE implementing parallel request patterns or high-volume data collection.

---

## 🎯 The One-Line Answer

Meta doesn't publish a hard "N calls per second" figure. Instead it gives every app **two moving quotas** and tells you, in the response headers, how close you are to maxing them out. You can fire as many concurrent requests as you like **until one of the two meters hits 100%**—then every extra call is throttled with HTTP 429.

---

## 📊 1. The Two Throttling Systems You Must Watch

| Header You Read                  | Scope                                          | What's Measured                                                         | When You're Blocked                                                                   |
| -------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **`X-FB-Ads-Insights-Throttle`** | *Per-app* and *per-ad-account*                 | Realtime "load %" over the last few seconds                             | `acc_id_util_pct` **or** `app_id_util_pct` = 100                                      |
| **`X-Business-Use-Case-Usage`**  | *Per business object* (ad-account, page, etc.) | Rolling one-hour totals:<br>`call_count`, `total_time`, `total_cputime` | Any of the three values = 100% → cooldown shown in `estimated_time_to_regain_access` |

> **🔑 Key Point:** Neither header is a "per-request" limit—you're looking at the **percentage of a hidden budget**. As long as those percentages stay < 100, you can run calls in parallel.

---

## ⚡ 2. How Many Requests Can I Run in Parallel?

Because the quota is expressed as **CPU-time and total-time**, the true answer depends on:

| Driver                                                                                           | Impact on Limits |
| ------------------------------------------------------------------------------------------------ | ---------------- |
| **What endpoint** you hit (Insights is costlier than `/ads`)                                     | High             |
| **Size of each query** (country/device breakdowns consume more "time")                           | High             |
| **Access tier** (*standard*, *development*, or higher paid tiers have different budgets)        | Medium           |
| **Time of day** (Meta performs maintenance around 04:00 UTC)                                     | Low              |

### 📈 Real-World Performance

Most production apps can sustain roughly **150–250 Insights requests per second** before `X-FB-Ads-Insights-Throttle` climbs past 90%—but the exact number is unique to your app/account combination. **You must discover it empirically by watching the headers.**

---

## 🛡️ 3. How to Operate Safely at Scale

### Step 1: Instrument Every Call
Parse the throttling headers and log the metrics:

```python
def parse_throttle_headers(response):
    """Extract and log Meta API throttling metrics"""
    insights_throttle = response.headers.get('X-FB-Ads-Insights-Throttle')
    business_usage = response.headers.get('X-Business-Use-Case-Usage')
    
    if insights_throttle:
        # Parse JSON: {"acc_id_util_pct": 45, "app_id_util_pct": 12}
        throttle_data = json.loads(insights_throttle)
        logging.info(f"Insights throttle: {throttle_data}")
        
    if business_usage:
        # Parse JSON with call_count, total_time, total_cputime percentages
        usage_data = json.loads(business_usage)
        logging.info(f"Business usage: {usage_data}")
        
    return throttle_data, usage_data
```

### Step 2: Open-Loop Concurrency Control

```python
def should_throttle_requests(throttle_data, usage_data):
    """Determine if we should slow down based on throttling headers"""
    
    # Check Insights throttle (per-app and per-account)
    if throttle_data:
        acc_util = throttle_data.get('acc_id_util_pct', 0)
        app_util = throttle_data.get('app_id_util_pct', 0)
        if acc_util > 85 or app_util > 85:
            return True, f"Insights throttle high: acc={acc_util}%, app={app_util}%"
    
    # Check Business Use Case usage
    if usage_data:
        call_count = usage_data.get('call_count', 0)
        total_time = usage_data.get('total_time', 0)
        total_cputime = usage_data.get('total_cputime', 0)
        if call_count > 90 or total_time > 90 or total_cputime > 90:
            return True, f"Business usage high: calls={call_count}%, time={total_time}%, cpu={total_cputime}%"
    
    return False, None

# Implementation pattern:
while has_work_to_do():
    if should_throttle_requests(last_throttle_data, last_usage_data):
        sleep(60)  # Back off a full minute
    else:
        launch_new_request()
```

### Step 3: Prefer Async Insights Jobs

**🚀 For anything larger than a 1-day window, use `async=true`:**
- Heavy lifting happens inside Meta's servers
- Each "job" request has tiny throttling cost
- Bulk download isn't rate-limited
- Better UX with progress tracking

```python
# Use this pattern for large requests:
def request_large_dataset(start_date, end_date, fields, breakdowns):
    """For date ranges > 1 day or heavy breakdowns"""
    if should_use_async(start_date, end_date, breakdowns):
        return start_async_meta_job(start_date, end_date, fields, breakdowns)
    else:
        return fetch_meta_data_sync(start_date, end_date, fields, breakdowns)
```

### Step 4: Smart Sharding Strategy

- **Run separate workers per ad-account** so one noisy client can't exhaust the shared app bucket
- **Distribute across time zones** to avoid peak usage periods
- **Avoid 04:00 UTC** syncs (Meta maintenance window)

---

## 🚨 4. Detecting Throttling in Real Time

| Symptom                                | Meaning                                                      | Fix                                                                    |
| -------------------------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------- |
| HTTP **429** + `error_subcode 1815753` | `X-FB-Ads-Insights-Throttle` budget exhausted                | Back-off 2–5 min, retry                                                |
| HTTP **400** + `error code 32`         | Business Use Case quota blown                                | Wait the `estimated_time_to_regain_access` minutes shown in the header |
| **Connection times out at 120s**       | Your SDK's local timeout, not Meta's (raise it or use async) | Increase timeout or switch to async jobs                               |

### Implementation Example:

```python
def handle_rate_limit_error(response, error_data):
    """Handle different types of rate limiting gracefully"""
    
    if response.status_code == 429:
        error_subcode = error_data.get('error', {}).get('error_subcode')
        if error_subcode == 1815753:
            logging.warning("Insights throttle exhausted, backing off 5 minutes")
            time.sleep(300)  # 5 minute backoff
            return True  # Retry
    
    elif response.status_code == 400:
        error_code = error_data.get('error', {}).get('code')
        if error_code == 32:
            # Parse X-Business-Use-Case-Usage header for wait time
            usage_header = response.headers.get('X-Business-Use-Case-Usage')
            if usage_header:
                usage_data = json.loads(usage_header)
                wait_time = usage_data.get('estimated_time_to_regain_access', 3600)
                logging.warning(f"Business usage quota exhausted, waiting {wait_time} seconds")
                time.sleep(wait_time)
                return True  # Retry
    
    return False  # Don't retry
```

---

## 📋 5. Implementation Checklist for Our System

### Current State
- ✅ **Async jobs implemented** for better UX and reduced throttling
- ✅ **10-minute timeout** set to handle large requests
- ✅ **Graceful fallback** from async to sync when needed

### Next Steps for Optimization
- [ ] **Add throttling header monitoring** to all Meta API calls
- [ ] **Implement concurrency control** based on throttle percentages  
- [ ] **Add retry logic** with exponential backoff for 429/400 errors
- [ ] **Create separate workers** for different ad accounts
- [ ] **Log throttling metrics** for performance analysis
- [ ] **Add circuit breaker pattern** to prevent cascading failures

### Files to Modify
- `meta/services/meta_service.py` - Add header parsing and throttling logic
- `meta/api/meta_routes.py` - Add throttling middleware
- `meta/services/meta_historical_service.py` - Apply same patterns to historical collection

---

## 🎯 Bottom Line for Our System

1. **Run as many simultaneous calls as you like** until throttle headers creep above ~85%
2. **Let the headers—not hardcoded limits—drive concurrency logic**
3. **Prefer async jobs** for any substantial data requests
4. **Monitor and log** throttling metrics to optimize performance
5. **The exact "max parallel" number is unique to our app** and will vary day-to-day

---

## 📚 References

- [Meta Insights Best Practices](https://developers.facebook.com/docs/marketing-api/insights/best-practices/)
- [Meta Rate Limiting Overview](https://developers.facebook.com/docs/graph-api/overview/rate-limiting/)
- [Marketing API Rate Limiting](https://developers.facebook.com/docs/marketing-api/overview/rate-limiting/)

---

**💡 Remember:** This information is critical for scaling beyond single-request testing. Always implement throttling monitoring before deploying high-volume parallel request patterns! 